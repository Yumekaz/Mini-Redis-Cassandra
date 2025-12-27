"""
TCP Server for handling client and cluster connections.
"""

import socket
import threading
import time
from typing import Callable, Dict, Optional, Any
from .protocol import Protocol, Message, MessageType


class TCPServer:
    """
    Multi-threaded TCP server for the database node.
    Handles both client commands and inter-node communication.
    """
    
    def __init__(self, host: str, port: int, handler: Callable[[Message, socket.socket], Message]):
        self.host = host
        self.port = port
        self.handler = handler
        
        self._server_socket: Optional[socket.socket] = None
        self._running = False
        self._clients: Dict[str, socket.socket] = {}
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
    
    def start(self):
        """Start the TCP server."""
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind((self.host, self.port))
        self._server_socket.listen(128)
        self._server_socket.settimeout(1.0)  # Allow checking _running flag
        
        self._running = True
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()
        
        print(f"TCP Server started on {self.host}:{self.port}")
    
    def _accept_loop(self):
        """Main accept loop for incoming connections."""
        while self._running:
            try:
                client_socket, address = self._server_socket.accept()
                client_socket.settimeout(30.0)
                
                # Handle each client in a separate thread
                client_thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket, address),
                    daemon=True
                )
                client_thread.start()
                
            except socket.timeout:
                continue
            except Exception as e:
                if self._running:
                    print(f"Accept error: {e}")
    
    def _handle_client(self, client_socket: socket.socket, address: tuple):
        """Handle a single client connection."""
        client_id = f"{address[0]}:{address[1]}"
        
        with self._lock:
            self._clients[client_id] = client_socket
        
        try:
            buffer = b""
            while self._running:
                try:
                    chunk = client_socket.recv(Protocol.BUFFER_SIZE)
                    if not chunk:
                        break
                    
                    buffer += chunk
                    
                    # Process complete messages
                    while b"\n" in buffer:
                        line, buffer = buffer.split(b"\n", 1)
                        if line:
                            try:
                                message = Message.decode(line)
                                response = self.handler(message, client_socket)
                                if response:
                                    Protocol.send_message(client_socket, response)
                            except Exception as e:
                                error_response = Protocol.create_response(
                                    False, error=str(e)
                                )
                                Protocol.send_message(client_socket, error_response)
                                
                except socket.timeout:
                    continue
                except Exception as e:
                    break
                    
        finally:
            with self._lock:
                if client_id in self._clients:
                    del self._clients[client_id]
            try:
                client_socket.close()
            except Exception:
                pass
    
    def stop(self):
        """Stop the TCP server."""
        self._running = False
        
        # Close all client connections
        with self._lock:
            for client_socket in self._clients.values():
                try:
                    client_socket.close()
                except Exception:
                    pass
            self._clients.clear()
        
        # Close server socket
        if self._server_socket:
            try:
                self._server_socket.close()
            except Exception:
                pass
        
        # Wait for thread
        if self._thread:
            self._thread.join(timeout=2.0)
        
        print(f"TCP Server stopped on {self.host}:{self.port}")
    
    def get_client_count(self) -> int:
        """Get number of connected clients."""
        with self._lock:
            return len(self._clients)
