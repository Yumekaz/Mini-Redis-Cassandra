"""
TCP Client for connecting to database nodes.
"""

import os
import socket
import threading
from typing import Optional, List, Any
from .protocol import Protocol, Message, MessageType


class TCPClient:
    """
    TCP client for connecting to database nodes.
    Used by CLI and for inter-node communication.
    """
    
    def __init__(self, host: str = "localhost", port: int = 7001, 
                 timeout: float = 10.0, node_id: str = ""):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.node_id = node_id or os.environ.get("NODE_ID", "")
        
        self._socket: Optional[socket.socket] = None
        self._connected = False
        self._lock = threading.Lock()
    
    def _check_proxy(self) -> bool:
        """Check if connection to target node is blocked or delayed by the proxy."""
        import os
        proxy_port = os.environ.get("PROXY_PORT")
        if not proxy_port:
            return True
        
        try:
            import urllib.request
            import json
            import time
            start_port = int(os.environ.get("START_PORT", "7001"))
            target_node = f"node-{self.port - start_port + 1}"
            from_node = self.node_id or os.environ.get("NODE_ID", "")
            
            if from_node and target_node:
                url = f"http://localhost:{proxy_port}/failforge/check?from={from_node}&to={target_node}"
                req = urllib.request.Request(url)
                with urllib.request.urlopen(req, timeout=1.0) as resp:
                    res = json.loads(resp.read().decode("utf-8"))
                    if res.get("blocked"):
                        return False
                    delay_ms = res.get("delay_ms", 0)
                    if delay_ms > 0:
                        time.sleep(delay_ms / 1000.0)
        except Exception:
            pass
        return True

    def connect(self) -> bool:
        """Connect to the server."""
        if not self._check_proxy():
            self._connected = False
            return False
        try:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.settimeout(self.timeout)
            self._socket.connect((self.host, self.port))
            self._connected = True
            return True
        except Exception as e:
            self._connected = False
            return False
    
    def disconnect(self):
        """Disconnect from the server."""
        with self._lock:
            self._connected = False
            if self._socket:
                try:
                    self._socket.close()
                except Exception:
                    pass
                self._socket = None
    
    def is_connected(self) -> bool:
        """Check if connected."""
        return self._connected
    
    def send_command(self, command: str, *args) -> Optional[Message]:
        """
        Send a command and wait for response.
        
        Args:
            command: The command (e.g., "SET", "GET")
            *args: Command arguments
            
        Returns:
            Response message or None on error
        """
        if not self._check_proxy():
            self.disconnect()
            return None

        if not self._connected:
            if not self.connect():
                return None
        
        with self._lock:
            try:
                # Create and send command
                message = Protocol.create_command(command, list(args), self.node_id)
                if not Protocol.send_message(self._socket, message):
                    return None
                
                # Read response
                buffer = b""
                while True:
                    chunk = self._socket.recv(Protocol.BUFFER_SIZE)
                    if not chunk:
                        return None
                    buffer += chunk
                    
                    if b"\n" in buffer:
                        line, _ = buffer.split(b"\n", 1)
                        return Message.decode(line)
                        
            except Exception as e:
                self._connected = False
                return None
    
    def send_message(self, message: Message) -> Optional[Message]:
        """Send a raw message and get response."""
        if not self._check_proxy():
            self.disconnect()
            return None

        if not self._connected:
            if not self.connect():
                return None
        
        with self._lock:
            try:
                if not Protocol.send_message(self._socket, message):
                    return None
                
                buffer = b""
                while True:
                    chunk = self._socket.recv(Protocol.BUFFER_SIZE)
                    if not chunk:
                        return None
                    buffer += chunk
                    
                    if b"\n" in buffer:
                        line, _ = buffer.split(b"\n", 1)
                        return Message.decode(line)
                        
            except Exception as e:
                self._connected = False
                return None
    
    def send_message_no_response(self, message: Message) -> bool:
        """Send a message without waiting for response."""
        if not self._check_proxy():
            self.disconnect()
            return False

        if not self._connected:
            if not self.connect():
                return False
        
        with self._lock:
            try:
                return Protocol.send_message(self._socket, message)
            except Exception:
                self._connected = False
                return False


class ConnectionPool:
    """
    Pool of connections to cluster nodes.

    Connections are thread-local. A single shared TCP stream used from multiple
    threads interleaved length-prefixed messages (gossip/replicate/election),
    which produced phantom key values under FailForge concurrent load.
    """
    
    def __init__(self, timeout: float = 5.0, node_id: str = ""):
        self.timeout = timeout
        self.node_id = node_id
        self._local = threading.local()
        self._all_clients: list = []
        self._lock = threading.Lock()
    
    def get_connection(self, host: str, port: int) -> TCPClient:
        """Get or create a thread-local connection to a node."""
        key = f"{host}:{port}"
        conns = getattr(self._local, "connections", None)
        if conns is None:
            conns = {}
            self._local.connections = conns

        client = conns.get(key)
        if client is None:
            client = TCPClient(host, port, self.timeout, self.node_id)
            conns[key] = client
            with self._lock:
                self._all_clients.append(client)

        if not client.is_connected():
            client.connect()

        return client
    
    def close_all(self):
        """Close all connections created by this pool."""
        with self._lock:
            for client in self._all_clients:
                client.disconnect()
            self._all_clients.clear()
