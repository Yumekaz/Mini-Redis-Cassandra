"""
Interactive CLI client for the distributed database.
"""

import sys
import time
import json
try:
    import readline
except ImportError:
    readline = None
from typing import Optional, List, Dict, Any

from .network.client import TCPClient
from .network.protocol import Message


class DatabaseCLI:
    """
    Interactive command-line interface for the database.
    
    Supports extended commands:
    - Basic: SET, GET, DELETE, EXISTS, KEYS, PING
    - Cluster: NODES, LEADER, RING, SHARDS, REPLICAS
    - Info: INFO, STATS, CONSISTENCY
    - Admin: REBALANCE, FAILOVER, DEBUG
    """
    
    COMMANDS = {
        # Basic commands
        "SET": "SET <key> <value> [ttl] - Set a key-value pair",
        "GET": "GET <key> [consistency] - Get a value (consistency: ONE|QUORUM|ALL|STRONG)",
        "DELETE": "DELETE <key> - Delete a key",
        "DEL": "DEL <key> - Alias for DELETE",
        "EXISTS": "EXISTS <key> - Check if key exists",
        "KEYS": "KEYS [pattern] - List keys matching pattern",
        "PING": "PING - Check server connectivity",
        
        # Cluster commands
        "NODES": "NODES - List all cluster nodes",
        "LEADER": "LEADER - Show current leader",
        "RING": "RING [samples] - Show consistent hash ring",
        "SHARDS": "SHARDS - Show shard distribution",
        "REPLICAS": "REPLICAS <key> - Show replicas for a key",
        "ROUTE": "ROUTE <key> - Show routing info for a key",
        
        # Info commands
        "INFO": "INFO [section] - Show server information",
        "STATS": "STATS - Show statistics",
        "CLUSTER": "CLUSTER - Show cluster information",
        "CONSISTENCY": "CONSISTENCY [level] - Get/set default consistency",
        
        # Admin commands
        "REBALANCE": "REBALANCE - Trigger cluster rebalance",
        "MIGRATE": "MIGRATE STATUS - Show migration status",
        "FAILOVER": "FAILOVER - Force leader election",
        "DEBUG": "DEBUG <on|off> - Toggle debug mode",
        
        # Help
        "HELP": "HELP [command] - Show help",
        "QUIT": "QUIT - Exit the CLI",
        "EXIT": "EXIT - Exit the CLI",
    }
    
    def __init__(self, host: str = "localhost", port: int = 7001):
        self.host = host
        self.port = port
        self.client = TCPClient(host, port, timeout=10.0, node_id="cli")
        self.debug_mode = False
        self.default_consistency = "ANY"
        
        # Command history
        self.history_file = ".minidb_history"
        self._load_history()
    
    def _load_history(self):
        """Load command history."""
        if readline:
            try:
                readline.read_history_file(self.history_file)
            except FileNotFoundError:
                pass
    
    def _save_history(self):
        """Save command history."""
        if readline:
            try:
                readline.write_history_file(self.history_file)
            except Exception:
                pass
    
    def connect(self) -> bool:
        """Connect to the server."""
        if self.client.connect():
            print(f"Connected to {self.host}:{self.port}")
            return True
        print(f"Failed to connect to {self.host}:{self.port}")
        return False
    
    def run(self):
        """Run the interactive CLI."""
        if not self.connect():
            return
        
        print("Mini-Redis/Cassandra CLI. Type 'HELP' for commands.")
        print()
        
        try:
            while True:
                try:
                    line = input(f"minidb:{self.port}> ").strip()
                    if not line:
                        continue
                    
                    result = self.execute(line)
                    if result is None:  # Exit command
                        break
                    
                    self._print_result(result)
                    
                except KeyboardInterrupt:
                    print()
                    continue
                except EOFError:
                    break
        finally:
            self._save_history()
            self.client.disconnect()
            print("Goodbye!")
    
    def execute(self, line: str) -> Optional[Any]:
        """
        Execute a command line.
        
        Returns:
            Result dict, or None to exit
        """
        parts = self._parse_line(line)
        if not parts:
            return {"error": "Empty command"}
        
        cmd = parts[0].upper()
        args = parts[1:]
        
        # Local commands
        if cmd in ("QUIT", "EXIT"):
            return None
        
        if cmd == "HELP":
            return self._cmd_help(args)
        
        if cmd == "DEBUG":
            return self._cmd_debug(args)
        
        # Remote commands
        handler = getattr(self, f"_cmd_{cmd.lower()}", None)
        if handler:
            return handler(args)
        
        # Default: send to server
        return self._send_command(cmd, args)
    
    def _parse_line(self, line: str) -> List[str]:
        """Parse command line respecting quotes."""
        parts = []
        current = ""
        in_quotes = False
        quote_char = None
        
        for char in line:
            if char in ('"', "'") and not in_quotes:
                in_quotes = True
                quote_char = char
            elif char == quote_char and in_quotes:
                in_quotes = False
                quote_char = None
            elif char == ' ' and not in_quotes:
                if current:
                    parts.append(current)
                    current = ""
            else:
                current += char
        
        if current:
            parts.append(current)
        
        return parts
    
    def _send_command(self, cmd: str, args: List[str]) -> Dict:
        """Send command to server."""
        start = time.time()
        response = self.client.send_command(cmd, *args)
        elapsed = time.time() - start
        
        if response is None:
            # Try to reconnect
            if self.client.connect():
                response = self.client.send_command(cmd, *args)
        
        if response is None:
            return {"error": "Connection failed", "time_ms": elapsed * 1000}
        
        result = {
            "success": response.payload.get("success", False),
            "time_ms": elapsed * 1000
        }
        
        if response.payload.get("success"):
            result["data"] = response.payload.get("data")
        else:
            result["error"] = response.payload.get("error", "Unknown error")
        
        return result
    
    def _print_result(self, result: Any):
        """Print command result."""
        if isinstance(result, dict):
            if "error" in result:
                print(f"ERROR: {result['error']}")
            elif "data" in result:
                data = result["data"]
                if isinstance(data, (dict, list)):
                    print(json.dumps(data, indent=2))
                else:
                    print(data)
            elif "message" in result:
                print(result["message"])
            else:
                print(json.dumps(result, indent=2))
            
            if self.debug_mode and "time_ms" in result:
                print(f"(took {result['time_ms']:.2f}ms)")
        else:
            print(result)
    
    # ============ Local Commands ============
    
    def _cmd_help(self, args: List[str]) -> Dict:
        """Show help."""
        if args:
            cmd = args[0].upper()
            if cmd in self.COMMANDS:
                return {"message": self.COMMANDS[cmd]}
            return {"error": f"Unknown command: {cmd}"}
        
        lines = ["Available commands:", ""]
        for cmd, desc in sorted(self.COMMANDS.items()):
            lines.append(f"  {desc}")
        
        return {"message": "\n".join(lines)}
    
    def _cmd_debug(self, args: List[str]) -> Dict:
        """Toggle debug mode."""
        if args:
            self.debug_mode = args[0].lower() in ("on", "true", "1")
        else:
            self.debug_mode = not self.debug_mode
        
        return {"message": f"Debug mode: {'ON' if self.debug_mode else 'OFF'}"}
    
    # ============ Extended Commands ============
    
    def _cmd_get(self, args: List[str]) -> Dict:
        """GET with optional consistency level."""
        if not args:
            return {"error": "GET requires key"}
        
        key = args[0]
        consistency = args[1].upper() if len(args) > 1 else self.default_consistency
        
        # Send GET with consistency header
        return self._send_command("GET", [key, consistency])
    
    def _cmd_nodes(self, args: List[str]) -> Dict:
        """List cluster nodes."""
        result = self._send_command("CLUSTER", [])
        if result.get("success") and "data" in result:
            members = result["data"].get("members", [])
            lines = ["Cluster Nodes:", ""]
            lines.append(f"{'NODE ID':<20} {'ADDRESS':<25} {'ROLE':<12} {'STATE':<10}")
            lines.append("-" * 70)
            
            for m in members:
                node_id = m.get("node_id", "unknown")
                host = m.get("host", "?")
                port = m.get("client_port", "?")
                role = m.get("role", "unknown")
                state = m.get("state", "unknown")
                lines.append(f"{node_id:<20} {host}:{port:<18} {role:<12} {state:<10}")
            
            return {"message": "\n".join(lines)}
        return result
    
    def _cmd_leader(self, args: List[str]) -> Dict:
        """Show current leader."""
        result = self._send_command("CLUSTER", [])
        if result.get("success") and "data" in result:
            leader_id = result["data"].get("leader_id")
            if leader_id:
                return {"message": f"Current leader: {leader_id}"}
            return {"message": "No leader elected"}
        return result
    
    def _cmd_ring(self, args: List[str]) -> Dict:
        """Show consistent hash ring."""
        return self._send_command("RING", args)
    
    def _cmd_shards(self, args: List[str]) -> Dict:
        """Show shard distribution."""
        return self._send_command("SHARDS", [])
    
    def _cmd_replicas(self, args: List[str]) -> Dict:
        """Show replicas for a key."""
        if not args:
            return {"error": "REPLICAS requires key"}
        return self._send_command("REPLICAS", args)
    
    def _cmd_route(self, args: List[str]) -> Dict:
        """Show routing info for a key."""
        if not args:
            return {"error": "ROUTE requires key"}
        return self._send_command("ROUTE", args)
    
    def _cmd_stats(self, args: List[str]) -> Dict:
        """Show statistics."""
        return self._send_command("STATS", [])
    
    def _cmd_consistency(self, args: List[str]) -> Dict:
        """Get/set default consistency."""
        if args:
            level = args[0].upper()
            valid = ["ONE", "QUORUM", "ALL", "ANY", "STRONG"]
            if level not in valid:
                return {"error": f"Invalid consistency level. Valid: {', '.join(valid)}"}
            self.default_consistency = level
            return {"message": f"Default consistency set to: {level}"}
        return {"message": f"Default consistency: {self.default_consistency}"}
    
    def _cmd_rebalance(self, args: List[str]) -> Dict:
        """Trigger cluster rebalance."""
        return self._send_command("REBALANCE", [])
    
    def _cmd_migrate(self, args: List[str]) -> Dict:
        """Show migration status."""
        return self._send_command("MIGRATE", args)
    
    def _cmd_failover(self, args: List[str]) -> Dict:
        """Force leader election."""
        return self._send_command("FAILOVER", [])


def run_cli(host: str = "localhost", port: int = 7001):
    """Run the CLI."""
    cli = DatabaseCLI(host, port)
    cli.run()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Mini-Redis/Cassandra CLI")
    parser.add_argument("host_pos", nargs="?", help="Server host")
    parser.add_argument("port_pos", nargs="?", type=int, help="Server port")
    parser.add_argument("-H", "--host", default="localhost", help="Server host")
    parser.add_argument("-p", "--port", type=int, default=7001, help="Server port")
    
    args = parser.parse_args()
    
    final_host = args.host_pos if args.host_pos else args.host
    final_port = args.port_pos if args.port_pos else args.port
    
    run_cli(final_host, final_port)
