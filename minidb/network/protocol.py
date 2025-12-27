"""
Custom TCP protocol similar to Redis RESP.
Used for both client-node and inter-node communication.
"""

import json
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field


class MessageType(Enum):
    """Protocol message types."""
    # Client commands
    COMMAND = "CMD"
    RESPONSE = "RSP"
    ERROR = "ERR"
    
    # Cluster messages
    HEARTBEAT = "HB"
    VOTE_REQUEST = "VRQ"
    VOTE_RESPONSE = "VRS"
    APPEND_ENTRIES = "AE"
    APPEND_RESPONSE = "AR"
    
    # Membership
    JOIN = "JOIN"
    LEAVE = "LEAVE"
    GOSSIP = "GSP"
    
    # Replication
    REPLICATE = "REP"
    REPLICATE_ACK = "RACK"
    SYNC_REQUEST = "SRQ"
    SYNC_RESPONSE = "SRS"
    
    # Anti-entropy
    MERKLE_REQUEST = "MRQ"
    MERKLE_RESPONSE = "MRS"
    REPAIR_DATA = "RPD"


@dataclass
class Message:
    """
    Protocol message structure.
    
    Format: LENGTH:TYPE:JSON_PAYLOAD\n
    """
    msg_type: MessageType
    payload: Dict[str, Any] = field(default_factory=dict)
    sender_id: str = ""
    term: int = 0  # Raft term
    sequence: int = 0  # Message sequence number
    
    def encode(self) -> bytes:
        """Encode message to bytes for transmission."""
        data = {
            "type": self.msg_type.value,
            "sender": self.sender_id,
            "term": self.term,
            "seq": self.sequence,
            "payload": self.payload
        }
        json_str = json.dumps(data)
        # Format: length:json\n
        msg = f"{len(json_str)}:{json_str}\n"
        return msg.encode("utf-8")
    
    @classmethod
    def decode(cls, data: bytes) -> 'Message':
        """Decode message from bytes."""
        text = data.decode("utf-8").strip()
        
        # Parse length:json format
        if ":" in text:
            parts = text.split(":", 1)
            if len(parts) == 2:
                text = parts[1]
        
        parsed = json.loads(text)
        return cls(
            msg_type=MessageType(parsed["type"]),
            payload=parsed.get("payload", {}),
            sender_id=parsed.get("sender", ""),
            term=parsed.get("term", 0),
            sequence=parsed.get("seq", 0)
        )


class Protocol:
    """
    Protocol handler for encoding/decoding messages.
    """
    
    BUFFER_SIZE = 65536
    
    @staticmethod
    def create_command(command: str, args: List[str], sender_id: str = "") -> Message:
        """Create a client command message."""
        return Message(
            msg_type=MessageType.COMMAND,
            payload={"cmd": command.upper(), "args": args},
            sender_id=sender_id
        )
    
    @staticmethod
    def create_response(success: bool, data: Any = None, error: str = None) -> Message:
        """Create a response message."""
        return Message(
            msg_type=MessageType.RESPONSE if success else MessageType.ERROR,
            payload={"success": success, "data": data, "error": error}
        )
    
    @staticmethod
    def create_heartbeat(sender_id: str, term: int, leader_id: str, 
                         members: List[Dict]) -> Message:
        """Create a heartbeat message."""
        return Message(
            msg_type=MessageType.HEARTBEAT,
            payload={"leader_id": leader_id, "members": members},
            sender_id=sender_id,
            term=term
        )
    
    @staticmethod
    def create_vote_request(sender_id: str, term: int, 
                            last_log_index: int, last_log_term: int) -> Message:
        """Create a vote request message."""
        return Message(
            msg_type=MessageType.VOTE_REQUEST,
            payload={
                "last_log_index": last_log_index,
                "last_log_term": last_log_term
            },
            sender_id=sender_id,
            term=term
        )
    
    @staticmethod
    def create_vote_response(sender_id: str, term: int, 
                             vote_granted: bool) -> Message:
        """Create a vote response message."""
        return Message(
            msg_type=MessageType.VOTE_RESPONSE,
            payload={"vote_granted": vote_granted},
            sender_id=sender_id,
            term=term
        )
    
    @staticmethod
    def create_append_entries(sender_id: str, term: int, prev_log_index: int,
                              prev_log_term: int, entries: List[Dict],
                              leader_commit: int) -> Message:
        """Create an append entries message."""
        return Message(
            msg_type=MessageType.APPEND_ENTRIES,
            payload={
                "prev_log_index": prev_log_index,
                "prev_log_term": prev_log_term,
                "entries": entries,
                "leader_commit": leader_commit
            },
            sender_id=sender_id,
            term=term
        )
    
    @staticmethod
    def create_replicate(sender_id: str, term: int, entry: Dict) -> Message:
        """Create a replication message."""
        return Message(
            msg_type=MessageType.REPLICATE,
            payload={"entry": entry},
            sender_id=sender_id,
            term=term
        )
    
    @staticmethod
    def create_gossip(sender_id: str, members: List[Dict]) -> Message:
        """Create a gossip message."""
        return Message(
            msg_type=MessageType.GOSSIP,
            payload={"members": members},
            sender_id=sender_id
        )
    
    @staticmethod
    def parse_command(payload: Dict) -> tuple:
        """Parse a command payload into (command, args)."""
        return payload.get("cmd", ""), payload.get("args", [])
    
    @staticmethod
    def read_message(sock) -> Optional[Message]:
        """Read a complete message from a socket."""
        try:
            # Read until we get a complete message
            buffer = b""
            while True:
                chunk = sock.recv(Protocol.BUFFER_SIZE)
                if not chunk:
                    return None
                buffer += chunk
                
                # Check if we have a complete message
                if b"\n" in buffer:
                    line, _ = buffer.split(b"\n", 1)
                    return Message.decode(line)
                    
        except Exception as e:
            return None
    
    @staticmethod
    def send_message(sock, message: Message) -> bool:
        """Send a message through a socket."""
        try:
            data = message.encode()
            sock.sendall(data)
            return True
        except Exception:
            return False
