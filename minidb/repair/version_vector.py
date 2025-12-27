"""
Version vectors for conflict detection and resolution.
"""

from typing import Dict, Optional, Tuple
from dataclasses import dataclass, field
import copy


@dataclass
class VersionVector:
    """
    Version vector for tracking causality.
    Used for conflict detection in distributed writes.
    """
    versions: Dict[str, int] = field(default_factory=dict)
    
    def increment(self, node_id: str):
        """Increment the version for a node."""
        self.versions[node_id] = self.versions.get(node_id, 0) + 1
    
    def get(self, node_id: str) -> int:
        """Get version for a node."""
        return self.versions.get(node_id, 0)
    
    def merge(self, other: 'VersionVector') -> 'VersionVector':
        """Merge with another version vector (take max of each)."""
        result = VersionVector(versions=copy.copy(self.versions))
        for node_id, version in other.versions.items():
            result.versions[node_id] = max(result.versions.get(node_id, 0), version)
        return result
    
    def compare(self, other: 'VersionVector') -> str:
        """
        Compare with another version vector.
        
        Returns:
            "before": self happened before other
            "after": self happened after other
            "concurrent": neither happened before the other
            "equal": same versions
        """
        self_newer = False
        other_newer = False
        
        all_nodes = set(self.versions.keys()) | set(other.versions.keys())
        
        for node_id in all_nodes:
            self_v = self.versions.get(node_id, 0)
            other_v = other.versions.get(node_id, 0)
            
            if self_v > other_v:
                self_newer = True
            elif other_v > self_v:
                other_newer = True
        
        if self_newer and other_newer:
            return "concurrent"
        elif self_newer:
            return "after"
        elif other_newer:
            return "before"
        else:
            return "equal"
    
    def to_dict(self) -> Dict[str, int]:
        """Convert to dictionary."""
        return copy.copy(self.versions)
    
    @classmethod
    def from_dict(cls, data: Dict[str, int]) -> 'VersionVector':
        """Create from dictionary."""
        return cls(versions=copy.copy(data))
    
    def __str__(self) -> str:
        return str(self.versions)


@dataclass
class VersionedValue:
    """A value with version vector."""
    value: any
    version: VersionVector
    timestamp: float = 0.0
    
    def to_dict(self) -> Dict:
        return {
            "value": self.value,
            "version": self.version.to_dict(),
            "timestamp": self.timestamp
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'VersionedValue':
        return cls(
            value=data["value"],
            version=VersionVector.from_dict(data["version"]),
            timestamp=data.get("timestamp", 0.0)
        )
