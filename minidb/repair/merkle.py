"""
Merkle trees for efficient data synchronization.
"""

import hashlib
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass


@dataclass
class MerkleNode:
    """A node in the Merkle tree."""
    hash_value: str
    left: Optional['MerkleNode'] = None
    right: Optional['MerkleNode'] = None
    key_range: Tuple[str, str] = ("", "")
    is_leaf: bool = False
    keys: List[str] = None
    
    def __post_init__(self):
        if self.keys is None:
            self.keys = []


class MerkleTree:
    """
    Merkle tree for efficient comparison of key-value sets.
    
    Used for anti-entropy repair to find divergent keys.
    """
    
    def __init__(self, bucket_size: int = 100):
        self.bucket_size = bucket_size
        self.root: Optional[MerkleNode] = None
        self._key_hashes: Dict[str, str] = {}  # key -> hash of value
    
    def _hash(self, data: str) -> str:
        """Generate hash for data."""
        return hashlib.sha256(data.encode()).hexdigest()[:16]
    
    def _combine_hashes(self, left: str, right: str) -> str:
        """Combine two hashes."""
        return self._hash(left + right)
    
    def build(self, key_values: Dict[str, Tuple[any, int]]):
        """
        Build Merkle tree from key-value pairs.
        
        Args:
            key_values: Dict of key -> (value, version)
        """
        # Store key hashes
        self._key_hashes = {}
        for key, (value, version) in key_values.items():
            self._key_hashes[key] = self._hash(f"{key}:{value}:{version}")
        
        if not self._key_hashes:
            self.root = None
            return
        
        # Sort keys for consistent ordering
        sorted_keys = sorted(self._key_hashes.keys())
        
        # Create leaf nodes (buckets)
        leaves = []
        for i in range(0, len(sorted_keys), self.bucket_size):
            bucket_keys = sorted_keys[i:i + self.bucket_size]
            bucket_hash = self._hash("".join(self._key_hashes[k] for k in bucket_keys))
            
            leaves.append(MerkleNode(
                hash_value=bucket_hash,
                key_range=(bucket_keys[0], bucket_keys[-1]),
                is_leaf=True,
                keys=bucket_keys
            ))
        
        # Build tree bottom-up
        self.root = self._build_tree(leaves)
    
    def _build_tree(self, nodes: List[MerkleNode]) -> Optional[MerkleNode]:
        """Build tree from leaf nodes."""
        if not nodes:
            return None
        if len(nodes) == 1:
            return nodes[0]
        
        parents = []
        for i in range(0, len(nodes), 2):
            left = nodes[i]
            right = nodes[i + 1] if i + 1 < len(nodes) else None
            
            if right:
                parent_hash = self._combine_hashes(left.hash_value, right.hash_value)
                parent = MerkleNode(
                    hash_value=parent_hash,
                    left=left,
                    right=right,
                    key_range=(left.key_range[0], right.key_range[1])
                )
            else:
                parent = left
            
            parents.append(parent)
        
        return self._build_tree(parents)
    
    def get_root_hash(self) -> Optional[str]:
        """Get root hash of the tree."""
        return self.root.hash_value if self.root else None
    
    def compare(self, other: 'MerkleTree') -> List[Tuple[str, str]]:
        """
        Compare with another Merkle tree.
        
        Returns:
            List of (key_range_start, key_range_end) tuples that differ
        """
        if not self.root or not other.root:
            return []
        
        differences = []
        self._compare_nodes(self.root, other.root, differences)
        return differences
    
    def _compare_nodes(self, node1: MerkleNode, node2: MerkleNode,
                       differences: List[Tuple[str, str]]):
        """Recursively compare nodes."""
        if node1.hash_value == node2.hash_value:
            return  # Subtrees are identical
        
        if node1.is_leaf or node2.is_leaf:
            # Found a difference at leaf level
            differences.append(node1.key_range)
            return
        
        # Compare children
        if node1.left and node2.left:
            self._compare_nodes(node1.left, node2.left, differences)
        if node1.right and node2.right:
            self._compare_nodes(node1.right, node2.right, differences)
    
    def get_keys_in_range(self, start: str, end: str) -> List[str]:
        """Get all keys in a range."""
        return [k for k in self._key_hashes.keys() if start <= k <= end]
    
    def to_dict(self) -> Dict:
        """Serialize tree structure for transmission."""
        if not self.root:
            return {"root": None}
        return {"root": self._node_to_dict(self.root)}
    
    def _node_to_dict(self, node: MerkleNode) -> Dict:
        """Convert node to dict."""
        return {
            "hash": node.hash_value,
            "range": node.key_range,
            "is_leaf": node.is_leaf,
            "left": self._node_to_dict(node.left) if node.left else None,
            "right": self._node_to_dict(node.right) if node.right else None
        }
