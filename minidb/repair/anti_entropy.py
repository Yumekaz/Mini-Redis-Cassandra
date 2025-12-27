"""
Anti-entropy repair manager.
"""

import time
import threading
from typing import Dict, List, Optional, Callable, Set, Tuple

from .merkle import MerkleTree
from .version_vector import VersionVector, VersionedValue


class AntiEntropyManager:
    """
    Manages anti-entropy repair between nodes.
    
    Features:
    - Periodic Merkle tree comparison
    - Delta synchronization for diverged keys
    - Version vector conflict resolution
    """
    
    def __init__(self, node_id: str, repair_interval: float = 60.0):
        self.node_id = node_id
        self.repair_interval = repair_interval
        
        self._merkle_tree = MerkleTree()
        self._lock = threading.RLock()
        self._running = False
        self._repair_thread: Optional[threading.Thread] = None
        
        # Callbacks
        self._get_local_data: Optional[Callable[[], Dict[str, Tuple]]] = None
        self._get_remote_merkle: Optional[Callable[[str], Optional[Dict]]] = None
        self._get_remote_keys: Optional[Callable[[str, List[str]], Dict]] = None
        self._apply_repair_data: Optional[Callable[[str, any, int], None]] = None
        self._get_peers: Optional[Callable[[], List[str]]] = None
    
    def set_callbacks(self, get_local_data=None, get_remote_merkle=None,
                      get_remote_keys=None, apply_repair_data=None, get_peers=None):
        """Set repair callbacks."""
        self._get_local_data = get_local_data
        self._get_remote_merkle = get_remote_merkle
        self._get_remote_keys = get_remote_keys
        self._apply_repair_data = apply_repair_data
        self._get_peers = get_peers
    
    def start(self):
        """Start anti-entropy manager."""
        self._running = True
        self._repair_thread = threading.Thread(target=self._repair_loop, daemon=True)
        self._repair_thread.start()
    
    def stop(self):
        """Stop anti-entropy manager."""
        self._running = False
        if self._repair_thread:
            self._repair_thread.join(timeout=2.0)
    
    def _repair_loop(self):
        """Main repair loop."""
        while self._running:
            try:
                self._run_repair_cycle()
            except Exception as e:
                print(f"Repair error: {e}")
            
            time.sleep(self.repair_interval)
    
    def _run_repair_cycle(self):
        """Run one repair cycle."""
        if not self._get_local_data or not self._get_peers:
            return
        
        # Build local Merkle tree
        local_data = self._get_local_data()
        self._merkle_tree.build(local_data)
        
        # Compare with each peer
        peers = self._get_peers()
        
        for peer_id in peers:
            if peer_id == self.node_id:
                continue
            
            self._repair_with_peer(peer_id)
    
    def _repair_with_peer(self, peer_id: str):
        """Repair data with a specific peer."""
        if not self._get_remote_merkle or not self._get_remote_keys:
            return
        
        try:
            # Get peer's Merkle tree
            remote_merkle_data = self._get_remote_merkle(peer_id)
            if not remote_merkle_data:
                return
            
            # Find differences
            local_hash = self._merkle_tree.get_root_hash()
            remote_hash = remote_merkle_data.get("root", {}).get("hash")
            
            if local_hash == remote_hash:
                return  # Trees are identical
            
            # Get differing key ranges
            # Simplified: just get all keys if hashes differ
            if self._get_local_data:
                local_data = self._get_local_data()
                local_keys = set(local_data.keys())
                
                # Request keys we might be missing
                remote_keys = self._get_remote_keys(peer_id, list(local_keys))
                
                if remote_keys and self._apply_repair_data:
                    for key, (value, version) in remote_keys.items():
                        local_version = local_data.get(key, (None, 0))[1]
                        if version > local_version:
                            self._apply_repair_data(key, value, version)
                            
        except Exception as e:
            print(f"Repair with {peer_id} failed: {e}")
    
    def get_merkle_root(self) -> Optional[str]:
        """Get current Merkle tree root hash."""
        with self._lock:
            return self._merkle_tree.get_root_hash()
    
    def rebuild_tree(self, data: Dict[str, Tuple]):
        """Rebuild Merkle tree with new data."""
        with self._lock:
            self._merkle_tree.build(data)
    
    def get_keys_for_repair(self, key_ranges: List[Tuple[str, str]]) -> List[str]:
        """Get keys that fall within specified ranges."""
        keys = []
        for start, end in key_ranges:
            keys.extend(self._merkle_tree.get_keys_in_range(start, end))
        return keys
