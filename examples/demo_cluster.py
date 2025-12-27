"""
Interactive demo of the distributed database cluster.
Shows cluster formation, leader election, replication, and failover.
"""

import time
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from minidb.node import create_node
from minidb.network.client import TCPClient



def print_header(text):
    """Print a section header."""
    print("\n" + "=" * 60)
    print(f" {text}")
    print("=" * 60)


def wait_and_print(seconds, message):
    """Wait with a countdown."""
    for i in range(seconds, 0, -1):
        print(f"\r{message} ({i}s)...", end="", flush=True)
        time.sleep(1)
    print(f"\r{message}... Done!          ")


def demo():
    """Run the cluster demo."""
    print_header("Mini-Redis/Cassandra Distributed Database Demo")
    print("""
This demo will:
1. Start a 3-node cluster
2. Demonstrate leader election
3. Show write replication
4. Demonstrate read from any node
5. Show cluster state
    """)
    
    nodes = []
    
    try:
        # Step 1: Start nodes
        print_header("Step 1: Starting 3-Node Cluster")
        
        for i in range(3):
            node = create_node(
                f"node{i+1}",
                client_port=7001 + i,
                cluster_port=8001 + i,
                data_dir=f"./demo_data/node{i+1}",
                aof_enabled=False,
                snapshot_enabled=False
            )
            nodes.append(node)
            node.start()
            print(f"  Started node{i+1} (client: 700{i+1}, cluster: 800{i+1})")
        
        wait_and_print(3, "Waiting for leader election")
        
        # Step 2: Form cluster
        print_header("Step 2: Forming Cluster")
        
        for i in range(1, 3):
            nodes[i].join_cluster("localhost:8001")
            print(f"  node{i+1} joined cluster via node1")
        
        wait_and_print(2, "Waiting for cluster to stabilize")
        
        # Step 3: Check leader
        print_header("Step 3: Leader Election Result")
        
        leader_idx = None
        for i, node in enumerate(nodes):
            role = "LEADER" if node.cluster.is_leader() else "FOLLOWER"
            print(f"  node{i+1}: {role} (term: {node.cluster.election.current_term})")
            if node.cluster.is_leader():
                leader_idx = i
        
        if leader_idx is None:
            print("  ERROR: No leader elected!")
            return
        
        # Step 4: Write data through leader
        print_header("Step 4: Writing Data Through Leader")
        
        client = TCPClient("localhost", 7001 + leader_idx)
        client.connect()
        
        data = {
            "user:alice": '{"name": "Alice", "age": 30}',
            "user:bob": '{"name": "Bob", "age": 25}',
            "user:charlie": '{"name": "Charlie", "age": 35}',
            "config:app": "production",
            "counter": "100"
        }
        
        for key, value in data.items():
            response = client.send_command("SET", key, value)
            status = "OK" if response and response.payload.get('success') else "FAIL"
            print(f"  SET {key}: {status}")
        
        client.disconnect()
        
        wait_and_print(1, "Waiting for replication")
        
        # Step 5: Read from followers
        print_header("Step 5: Reading Data from All Nodes")
        
        for i in range(3):
            client = TCPClient("localhost", 7001 + i)
            if client.connect():
                response = client.send_command("GET", "user:alice")
                value = response.payload.get('data') if response else None
                print(f"  node{i+1} GET user:alice: {value}")
                client.disconnect()
        
        # Step 6: Cluster info
        print_header("Step 6: Cluster State")
        
        client = TCPClient("localhost", 7001 + leader_idx)
        client.connect()
        
        response = client.send_command("CLUSTER")
        if response:
            info = response.payload.get('data', {})
            print(f"  Current term: {info.get('term')}")
            print(f"  Leader: {info.get('leader_id')}")
            print(f"  Alive nodes: {info.get('alive_count')}")
            print(f"  Log entries: {info.get('log_length')}")
            print(f"  Commit index: {info.get('commit_index')}")
        
        response = client.send_command("INFO")
        if response:
            info = response.payload.get('data', {})
            print(f"\n  Key count: {info.get('keys')}")
            stats = info.get('stats', {})
            print(f"  Total operations: {stats.get('sets', 0) + stats.get('hits', 0)}")
        
        client.disconnect()
        
        # Step 7: Keys listing
        print_header("Step 7: All Keys")
        
        client = TCPClient("localhost", 7001 + leader_idx)
        client.connect()
        
        response = client.send_command("KEYS", "*")
        if response:
            keys = response.payload.get('data', [])
            print(f"  Total keys: {len(keys)}")
            for key in keys:
                print(f"    - {key}")
        
        client.disconnect()
        
        print_header("Demo Complete!")
        print("""
The cluster is still running. You can connect to it using:

  python -c "
from minidb.network.client import TCPClient
c = TCPClient('localhost', 7001)
c.connect()
print(c.send_command('GET', 'user:alice').payload)
c.disconnect()
"

Press Ctrl+C to stop the cluster.
        """)
        
        # Keep running
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n\nShutting down cluster...")
    finally:
        for node in nodes:
            try:
                node.stop()
            except:
                pass
        print("Cluster stopped.")


if __name__ == "__main__":
    demo()
