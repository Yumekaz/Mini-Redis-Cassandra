"""
Main entry point for running a database node.
"""

import argparse
import signal
import sys
import time

from .config import NodeConfig, ConsistencyLevel
from .node import DatabaseNode


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Mini-Redis/Cassandra - Distributed Key-Value Database"
    )
    
    parser.add_argument(
        "--node-id",
        type=str,
        required=True,
        help="Unique node identifier"
    )
    
    parser.add_argument(
        "--host",
        type=str,
        default="localhost",
        help="Host to bind to (default: localhost)"
    )
    
    parser.add_argument(
        "--port",
        type=int,
        default=7001,
        help="Client port (default: 7001)"
    )
    
    parser.add_argument(
        "--cluster-port",
        type=int,
        default=8001,
        help="Cluster port (default: 8001)"
    )
    
    parser.add_argument(
        "--peers",
        type=str,
        default="",
        help="Comma-separated list of peer addresses (host:cluster_port)"
    )
    
    parser.add_argument(
        "--seed",
        type=str,
        default="",
        help="Seed node address (host:cluster_port)"
    )
    
    parser.add_argument(
        "--data-dir",
        type=str,
        default="./data",
        help="Data directory (default: ./data)"
    )
    
    parser.add_argument(
        "--replication-factor",
        type=int,
        default=3,
        help="Replication factor (default: 3)"
    )
    
    parser.add_argument(
        "--consistency",
        type=str,
        default="QUORUM",
        choices=["ONE", "QUORUM", "ALL", "ANY", "STRONG"],
        help="Default consistency level (default: QUORUM)"
    )
    
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()
    
    # Create config
    config = NodeConfig(
        node_id=args.node_id,
        host=args.host,
        client_port=args.port,
        cluster_port=args.cluster_port,
        data_dir=args.data_dir,
        replication_factor=args.replication_factor,
        default_consistency=ConsistencyLevel(args.consistency)
    )
    
    # Create node
    node = DatabaseNode(config)
    
    # Set up signal handlers
    def signal_handler(signum, frame):
        print("\nReceived shutdown signal...")
        node.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start node
    node.start()
    
    # Join peers if specified
    peers = []
    if args.peers:
        peers.extend(p.strip() for p in args.peers.split(","))
    if args.seed:
        peers.extend(p.strip() for p in args.seed.split(","))
        
    if peers:
        time.sleep(1)  # Wait for startup
        for peer in peers:
            peer = peer.strip()
            if peer:
                print(f"Joining cluster via {peer}...")
                if node.join_cluster(peer):
                    print(f"Successfully joined via {peer}")
                    break
                else:
                    print(f"Failed to join via {peer}")
    
    # Keep running
    print(f"\n{'='*50}")
    print(f"Mini-Redis/Cassandra Node '{args.node_id}' is running")
    print(f"Client: {args.host}:{args.port}")
    print(f"Cluster: {args.host}:{args.cluster_port}")
    print(f"{'='*50}\n")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        node.stop()


if __name__ == "__main__":
    main()
