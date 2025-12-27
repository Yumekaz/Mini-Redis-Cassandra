#!/bin/bash
# Mini-Redis/Cassandra Control Script

set -e

case "$1" in
    demo)
        echo "Running Automated Failure Demo..."
        python examples/failure_demo.py
        ;;
    cluster)
        echo "Starting 3-Node Cluster..."
        python -m minidb.main --node-id node1 --port 7001 --cluster-port 8001 --data-dir ./data/node1 &
        sleep 2
        python -m minidb.main --node-id node2 --port 7002 --cluster-port 8002 --data-dir ./data/node2 --seed localhost:8001 &
        sleep 2
        python -m minidb.main --node-id node3 --port 7003 --cluster-port 8003 --data-dir ./data/node3 --seed localhost:8001 &
        echo "Cluster started! Connect via: python -m minidb.cli localhost 7001"
        ;;
    cli)
        echo "Launching CLI Client..."
        python -m minidb.cli localhost 7001
        ;;
    validate)
        echo "Running Project Validation..."
        python tests/test_validation.py
        ;;
    quick)
        echo "Running Quick Health Check..."
        python tests/test_quick.py
        ;;
    *)
        echo "Mini-Redis/Cassandra Control Script"
        echo ""
        echo "Usage: ./run.sh <command>"
        echo ""
        echo "Commands:"
        echo "  demo      - Run automated failure demo"
        echo "  cluster   - Start 3-node cluster"
        echo "  cli       - Launch CLI client"
        echo "  validate  - Run project validation"
        echo "  quick     - Run quick health check"
        ;;
esac
