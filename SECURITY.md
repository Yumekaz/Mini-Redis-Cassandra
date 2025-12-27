# Security Policy

## Reporting Vulnerabilities

Please do not open public issues for security vulnerabilities.

Instead, report them privately via email:
- pathaktarun431@gmail.com

## Scope

Mini-Redis-Cassandra is an educational project intended to demonstrate distributed database concepts using Python.

It is **not designed for production use** or for handling sensitive data.

## Security Considerations

The project explores mechanisms such as:
- Distributed consensus (Raft-lite)
- Gossip-based cluster membership
- Consistent hashing and sharding
- Persistence (AOF + snapshots)

These are provided for learning purposes and may be incomplete.

For production distributed databases, use established systems such as:
- Redis Cluster
- Apache Cassandra
- etcd
- CockroachDB
