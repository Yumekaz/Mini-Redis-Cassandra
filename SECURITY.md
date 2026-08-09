# Security Policy

## Reporting Vulnerabilities

Please do not open public issues for security vulnerabilities.

Instead, report them privately via email:
- pathaktarun431@gmail.com

## Scope

Mini-Redis-Cassandra is an experimental replicated distributed datastore and systems-infrastructure project implemented in Python.

It is **not production-ready** and must not be used for sensitive data or exposed to untrusted networks without a separate security layer.

## Security Considerations

The project explores mechanisms such as:
- Distributed consensus (Raft-lite)
- Gossip-based cluster membership
- Consistent hashing and sharding
- Persistence (AOF + snapshots)

These mechanisms are intentionally bounded and may be incomplete under adversarial network conditions, partitions, or node compromise.

For production distributed databases, use established systems such as:
- Redis Cluster
- Apache Cassandra
- etcd
- CockroachDB
