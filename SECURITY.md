# Security policy

## Reporting a vulnerability

[Mihir Swarnkar](https://github.com/Yumekaz) is the project maintainer.

Please use a private contact method listed on the maintainer's profile. If no private channel is available, open an issue **only to request a private disclosure channel**, without exploit details, sensitive data or affected credentials. Do not publish vulnerability details in a public issue.

Include the source revision, affected configuration, reproduction steps and impact in the private report. Do not attach real application data or secrets.

## Scope

MiniDB is a replicated key-value datastore for controlled small-cluster deployments. It implements membership, simplified term-based election, consistent-hash sharding, replica acknowledgements, repair, AOF persistence and snapshots.

It does not implement consensus-grade split-brain fencing, TLS, authentication or authorization. Its security and fault guarantees are bounded by the documented implementation and tests; it has not received an independent hostile-network or tenant-isolation audit.

Keep nodes on a trusted/private network. A separately secured access layer is required before use outside that scope. Fault injection must target disposable data, not unrelated files or a live application database.

See [the README](README.md) and [design decisions](docs/DESIGN_DECISIONS.md) for the current guarantees and limitations.
