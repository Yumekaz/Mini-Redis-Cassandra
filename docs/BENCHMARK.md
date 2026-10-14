# Benchmark protocol

MiniDB performance results must identify the source revision and the durability/consistency settings being measured. No release-grade throughput or latency figure is published in this document.

Historical approximate figures without retained commands, hardware details and raw results are not current benchmark evidence. The synchronously flushed committed-write path must be measured separately from buffered or persistence-disabled writes.

## Record the environment

For every published run, retain:

| Field | Required detail |
| --- | --- |
| Source | Full commit ID and any working-tree patch |
| Host | CPU, RAM, storage device/filesystem and operating system |
| Runtime | Python version and process/node count |
| Topology | Localhost or multiple machines; client and cluster network setup |
| Data | Key count, value sizes, TTL behavior and warm-up |
| Consistency | Replication factor, write acknowledgement policy and read level |
| Persistence | AOF/snapshot settings and committed-write flush behavior |
| Load | Client count, operation mix, duration and offered request rate |
| Results | Successful/failed operation counts, throughput, p50/p95/p99/max latency |
| Resources | CPU, memory, disk growth, restarts and any missing samples |

## Campaign sequence

1. Allocate dedicated data directories and unused ports. Do not benchmark against an existing application database.
2. Record the revision and configuration before starting nodes.
3. Let membership settle, then seed a known data set and verify it.
4. Run separate single-node and replicated-cluster workloads; label them accurately.
5. Exercise read levels separately and keep write durability enabled for the durable-write result.
6. Keep a fixed workload for each measured interval and retain per-operation outcomes.
7. Verify final data and acknowledged writes after the interval.
8. Repeat fresh-fixture runs. Publish the repetitions and variance, not only the best result.

A single-node rate is not a three-node replicated-write rate. A short happy-path result is not a failure-recovery or sustained-load result. Record any faults as part of the workload rather than mixing them into an unlabeled average.

## Comparisons

Compare systems only with documented, equivalent data sets, hardware, client concurrency, persistence, replication and acknowledgement requirements. Do not infer Redis/Cassandra production parity from similar command names or hashing strategies.

The [validation script](../tests/test_validation.py) includes basic checks, but those checks alone do not provide a standardized production benchmark. [FailForge](https://github.com/Yumekaz/FAILFORGE) campaigns establish selected fault/operation coverage, not a throughput target.

See [the README](../README.md) for MiniDB's supported consistency and persistence boundaries.
