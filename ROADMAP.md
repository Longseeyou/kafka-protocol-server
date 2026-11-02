# Current status and roadmap

Updated: 2026-11-02

## Direction

Build a small, understandable Kafka protocol server that can run in a container for local development and protocol experiments. The first usability goal is a single-node produce-and-fetch workflow with a documented client configuration. Full Kafka compatibility and distributed operation are longer-term possibilities, not current promises.

This roadmap describes planned work; unchecked items are not implemented. Milestones are ordered by dependency rather than calendar deadlines.

## Current implementation

| Area | Available now | Limits |
| --- | --- | --- |
| TCP server | Framed requests, repeated requests on a connection, concurrent clients | No request-size cap, connection deadlines, or graceful shutdown |
| API discovery | ApiVersions flexible responses; advertises v3/v4 | Version handling and malformed requests need broader validation |
| Topic inspection | DescribeTopicPartitions v0 | No Metadata API for standard client discovery |
| Producing | Produce v11, multiple records and batches across topics and partitions, `acks=0` persistence | No idempotent or transactional guarantees; no replication |
| Storage | Batch framing and CRC checks, offset assignment, file appends with sync, recovery from existing logs | One storage mutex; rereads the whole partition log on every append; no segment rotation |
| Configuration | Listen address, data directory, startup topic definitions, optional metadata log | Topic changes require restart; no topic administration API |
| Packaging | Multi-stage Dockerfile, non-root runtime, Compose, persistent volume | Container image build and runtime verification remain pending |
| Tests | Batch appends, successive offsets, restart recovery, concurrency, invalid batches, partition results, zero acknowledgements, topic configuration | No complete external-client interoperability suite |

Go tests, vet, and a local TCP ApiVersions smoke check passed during initial setup. Compose configuration was validated; Docker image execution was not verified because the engine was unavailable. These are baseline checks, not an ongoing CI guarantee.

Fetch, consumer groups, authentication, TLS, retention, replication, and transactions are not implemented. Encoded batches are retained without fully validating individual record contents or establishing compression compatibility.

## Milestone 1 — Verify and harden the container baseline

Make the existing feature set reproducible before expanding the protocol.

- [ ] Build and run the image on a working Docker engine.
- [ ] Add a repeatable TCP smoke test and verify records survive container recreation with the same volume.
- [ ] Add CI for formatting, Go tests, vet, race detection where supported, and container build checks.
- [ ] Bound request sizes and decoder allocations; handle truncated input without panics.
- [ ] Make unsupported API/version behavior explicit and consistent with advertised support.
- [ ] Add connection deadlines, graceful termination, and useful structured error logs.
- [ ] Document what a successful acknowledgement guarantees on this single-node server.

**Completion check:** a clean checkout builds and starts through Compose, passes automated protocol checks, preserves stored records across restart, and handles tested invalid inputs without crashing or allocating unbounded memory.

## Milestone 2 — Complete the basic produce-and-read loop

Provide the smallest useful single-node workflow.

- [ ] Implement a documented Metadata API version for broker, topic, and partition discovery.
- [ ] Add an advertised broker address distinct from the listen address for host/container networking.
- [ ] Implement a documented Fetch API version with offset lookup, byte limits, empty responses, and partition errors.
- [ ] Implement ListOffsets for supported earliest/latest offset queries.
- [ ] Select and pin an external Kafka client for interoperability tests; document required options and unsupported defaults.
- [ ] Test produce, fetch, restart, and fetch again across multiple partitions.

**Completion check:** a documented client can discover the server, produce records, and read them back using explicit partition assignment, including after restart. Consumer-group coordination is not required for this milestone.

## Milestone 3 — Improve storage correctness and performance

Remove the main bottlenecks and define recovery behavior.

- [ ] Maintain next offsets in memory after an initial recovery scan.
- [ ] Replace the global storage lock with partition-level synchronization.
- [ ] Introduce segment rotation and an offset-to-file-position index for reads.
- [ ] Define and test recovery from incomplete trailing writes and corrupted batches.
- [ ] Strengthen record-count, offset-delta, attributes, and supported compression validation.
- [ ] Add failure-injection tests for write, sync, and recovery errors.
- [ ] Benchmark throughput, latency, memory use, and startup time with growing logs.

**Completion check:** normal appends no longer scan the full log, separate partitions can progress concurrently, and documented recovery tests pass. Publish benchmark conditions and results before setting performance targets.

## Milestone 4 — Make local operation convenient

- [ ] Add readiness/liveness checks and basic request, error, byte, and latency metrics.
- [ ] Publish versioned container images with repeatable release checks.
- [ ] Verify and publish supported CPU architectures.
- [ ] Document volume permissions, backup/restore, upgrades, and troubleshooting.
- [ ] Add bounded storage retention with explicit data-deletion settings and tests.
- [ ] Reorganize protocol decoding, request handling, storage, and configuration into clearer packages as the implementation grows.

**Completion check:** a user can run a tagged image, check its health, retain data across upgrades within documented constraints, and diagnose common configuration or storage problems.

## Later work — Decide from actual use

These items need separate design decisions and are not prerequisites for the initial local-use target:

- Consumer-group coordination and committed offsets.
- Dynamic topic administration.
- Idempotent producers and transactions.
- Authentication, TLS, and authorization for use beyond a trusted local environment.
- Replication, leader election, and multi-node operation.
- Broader API-version and compression support.

## How to maintain this plan

Keep changes incremental: one observable behavior, focused tests, and updated compatibility notes per change. Mark a task complete only when its acceptance evidence is available. Update this document when scope changes; avoid treating planned features as supported behavior in the README.

The immediate next task is **Milestone 1: verify the existing Docker image and persistent-volume behavior**, without adding more protocol APIs.
