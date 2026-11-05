# Kafka Protocol Server

An experimental Kafka wire protocol server written in Go, with persistent partition logs and Docker support. It implements a subset of the protocol and is not yet a complete Kafka broker or a drop-in replacement.

## Run in Docker

```sh
docker compose up --build -d
docker compose logs -f
```

The server listens on localhost:9092. Compose creates the `demo` topic with three partitions and stores records in a named volume. Stop with `docker compose down`; the volume survives. `docker compose down -v` deletes stored records.

To build and run without Compose:

```sh
docker build -t kafka-protocol-server .
docker run --rm -p 127.0.0.1:9092:9092 -v kafka-data:/data -e TOPICS=demo:3 kafka-protocol-server
```

## Configuration

| Variable | Default outside Docker | Purpose |
| --- | --- | --- |
| `LISTEN_ADDR` | `0.0.0.0:9092` | TCP listen address |
| `DATA_DIR` | `./data` | Partition log directory (`/data` inside Docker) |
| `TOPICS` | `demo:3` | Comma-separated topic names and partition counts, e.g. `orders:3,events:2` |
| `METADATA_LOG` | unset | Optional path to an existing KRaft metadata log; overrides `TOPICS` |

Topic configuration is read at startup. Keep it consistent across restarts; changing it does not migrate or delete existing partition logs. Topic IDs generated from names remain stable. Run one server instance per data directory.

## Current scope

- ApiVersions with flexible responses (v3/v4).
- DescribeTopicPartitions v0.
- Fetch v16 request decoding, empty responses, and topic/partition validation.
- Produce v11 with multiple records, batches, topics, and partitions.
- Disk appends, CRC checks, offset recovery, and zero-acknowledgement writes.

Fetch currently returns empty records for valid requested partitions, even when their logs contain data; disk reads, fetch offsets, byte limits, and sessions are not implemented. Unknown topic IDs return error 100 and invalid partitions return error 3.

Metadata, consumer groups, replication, transactions, authentication, and TLS are not implemented. Standard Kafka producer/consumer tools generally need additional APIs and are not supported end to end yet. The server is intended for local protocol experiments; malformed-input handling and storage performance still need work.

Topics can be configured at startup through environment variables or loaded from an existing metadata log.

## Development

Requires Go 1.26 or later.

```sh
go test ./...
go vet ./...
go run ./app
```

See [current status and roadmap](ROADMAP.md) for implementation limits, planned milestones, and completion checks. The immediate priority is verifying and hardening the existing container setup, followed by a basic produce-and-read workflow.
