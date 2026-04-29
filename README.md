# gokafk

A Kafka broker built from scratch in Go. Real Kafka wire protocol — any Kafka client connects directly.

## What it does

- Implements the **real Kafka binary protocol** (not a custom API) — KafkaJS, Sarama, etc. work out of the box
- **Append-only log storage** on disk with sparse index for O(1) offset lookups
- **Partitioned topics** with FNV-32a key-based routing (same key → same partition)
- **Consumer groups**: full protocol (JoinGroup → SyncGroup → Heartbeat → LeaveGroup) with leader election & generation tracking
- **Persistent offsets**: committed to `__consumer_offsets` topic, recovered on restart


## Quick start

### Startup

- with binary:

```bash
./gokafk server
```

- with source:

```bash
go run cmd/gokafk/main.go server
```

- with docker:

```bash
docker build -t gokafk .
docker run -p 10000:10000 gokafk
```

### Test with KafkaJS

```javascript
const { Kafka } = require('kafkajs')
const kafka = new Kafka({ brokers: ['localhost:10000'] })

// Produce
const p = kafka.producer()
await p.connect()
await p.send({ topic: 'orders', messages: [{ key: 'u1', value: 'hello' }] })

// Consume
const c = kafka.consumer({ groupId: 'orders-group' })
await c.connect()
await c.subscribe({ topic: 'orders', fromBeginning: true })
await c.run({ eachMessage: async ({ message }) => console.log(message.value.toString()) })
```

## 12 Kafka APIs

Produce · Fetch · ListOffsets · Metadata · OffsetCommit · OffsetFetch · FindCoordinator · JoinGroup · Heartbeat · LeaveGroup · SyncGroup · ApiVersions

## Architecture

```
  Producer ──TCP──→ ┌─────────────────────────────┐
  Consumer ──TCP──→ │  Broker (:10000)             │
                    │  ┌─────────────────────────┐ │
                    │  │ KafkaCodec → routeMessage│ │
                    │  │   ├ Produce → Topic      │ │
                    │  │   ├ Fetch    → Topic      │ │
                    │  │   ├ JoinGroup → GroupMeta │ │──→ [offset|ts|len|data]
                    │  │   ├ OffsetCommit → CG     │ │     segment.log
                    │  │   └ ...                   │ │
                    │  └─────────────────────────┘ │
                    └─────────────────────────────┘
```

**3 layers:**
1. **Protocol** (`pkg/proto/`) — encode/decode Kafka binary frames, parse requests, build responses
2. **Broker** (`internal/broker/`) — TCP server, route by API key, topic/partition management, consumer group coordination
3. **Storage** (`internal/storage/`) — append-only segment files with in-memory sparse index

**Consumer group flow:** Metadata → FindCoordinator → JoinGroup → SyncGroup → Heartbeat (loop) → Fetch → OffsetCommit → LeaveGroup

## Project structure

```
cmd/gokafk/main.go              entry point
pkg/proto/              wire protocol (encoder, decoder, 12 API handlers)
internal/broker/                TCP server, routing, topics, consumer groups
internal/storage/               append-only segment with sparse index
internal/config/                defaults (port, data dir, partitions)
test/kafkajs/                   KafkaJS integration tests
.github/workflows/              CI: build+test, GoReleaser
```

~2,700 lines production · ~2,750 lines tests · 87%+ coverage

## Key features

| Feature | Implementation |
|----------|-----|
| Real Kafka protocol | Any client works. No SDK lock-in. |
| Append-only segments | Sequential writes = fast. Sparse index = O(1) reads. |
| FNV-32a partitioning | Same key → same partition → per-key ordering. |
| `__consumer_offsets` topic | Offsets survive restarts. Same pattern as real Kafka. |
| Pull-based consumption | Natural backpressure. Consumers own their pace. |
| 2-layer group model | GroupMetadata = protocol. ConsumerGroup = offsets. Clean separation. |

## Stats

| | |
|---|---|
| Go | 1.23 |
| Lines | ~5,500 (prod + test) |
| Test ratio | ~1:1 |
| Coverage | 87%+ across packages |
| APIs | 12 Kafka APIs |

> [Wiki](https://github.com/EricNguyen1206/gokafk/wiki)
