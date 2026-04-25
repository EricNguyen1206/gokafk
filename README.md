# gokafk

A Kafka-compatible message broker built from scratch in Go. Implements the real Apache Kafka wire protocol, enabling compatibility with standard Kafka clients like [KafkaJS](https://kafka.js.org/).

## Features

- **Kafka Wire Protocol**: Implements the actual Kafka binary protocol — not a custom one. Standard Kafka clients connect and work out of the box.
- **Append-Only Log Storage**: High-performance disk storage using binary-frame segment files with in-memory sparse indexing for O(1) offset lookups.
- **Partitioned Topics**: Topics are split into configurable partitions (default: 3) for concurrent read/write throughput.
- **Key-Based Routing**: Deterministic FNV-32a hashing ensures messages with the same key always land on the same partition.
- **Consumer Group Coordination**: Full group protocol — JoinGroup, SyncGroup, LeaveGroup, Heartbeat — with leader election, generation tracking, and channel-based follower synchronization.
- **Persistent Consumer Offsets**: Offsets are committed to an internal `__consumer_offsets` topic and automatically recovered on broker restart.
- **KafkaJS Tested**: End-to-end integration tests with KafkaJS covering produce, fetch, consumer groups, and offset management.

## Supported Kafka APIs

| API | Key | Versions | Description |
|-----|-----|----------|-------------|
| Produce | 0 | 0-8 | Write messages to topics |
| Fetch | 1 | 0-11 | Read messages from partitions |
| ListOffsets | 2 | 0-5 | Query earliest/latest/timestamp offsets |
| Metadata | 3 | 0-9 | Discover topics, partitions, and brokers |
| OffsetCommit | 8 | 0-8 | Commit consumer offsets |
| OffsetFetch | 9 | 0-5 | Retrieve committed offsets |
| FindCoordinator | 10 | 0-3 | Locate group coordinator |
| JoinGroup | 11 | 0-7 | Join a consumer group |
| Heartbeat | 12 | 0-4 | Keep consumer group membership alive |
| LeaveGroup | 13 | 0-4 | Leave a consumer group |
| SyncGroup | 14 | 0-5 | Distribute partition assignments |
| ApiVersions | 18 | 0-3 | Negotiate supported API versions |

## Architecture

```mermaid
graph LR
    subgraph Clients
        P["Producer\n(KafkaJS / any Kafka client)"]
        C["Consumer\n(KafkaJS / any Kafka client)"]
    end

    subgraph Broker["Broker :10000"]
        direction TB
        CODEC["KafkaCodec\n(length-prefixed framing)"]
        ROUTER["routeMessage()\n(API key dispatch)"]

        subgraph Protocol["Kafka Protocol Layer"]
            direction TB
            PRODUCE["Produce"]
            FETCH["Fetch"]
            GRP["JoinGroup / SyncGroup\nLeaveGroup / Heartbeat"]
            OFFSET["OffsetCommit / OffsetFetch"]
            META["Metadata / ApiVersions\nFindCoordinator / ListOffsets"]
        end

        subgraph Topics["Topic Management"]
            direction TB
            T1["Topic (e.g. 'orders')"]
            P0["Partition 0"]
            P1["Partition 1"]
            P2["Partition 2"]
            T1 --> P0
            T1 --> P1
            T1 --> P2
        end

        subgraph Coordination["Group Coordination"]
            CG["CoordinatedGroup\n(leader election, generation tracking)"]
            CGO["ConsumerGroup\n(offset tracking per partition)"]
        end

        CODEC --> ROUTER
        ROUTER --> Protocol
        PRODUCE --> Topics
        FETCH --> Topics
        GRP --> Coordination
        OFFSET --> CGO
    end

    subgraph Storage["Disk Storage"]
        S0[("partition_0/topic_0.log")]
        S1[("partition_1/topic_1.log")]
        S2[("partition_2/topic_2.log")]
        P0 --> S0
        P1 --> S1
        P2 --> S2
    end

    P -- "TCP (Kafka protocol)" --> CODEC
    C -- "TCP (Kafka protocol)" --> CODEC
```

## Message Flow

```mermaid
sequenceDiagram
    participant P as Producer
    participant B as Broker
    participant D as Disk (Segment)
    participant C as Consumer

    Note over P,B: Connection Handshake
    P->>B: ApiVersions
    B-->>P: Supported API versions
    P->>B: Metadata (topics)
    B-->>P: Brokers, topics, partitions

    Note over P,D: Produce Flow
    P->>B: Produce {topic, key, value}
    B->>B: FNV-32a(key) → Partition X
    B->>D: Append(value) → binary frame
    B-->>P: ProduceResponse {partition, offset}

    Note over C,B: Consumer Group Join
    C->>B: FindCoordinator {groupId}
    B-->>C: Coordinator = Node 0
    C->>B: JoinGroup {groupId, protocols}
    B->>B: Register member, elect leader
    B-->>C: JoinGroupResponse {generationId, leaderId, memberId, members}
    C->>B: SyncGroup {groupId, memberId, assignments}
    B-->>C: SyncGroupResponse {assignment bytes}

    Note over C,D: Fetch Flow
    C->>B: OffsetFetch {groupId, topic, partitions}
    B-->>C: Committed offsets (or -1)
    C->>B: Fetch {topic, partition, offset}
    B->>D: Read(partition, offset)
    D-->>B: Message bytes
    B-->>C: FetchResponse {records}
    C->>B: OffsetCommit {groupId, topic, partition, offset}
    B->>B: Persist to __consumer_offsets
    B-->>C: OffsetCommitResponse (OK)
```

## Design Decisions

### Kafka Wire Protocol Compatibility
Rather than inventing a custom protocol, gokafk implements the real Kafka binary protocol. This means any Kafka client library (KafkaJS, Sarama, librdkafka, etc.) can connect directly. The broker uses length-prefixed framing with Big-Endian encoding, matching the [Kafka protocol spec](https://kafka.apache.org/protocol.html).

### Append-Only Segment Logs
Data is written sequentially (append-only) to binary-frame segment files. Each frame stores: `[timestamp(8) | data_length(4) | data(N)]`. An in-memory sparse index maps `offset → byte_position` for O(1) reads. Index is automatically rebuilt from the log file on broker restart.

### Partitions & Key-Based Routing
Topics are sharded into partitions (default: 3, configurable). Using `FNV-32a`, messages with the same key are consistently routed to the same partition — guaranteeing per-key ordering while allowing horizontal read throughput.

### Two-Layer Consumer Group Architecture
Consumer groups are managed at two levels:
- **`CoordinatedGroup`** (broker-level): Handles the group protocol — JoinGroup/SyncGroup/LeaveGroup. Manages leader election, generation tracking, and assignment distribution via channel-based synchronization.
- **`ConsumerGroup`** (topic-level): Tracks committed offsets per partition. Uses Range Assignor for partition distribution.

The broker acts as a pass-through for partition assignments: the leader consumer computes assignments, the broker stores and distributes them.

### Persistent Offset Recovery
Consumer offsets are persisted to an internal `__consumer_offsets` topic using key-value encoding (`groupID:topic:partition → offset`). On broker restart, offsets are replayed from disk and restored to memory — preventing duplicate consumption.

### Pull-Based Consumption
Consumers pull messages at their own pace via the Fetch API, naturally applying backpressure. The broker remains stateless regarding consumption progress — consumers track their own offsets and commit when ready.

## Usage

### Start the broker

```bash
go run cmd/gokafk/main.go server
```

The broker listens on `:10000` by default.

### Connect with KafkaJS

```javascript
const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:10000'],
})

// Produce
const producer = kafka.producer()
await producer.connect()
await producer.send({
  topic: 'my-topic',
  messages: [{ key: 'user-1', value: 'Hello gokafk!' }],
})

// Consume
const consumer = kafka.consumer({ groupId: 'my-group' })
await consumer.connect()
await consumer.subscribe({ topic: 'my-topic', fromBeginning: true })
await consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    console.log(message.value.toString())
  },
})
```

### Using Docker

```bash
# Build & run broker
docker compose up -d broker

# View logs
docker compose logs -f broker

# Stop
docker compose down
```

Or build manually:

```bash
docker build -t gokafk:dev .
docker run -d --name gokafk-broker -p 10000:10000 gokafk:dev
```

### Run integration tests

```bash
# Starts broker + KafkaJS test suite via Docker Compose
docker compose up --build --abort-on-container-exit test-runner
```

## Project Structure

```
gokafk/
├── cmd/gokafk/main.go               # Entry point (server command)
├── pkg/
│   └── kafkaprotocol/                # Kafka wire protocol implementation
│       ├── codec.go                  #   Length-prefixed framing & request header parsing
│       ├── primitives.go             #   Encoder/Decoder for Kafka primitives (int8-64, string, bytes, varint)
│       ├── types.go                  #   API key constants
│       ├── apiversions.go            #   ApiVersions response
│       ├── metadata.go               #   Metadata response
│       ├── produce.go                #   Produce request/response
│       ├── fetch.go                  #   Fetch request/response
│       ├── listoffsets.go            #   ListOffsets request/response
│       ├── findcoordinator.go        #   FindCoordinator response
│       ├── joingroup.go              #   JoinGroup request/response (v5)
│       ├── syncgroup.go              #   SyncGroup request/response (v3)
│       ├── leavegroup.go             #   LeaveGroup request/response (v2)
│       ├── handle_offset_commit.go   #   OffsetCommit request/response
│       └── offset_fetch.go           #   OffsetFetch request/response
├── internal/
│   ├── broker/                       # Core broker logic
│   │   ├── broker.go                 #   TCP server, connection handling, lifecycle
│   │   ├── handler.go                #   API key routing & request handlers
│   │   ├── topic.go                  #   Topic management, key-based partition routing
│   │   ├── partition.go              #   Partition wrapper over storage segments
│   │   ├── group_coordinator.go      #   CoordinatedGroup: JoinGroup/SyncGroup/LeaveGroup protocol
│   │   ├── consumer_group.go         #   ConsumerGroup: offset tracking & range assignor
│   │   └── consumer_offsets.go       #   Persistent offset storage via __consumer_offsets topic
│   ├── config/                       # Configuration defaults
│   └── storage/                      # Disk storage engine
│       ├── store.go                  #   Store interface
│       └── segment.go                #   Append-only binary segment with sparse index
├── test/
│   └── kafkajs/                      # KafkaJS integration tests
│       └── getting-started.test.js   #   End-to-end: produce, consume, consumer groups
├── Dockerfile                        # Multi-stage build
└── docker-compose.yml                # Broker + test runner services
```

## Read More

> [Wiki](https://github.com/EricNguyen1206/gokafk/wiki)
