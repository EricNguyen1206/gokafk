# gokafk

A distributed message broker inspired by Apache Kafka, built from scratch in Go. 

## Features
- **Append-Only Log Storage**: High-performance disk storage using segmented files with sparse memory indexing.
- **Partitioning System**: Topics are split into multiple partitions for high concurrency.
- **Key-based Routing**: Deterministic routing using FNV-32a hashing ensures messages with the same key go to the same partition.
- **Consumer Group Rebalancing**: Implements "Range Assignor" algorithm to distribute partitions evenly among connected consumers.
- **Custom Binary Protocol**: Lightweight custom TCP protocol using Big-Endian formatting, CRC32 checksum integration, and explicit correlation mapping.

## Architecture

```mermaid
graph LR
    subgraph Producers
        P1["Producer A\n(Topic: 1, Key: 'user1')"]
        P2["Producer B\n(Topic: 1, Key: 'user2')"]
    end

    subgraph Broker["Broker :10000"]
        direction TB
        BH["handleConnection()\n(goroutine per conn)"]
        BM["routeMessage()"]
        
        subgraph T1["Topic 1"]
            direction TB
            Part0["Partition 0"]
            Part1["Partition 1"]
            Part2["Partition 2"]
        end
        
        BH --> BM
        BM -- "FNV-32a Hash" --> T1
    end

    subgraph Storage["Disk Storage (Append-Only Segment Logs)"]
        F1[("data/logs/topic_1/partition_0/topic_0.log")]
        F2[("data/logs/topic_1/partition_1/topic_1.log")]
        F3[("data/logs/topic_1/partition_2/topic_2.log")]
        Part0 --> F1
        Part1 --> F2
        Part2 --> F3
    end

    subgraph ConsumerGroups["Consumer Group 1 (Topic 1)"]
        direction TB
        C1["Consumer 1\n(Assigned: Part 0, 1)"]
        C2["Consumer 2\n(Assigned: Part 2)"]
    end

    P1 -- "TCP: ProduceMsg" --> BH
    P2 -- "TCP: ProduceMsg" --> BH
    
    C1 -- "TCP: FetchReq" --> BH
    C2 -- "TCP: FetchReq" --> BH
    
    BM -- "FetchResp (bytes)" --> C1
    BM -- "FetchResp (bytes)" --> C2
```

## Message Flow

```mermaid
sequenceDiagram
    participant P as Producer
    participant B as Broker
    participant D as Disk (Partition X)
    participant C as Consumer

    P->>B: P_REG {port, topicID}
    B-->>P: P_REG_RESP (OK)

    loop Producer sends messages
        P->>B: ProduceMsg {TopicID, Key, Value}
        B->>B: Hash(Key) -> Partition X
        B->>D: Append(data bytes)
        B-->>P: ProduceResp (OK)
    end

    C->>B: C_REG {port, groupID, topicID}
    B->>B: Rebalance(Range Assignor)
    B-->>C: C_REG_RESP (OK, assigned MemberID)

    loop Consumer pulls messages
        C->>B: FetchReq {TopicID, GroupID, MemberID}
        B->>B: Check Assigned Partitions
        B->>D: Read(assigned Partition's Offset)
        D-->>B: message bytes
        B-->>C: FetchResp {PartitionID, Offset, Data}
        Note over C: process data & record offset
    end
```

## Design Decisions

### Append-Only Log & Sparse Index
Data is written sequentially (append-only) instead of random updates. This yields 10-100x disk I/O performance. An in-memory sparse index tracks `[message_offset -> byte_position]` allowing for O(1) read lookups without scanning the file.

### Partitions & Key-Based Routing
Topics are sharded into partitions. Using `FNV-32a`, messages containing the same `Key` are consistently written to the same partition, guaranteeing ordered consumption per key while allowing horizontal scaling overall.

### Consumer Group Range Assignor
Consumers subscribing to the same `GroupID` share the topic workload. Partitions are evenly divided among consumers using a deterministic Range Assignor algorithm. When consumers join or leave, the group inherently rebalances to ensure zero starved partitions.

### Pull-Based Consumption
Consumers request (pull) data at their own pace, naturally applying backpressure. This removes the burden of tracking state from the broker, keeping it stateless and highly efficient.

## Usage

### Using Go

```bash
# Terminal 1 — Run broker
go run cmd/gokafk/main.go server

# Terminal 2 — Run producer (port=8000, topicID=1)
# Usage: gokafk producer <port> <topicID> [optional-key]
go run cmd/gokafk/main.go producer 8000 1 user_key

# Terminal 3 — Run consumer (port=0, topicID=1, groupID=1)
# Usage: gokafk consumer <port> <topicID> <groupID>
go run cmd/gokafk/main.go consumer 0 1 1
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
# Build image
docker build -t gokafk:dev .

# Run broker
docker run -d --name gokafk-broker -p 10000:10000 gokafk:dev
```

### Run integration tests (Docker)

```bash
# Starts broker + KafkaJS test suite
docker compose up --build --abort-on-container-exit test-runner
```

## Project Structure

```
gokafk/
├── cmd/gokafk/main.go          # Entry point (server | producer | consumer)
├── pkg/
│   ├── client/                 # Public SDK Consumer/Producer TCP client
│   └── protocol/               # Binary codec (Marshal/Unmarshal, CRC32 verification)
├── internal/
│   ├── broker/                 # TCP server, grouping, partition & topic controllers
│   ├── config/                 # Default configurations and environments
│   ├── cli/                    # CLI logic for producer and consumer commands
│   └── storage/                # Disk storage layer (Segment, Sparse Index)
```

## Readmore

> [Wiki](https://gistcdn.githack.com/EricNguyen1206/e6395e9b4b6323f2125312dd8e80d60b/raw/index.html)

