# Partition System & Production-Grade Improvements — Design Spec

## Overview

Two-phase upgrade of gokafk from prototype to production-grade distributed message broker with partition support.

- **Phase 1**: Foundation refactor — clean architecture, protocol upgrade, storage improvements, context/graceful shutdown
- **Phase 2**: Partition system — topic partitioning, key-based routing, consumer group rebalancing

## Phase Summary

| Phase | Focus | Key Deliverables |
|-------|-------|-----------------|
| 1 | Foundation | Package restructure, 4-byte protocol + CRC32, storage interface + sparse index, context.Context, graceful shutdown, error handling |
| 2 | Partitions | N partitions per topic, FNV-32a key hashing, consumer group range assignor, per-partition offset tracking |

---

## Section 1: Package Structure & Dependency Flow

### Current Problems
- `broker` imports `consumer` (tight coupling, near-circular)
- Queue uses global variables
- Types scattered across packages
- No centralized configuration

### Proposed Structure

```
gokafk/
├── cmd/gokafk/main.go              # Entry point, signal handling, graceful shutdown
├── internal/
│   ├── config/
│   │   └── config.go                # Centralized config (ports, paths, defaults)
│   ├── protocol/                    # Renamed from message/
│   │   ├── codec.go                 # Read/Write stream (4-byte header + CRC32)
│   │   ├── message.go               # Message struct & constants
│   │   ├── producer_msg.go          # ProducerRegisterMessage
│   │   └── consumer_msg.go          # ConsumerRegisterMessage
│   ├── broker/
│   │   ├── broker.go                # TCP server, lifecycle (context.Context)
│   │   ├── handler.go               # Message routing (extracted from broker.go)
│   │   ├── topic.go                 # Topic management
│   │   ├── partition.go             # Phase 2
│   │   └── consumer_group.go        # CGroup moved FROM consumer/ — belongs here
│   ├── storage/
│   │   ├── store.go                 # Storage interface
│   │   ├── segment.go               # Append-only log implementation
│   │   └── index.go                 # Sparse offset index (Phase 2)
│   ├── consumer/
│   │   └── consumer.go              # Consumer TCP client only (no CGroup)
│   └── producer/
│       └── producer.go              # Producer TCP client only
└── data/logs/                       # Phase 2: data/logs/topic_N/partition_M/
```

### Dependency Flow (Unidirectional)

```
cmd/gokafk → broker, consumer, producer, config
broker     → protocol, storage, config
consumer   → protocol, config
producer   → protocol, config
storage    → config
protocol   → (no internal deps)
config     → (no internal deps)
```

No circular dependencies. CGroup belongs in broker — broker manages consumer groups, not the consumer client.

---

## Section 2: Protocol Layer (4-byte Header + CRC32)

### Current Problems
- 1-byte length header → max 255 bytes payload
- No checksum — silent corruption undetected
- No correlation ID — can't match async request/response
- C_REG used for both registration AND fetch — confusing dual-purpose

### Wire Format

```
┌──────────────────────────────────────────────────────────┐
│  4 bytes     │ 1 byte  │ 4 bytes │ 4 bytes │ N bytes    │
│  Length (BE)  │  Type   │ CorrID  │  CRC32  │  Payload   │
└──────────────────────────────────────────────────────────┘
     └─ total payload size (type + corrID + CRC + payload)
```

- **Length**: uint32 big-endian. Max ~4GB.
- **Type**: Message type byte.
- **CorrID**: Correlation ID — match request → response.
- **CRC32**: IEEE CRC32 over payload bytes. Detect corruption.
- **Payload**: Type-specific data.

### Codec Interface

```go
// internal/protocol/codec.go
type Codec struct {
    rw *bufio.ReadWriter
}

func (c *Codec) ReadMessage(ctx context.Context) (*Message, error)
func (c *Codec) WriteMessage(ctx context.Context, msg *Message) error
```

Context enables timeout/cancellation — hung connections get killed.

### Message Types

```go
const (
    TypeEcho    uint8 = 1
    TypePReg    uint8 = 2   // Producer Register
    TypeCReg    uint8 = 3   // Consumer Register
    TypeProduce uint8 = 4   // Produce message (renamed from PCM)
    TypeFetch   uint8 = 5   // Fetch message (NEW — replaces C_REG dual-purpose)

    // Responses = type + 100
    TypeEchoResp    uint8 = 101
    TypePRegResp    uint8 = 102
    TypeCRegResp    uint8 = 103
    TypeProduceResp uint8 = 104
    TypeFetchResp   uint8 = 105
)
```

Key fix: Split C_REG (register) and Fetch (pull message) into separate message types.

---

## Section 3: Storage Layer — Interface + Offset Index

### Current Problems
- `ReadOffset()` scans from line 0 every time → O(n)
- Write file handle never closed
- Error from `NewSegment` ignored in `topic.init()`

### Storage Interface

```go
// internal/storage/store.go
type Store interface {
    Append(data []byte) (offset int64, err error)
    Read(offset int64) ([]byte, error)
    Close() error
    CurrentOffset() int64
}
```

Why interface? Broker depends on Store interface, not concrete Segment. Easy to mock in tests. Phase 2 swaps to partition-aware store without touching broker code.

### Segment Improvements

```go
type Segment struct {
    mu       sync.RWMutex        // RWMutex — reads don't block each other
    file     *os.File            // write handle
    path     string
    offset   int64               // current message count
    index    map[int64]int64     // sparse index: message offset → byte position
    bytePos  int64               // current byte position in file
}
```

### Sparse Index — O(1) Reads

- On `Append()`: store `index[messageOffset] = bytePosition`
- On `Read(offset)`: lookup `index[offset]` → `file.Seek(bytePos)` → read line → **O(1)**
- Index in-memory, rebuild from file on restart (scan once at startup)

```go
func (s *Segment) Append(data []byte) (int64, error) {
    s.mu.Lock()
    defer s.mu.Unlock()

    clean := bytes.TrimSpace(data)
    n, err := s.file.Write(append(clean, '\n'))
    if err != nil {
        return -1, fmt.Errorf("segment append: %w", err)
    }

    offset := s.offset
    s.index[offset] = s.bytePos
    s.bytePos += int64(n)
    s.offset++
    return offset, nil
}

func (s *Segment) Read(offset int64) ([]byte, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()

    bytePos, ok := s.index[offset]
    if !ok {
        return nil, ErrOffsetNotFound
    }
    // Seek + read single line from byte position
}
```

### Error Handling
All methods return error. No ignoring. `NewSegment` failure propagates to caller.

### Phase 2 Data Layout

```
data/logs/
└── topic_1/
    ├── partition_0/
    │   ├── 00000000.log
    │   └── 00000000.index    # persistent index (optional, rebuild from log)
    ├── partition_1/
    │   └── ...
    └── partition_2/
        └── ...
```

---

## Section 4: Broker — Context, Graceful Shutdown, Concurrency

### Current Problems
- No context → no cancel/timeout. Goroutine leaks on hung connections.
- Single `sync.Mutex` across entire broker → serializes everything.
- `Broker.init()` not idiomatic Go (should use constructor).

### Broker Struct

```go
type Broker struct {
    cfg      *config.Config
    mu       sync.RWMutex
    topics   map[string]*Topic      // map for O(1) lookup
    listener net.Listener
    wg       sync.WaitGroup         // track goroutines for graceful shutdown
}

func NewBroker(cfg *config.Config) *Broker {
    return &Broker{
        cfg:    cfg,
        topics: make(map[string]*Topic),
    }
}
```

### Graceful Shutdown

```go
func (b *Broker) Start(ctx context.Context) error {
    ln, err := net.Listen("tcp", fmt.Sprintf(":%d", b.cfg.BrokerPort))
    if err != nil {
        return fmt.Errorf("broker listen: %w", err)
    }
    b.listener = ln

    go func() {
        <-ctx.Done()
        slog.Info("shutting down broker")
        ln.Close()
    }()

    for {
        conn, err := ln.Accept()
        if err != nil {
            if ctx.Err() != nil {
                break
            }
            slog.Error("accept error", "err", err)
            continue
        }
        b.wg.Add(1)
        go func() {
            defer b.wg.Done()
            b.handleConnection(ctx, conn)
        }()
    }

    b.wg.Wait()
    return nil
}
```

### main.go Signal Handling

```go
ctx, cancel := signal.NotifyContext(context.Background(),
    syscall.SIGINT, syscall.SIGTERM)
defer cancel()
```

### Concurrency Improvements

| Before | After |
|--------|-------|
| `sync.Mutex` whole broker | `sync.RWMutex` — reads don't block |
| Topic lookup = linear scan slice | `map[string]*Topic` — O(1) |
| Each Topic also gets its own `sync.RWMutex` | Partition writes on different topics fully independent |
| Connection goroutines untracked | `sync.WaitGroup` tracks all, drains before shutdown |

### Handler Extraction

```go
// internal/broker/handler.go
func (b *Broker) handleProduce(ctx context.Context, msg *protocol.Message, conn net.Conn) (*protocol.Message, error)
func (b *Broker) handleProducerRegister(ctx context.Context, msg *protocol.Message, conn net.Conn) (*protocol.Message, error)
func (b *Broker) handleConsumerRegister(ctx context.Context, msg *protocol.Message) (*protocol.Message, error)
func (b *Broker) handleFetch(ctx context.Context, msg *protocol.Message) (*protocol.Message, error)
```

---

## Section 5: Partition System (Phase 2)

### Core Concept
Each Topic has N partitions. Each partition = independent append-only log. Producer selects partition via key hashing. Consumer groups rebalance partitions among members.

### Topic + Partition Structs

```go
// internal/broker/topic.go
type Topic struct {
    mu         sync.RWMutex
    name       string
    partitions []*Partition
    numParts   int
    rrCounter  uint64              // atomic round-robin counter
}

// internal/broker/partition.go
type Partition struct {
    mu        sync.RWMutex
    id        int
    topicName string
    store     storage.Store
}
```

### Producer Partition Assignment — FNV-32a Hash

```go
func (t *Topic) partitionFor(key []byte) int {
    if key == nil {
        return int(atomic.AddUint64(&t.rrCounter, 1)) % t.numParts
    }
    h := fnv.New32a()
    h.Write(key)
    return int(h.Sum32()) % t.numParts
}
```

- FNV-32a from Go stdlib — zero dependency.
- Same key → same partition → **message ordering guarantee per key**.
- No key → round-robin → max throughput, no ordering guarantee.

### Produce Message Format

```
TypeProduce payload = [2 bytes topicID][2 bytes keyLen][N bytes key][M bytes value]
                      keyLen = 0 → no key → round-robin
```

### Consumer Group Rebalancing — Range Assignor

```go
// internal/broker/consumer_group.go
type ConsumerGroup struct {
    mu          sync.RWMutex
    groupID     string
    members     map[string]*GroupMember
    assignments map[string][]int          // memberID → assigned partition IDs
    offsets     map[int]int64             // partitionID → committed offset
}

type GroupMember struct {
    memberID string
    conn     net.Conn
    joinedAt time.Time
}
```

### Rebalance Algorithm

```go
func (cg *ConsumerGroup) rebalance(numPartitions int) {
    cg.mu.Lock()
    defer cg.mu.Unlock()

    members := sortedMemberIDs(cg.members)
    cg.assignments = make(map[string][]int)

    perMember := numPartitions / len(members)
    remainder := numPartitions % len(members)

    idx := 0
    for i, memberID := range members {
        count := perMember
        if i < remainder {
            count++
        }
        for j := 0; j < count; j++ {
            cg.assignments[memberID] = append(cg.assignments[memberID], idx)
            idx++
        }
    }
}
```

6 partitions, 2 consumers → consumer 0 gets [0,1,2], consumer 1 gets [3,4,5].

### Fetch Flow

```
Consumer → Fetch{topicID, groupID, memberID}
Broker   → lookup assignments[memberID] → get partitions
         → for each assigned partition, read from committed offset
         → return batch of messages
         → consumer ACK → advance offset
```

---

## Section 6: Testing, Fault Tolerance & DevOps

### Testing Strategy — 3 Layers

| Layer | Scope |
|-------|-------|
| Unit | Each package: storage, protocol, broker logic |
| Integration | Broker + producer + consumer end-to-end via TCP |
| Benchmark | Storage throughput, protocol encode/decode speed |

### Unit Tests

| Package | Tests |
|---------|-------|
| `protocol` | Codec round-trip, CRC32 validation, corrupt data detection, header edge cases |
| `storage` | Append + read by offset, concurrent append (race detector), index rebuild, close idempotent |
| `broker` | Partition assignment determinism, consumer group rebalance math, handler routing |

### Integration Test Pattern

```go
func TestProduceAndConsume(t *testing.T) {
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    broker := broker.NewBroker(testConfig())
    go broker.Start(ctx)

    // Producer sends messages
    // Consumer registers + fetches
    // Assert: consumer receives all messages, correct order per partition
}
```

### Benchmark Tests

```go
func BenchmarkSegmentAppend(b *testing.B)    // append throughput
func BenchmarkCodecRoundTrip(b *testing.B)   // protocol encode+decode latency
```

Run with `go test -race ./...` — race detector catches concurrency bugs.

### Fault Tolerance

| Scenario | Handling |
|----------|---------|
| Producer disconnect | Broker detects via read error → cleanup connection |
| Consumer disconnect | Remove from group → trigger rebalance → partitions reassign |
| Broker restart | Segment rebuilds index from log on startup. Consumer re-registers |
| Corrupt message | CRC32 mismatch → drop message, return error |
| Disk full | `Append()` returns error → propagates to producer |

### DevOps

**Makefile:**
```makefile
.PHONY: build test bench lint

build:
    go build -o bin/gokafk ./cmd/gokafk

test:
    go test -race -cover ./...

bench:
    go test -bench=. -benchmem ./internal/storage/ ./internal/protocol/

lint:
    golangci-lint run ./...
```

**Dockerfile:**
```dockerfile
FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /gokafk ./cmd/gokafk

FROM alpine:3.19
COPY --from=builder /gokafk /gokafk
ENTRYPOINT ["/gokafk"]
```

**GitHub Actions CI** (on PR/push to main):
- `golangci-lint run ./...`
- `go vet ./...`
- `go test -race -cover ./...`
- `go build ./cmd/gokafk`
- Coverage badge in README

---

## Implementation Phases

### Phase 1: Foundation Refactor
1. Package restructure (move files, fix imports)
2. Protocol upgrade (4-byte header, CRC32, codec)
3. Storage improvements (interface, sparse index, RWMutex, error handling)
4. Broker refactor (context, graceful shutdown, constructor, RWMutex, handler extraction)
5. Consumer/Producer client updates (use new protocol)
6. Config package
7. Unit tests for all new code
8. Makefile, Dockerfile, CI

### Phase 2: Partition System
1. Partition struct + per-partition storage
2. Topic partition management (create N partitions)
3. Producer key hashing (FNV-32a) + round-robin
4. Fetch message type + handler
5. Consumer group + range assignor rebalancing
6. Consumer/Producer client updates for partitions
7. Integration tests (produce → partition → consume)
8. Benchmark tests
9. Update README with new architecture diagrams
