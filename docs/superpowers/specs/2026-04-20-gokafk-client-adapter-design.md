# Gokafk Client Adapter Design
Date: 2026-04-20

## 1. Overview
The goal is to build an authentic string-based, high-level client for the `gokafk` broker, offering a developer experience inspired by KafkaJS. To accomplish this, we will update the core `gokafk` protocol to natively support string-based Topic and Group IDs rather than `uint16`, avoiding hash collisions or casting.

We will introduce a top-level `pkg/client` library that exposes `.Producer()` and `.Consumer()` semantics modeled after KafkaJS.

## 2. Protocol Changes (internal/protocol)
We will switch all topic and consumer group IDs from `uint16` to dynamically sized strings using a length-prefixed encoding format.

### Target Messages:
1. `ProduceMessage`
   - Field: `Topic string`
   - Encoding format: `[2 bytes topic len] [N bytes topic] [2 bytes key len] [M bytes key] [K bytes value]`
2. `FetchRequest`
   - Fields: `Topic string`, `GroupID string`
   - Encoding format: `[2 bytes topic len] [N bytes topic] [2 bytes group len] [L bytes group] [2 bytes member id len] [J bytes member id]`
3. `ProducerRegisterMessage`
   - Field: `Topic string`
   - Encoding format: `[2 bytes port] [2 bytes topic len] [N bytes topic]`
4. `ConsumerRegisterMessage`
   - Fields: `GroupID string`, `Topic string`
   - Encoding format: `[2 bytes port] [2 bytes group len] [L bytes group] [2 bytes topic len] [N bytes topic]`

## 3. Broker Core Updates (internal/broker)
All internal representations of topics and consumer groups in the broker will migrate from integers to strings.

- `Broker.topics`: `map[string]*Topic`
- `Broker.producers`: `map[net.Conn]string`
- `Topic.name`: `string`
- `ConsumerGroup.id`: `string`

**Partition Naming:**
Partitions will continue saving to disk at `dataDir/topicName/partition_N`, taking advantage of the string-based topic names.

## 4. Client API (pkg/client)
The new Go adapter will be placed in `pkg/client` (or similar, like `pkg/gokafk`) to abstract away broker communication entirely.

### Types and Interfaces (Conceptual)
```go
package client

import "context"

type Client interface {
    Producer() Producer
    Consumer(cfg ConsumerConfig) Consumer
}

type Message struct {
    Key   []byte
    Value []byte
}

type ProduceRequest struct {
    Topic    string
    Messages []Message
}

type Producer interface {
    Connect(ctx context.Context) error
    Send(ctx context.Context, req ProduceRequest) error
    Disconnect() error
}

type ConsumerConfig struct {
    GroupID string
}

type SubscribeRequest struct {
    Topic string
}

type Payload struct {
    Message Message
    Topic   string
}

type RunConfig struct {
    EachMessage func(ctx context.Context, payload Payload) error
}

type Consumer interface {
    Connect(ctx context.Context) error
    Subscribe(req SubscribeRequest) error
    Run(ctx context.Context, cfg RunConfig) error
    Disconnect() error
}
```

## 5. Development Steps
1. **Core Refactor**: 
   - Update `internal/protocol` structs and serialization logic. Fix affected tests.
   - Update `internal/broker` and `internal/storage` file path logic to support string-based identifiers. Fix affected tests.
2. **Client Implementation**:
   - Implement `pkg/client/client.go`, `producer.go`, and `consumer.go`.
   - Wire them to the raw underlying TCP codec.
3. **CLI Refactor (cmd/gokafk)**:
   - Update `main.go` consumer and producer subcommands to use the new `pkg/client` package instead of raw TCP loops.
