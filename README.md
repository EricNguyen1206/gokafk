# gokafk

A distributed message broker inspired by Apache Kafka, built from scratch in Go.

## Architecture

```mermaid
graph LR
    subgraph Producers
        P1["Producer A\n(Port: 8000, Topic: 1)"]
        P2["Producer B\n(Port: 8001, Topic: 2)"]
    end

    subgraph Broker["Broker :10000"]
        direction TB
        BH["handleConnection()\n(goroutine per conn)"]
        BM["processBrokerMessage()"]
        T1["Topic 1"]
        T2["Topic 2"]
        BH --> BM
        BM --> T1
        BM --> T2
    end

    subgraph Storage["Disk Storage (Append-Only Log)"]
        F1[("data/logs/topic_1.log")]
        F2[("data/logs/topic_2.log")]
    end

    subgraph Consumers
        C1["Consumer Group 1\n(GroupID: 1, offset++)"]
        C2["Consumer Group 2\n(GroupID: 2, offset++)"]
    end

    P1 -- "TCP: PCM (bytes)" --> BH
    P2 -- "TCP: PCM (bytes)" --> BH
    T1 -- "Append()" --> F1
    T2 -- "Append()" --> F2
    F1 -- "ReadOffset(n)" --> T1
    F2 -- "ReadOffset(n)" --> T2
    C1 -- "TCP: C_REG pull" --> BH
    C2 -- "TCP: C_REG pull" --> BH
    BM -- "R_C_REG (bytes)" --> C1
    BM -- "R_C_REG (bytes)" --> C2
```

## Message Flow

```mermaid
sequenceDiagram
    participant P as Producer
    participant B as Broker
    participant D as Disk (topic_N.log)
    participant C as Consumer

    P->>B: P_REG {port, topicID}
    B-->>P: R_P_REG (OK)

    loop Producer sends messages
        P->>B: PCM {bytes}
        B->>D: Append(bytes + "\n")
        B-->>P: R_PCM (OK)
    end

    C->>B: C_REG {port, groupID, topicID}
    B-->>C: R_C_REG (OK, registered)

    loop Consumer pulls messages
        C->>B: C_REG {pull request}
        B->>D: ReadOffset(currentOffset)
        D-->>B: line bytes
        B-->>C: R_C_REG {message bytes}
        Note over C: currentOffset++
        Note over C: print to stdout
    end
```

## Design Decisions

### Why Append-Only Log?
Kafka's core insight — ghi dữ liệu nối tiếp vào cuối file (append-only) thay vì cập nhật ngẫu nhiên. Lợi ích:
- **Hiệu năng ghi cao**: Sequential write nhanh hơn random write 10-100x trên ổ cứng.
- **Immutability**: Dữ liệu không bao giờ bị ghi đè, dễ debug và replay.
- **Offset-based reads**: Consumer chỉ cần lưu một số nguyên (offset) để biết đã đọc đến đâu.

### Why Pull-based Consumer?
Consumer chủ động kéo (pull) thay vì Broker đẩy (push). Lợi ích:
- **Backpressure tự nhiên**: Consumer chỉ kéo khi sẵn sàng, không bị quá tải.
- **Replay dễ dàng**: Consumer có thể reset offset về 0 để đọc lại toàn bộ log.
- **Stateless Broker**: Broker không cần theo dõi Consumer đang xử lý đến đâu.

### Why `sync.Mutex` on Broker?
Mỗi kết nối TCP chạy trong một Goroutine riêng biệt. Nhiều Producer/Consumer có thể đồng thời ghi vào `b.topics` gây race condition. `sync.Mutex` bảo vệ shared state này.

## Usage

```bash
# Terminal 1 — Run broker
go run cmd/gokafk/main.go server

# Terminal 2 — Run producer (port=8000, topicID=1)
go run cmd/gokafk/main.go producer 8000 1

# Terminal 3 — Run consumer (port=0, topicID=1, groupID=1)
go run cmd/gokafk/main.go consumer 0 1 1
```

## Project Structure

```
gokafk/
├── cmd/gokafk/main.go          # Entry point (server | producer | consumer)
├── internal/
│   ├── broker/
│   │   ├── broker.go           # TCP server, message routing, mutex
│   │   └── topic.go            # Topic state (producers, consumers, segment)
│   ├── consumer/
│   │   └── consumer.go         # Consumer TCP client, pull loop
│   ├── producer/
│   │   └── producer.go         # Producer TCP client, send loop
│   ├── message/
│   │   ├── message.go          # Binary protocol (parse & write)
│   │   ├── consumerRegisterMessage.go
│   │   └── producerRegisterMessage.go
│   └── storage/
│       ├── segment.go          # Append-only log (disk I/O)
│       └── segment_test.go     # Unit tests
└── data/logs/                  # Runtime log files (gitignored)
```
