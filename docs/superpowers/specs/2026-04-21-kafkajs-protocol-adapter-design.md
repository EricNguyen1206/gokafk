# Kafka Protocol Adapter Design Spec

## Goal
Implement a minimal subset of the Apache Kafka Wire Protocol in `gokafk` to pass the KafkaJS integration test suite (`TC-01` to `TC-06`). This will replace the custom binary protocol currently used by the broker, enabling compatibility with standard Kafka client SDKs.

## Architecture & Scope
The implementation will be housed in a new package `pkg/kafkaprotocol`. We will bypass the existing `pkg/protocol` layer entirely for external connections.

### Approach: Partial Implementation + Semi-Mocking
Writing a full 100% compliant Kafka broker from scratch is a massive undertaking. To achieve our goal in ~400-500 LOC, we will:
1. **Implement exactly** what is needed to receive requests, parse headers, and unwrap standard payloads to interface with `gokafk`'s existing `Topic` and `Segment` storage logic.
2. **Mock fully or partially** the complex control-plane APIs (like Consumer Group coordination) using static structural responses, allowing KafkaJS to proceed to the core `Produce` and `Fetch` actions.

## 1. Core Codec (Reader/Writer)
A new `Codec` struct will be created to read the Standard Kafka Frame:
`[Length int32] [ApiKey int16] [ApiVersion int16] [CorrelationId int32] [ClientId NullableString] [Body...]`

- Size: 4 bytes (int32)
- Read `Size` bytes into a buffer.
- Parse `ApiKey`, `ApiVersion`, `CorrelationId`.
- Dispatch to specific API handlers based on `ApiKey`.

## 2. API Handlers

| API Key | Action | Implementation Strategy |
| :--- | :--- | :--- |
| **ApiVersions (18)** | Client asks what versions the broker supports. | **100% Mock:** Return a hardcoded byte array declaring support for basic API keys (Produce, Fetch, Metadata, etc.). |
| **Metadata (3)** | Client requests partition info for topics. | **Semi-Mock:** Parse the requested topics. Return a payload indicating the topic has 1 partition (ID: 0) and the Leader is our single broker. |
| **Produce (0)** | Client sends messages. | **Implement:** Parse the `ProduceRequest` v0/v1 structure. Extract `Topic`, `Partition`, `Key`, and `Value`. Call existing `b.getOrCreateTopic().Append()`. Return `ProduceResponse`. |
| **FindCoordinator (10)** | Consumer searches for group manager. | **100% Mock:** Return a hardcoded response pointing to our single broker (Node ID 0). |
| **JoinGroup (11)** | Consumer joins a group. | **Semi-Mock:** Assign a random `MemberID`, designate the consumer as the group leader, and return success. |
| **SyncGroup (14)** | Consumer asks for partition assignments. | **Semi-Mock:** Hardcode the assignment to Partition 0. |
| **Fetch (1)** | Consumer reads messages. | **Implement:** Parse `FetchRequest`. Call `b.getOrCreateTopic().ReadFromPartition()`. Wrap the returned bytes into standard Kafka `RecordBatch` format and return `FetchResponse`. |

## 3. Storage Integration
The new protocol layer will map directly to the existing concepts in `internal/broker/broker.go`:
- `Produce` -> `tp.Append(key, value)`
- `Fetch` -> `tp.ReadFromPartition(partID, offset)`

The existing `handleConnection` loop in `broker.go` will be refactored to use `kafkaprotocol.NewCodec(conn)`, discarding the old `protocol.Codec`.

## 4. Primitive Types Helper
To parse the byte arrays manually, we will implement helpers for Kafka-specific standard types in `primitives.go`:
- `ReadInt8`, `ReadInt16`, `ReadInt32`, `ReadInt64`
- `ReadString` (int16 length prefix)
- `ReadArray` (int32 length prefix)
- `ReadVarInt` (Leb128, for newer versions if needed)

## Constraints & Trade-offs
- **Single Partition Limitation:** We will temporarily lock `gokafk` to 1 partition for this phase to simplify Metadata and Fetch responses, avoiding complex partition routing logic in the initial wire protocol adaptation.
- **KafkaJS Specific Versions:** We will target the exact `ApiVersion` values requested by KafkaJS (usually v0 or v1 for older clients, or v2+ for newer). We will ignore schema versioning mismatches for fields we don't care about by strategically slicing the byte buffer.
