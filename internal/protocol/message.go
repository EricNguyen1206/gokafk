package protocol

// TODO [Phase 1 - Task 2]: Message types, constants, and Message struct
// See: docs/plans/2026-04-18-phase1-foundation-refactor.md — Task 2
//
// This replaces the old internal/message/ package.
//
// Implement:
// - Request type constants: TypeEcho(1), TypePReg(2), TypeCReg(3), TypeProduce(4), TypeFetch(5)
// - Response type constants: TypeEchoResp(101), TypePRegResp(102), etc.
// - Header size constants: LengthFieldSize(4), TypeFieldSize(1), CorrIDFieldSize(4), CRC32FieldSize(4)
// - MinHeaderSize = Type + CorrID + CRC32 = 9 bytes
// - Message struct { Type uint8, CorrID uint32, Payload []byte }
// - ResponseTypeFor(reqType uint8) uint8

const (
	// TODO: Request types
	TypeEcho    uint8 = 1
	TypePReg    uint8 = 2
	TypeCReg    uint8 = 3
	TypeProduce uint8 = 4
	TypeFetch   uint8 = 5

	TypeEchoResp    uint8 = 101
	TypePRegResp    uint8 = 102
	TypeCRegResp    uint8 = 103
	TypeProduceResp uint8 = 104
	TypeFetchResp   uint8 = 105

	LengthFieldSize = 4
	TypeFieldSize   = 1
	CorrIDFieldSize = 4
	CRC32FieldSize  = 4
	MinHeaderSize   = TypeFieldSize + CorrIDFieldSize + CRC32FieldSize // 9
)

// Message represents a wire-format message with type, correlation ID, and payload.
type Message struct {
	Type    uint8
	CorrID  uint32
	Payload []byte
}

// ResponseTypeFor returns the response type for a given request type.
func ResponseTypeFor(reqType uint8) uint8 {
	return reqType + 100
}
