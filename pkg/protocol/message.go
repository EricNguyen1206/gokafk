package protocol

const (
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
