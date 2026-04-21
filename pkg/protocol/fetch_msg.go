package protocol

import (
	"encoding/binary"
	"errors"
)

type FetchRequest struct {
	Topic    string
	Group    string
	MemberID string
}

func (m *FetchRequest) Marshal() []byte {
	tLen := uint16(len(m.Topic))
	gLen := uint16(len(m.Group))
	mLen := uint16(len(m.MemberID))

	// [2][Topic] + [2][Group] + [2][MemberID]
	bufSize := 2 + int(tLen) + 2 + int(gLen) + 2 + int(mLen)
	buf := make([]byte, bufSize)

	binary.BigEndian.PutUint16(buf[0:2], tLen)
	copy(buf[2:2+int(tLen)], m.Topic)

	offset := 2 + int(tLen)
	binary.BigEndian.PutUint16(buf[offset:offset+2], gLen)
	copy(buf[offset+2:offset+2+int(gLen)], m.Group)

	offset += 2 + int(gLen)
	binary.BigEndian.PutUint16(buf[offset:offset+2], mLen)
	copy(buf[offset+2:], m.MemberID)

	return buf
}

func (m *FetchRequest) Unmarshal(data []byte) error {
	if len(data) < 2 {
		return errors.New("fetch request data too short")
	}

	// Read Topic
	tLen := binary.BigEndian.Uint16(data[0:2])
	if len(data) < 2+int(tLen)+2 {
		return errors.New("data too short for topic/group length")
	}
	m.Topic = string(data[2 : 2+int(tLen)])

	// Read Group
	offset := 2 + int(tLen)
	gLen := binary.BigEndian.Uint16(data[offset : offset+2])
	if len(data) < offset+2+int(gLen)+2 {
		return errors.New("data too short for group/member length")
	}
	m.Group = string(data[offset+2 : offset+2+int(gLen)])

	// Read MemberID
	offset += 2 + int(gLen)
	mLen := binary.BigEndian.Uint16(data[offset : offset+2])
	if len(data) < offset+2+int(mLen) {
		return errors.New("fetch request member id truncated")
	}
	m.MemberID = string(data[offset+2 : offset+2+int(mLen)])

	return nil
}

type FetchResponse struct {
	PartitionID int32
	Offset      int64
	Data        []byte
}

func (m *FetchResponse) Marshal() []byte {
	buf := make([]byte, 12+len(m.Data))
	binary.BigEndian.PutUint32(buf[0:4], uint32(m.PartitionID))
	binary.BigEndian.PutUint64(buf[4:12], uint64(m.Offset))
	copy(buf[12:], m.Data)
	return buf
}

func (m *FetchResponse) Unmarshal(data []byte) error {
	if len(data) < 12 {
		return errors.New("fetch response data too short")
	}
	m.PartitionID = int32(binary.BigEndian.Uint32(data[0:4]))
	m.Offset = int64(binary.BigEndian.Uint64(data[4:12]))
	m.Data = make([]byte, len(data)-12)
	copy(m.Data, data[12:])
	return nil
}
