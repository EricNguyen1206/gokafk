package protocol

import (
	"encoding/binary"
	"errors"
)

type FetchRequest struct {
	TopicID  uint16
	GroupID  uint16
	MemberID string
}

func (m *FetchRequest) Marshal() []byte {
	buf := make([]byte, 6+len(m.MemberID))
	binary.BigEndian.PutUint16(buf[0:2], m.TopicID)
	binary.BigEndian.PutUint16(buf[2:4], m.GroupID)
	binary.BigEndian.PutUint16(buf[4:6], uint16(len(m.MemberID)))
	copy(buf[6:], m.MemberID)
	return buf
}

func (m *FetchRequest) Unmarshal(data []byte) error {
	if len(data) < 6 {
		return errors.New("fetch request data too short")
	}
	m.TopicID = binary.BigEndian.Uint16(data[0:2])
	m.GroupID = binary.BigEndian.Uint16(data[2:4])
	idLen := int(binary.BigEndian.Uint16(data[4:6]))
	if len(data) < 6+idLen {
		return errors.New("fetch request member id truncated")
	}
	m.MemberID = string(data[6 : 6+idLen])
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
