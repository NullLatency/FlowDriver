package transport

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Envelope represents a message exchanged between the client and server.
type Envelope struct {
	// SessionID is the unique identifier for the connection (UUID).
	SessionID string `json:"session_id"`

	// Seq is the sequence number for ordering packets.
	Seq uint64 `json:"seq"`

	// TargetAddr is used by the client on the first sequence to tell the server where to connect.
	TargetAddr string `json:"target_addr,omitempty"`

	// Payload contains the actual application data.
	Payload []byte `json:"payload,omitempty"`

	// Close implies that the sender is closing its write side of the session.
	Close bool `json:"close,omitempty"`
}

const (
	MagicByte = 0x1F
)

// MarshalBinary serializes the envelope into the custom Flow binary format.
func (e *Envelope) MarshalBinary() ([]byte, error) {
	// Pre-calculate sizing
	totalSize := 1 + 1 + len(e.SessionID) + 8 + 1 + len(e.TargetAddr) + 1 + 4 + len(e.Payload)
	buf := make([]byte, totalSize)
	
	buf[0] = MagicByte
	buf[1] = uint8(len(e.SessionID))
	offset := 2
	copy(buf[offset:], e.SessionID)
	offset += len(e.SessionID)
	
	binary.BigEndian.PutUint64(buf[offset:], e.Seq)
	offset += 8
	
	buf[offset] = uint8(len(e.TargetAddr))
	offset++
	copy(buf[offset:], e.TargetAddr)
	offset += len(e.TargetAddr)
	
	if e.Close {
		buf[offset] = 1
	} else {
		buf[offset] = 0
	}
	offset++
	
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(e.Payload)))
	offset += 4
	
	copy(buf[offset:], e.Payload)
	return buf, nil
}

// UnmarshalBinary deserializes the envelope from the custom Flow binary format.
// It returns the number of bytes read or an error.
func (e *Envelope) UnmarshalBinary(data []byte) (int, error) {
	if len(data) < 1 {
		return 0, io.ErrUnexpectedEOF
	}
	if data[0] != MagicByte {
		return 0, fmt.Errorf("invalid magic byte: expected 0x%X, got 0x%X", MagicByte, data[0])
	}
	
	offset := 1
	if len(data) < offset+1 { return 0, io.ErrUnexpectedEOF }
	sidLen := int(data[offset])
	offset++
	
	if len(data) < offset+sidLen { return 0, io.ErrUnexpectedEOF }
	e.SessionID = string(data[offset : offset+sidLen])
	offset += sidLen
	
	if len(data) < offset+8 { return 0, io.ErrUnexpectedEOF }
	e.Seq = binary.BigEndian.Uint64(data[offset:])
	offset += 8
	
	if len(data) < offset+1 { return 0, io.ErrUnexpectedEOF }
	addrLen := int(data[offset])
	offset++
	
	if len(data) < offset+addrLen { return 0, io.ErrUnexpectedEOF }
	e.TargetAddr = string(data[offset : offset+addrLen])
	offset += addrLen
	
	if len(data) < offset+1 { return 0, io.ErrUnexpectedEOF }
	e.Close = data[offset] == 1
	offset++
	
	if len(data) < offset+4 { return 0, io.ErrUnexpectedEOF }
	payloadLen := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	
	if len(data) < offset+payloadLen { return 0, io.ErrUnexpectedEOF }
	e.Payload = make([]byte, payloadLen)
	copy(e.Payload, data[offset:offset+payloadLen])
	offset += payloadLen
	
	return offset, nil
}
