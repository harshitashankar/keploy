//go:build linux

package wire

import (
	"encoding/binary"
	"fmt"
)

// ExtractRequestID extracts the request ID from a Pulsar request packet
// Pulsar protobuf format: BaseCommand contains request_id field
// We need to parse the protobuf payload to find the request_id field
func ExtractRequestID(packet []byte) (uint64, error) {
	if len(packet) < 4 {
		return 0, fmt.Errorf("packet too short")
	}

	// Skip the 4-byte length header
	payload := packet[4:]
	
	// Parse protobuf to find request_id
	// In Pulsar protobuf, request_id is typically field 1 in BaseCommand
	// We'll use a simple byte pattern search for the request_id field
	// Field tag for request_id is typically 0x08 (field 1, wire type 0 - varint)
	
	// Search for the request_id field pattern in the protobuf
	// This is a simplified approach - for production, use proper protobuf parsing
	for i := 0; i < len(payload)-8; i++ {
		// Look for field tag 0x08 (request_id field)
		if payload[i] == 0x08 {
			// Read the varint value (request_id)
			requestID, bytesRead := readVarint(payload[i+1:])
			if bytesRead > 0 {
				return requestID, nil
			}
		}
	}
	
	return 0, fmt.Errorf("request_id not found in packet")
}

// ReplaceRequestID replaces the request ID in a Pulsar response packet
func ReplaceRequestID(packet []byte, newRequestID uint64) ([]byte, error) {
	if len(packet) < 4 {
		return nil, fmt.Errorf("packet too short")
	}

	// Skip the 4-byte length header
	payload := packet[4:]
	
	// Find and replace the request_id in the protobuf payload
	newPayload := make([]byte, len(payload))
	copy(newPayload, payload)
	
	// Search for request_id field and replace it
	found := false
	for i := 0; i < len(newPayload)-8; i++ {
		if newPayload[i] == 0x08 {
			// Found request_id field tag
			_, bytesRead := readVarint(newPayload[i+1:])
			if bytesRead > 0 {
				// Replace with new request ID
				newVarint := encodeVarint(newRequestID)
				// Calculate size difference
				sizeDiff := len(newVarint) - bytesRead
				
				// Create new payload with replaced request ID
				newPayload2 := make([]byte, 0, len(newPayload)+sizeDiff)
				newPayload2 = append(newPayload2, newPayload[:i+1]...)
				newPayload2 = append(newPayload2, newVarint...)
				newPayload2 = append(newPayload2, newPayload[i+1+bytesRead:]...)
				
				newPayload = newPayload2
				found = true
				break
			}
		}
	}
	
	if !found {
		return nil, fmt.Errorf("request_id not found in response packet")
	}
	
	// Update the length header
	newLength := uint32(4 + len(newPayload))
	newPacket := make([]byte, 4+len(newPayload))
	binary.BigEndian.PutUint32(newPacket[0:4], newLength)
	copy(newPacket[4:], newPayload)
	
	return newPacket, nil
}

// readVarint reads a protobuf varint from the buffer
func readVarint(buf []byte) (uint64, int) {
	var result uint64
	var shift uint
	var bytesRead int
	
	for i := 0; i < len(buf) && i < 10; i++ {
		b := buf[i]
		result |= uint64(b&0x7F) << shift
		bytesRead++
		if b&0x80 == 0 {
			return result, bytesRead
		}
		shift += 7
	}
	
	return 0, 0
}

// encodeVarint encodes a uint64 as a protobuf varint
func encodeVarint(value uint64) []byte {
	var buf []byte
	for value >= 0x80 {
		buf = append(buf, byte(value&0x7F|0x80))
		value >>= 7
	}
	buf = append(buf, byte(value&0x7F))
	return buf
}
