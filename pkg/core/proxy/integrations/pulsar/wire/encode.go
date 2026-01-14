//go:build linux

package wire

import (
	"encoding/binary"
	"fmt"

	"go.keploy.io/server/v2/pkg/models/pulsar"
)

// EncodePacket encodes a Pulsar response into binary format
// Format: [4-byte length header][protobuf payload]
func EncodePacket(resp *pulsar.Response) ([]byte, error) {
	if resp == nil {
		return nil, fmt.Errorf("response is nil")
	}

	// If we have raw packet, use it directly
	if len(resp.RawPacket) > 0 {
		return resp.RawPacket, nil
	}

	// Otherwise, reconstruct from payload
	// Calculate total length: 4 bytes header + payload length
	totalLength := uint32(4 + len(resp.Payload))

	packet := make([]byte, totalLength)

	// Write length header (big-endian)
	binary.BigEndian.PutUint32(packet[0:4], totalLength)

	// Copy payload
	copy(packet[4:], resp.Payload)

	return packet, nil
}

// EncodeRequest encodes a Pulsar request into binary format
func EncodeRequest(req *pulsar.Request) ([]byte, error) {
	if req == nil {
		return nil, fmt.Errorf("request is nil")
	}

	// If we have raw packet, use it directly
	if len(req.RawPacket) > 0 {
		return req.RawPacket, nil
	}

	// Otherwise, reconstruct from payload
	totalLength := uint32(4 + len(req.Payload))

	packet := make([]byte, totalLength)

	// Write length header (big-endian)
	binary.BigEndian.PutUint32(packet[0:4], totalLength)

	// Copy payload
	copy(packet[4:], req.Payload)

	return packet, nil
}
