//go:build linux

package wire

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"go.keploy.io/server/v2/pkg/core/proxy/util"
	"go.uber.org/zap"
)

// Packet represents a Pulsar packet
type Packet struct {
	Length  uint32
	Payload []byte
}

// ReadPacketBuffer reads a Pulsar packet from the connection
// Pulsar packet format: [4-byte length header (big-endian)][protobuf payload]
func ReadPacketBuffer(ctx context.Context, logger *zap.Logger, conn net.Conn) ([]byte, error) {
	var packetBuffer []byte

	// Read the 4-byte length header
	header, err := util.ReadRequiredBytes(ctx, logger, conn, 4)
	if err != nil {
		if err == io.EOF {
			return nil, err
		}
		return packetBuffer, fmt.Errorf("failed to read packet header: %w", err)
	}

	packetBuffer = append(packetBuffer, header...)

	// Read the payload length (big-endian uint32)
	totalLength := binary.BigEndian.Uint32(header)
	if totalLength < 4 {
		return packetBuffer, nil // Empty packet or just header
	}

	// Payload length is totalLength - 4 (excluding the 4-byte header)
	payloadLength := totalLength - 4
	if payloadLength > 0 {
		payload, err := util.ReadRequiredBytes(ctx, logger, conn, int(payloadLength))
		if err != nil {
			if err == io.EOF {
				return nil, fmt.Errorf("unexpected EOF while reading payload (expected %d bytes): %w", payloadLength, err)
			}
			return packetBuffer, fmt.Errorf("failed to read packet payload: %w", err)
		}
		packetBuffer = append(packetBuffer, payload...)
	}

	return packetBuffer, nil
}

// BytesToPulsarPacket parses raw bytes into a Packet struct
func BytesToPulsarPacket(data []byte) (*Packet, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("packet too short: expected at least 4 bytes, got %d", len(data))
	}

	length := binary.BigEndian.Uint32(data[0:4])
	if len(data) < int(length) {
		return nil, fmt.Errorf("packet incomplete: expected %d bytes, got %d", length, len(data))
	}

	return &Packet{
		Length:  length,
		Payload: data[4:length],
	}, nil
}
