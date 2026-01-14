//go:build linux

package wire

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"

	"go.keploy.io/server/v2/pkg/core/proxy/util"
	"go.uber.org/zap"
)

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
		return packetBuffer, err
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
				return nil, err
			}
			return packetBuffer, err
		}
		packetBuffer = append(packetBuffer, payload...)
	}

	return packetBuffer, nil
}

// BytesToPulsarPacket converts a byte slice to a Pulsar packet structure
func BytesToPulsarPacket(buffer []byte) (Packet, error) {
	if len(buffer) < 4 {
		return Packet{}, errors.New("buffer is too short to be a valid Pulsar packet")
	}

	totalLength := binary.BigEndian.Uint32(buffer[0:4])
	if len(buffer) < int(totalLength) {
		return Packet{}, errors.New("buffer is shorter than indicated packet length")
	}

	payload := buffer[4:totalLength]

	return Packet{
		TotalLength: totalLength,
		Payload:     payload,
		RawPacket:   buffer[:totalLength],
	}, nil
}

// Packet represents a Pulsar protocol packet
type Packet struct {
	TotalLength uint32 // Total packet length including 4-byte header
	Payload     []byte // Protobuf-encoded BaseCommand (without length header)
	RawPacket   []byte // Complete packet including length header
}
