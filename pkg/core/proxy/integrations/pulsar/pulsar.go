//go:build linux

// Package pulsar provides the Pulsar integration.
package pulsar

import (
	"context"
	"encoding/binary"
	"io"
	"net"

	"go.keploy.io/server/v2/pkg/core/proxy/integrations"
	"go.keploy.io/server/v2/pkg/core/proxy/integrations/pulsar/recorder"
	"go.keploy.io/server/v2/pkg/core/proxy/integrations/pulsar/replayer"

	"go.keploy.io/server/v2/utils"

	"go.keploy.io/server/v2/pkg/models"
	"go.uber.org/zap"
)

func init() {
	integrations.Register(integrations.PULSAR, &integrations.Parsers{
		Initializer: New,
		Priority:    100,
	})
}

type Pulsar struct {
	logger *zap.Logger
}

func New(logger *zap.Logger) integrations.Integrations {
	return &Pulsar{
		logger: logger,
	}
}

// MatchType detects Pulsar protocol from packet structure
// Pulsar packets start with: [4-byte length header][protobuf-encoded BaseCommand]
// We check for the protobuf pattern indicating CONNECT or CONNECTED commands
func (p *Pulsar) MatchType(ctx context.Context, buf []byte) bool {
	// Minimum size check - need at least 4 bytes for length header
	if len(buf) < 4 {
		return false
	}

	// Read length header (first 4 bytes, big-endian)
	totalLength := binary.BigEndian.Uint32(buf[0:4])

	// Validate length - Pulsar packets typically range from ~10 bytes to a few MB
	if totalLength < 4 || totalLength > 10*1024*1024 { // 10MB max
		return false
	}

	// Ensure we have enough buffer to check the payload
	// We need at least the length header + some payload bytes
	if len(buf) < 8 {
		// Buffer too small, but length header looks valid - could be Pulsar
		// Request more data by returning false for now, but this shouldn't happen
		// as ReadInitialBuf should read enough
		return false
	}

	// Check for Pulsar protobuf pattern
	// Protobuf field tag for BaseCommand.type: 08 (field 1, wire type 0 = varint)
	// We need to search for this pattern anywhere in the first few bytes of payload
	// since protobuf fields can be in any order

	// Search for protobuf field tag 0x08 in the first 20 bytes of payload
	// This covers most Pulsar command packets
	payloadStart := 4
	searchEnd := payloadStart + 20
	if searchEnd > len(buf) {
		searchEnd = len(buf)
	}

	for i := payloadStart; i < searchEnd-1; i++ {
		// Look for protobuf field tag 0x08 (BaseCommand.type)
		if buf[i] == 0x08 {
			// Next byte should be the command type (varint-encoded)
			commandType := buf[i+1]

			// Check for valid Pulsar command types
			// CONNECT=2, CONNECTED=3, SUBSCRIBE=4, PRODUCER=5, SEND=6, etc.
			if commandType >= 0x02 && commandType <= 0x28 {
				p.logger.Debug("Detected Pulsar protocol",
					zap.Uint8("command_type", commandType),
					zap.Int("payload_offset", i),
					zap.Uint32("total_length", totalLength))
				return true
			}
		}
	}

	// Additional check: if the length header is valid and the packet structure
	// looks like Pulsar (has reasonable size), we can be more lenient
	// This helps catch cases where the command type field appears later
	if totalLength >= 8 && totalLength <= 10000 && len(buf) >= int(totalLength) {
		// Check if the entire packet structure looks valid
		// Pulsar packets should have protobuf-encoded data after the length header
		// Look for common protobuf patterns in the payload
		for i := payloadStart; i < len(buf)-1 && i < payloadStart+50; i++ {
			if buf[i] == 0x08 || buf[i] == 0x0A || buf[i] == 0x12 {
				// Found protobuf field tags - likely protobuf-encoded
				// Combined with valid length header, this is likely Pulsar
				p.logger.Debug("Detected potential Pulsar protocol based on protobuf structure",
					zap.Uint32("total_length", totalLength),
					zap.Uint8("protobuf_tag", buf[i]))
				return true
			}
		}
	}

	return false
}

func (p *Pulsar) RecordOutgoing(ctx context.Context, src net.Conn, dst net.Conn, mocks chan<- *models.Mock, opts models.OutgoingOptions) error {
	logger := p.logger.With(zap.Any("Client ConnectionID", ctx.Value(models.ClientConnectionIDKey).(string)), zap.Any("Destination ConnectionID", ctx.Value(models.DestConnectionIDKey).(string)), zap.Any("Client IP Address", src.RemoteAddr().String()))

	err := recorder.Record(ctx, logger, src, dst, mocks, opts)
	if err != nil {
		utils.LogError(logger, err, "failed to encode the pulsar message into the yaml")
		return err
	}
	return nil
}

func (p *Pulsar) MockOutgoing(ctx context.Context, src net.Conn, dstCfg *models.ConditionalDstCfg, mockDb integrations.MockMemDb, opts models.OutgoingOptions) error {
	logger := p.logger.With(zap.Any("Client ConnectionID", ctx.Value(models.ClientConnectionIDKey).(string)), zap.Any("Destination ConnectionID", ctx.Value(models.DestConnectionIDKey).(string)), zap.Any("Client IP Address", src.RemoteAddr().String()))
	err := replayer.Replay(ctx, logger, src, dstCfg, mockDb, opts)
	if err != nil && err != io.EOF {
		utils.LogError(logger, err, "failed to decode the pulsar message from the yaml")
		return err
	}
	return nil
}
