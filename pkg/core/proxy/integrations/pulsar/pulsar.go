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
	// Minimum size check
	if len(buf) < 8 {
		return false
	}

	// Read length header (first 4 bytes, big-endian)
	totalLength := binary.BigEndian.Uint32(buf[0:4])

	// Validate length
	if totalLength < 8 || totalLength > 10*1024*1024 { // 10MB max
		return false
	}

	// Check for Pulsar protobuf pattern: look for CONNECT command
	// Protobuf field tag for BaseCommand.type: 08 (field 1, wire type 0 = varint)
	// CONNECT value: 02 (varint-encoded 2)
	// CONNECTED value: 03 (varint-encoded 3)
	// Pattern: 08 02 or 08 03 appears early in protobuf payload (after length header)
	if len(buf) >= 10 {
		// Check bytes 4-5 for protobuf field tag + CONNECT/CONNECTED value
		if buf[4] == 0x08 {
			// Check for CONNECT (value 2) or CONNECTED (value 3)
			if buf[5] == 0x02 || buf[5] == 0x03 {
				p.logger.Debug("Detected Pulsar protocol", zap.Uint8("command_type", buf[5]))
				return true
			}
			// Also check for other common Pulsar commands (PRODUCER=5, SUBSCRIBE=4, etc.)
			// These might appear if we're detecting mid-connection
			if buf[5] >= 0x04 && buf[5] <= 0x14 { // Common command range
				// Additional validation: check if this looks like valid protobuf
				// For now, accept it as potential Pulsar
				p.logger.Debug("Detected potential Pulsar protocol", zap.Uint8("command_type", buf[5]))
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
