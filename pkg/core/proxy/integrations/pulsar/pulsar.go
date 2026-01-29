//go:build linux

package pulsar

import (
	"context"
	"io"
	"net"

	"go.keploy.io/server/v2/pkg/core/proxy/integrations"
	"go.keploy.io/server/v2/pkg/core/proxy/integrations/pulsar/recorder"
	"go.keploy.io/server/v2/pkg/core/proxy/integrations/pulsar/replayer"
	"go.keploy.io/server/v2/pkg/models"
	"go.keploy.io/server/v2/utils"
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

func (p *Pulsar) MatchType(ctx context.Context, _ []byte) bool {
	// Check if destination port is Pulsar port (6650) from context
	destPort, ok := ctx.Value(models.DestPortKey).(uint32)
	if !ok {
		return false
	}
	return destPort == 6650
}

func (p *Pulsar) RecordOutgoing(ctx context.Context, src net.Conn, dst net.Conn, mocks chan<- *models.Mock, opts models.OutgoingOptions) error {
	logger := p.logger.With(
		zap.Any("Client ConnectionID", ctx.Value(models.ClientConnectionIDKey).(string)),
		zap.Any("Destination ConnectionID", ctx.Value(models.DestConnectionIDKey).(string)),
		zap.Any("Client IP Address", src.RemoteAddr().String()),
	)

	err := recorder.Record(ctx, logger, src, dst, mocks, opts)
	if err != nil {
		utils.LogError(logger, err, "failed to record Pulsar messages")
		return err
	}
	return nil
}

func (p *Pulsar) MockOutgoing(ctx context.Context, src net.Conn, dstCfg *models.ConditionalDstCfg, mockDb integrations.MockMemDb, opts models.OutgoingOptions) error {
	logger := p.logger.With(
		zap.Any("Client ConnectionID", ctx.Value(models.ClientConnectionIDKey).(string)),
		zap.Any("Destination ConnectionID", ctx.Value(models.DestConnectionIDKey).(string)),
		zap.Any("Client IP Address", src.RemoteAddr().String()),
	)

	err := replayer.Replay(ctx, logger, src, dstCfg, mockDb, opts)
	if err != nil && err != io.EOF {
		utils.LogError(logger, err, "failed to replay Pulsar messages")
		return err
	}
	return nil
}
