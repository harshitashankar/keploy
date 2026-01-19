//go:build linux

package replayer

import (
	"context"
	"io"
	"net"

	"go.keploy.io/server/v2/pkg/core/proxy/integrations"
	"go.keploy.io/server/v2/pkg/core/proxy/integrations/pulsar/wire"
	"go.keploy.io/server/v2/pkg/models"
	"go.keploy.io/server/v2/utils"
	"go.uber.org/zap"
)

// Replay replays Pulsar mocks to the client
func Replay(ctx context.Context, logger *zap.Logger, clientConn net.Conn, _ *models.ConditionalDstCfg, mockDb integrations.MockMemDb, opts models.OutgoingOptions) error {
	unfiltered, err := mockDb.GetUnFilteredMocks()
	if err != nil {
		utils.LogError(logger, err, "failed to get unfiltered mocks")
		return err
	}

	// Filter for Pulsar mocks
	var pulsarMocks []*models.Mock
	for _, mock := range unfiltered {
		if mock.Kind == models.PULSAR {
			pulsarMocks = append(pulsarMocks, mock)
		}
	}

	if len(pulsarMocks) == 0 {
		logger.Debug("no Pulsar mocks found")
		return nil
	}

	logger.Info("Pulsar replay session starting",
		zap.Int("total_pulsar_mocks", len(pulsarMocks)))

	decodeCtx := &wire.DecodeContext{
		Mode: string(models.MODE_TEST),
	}

	// Simulate initial handshake
	err = simulateInitialHandshake(ctx, logger, clientConn, pulsarMocks, decodeCtx)
	if err != nil {
		utils.LogError(logger, err, "failed to simulate initial handshake")
		return err
	}

	// Simulate command phase
	return simulateCommandPhase(ctx, logger, clientConn, pulsarMocks, decodeCtx)
}

func simulateInitialHandshake(ctx context.Context, logger *zap.Logger, clientConn net.Conn, mocks []*models.Mock, decodeCtx *wire.DecodeContext) error {
	// Find CONNECT/CONNECTED mock
	var handshakeMock *models.Mock
	for _, mock := range mocks {
		if mock.Spec.Metadata["type"] == "config" {
			handshakeMock = mock
			break
		}
	}

	if handshakeMock == nil {
		return io.EOF
	}

	// Read CONNECT from client (but don't process it)
	_, err := wire.ReadPacketBuffer(ctx, logger, clientConn)
	if err != nil {
		return err
	}

	// Send CONNECTED response
	if len(handshakeMock.Spec.PulsarResponses) > 0 {
		resp := handshakeMock.Spec.PulsarResponses[0]
		packet, err := wire.EncodePacket(&resp)
		if err != nil {
			return err
		}
		_, err = clientConn.Write(packet)
		return err
	}

	return io.EOF
}

func simulateCommandPhase(ctx context.Context, logger *zap.Logger, clientConn net.Conn, mocks []*models.Mock, decodeCtx *wire.DecodeContext) error {
	// Filter data mocks
	var dataMocks []*models.Mock
	for _, mock := range mocks {
		if mock.Spec.Metadata["type"] == "data" {
			dataMocks = append(dataMocks, mock)
		}
	}

	mockIndex := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Read command from client
		_, err := wire.ReadPacketBuffer(ctx, logger, clientConn)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		// Find matching mock
		if mockIndex >= len(dataMocks) {
			logger.Debug("no more mocks available")
			return nil
		}

		mock := dataMocks[mockIndex]
		mockIndex++

		// Send response
		if len(mock.Spec.PulsarResponses) > 0 {
			resp := mock.Spec.PulsarResponses[0]
			packet, err := wire.EncodePacket(&resp)
			if err != nil {
				utils.LogError(logger, err, "failed to encode response packet")
				continue
			}
			_, err = clientConn.Write(packet)
			if err != nil {
				return err
			}
		}
	}
}
