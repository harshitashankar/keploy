//go:build linux

package replayer

import (
	"context"
	"io"
	"net"

	"go.keploy.io/server/v2/pkg/core/proxy/integrations"
	"go.keploy.io/server/v2/pkg/core/proxy/integrations/pulsar/wire"
	pUtil "go.keploy.io/server/v2/pkg/core/proxy/util"
	"go.keploy.io/server/v2/pkg/models"
	"go.keploy.io/server/v2/utils"
	"go.uber.org/zap"
)

// Replay replays Pulsar mocks to the client - same pattern as HTTP decodeHTTP
func Replay(ctx context.Context, logger *zap.Logger, reqBuf []byte, clientConn net.Conn, _ *models.ConditionalDstCfg, mockDb integrations.MockMemDb, opts models.OutgoingOptions) error {
	errCh := make(chan error, 1)
	go func(errCh chan error, reqBuf []byte, opts models.OutgoingOptions) {
		defer pUtil.Recover(logger, clientConn, nil)
		defer close(errCh)

		unfiltered, err := mockDb.GetUnFilteredMocks()
		if err != nil {
			utils.LogError(logger, err, "failed to get unfiltered mocks")
			errCh <- err
			return
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
			errCh <- nil
			return
		}

		logger.Info("Pulsar replay session starting",
			zap.Int("total_pulsar_mocks", len(pulsarMocks)))

		mockIndex := 0

		// Loop to handle requests - same as HTTP
		for {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			default:
			}

			// Process current request buffer
			logger.Debug("Processing Pulsar request", zap.Int("size", len(reqBuf)))

			// Find matching mock
			if mockIndex >= len(pulsarMocks) {
				logger.Debug("no more mocks available")
				errCh <- nil
				return
			}

			mock := pulsarMocks[mockIndex]
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
					if ctx.Err() != nil {
						return
					}
					utils.LogError(logger, err, "failed to write response to client")
					errCh <- err
					return
				}
				logger.Debug("Sent Pulsar response to client", zap.Int("mock_index", mockIndex-1))
			}

			// Read next request from client (keep connection alive - same as HTTP)
			reqBuf, err = pUtil.ReadBytes(ctx, logger, clientConn)
			if err != nil {
				if err == io.EOF {
					logger.Debug("Client closed connection")
					errCh <- nil
					return
				}
				logger.Debug("failed to read the request message from the client", zap.Error(err))
				errCh <- nil
				return
			}
		}
	}(errCh, reqBuf, opts)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		if err == io.EOF {
			return nil
		}
		return err
	}
}
