//go:build linux

// Package replayer is used to mock the Pulsar traffic between the client and the server.
package replayer

import (
	"context"
	"fmt"
	"io"
	"net"

	"go.keploy.io/server/v2/pkg/core/proxy/integrations"
	"go.keploy.io/server/v2/pkg/core/proxy/integrations/pulsar/wire"
	pUtil "go.keploy.io/server/v2/pkg/core/proxy/util"
	"go.keploy.io/server/v2/pkg/models"
	"go.keploy.io/server/v2/utils"
	"go.uber.org/zap"
)

// Replay replays recorded Pulsar mocks
func Replay(ctx context.Context, logger *zap.Logger, clientConn net.Conn, _ *models.ConditionalDstCfg, mockDb integrations.MockMemDb, opts models.OutgoingOptions) error {
	errCh := make(chan error, 1)

	unfiltered, err := mockDb.GetUnFilteredMocks()
	if err != nil {
		utils.LogError(logger, err, "failed to get unfiltered mocks")
		return err
	}

	var configMocks []*models.Mock
	var hasPulsarMocks bool
	var totalPulsarMocks int
	var dataMocks int

	// Get the mocks having "config" metadata and check for any Pulsar mocks
	for _, mock := range unfiltered {
		if mock.Kind == models.PULSAR {
			hasPulsarMocks = true
			totalPulsarMocks++
			if mock.Spec.Metadata["type"] == "config" {
				configMocks = append(configMocks, mock)
			} else {
				dataMocks++
			}
		}
	}

	logger.Info("Pulsar replay session starting",
		zap.Int("total_unfiltered_mocks", len(unfiltered)),
		zap.Int("total_pulsar_mocks", totalPulsarMocks),
		zap.Int("config_mocks", len(configMocks)),
		zap.Int("data_mocks", dataMocks),
		zap.Bool("has_pulsar_mocks", hasPulsarMocks))

	if !hasPulsarMocks {
		utils.LogError(logger, nil, "no pulsar mocks found")
		return nil
	}

	if len(configMocks) == 0 {
		utils.LogError(logger, nil, "no pulsar config mocks found for handshake")
		return nil
	}

	go func(errCh chan error, configMocks []*models.Mock, mockDb integrations.MockMemDb, opts models.OutgoingOptions) {
		defer pUtil.Recover(logger, clientConn, nil)
		defer close(errCh)

		// Create decode context for tracking state
		decodeCtx := wire.NewDecodeContext(string(models.MODE_TEST))

		// Simulate the initial handshake
		_, err := simulateInitialHandshake(ctx, logger, clientConn, configMocks, mockDb, decodeCtx, opts)
		if err != nil {
			utils.LogError(logger, err, "failed to simulate initial handshake")
			errCh <- err
			return
		}

		logger.Debug("Initial handshake completed successfully")

		// Simulate subsequent commands
		err = simulateCommandPhase(ctx, logger, clientConn, mockDb, decodeCtx, opts)
		if err != nil {
			if err != io.EOF {
				utils.LogError(logger, err, "failed to simulate command phase")
			}
			errCh <- err
			return
		}

	}(errCh, configMocks, mockDb, opts)

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

// handshakeReplayResult holds the result of handshake replay
type handshakeReplayResult struct {
}

// simulateInitialHandshake simulates the CONNECT/CONNECTED handshake
func simulateInitialHandshake(ctx context.Context, logger *zap.Logger, clientConn net.Conn, configMocks []*models.Mock, mockDb integrations.MockMemDb, decodeCtx *wire.DecodeContext, opts models.OutgoingOptions) (*handshakeReplayResult, error) {
	// Read CONNECT command from client
	clientPacket, err := wire.ReadPacketBuffer(ctx, logger, clientConn)
	if err != nil {
		return nil, err
	}

	// Decode request
	req, err := wire.DecodePacket(ctx, logger, clientPacket, decodeCtx)
	if err != nil {
		utils.LogError(logger, err, "failed to decode CONNECT packet")
		return nil, err
	}

	// Find matching mock
	mock, err := matchRequest(ctx, logger, req, configMocks, mockDb)
	if err != nil {
		utils.LogError(logger, err, "failed to find matching CONNECT mock")
		return nil, err
	}

	if mock == nil {
		return nil, models.ParserError{
			ParserErrorType: models.ErrMockNotFound,
			Err:             fmt.Errorf("no matching CONNECT mock found"),
		}
	}

	// Get the response from the mock
	if len(mock.Spec.PulsarResponses) == 0 {
		return nil, models.ParserError{
			ParserErrorType: models.ErrMockNotFound,
			Err:             fmt.Errorf("mock has no CONNECTED response"),
		}
	}

	resp := mock.Spec.PulsarResponses[0]

	// Encode and send response
	responsePacket, err := wire.EncodePacket(&resp)
	if err != nil {
		utils.LogError(logger, err, "failed to encode CONNECTED response")
		return nil, err
	}

	_, err = clientConn.Write(responsePacket)
	if err != nil {
		utils.LogError(logger, err, "failed to send CONNECTED response to client")
		return nil, err
	}

	// Mark mock as used
	mock.TestModeInfo.IsFiltered = false
	mockDb.UpdateUnFilteredMock(mock, mock)

	return &handshakeReplayResult{}, nil
}

// simulateCommandPhase simulates subsequent Pulsar commands
func simulateCommandPhase(ctx context.Context, logger *zap.Logger, clientConn net.Conn, mockDb integrations.MockMemDb, decodeCtx *wire.DecodeContext, opts models.OutgoingOptions) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Read command from client
			clientPacket, err := wire.ReadPacketBuffer(ctx, logger, clientConn)
			if err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}

			// Decode request
			req, err := wire.DecodePacket(ctx, logger, clientPacket, decodeCtx)
			if err != nil {
				utils.LogError(logger, err, "failed to decode request packet")
				return err
			}

			// Get all mocks
			unfiltered, err := mockDb.GetUnFilteredMocks()
			if err != nil {
				utils.LogError(logger, err, "failed to get unfiltered mocks")
				return err
			}

			// Filter Pulsar mocks
			var pulsarMocks []*models.Mock
			for _, mock := range unfiltered {
				if mock.Kind == models.PULSAR {
					pulsarMocks = append(pulsarMocks, mock)
				}
			}

			// Find matching mock
			mock, err := matchRequest(ctx, logger, req, pulsarMocks, mockDb)
			if err != nil {
				utils.LogError(logger, err, "failed to find matching mock")
				return models.ParserError{
					ParserErrorType: models.ErrMockNotFound,
					Err:             err,
				}
			}

			if mock == nil {
				utils.LogError(logger, nil, "no matching mock found for request", zap.Uint32("command_type", req.CommandType))
				return models.ParserError{
					ParserErrorType: models.ErrMockNotFound,
					Err:             fmt.Errorf("no matching mock found for command type %d", req.CommandType),
				}
			}

			// Get the response from the mock
			if len(mock.Spec.PulsarResponses) == 0 {
				utils.LogError(logger, nil, "mock has no responses", zap.String("mock_name", mock.Name))
				return models.ParserError{
					ParserErrorType: models.ErrMockNotFound,
					Err:             fmt.Errorf("mock %s has no responses", mock.Name),
				}
			}

			resp := mock.Spec.PulsarResponses[0]

			// Encode and send response
			responsePacket, err := wire.EncodePacket(&resp)
			if err != nil {
				utils.LogError(logger, err, "failed to encode response")
				return err
			}

			_, err = clientConn.Write(responsePacket)
			if err != nil {
				utils.LogError(logger, err, "failed to send response to client")
				return err
			}

			// Mark mock as used
			mock.TestModeInfo.IsFiltered = false
			mockDb.UpdateUnFilteredMock(mock, mock)
		}
	}
}
