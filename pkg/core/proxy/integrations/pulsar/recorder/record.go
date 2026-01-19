//go:build linux

package recorder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"golang.org/x/sync/errgroup"

	"go.keploy.io/server/v2/pkg/core/proxy/integrations/pulsar/wire"
	pUtil "go.keploy.io/server/v2/pkg/core/proxy/util"
	"go.keploy.io/server/v2/pkg/models"
	"go.keploy.io/server/v2/pkg/models/pulsar"
	"go.keploy.io/server/v2/utils"
	"go.uber.org/zap"
)

// Record records Pulsar traffic between client and destination
func Record(ctx context.Context, logger *zap.Logger, clientConn, destConn net.Conn, mocks chan<- *models.Mock, opts models.OutgoingOptions) error {
	var (
		requests  []pulsar.Request
		responses []pulsar.Response
	)

	errCh := make(chan error, 1)

	g, ok := ctx.Value(models.ErrGroupKey).(*errgroup.Group)
	if !ok {
		return errors.New("failed to get error group from context")
	}

	g.Go(func() error {
		defer pUtil.Recover(logger, clientConn, destConn)
		defer close(errCh)

		decodeCtx := &wire.DecodeContext{
			Mode: string(models.MODE_RECORD),
		}

		// Handle initial handshake (CONNECT/CONNECTED)
		result, err := handleInitialHandshake(ctx, logger, clientConn, destConn, decodeCtx, opts)
		if err != nil {
			utils.LogError(logger, err, "failed to handle initial handshake")
			errCh <- err
			return nil
		}

		requests = append(requests, result.req...)
		responses = append(responses, result.resp...)

		reqTimestamp := result.reqTimestamp

		// Record handshake mock
		recordMock(ctx, requests, responses, "config", result.requestOperation, result.responseOperation, mocks, reqTimestamp)

		// Reset for data phase
		requests = []pulsar.Request{}
		responses = []pulsar.Response{}

		// Handle subsequent commands (PRODUCER, SEND, etc.)
		return handleClientCommands(ctx, logger, clientConn, destConn, decodeCtx, mocks, opts)
	})

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

type handshakeResult struct {
	req               []pulsar.Request
	resp              []pulsar.Response
	reqTimestamp      time.Time
	requestOperation  string
	responseOperation string
}

func handleInitialHandshake(ctx context.Context, logger *zap.Logger, clientConn, destConn net.Conn, decodeCtx *wire.DecodeContext, opts models.OutgoingOptions) (*handshakeResult, error) {
	reqTimestamp := time.Now()

	if clientConn == nil {
		return nil, errors.New("client connection is nil")
	}
	if destConn == nil {
		return nil, errors.New("destination connection is nil")
	}

	logger.Debug("Starting Pulsar handshake",
		zap.String("clientAddr", clientConn.RemoteAddr().String()),
		zap.String("destAddr", destConn.RemoteAddr().String()))

	// Read CONNECT command from client
	clientPacket, err := wire.ReadPacketBuffer(ctx, logger, clientConn)
	if err != nil {
		utils.LogError(logger, err, "failed to read CONNECT packet from client")
		return nil, fmt.Errorf("failed to read CONNECT: %w", err)
	}

	logger.Debug("Read CONNECT packet", zap.Int("size", len(clientPacket)))

	// Forward to destination
	n, err := destConn.Write(clientPacket)
	if err != nil {
		utils.LogError(logger, err, "failed to forward CONNECT to destination")
		return nil, fmt.Errorf("failed to forward CONNECT: %w", err)
	}
	logger.Debug("Forwarded CONNECT packet", zap.Int("bytes", n))

	// Decode request
	req, err := wire.DecodePacket(ctx, logger, clientPacket, decodeCtx)
	if err != nil {
		utils.LogError(logger, err, "failed to decode CONNECT packet")
		return nil, fmt.Errorf("failed to decode CONNECT: %w", err)
	}

	requests := []pulsar.Request{*req}

	// Read CONNECTED response from server
	logger.Debug("Reading CONNECTED response from server",
		zap.String("destConnLocalAddr", destConn.LocalAddr().String()),
		zap.String("destConnRemoteAddr", destConn.RemoteAddr().String()))
	serverPacket, err := wire.ReadPacketBuffer(ctx, logger, destConn)
	if err != nil {
		utils.LogError(logger, err, "failed to read CONNECTED response from server",
			zap.String("error", err.Error()),
			zap.String("destConnLocalAddr", destConn.LocalAddr().String()),
			zap.String("destConnRemoteAddr", destConn.RemoteAddr().String()))
		return nil, fmt.Errorf("failed to read CONNECTED: %w", err)
	}

	logger.Debug("Read CONNECTED packet", zap.Int("size", len(serverPacket)))

	// Forward to client
	n, err = clientConn.Write(serverPacket)
	if err != nil {
		utils.LogError(logger, err, "failed to forward CONNECTED to client")
		return nil, fmt.Errorf("failed to forward CONNECTED: %w", err)
	}
	logger.Debug("Forwarded CONNECTED packet", zap.Int("bytes", n))

	// Decode response
	resp, err := wire.DecodeResponse(ctx, logger, serverPacket, decodeCtx)
	if err != nil {
		utils.LogError(logger, err, "failed to decode CONNECTED packet")
		return nil, fmt.Errorf("failed to decode CONNECTED: %w", err)
	}

	responses := []pulsar.Response{*resp}

	return &handshakeResult{
		req:               requests,
		resp:              responses,
		reqTimestamp:      reqTimestamp,
		requestOperation:  "CONNECT",
		responseOperation: "CONNECTED",
	}, nil
}

func handleClientCommands(ctx context.Context, logger *zap.Logger, clientConn, destConn net.Conn, decodeCtx *wire.DecodeContext, mocks chan<- *models.Mock, opts models.OutgoingOptions) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Read command from client
		clientPacket, err := wire.ReadPacketBuffer(ctx, logger, clientConn)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			utils.LogError(logger, err, "failed to read command from client")
			return err
		}

		// Forward to destination
		_, err = destConn.Write(clientPacket)
		if err != nil {
			utils.LogError(logger, err, "failed to forward command to destination")
			return err
		}

		// Decode request
		req, err := wire.DecodePacket(ctx, logger, clientPacket, decodeCtx)
		if err != nil {
			utils.LogError(logger, err, "failed to decode command packet")
			continue
		}

		reqTimestamp := time.Now()

		// Read response from server
		serverPacket, err := wire.ReadPacketBuffer(ctx, logger, destConn)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			utils.LogError(logger, err, "failed to read response from server")
			return err
		}

		// Forward to client
		_, err = clientConn.Write(serverPacket)
		if err != nil {
			utils.LogError(logger, err, "failed to forward response to client")
			return err
		}

		// Decode response
		resp, err := wire.DecodeResponse(ctx, logger, serverPacket, decodeCtx)
		if err != nil {
			utils.LogError(logger, err, "failed to decode response packet")
			continue
		}

		// Record mock
		recordMock(ctx, []pulsar.Request{*req}, []pulsar.Response{*resp}, "data", "COMMAND", "RESPONSE", mocks, reqTimestamp)
	}
}

func recordMock(ctx context.Context, requests []pulsar.Request, responses []pulsar.Response, mockType, reqOp, respOp string, mocks chan<- *models.Mock, reqTimestamp time.Time) {
	mock := &models.Mock{
		Version: models.V1Beta1,
		Kind:    models.PULSAR,
		Spec: models.MockSpec{
			Metadata: map[string]string{
				"type":              mockType,
				"requestOperation":  reqOp,
				"responseOperation": respOp,
			},
			PulsarRequests:   requests,
			PulsarResponses:  responses,
			ReqTimestampMock: reqTimestamp,
			ResTimestampMock: time.Now(),
		},
	}

	mocks <- mock
}
