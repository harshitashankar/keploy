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

		// Handle subsequent commands (PRODUCER, SEND, etc.) using concurrent reading
		return handleClientCommandsConcurrent(ctx, logger, clientConn, destConn, decodeCtx, mocks, opts)
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

	// Set TCP_NODELAY on destination connection before reading/writing
	if tcpConn, ok := destConn.(*net.TCPConn); ok {
		if err := tcpConn.SetNoDelay(true); err != nil {
			logger.Debug("Failed to set TCP_NODELAY on destConn, continuing anyway", zap.Error(err))
		}
	}
	if tcpConn, ok := clientConn.(*net.TCPConn); ok {
		if err := tcpConn.SetNoDelay(true); err != nil {
			logger.Debug("Failed to set TCP_NODELAY on clientConn, continuing anyway", zap.Error(err))
		}
	}

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
	logger.Debug("Reading CONNECTED response from server")

	serverPacket, err := wire.ReadPacketBuffer(ctx, logger, destConn)
	if err != nil {
		utils.LogError(logger, err, "failed to read CONNECTED response from server")
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

// handleClientCommandsConcurrent handles subsequent Pulsar commands using concurrent reading
func handleClientCommandsConcurrent(ctx context.Context, logger *zap.Logger, clientConn, destConn net.Conn, decodeCtx *wire.DecodeContext, mocks chan<- *models.Mock, opts models.OutgoingOptions) error {
	clientBuffChan := make(chan []byte)
	destBuffChan := make(chan []byte)
	errChan := make(chan error, 2)

	g, ok := ctx.Value(models.ErrGroupKey).(*errgroup.Group)
	if !ok {
		return errors.New("failed to get error group from context")
	}

	// Start reading from client concurrently
	g.Go(func() error {
		defer pUtil.Recover(logger, clientConn, destConn)
		defer close(clientBuffChan)
		readPulsarPackets(ctx, logger, clientConn, clientBuffChan, errChan)
		return nil
	})

	// Start reading from destination concurrently
	g.Go(func() error {
		defer pUtil.Recover(logger, clientConn, destConn)
		defer close(destBuffChan)
		readPulsarPackets(ctx, logger, destConn, destBuffChan, errChan)
		return nil
	})

	var currentReq *pulsar.Request
	var reqTimestamp time.Time
	prevChunkWasReq := false

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case clientPacket, ok := <-clientBuffChan:
			if !ok {
				// Channel closed, connection ended
				return nil
			}

			// Forward request to destination
			_, err := destConn.Write(clientPacket)
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

			currentReq = req
			reqTimestamp = time.Now()
			prevChunkWasReq = true

			logger.Debug("Received and forwarded command from client")

		case serverPacket, ok := <-destBuffChan:
			if !ok {
				// Channel closed, connection ended
				return nil
			}

			// Forward response to client
			_, err := clientConn.Write(serverPacket)
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

			// Record mock if we have both request and response
			if prevChunkWasReq && currentReq != nil {
				recordMock(ctx, []pulsar.Request{*currentReq}, []pulsar.Response{*resp}, "data", "COMMAND", "RESPONSE", mocks, reqTimestamp)
				currentReq = nil
				prevChunkWasReq = false
			}

			logger.Debug("Received and forwarded response from server")

		case err := <-errChan:
			if err == io.EOF {
				return nil
			}
			if err != nil {
				utils.LogError(logger, err, "error reading Pulsar packets")
				return err
			}
		}
	}
}

// readPulsarPackets continuously reads Pulsar packets from a connection and sends them to a channel
func readPulsarPackets(ctx context.Context, logger *zap.Logger, conn net.Conn, bufferChannel chan []byte, errChannel chan error) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if conn == nil {
				logger.Debug("connection is nil")
				return
			}

			packet, err := wire.ReadPacketBuffer(ctx, logger, conn)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				if err != io.EOF {
					utils.LogError(logger, err, "failed to read Pulsar packet")
				}
				errChannel <- err
				return
			}

			if ctx.Err() != nil {
				return
			}

			bufferChannel <- packet
		}
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
