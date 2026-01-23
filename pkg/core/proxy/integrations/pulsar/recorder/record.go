//go:build linux

package recorder

import (
	"context"
	"encoding/binary"
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

		// Read initial CONNECT packet
		initialPacket, err := wire.ReadPacketBuffer(ctx, logger, clientConn)
		if err != nil {
			utils.LogError(logger, err, "failed to read initial CONNECT packet")
			errCh <- err
			return nil
		}

		logger.Debug("Read initial CONNECT packet", zap.Int("size", len(initialPacket)))

		// Set TCP_NODELAY on both connections
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

		// Write initial CONNECT packet to destination immediately
		_, err = destConn.Write(initialPacket)
		if err != nil {
			utils.LogError(logger, err, "failed to forward CONNECT to destination")
			errCh <- err
			return nil
		}
		logger.Debug("Forwarded CONNECT packet", zap.Int("bytes", len(initialPacket)))

		// Decode initial request
		initialReq, err := wire.DecodePacket(ctx, logger, initialPacket, decodeCtx)
		if err != nil {
			utils.LogError(logger, err, "failed to decode CONNECT packet")
			errCh <- err
			return nil
		}

		initialReqTimestamp := time.Now()

		// Read CONNECTED response using ReadBytes (like HTTP does)
		// This reads until EOF/timeout, then we'll parse it as a Pulsar packet
		logger.Debug("Reading CONNECTED response immediately after CONNECT")
		connectedData, err := pUtil.ReadBytes(ctx, logger, destConn)
		if err != nil {
			if err == io.EOF {
				// EOF means connection closed, but check if we got any data first
				if len(connectedData) == 0 {
					utils.LogError(logger, err, "broker closed connection without sending CONNECTED")
					errCh <- err
					return nil
				}
				// We got some data before EOF, continue processing
				logger.Debug("Received CONNECTED before EOF", zap.Int("bytes", len(connectedData)))
			} else {
				utils.LogError(logger, err, "failed to read CONNECTED response from server")
				errCh <- err
				return nil
			}
		}

		// Parse the CONNECTED packet from the data we read
		// ReadBytes might read more than one packet, so we need to extract just the first packet
		connectedPacket, err := extractFirstPacket(connectedData, logger)
		if err != nil {
			utils.LogError(logger, err, "failed to extract CONNECTED packet from response data")
			errCh <- err
			return nil
		}
		logger.Debug("Read CONNECTED packet", zap.Int("size", len(connectedPacket)))

		// Forward CONNECTED to client immediately
		_, err = clientConn.Write(connectedPacket)
		if err != nil {
			utils.LogError(logger, err, "failed to forward CONNECTED to client")
			errCh <- err
			return nil
		}
		logger.Debug("Forwarded CONNECTED packet", zap.Int("bytes", len(connectedPacket)))

		// Decode CONNECTED response
		connectedResp, err := wire.DecodeResponse(ctx, logger, connectedPacket, decodeCtx)
		if err != nil {
			utils.LogError(logger, err, "failed to decode CONNECTED packet")
			errCh <- err
			return nil
		}

		// Record handshake mock
		logger.Debug("Recording handshake mock (CONNECT/CONNECTED)")
		recordMock(ctx, []pulsar.Request{*initialReq}, []pulsar.Response{*connectedResp}, "config", "CONNECT", "CONNECTED", mocks, initialReqTimestamp)

		// If we read more than one packet, log it (remaining data will be handled by concurrent reader)
		if len(connectedData) > len(connectedPacket) {
			logger.Debug("Received additional data after CONNECTED", zap.Int("remaining_bytes", len(connectedData)-len(connectedPacket)))
			// Note: The remaining data will be picked up by the concurrent reader if the connection stays open
		}

		// Now start concurrent reading for subsequent packets
		return handleConcurrentTraffic(ctx, logger, clientConn, destConn, decodeCtx, mocks, opts)
	})

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// handleConcurrentTraffic handles subsequent Pulsar traffic concurrently (after handshake is complete)
func handleConcurrentTraffic(ctx context.Context, logger *zap.Logger, clientConn, destConn net.Conn, decodeCtx *wire.DecodeContext, mocks chan<- *models.Mock, opts models.OutgoingOptions) error {
	clientBuffChan := make(chan []byte)
	destBuffChan := make(chan []byte)
	errChan := make(chan error, 2)

	g, ok := ctx.Value(models.ErrGroupKey).(*errgroup.Group)
	if !ok {
		return errors.New("failed to get error group from context")
	}

	logger.Debug("Starting concurrent reading from client and destination")

	// Start reading from client concurrently
	g.Go(func() error {
		defer pUtil.Recover(logger, clientConn, destConn)
		defer close(clientBuffChan)
		logger.Debug("Starting readPulsarPackets for client connection")
		readPulsarPackets(ctx, logger, clientConn, clientBuffChan, errChan, "client")
		return nil
	})

	// Start reading from destination concurrently
	g.Go(func() error {
		defer pUtil.Recover(logger, clientConn, destConn)
		defer close(destBuffChan)
		logger.Debug("Starting readPulsarPackets for destination connection")
		readPulsarPackets(ctx, logger, destConn, destBuffChan, errChan, "destination")
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
				logger.Debug("Client buffer channel closed")
				return nil
			}

			logger.Debug("Received packet from client", zap.Int("size", len(clientPacket)))

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
				logger.Debug("Destination buffer channel closed")
				return nil
			}

			logger.Debug("Received packet from destination", zap.Int("size", len(serverPacket)))

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
			logger.Debug("Received error from readPulsarPackets", zap.Error(err))
			if err == io.EOF {
				logger.Debug("EOF received, closing connection")
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
func readPulsarPackets(ctx context.Context, logger *zap.Logger, conn net.Conn, bufferChannel chan []byte, errChannel chan error, connType string) {
	logger.Debug("readPulsarPackets started", zap.String("connType", connType), zap.String("remoteAddr", conn.RemoteAddr().String()))
	defer logger.Debug("readPulsarPackets exiting", zap.String("connType", connType))

	for {
		select {
		case <-ctx.Done():
			logger.Debug("readPulsarPackets: context cancelled", zap.String("connType", connType))
			return
		default:
			if conn == nil {
				logger.Debug("connection is nil", zap.String("connType", connType))
				return
			}

			logger.Debug("readPulsarPackets: attempting to read packet", zap.String("connType", connType))
			packet, err := wire.ReadPacketBuffer(ctx, logger, conn)
			if err != nil {
				logger.Debug("readPulsarPackets: read error", zap.String("connType", connType), zap.Error(err))
				if ctx.Err() != nil {
					logger.Debug("readPulsarPackets: context error", zap.String("connType", connType), zap.Error(ctx.Err()))
					return
				}
				if err != io.EOF {
					utils.LogError(logger, err, "failed to read Pulsar packet", zap.String("connType", connType))
				} else {
					logger.Debug("readPulsarPackets: EOF received", zap.String("connType", connType))
				}
				errChannel <- err
				return
			}

			logger.Debug("readPulsarPackets: successfully read packet", zap.String("connType", connType), zap.Int("size", len(packet)))
			if ctx.Err() != nil {
				logger.Debug("readPulsarPackets: context error after read", zap.String("connType", connType))
				return
			}

			bufferChannel <- packet
		}
	}
}

// extractFirstPacket extracts the first complete Pulsar packet from the data buffer
func extractFirstPacket(data []byte, logger *zap.Logger) ([]byte, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("data too short for Pulsar packet header: got %d bytes", len(data))
	}

	// Read the length header (first 4 bytes, big-endian)
	totalLength := binary.BigEndian.Uint32(data[0:4])

	if totalLength < 4 {
		return nil, fmt.Errorf("invalid packet length: %d (must be at least 4)", totalLength)
	}

	// Check if we have the complete packet
	if len(data) < int(totalLength) {
		return nil, fmt.Errorf("incomplete packet: expected %d bytes, got %d", totalLength, len(data))
	}

	// Return the first complete packet
	return data[0:totalLength], nil
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
