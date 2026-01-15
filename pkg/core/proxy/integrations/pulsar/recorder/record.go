//go:build linux

// Package recorder is used to record the Pulsar traffic between the client and the server.
package recorder

import (
	"context"
	"encoding/base64"
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

// Record records Pulsar protocol traffic between client and server
func Record(ctx context.Context, logger *zap.Logger, clientConn, destConn net.Conn, mocks chan<- *models.Mock, opts models.OutgoingOptions) error {
	var (
		requests  []pulsar.Request
		responses []pulsar.Response
	)

	errCh := make(chan error, 1)

	// Get the error group from the context
	g, ok := ctx.Value(models.ErrGroupKey).(*errgroup.Group)
	if !ok {
		return errors.New("failed to get the error group from the context")
	}

	g.Go(func() error {
		defer pUtil.Recover(logger, clientConn, destConn)
		defer close(errCh)

		// Create decode context for tracking state
		decodeCtx := wire.NewDecodeContext(string(models.MODE_RECORD))

		// Handle initial connection handshake
		result, err := handleInitialHandshake(ctx, logger, clientConn, destConn, decodeCtx, opts)
		if err != nil {
			utils.LogError(logger, err, "failed to handle initial handshake")
			errCh <- err
			return nil
		}
		requests = append(requests, result.req...)
		responses = append(responses, result.resp...)

		reqTimestamp := result.reqTimestamp

		// Record the handshake mock
		recordMock(ctx, requests, responses, "config", result.requestOperation, result.responseOperation, mocks, reqTimestamp)

		// Reset for subsequent requests
		requests = []pulsar.Request{}
		responses = []pulsar.Response{}

		// Handle subsequent client-server interactions
		err = handleClientCommands(ctx, logger, clientConn, destConn, mocks, decodeCtx, opts)
		if err != nil {
			if err != io.EOF {
				utils.LogError(logger, err, "failed to handle client commands")
			}
			errCh <- err
			return nil
		}
		return nil
	})

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

// handshakeResult holds the result of initial handshake
type handshakeResult struct {
	req                []pulsar.Request
	resp               []pulsar.Response
	reqTimestamp       time.Time
	requestOperation   string
	responseOperation  string
}

// handleInitialHandshake handles the CONNECT/CONNECTED handshake
func handleInitialHandshake(ctx context.Context, logger *zap.Logger, clientConn, destConn net.Conn, decodeCtx *wire.DecodeContext, opts models.OutgoingOptions) (*handshakeResult, error) {
	reqTimestamp := time.Now()

	// Validate connections
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
	logger.Debug("Reading CONNECTED response from server")
	serverPacket, err := wire.ReadPacketBuffer(ctx, logger, destConn)
	if err != nil {
		utils.LogError(logger, err, "failed to read CONNECTED response from server",
			zap.String("error", err.Error()))
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

// handleClientCommands handles subsequent Pulsar commands (PRODUCER, SEND, etc.)
func handleClientCommands(ctx context.Context, logger *zap.Logger, clientConn, destConn net.Conn, mocks chan<- *models.Mock, decodeCtx *wire.DecodeContext, opts models.OutgoingOptions) error {
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

			// Forward to destination
			_, err = destConn.Write(clientPacket)
			if err != nil {
				utils.LogError(logger, err, "failed to forward command to destination")
				return err
			}

			// Decode request
			req, err := wire.DecodePacket(ctx, logger, clientPacket, decodeCtx)
			if err != nil {
				utils.LogError(logger, err, "failed to decode request packet")
				return err
			}

			reqTimestamp := time.Now()

			// Read response from server
			serverPacket, err := wire.ReadPacketBuffer(ctx, logger, destConn)
			if err != nil {
				if err == io.EOF {
					return nil
				}
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
				return err
			}

			// Determine operation type
			requestOp := getOperationName(req.CommandType)
			responseOp := getOperationName(resp.CommandType)

			// Record the mock
			recordMock(ctx, []pulsar.Request{*req}, []pulsar.Response{*resp}, "data", requestOp, responseOp, mocks, reqTimestamp)
		}
	}
}

// getOperationName returns the operation name for a command type
func getOperationName(commandType uint32) string {
	switch commandType {
	case pulsar.CommandType_CONNECT:
		return "CONNECT"
	case pulsar.CommandType_CONNECTED:
		return "CONNECTED"
	case pulsar.CommandType_PRODUCER:
		return "PRODUCER"
	case pulsar.CommandType_PRODUCER_SUCCESS:
		return "PRODUCER_SUCCESS"
	case pulsar.CommandType_SEND:
		return "SEND"
	case pulsar.CommandType_SEND_RECEIPT:
		return "SEND_RECEIPT"
	case pulsar.CommandType_SUBSCRIBE:
		return "SUBSCRIBE"
	case pulsar.CommandType_MESSAGE:
		return "MESSAGE"
	case pulsar.CommandType_ACK:
		return "ACK"
	default:
		return "UNKNOWN"
	}
}

// recordMock creates and sends a mock to the mocks channel
func recordMock(ctx context.Context, requests []pulsar.Request, responses []pulsar.Response, mockType, requestOperation, responseOperation string, mocks chan<- *models.Mock, reqTimestampMock time.Time) {
	// Convert requests and responses to YAML format
	requestYamls := make([]pulsar.RequestYaml, len(requests))
	for i, req := range requests {
		requestYamls[i] = pulsar.RequestYaml{
			CommandType: req.CommandType,
			Topic:       req.Topic,
			ProducerID:  maskUUID(req.ProducerID), // Mask UUIDs for better matching
			ConsumerID:  maskUUID(req.ConsumerID),
			RequestID:   req.RequestID,
			MessageID:   maskUUID(req.MessageID),
			Payload:     base64.StdEncoding.EncodeToString(req.Payload),
			Metadata:    req.Metadata,
			RawPacket:   base64.StdEncoding.EncodeToString(req.RawPacket),
		}
	}

	responseYamls := make([]pulsar.ResponseYaml, len(responses))
	for i, resp := range responses {
		responseYamls[i] = pulsar.ResponseYaml{
			CommandType: resp.CommandType,
			RequestID:   resp.RequestID,
			ProducerID:  maskUUID(resp.ProducerID), // Mask UUIDs for better matching
			ConsumerID:  maskUUID(resp.ConsumerID),
			MessageID:   maskUUID(resp.MessageID),
			Payload:     base64.StdEncoding.EncodeToString(resp.Payload),
			Metadata:    resp.Metadata,
			RawPacket:   base64.StdEncoding.EncodeToString(resp.RawPacket),
		}
	}

	meta := map[string]string{
		"type":              mockType,
		"requestOperation":  requestOperation,
		"responseOperation": responseOperation,
		"connID":            ctx.Value(models.ClientConnectionIDKey).(string),
	}

	// Convert to models format
	pulsarRequests := make([]pulsar.Request, len(requests))
	pulsarResponses := make([]pulsar.Response, len(responses))
	copy(pulsarRequests, requests)
	copy(pulsarResponses, responses)

	pulsarMock := &models.Mock{
		Version: models.GetVersion(),
		Kind:    models.PULSAR,
		Name:    mockType,
		Spec: models.MockSpec{
			Metadata:         meta,
			PulsarRequests:    pulsarRequests,
			PulsarResponses:   pulsarResponses,
			Created:           time.Now().Unix(),
			ReqTimestampMock:  reqTimestampMock,
			ResTimestampMock:  time.Now(),
		},
	}
	mocks <- pulsarMock
}

// maskUUID masks UUIDs by replacing them with a placeholder
// This helps with matching during replay when UUIDs differ
func maskUUID(uuid string) string {
	if uuid == "" {
		return uuid
	}
	// Simple UUID pattern matching - replace with placeholder
	// More sophisticated masking can be added later
	return "00000000-0000-0000-0000-000000000000"
}
