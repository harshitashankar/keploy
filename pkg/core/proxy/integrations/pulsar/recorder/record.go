//go:build linux

package recorder

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
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
func Record(ctx context.Context, logger *zap.Logger, reqBuf []byte, clientConn, destConn net.Conn, mocks chan<- *models.Mock, opts models.OutgoingOptions) error {
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

		// Write initial buffer (CONNECT packet) to destination immediately - same as HTTP
		_, err := destConn.Write(reqBuf)
		if err != nil {
			utils.LogError(logger, err, "failed to write request message to the destination server")
			errCh <- err
			return nil
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		logger.Debug("This is the initial request", zap.Int("size", len(reqBuf)))
		var finalReq []byte
		finalReq = append(finalReq, reqBuf...)

		// Now handle responses and subsequent requests in a loop (same as HTTP)
		for {
			// Capture request timestamp
			reqTimestampMock := time.Now()

			// Read response from destination
			resp, err := pUtil.ReadBytes(ctx, logger, destConn)
			if err != nil {
				if err == io.EOF {
					logger.Debug("Response complete, exiting the loop.")
					// If there is any buffer left before EOF, we must send it to the client and save this as mock
					if len(resp) != 0 {
						// Write response to client
						_, err = clientConn.Write(resp)
						if err != nil {
							if ctx.Err() != nil {
								return ctx.Err()
							}
							utils.LogError(logger, err, "failed to write response message to the user client")
							errCh <- err
							return nil
						}

						// Decode and record mock
						req, decodeErr := wire.DecodePacket(ctx, logger, finalReq, decodeCtx)
						respDecoded, respDecodeErr := wire.DecodeResponse(ctx, logger, resp, decodeCtx)
						if decodeErr == nil && respDecodeErr == nil {
							recordMock(ctx, logger, []pulsar.Request{*req}, []pulsar.Response{*respDecoded}, "data", "COMMAND", "RESPONSE", mocks, reqTimestampMock)
						}
					}
					break
				}
				utils.LogError(logger, err, "failed to read the response message from the destination server")
				errCh <- err
				return nil
			}

			// Write response to client
			_, err = clientConn.Write(resp)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				utils.LogError(logger, err, "failed to write response message to the user client")
				errCh <- err
				return nil
			}

			var finalResp []byte
			finalResp = append(finalResp, resp...)
			logger.Debug("This is the initial response", zap.Int("size", len(resp)))

			// Decode request and response
			req, err := wire.DecodePacket(ctx, logger, finalReq, decodeCtx)
			if err != nil {
				utils.LogError(logger, err, "failed to decode request packet")
				// Continue even if decode fails
			}

			respDecoded, err := wire.DecodeResponse(ctx, logger, finalResp, decodeCtx)
			if err != nil {
				utils.LogError(logger, err, "failed to decode response packet")
				// Continue even if decode fails
			}

			// Record mock if both decoded successfully
			if req != nil && respDecoded != nil {
				recordMock(ctx, logger, []pulsar.Request{*req}, []pulsar.Response{*respDecoded}, "data", "COMMAND", "RESPONSE", mocks, reqTimestampMock)
			}

			// Reset for next request/response
			finalReq = []byte("")
			finalResp = []byte("")

			// Read next request from client (keep connection alive - same as HTTP)
			logger.Debug("Reading the request from the user client again from the same connection")
			finalReq, err = pUtil.ReadBytes(ctx, logger, clientConn)
			if err != nil {
				if err != io.EOF {
					logger.Debug("failed to read the request message from the user client", zap.Error(err))
					errCh <- nil
					return nil
				}
				errCh <- err
				return nil
			}

			// Write request to destination
			_, err = destConn.Write(finalReq)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				utils.LogError(logger, err, "failed to write request message to the destination server")
				errCh <- err
				return nil
			}
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

func recordMock(ctx context.Context, logger *zap.Logger, requests []pulsar.Request, responses []pulsar.Response, mockType, reqOp, respOp string, mocks chan<- *models.Mock, reqTimestamp time.Time) {
	// Log what we're recording before encoding
	logger.Info("Recording Pulsar mock",
		zap.String("mockType", mockType),
		zap.String("requestOperation", reqOp),
		zap.String("responseOperation", respOp),
		zap.Int("num_requests", len(requests)),
		zap.Int("num_responses", len(responses)),
		zap.Time("reqTimestamp", reqTimestamp),
	)

	// Log request details with raw bytes
	for i, req := range requests {
		// Log raw packet bytes as hex dump
		var hexDump string
		var firstBytes []byte
		if req.RawPacket != nil && len(req.RawPacket) > 0 {
			hexDump = hex.Dump(req.RawPacket)
			// Show first 64 bytes for preview
			if len(req.RawPacket) > 64 {
				firstBytes = req.RawPacket[:64]
			} else {
				firstBytes = req.RawPacket
			}
		}

		logger.Info("Pulsar Request details",
			zap.Int("request_index", i),
			zap.Any("header", req.Header),
			zap.Int("raw_packet_size", len(req.RawPacket)),
			zap.String("raw_packet_hex_preview", hex.EncodeToString(firstBytes)),
			zap.String("raw_packet_hex_dump", hexDump),
			zap.String("raw_packet_base64", func() string {
				if req.RawPacket != nil && len(req.RawPacket) > 0 {
					return base64.StdEncoding.EncodeToString(req.RawPacket)
				}
				return ""
			}()),
			zap.Any("message", req.Message),
		)

		// Log packet structure details
		if req.RawPacket != nil && len(req.RawPacket) >= 4 {
			// Parse length header (big-endian)
			packetLength := uint32(req.RawPacket[0])<<24 | uint32(req.RawPacket[1])<<16 | uint32(req.RawPacket[2])<<8 | uint32(req.RawPacket[3])
			logger.Debug("Request packet structure",
				zap.Int("request_index", i),
				zap.Uint32("packet_length_header", packetLength),
				zap.Int("actual_packet_size", len(req.RawPacket)),
				zap.Int("payload_size", len(req.RawPacket)-4),
			)
		}
	}

	// Log response details with raw bytes
	for i, resp := range responses {
		// Log raw packet bytes as hex dump
		var hexDump string
		var firstBytes []byte
		if resp.RawPacket != nil && len(resp.RawPacket) > 0 {
			hexDump = hex.Dump(resp.RawPacket)
			// Show first 64 bytes for preview
			if len(resp.RawPacket) > 64 {
				firstBytes = resp.RawPacket[:64]
			} else {
				firstBytes = resp.RawPacket
			}
		}

		logger.Info("Pulsar Response details",
			zap.Int("response_index", i),
			zap.Any("header", resp.Header),
			zap.Int("raw_packet_size", len(resp.RawPacket)),
			zap.String("raw_packet_hex_preview", hex.EncodeToString(firstBytes)),
			zap.String("raw_packet_hex_dump", hexDump),
			zap.String("raw_packet_base64", func() string {
				if resp.RawPacket != nil && len(resp.RawPacket) > 0 {
					return base64.StdEncoding.EncodeToString(resp.RawPacket)
				}
				return ""
			}()),
			zap.Any("message", resp.Message),
		)

		// Log packet structure details
		if resp.RawPacket != nil && len(resp.RawPacket) >= 4 {
			// Parse length header (big-endian)
			packetLength := uint32(resp.RawPacket[0])<<24 | uint32(resp.RawPacket[1])<<16 | uint32(resp.RawPacket[2])<<8 | uint32(resp.RawPacket[3])
			logger.Debug("Response packet structure",
				zap.Int("response_index", i),
				zap.Uint32("packet_length_header", packetLength),
				zap.Int("actual_packet_size", len(resp.RawPacket)),
				zap.Int("payload_size", len(resp.RawPacket)-4),
			)
		}
	}

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

	logger.Info("Pulsar mock structure created",
		zap.String("version", string(mock.Version)),
		zap.String("kind", string(mock.Kind)),
		zap.Any("metadata", mock.Spec.Metadata),
		zap.Int("pulsar_requests_count", len(mock.Spec.PulsarRequests)),
		zap.Int("pulsar_responses_count", len(mock.Spec.PulsarResponses)),
	)

	mocks <- mock
	logger.Debug("Pulsar mock sent to mocks channel")
}
