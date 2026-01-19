//go:build linux

package wire

import (
	"context"
	"encoding/base64"
	"fmt"

	"go.keploy.io/server/v2/pkg/models/pulsar"
	"go.uber.org/zap"
)

// DecodeContext holds context for decoding Pulsar packets
type DecodeContext struct {
	Mode string // "record" or "test"
}

// DecodePacket decodes a Pulsar packet into a Request struct
func DecodePacket(ctx context.Context, logger *zap.Logger, packetData []byte, decodeCtx *DecodeContext) (*pulsar.Request, error) {
	if len(packetData) < 4 {
		return nil, fmt.Errorf("packet too short")
	}

	// Store raw packet as base64 for replay
	rawPacketBase64 := base64.StdEncoding.EncodeToString(packetData)

	req := &pulsar.Request{
		Header: map[string]string{
			"packet_length": fmt.Sprintf("%d", len(packetData)),
		},
		RawPacket: packetData,
		Message: map[string]interface{}{
			"raw_base64": rawPacketBase64,
		},
	}

	return req, nil
}

// DecodeResponse decodes a Pulsar response packet into a Response struct
func DecodeResponse(ctx context.Context, logger *zap.Logger, packetData []byte, decodeCtx *DecodeContext) (*pulsar.Response, error) {
	if len(packetData) < 4 {
		return nil, fmt.Errorf("packet too short")
	}

	// Store raw packet as base64 for replay
	rawPacketBase64 := base64.StdEncoding.EncodeToString(packetData)

	resp := &pulsar.Response{
		Header: map[string]string{
			"packet_length": fmt.Sprintf("%d", len(packetData)),
		},
		RawPacket: packetData,
		Message: map[string]interface{}{
			"raw_base64": rawPacketBase64,
		},
	}

	return resp, nil
}
