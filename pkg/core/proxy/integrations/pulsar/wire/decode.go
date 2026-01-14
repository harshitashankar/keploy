//go:build linux

package wire

import (
	"context"
	"encoding/binary"
	"fmt"

	"go.keploy.io/server/v2/pkg/models/pulsar"
	"go.uber.org/zap"
)

// DecodeContext holds state for decoding Pulsar packets
type DecodeContext struct {
	Mode            string            // MODE_RECORD or MODE_TEST
	ProducerIDs     map[string]string // Map request ID to producer ID
	ConsumerIDs     map[string]string // Map request ID to consumer ID
	RequestIDToUUID map[uint64]string // Map request ID to UUID for masking
}

// NewDecodeContext creates a new decode context
func NewDecodeContext(mode string) *DecodeContext {
	return &DecodeContext{
		Mode:            mode,
		ProducerIDs:     make(map[string]string),
		ConsumerIDs:     make(map[string]string),
		RequestIDToUUID: make(map[uint64]string),
	}
}

// DecodePacket decodes a Pulsar packet from raw bytes
// Pulsar packets are: [4-byte length][protobuf BaseCommand]
func DecodePacket(ctx context.Context, logger *zap.Logger, data []byte, decodeCtx *DecodeContext) (*pulsar.Request, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("packet too short: %d bytes", len(data))
	}

	// Parse length header
	totalLength := binary.BigEndian.Uint32(data[0:4])
	if len(data) < int(totalLength) {
		return nil, fmt.Errorf("packet incomplete: expected %d bytes, got %d", totalLength, len(data))
	}

	// Extract protobuf payload (skip 4-byte length header)
	protobufPayload := data[4:totalLength]

	// Parse protobuf to extract command type and fields
	// For now, we'll do basic parsing. Full protobuf parsing can be added later
	commandType, err := extractCommandType(protobufPayload)
	if err != nil {
		return nil, fmt.Errorf("failed to extract command type: %w", err)
	}

	// Extract topic, producer ID, etc. from protobuf
	topic, producerID, requestID := extractCommandFields(protobufPayload, commandType, logger)

	req := &pulsar.Request{
		CommandType: commandType,
		Topic:       topic,
		ProducerID:  producerID,
		RequestID:   requestID,
		Payload:     protobufPayload,
		RawPacket:   data[:totalLength],
		Metadata:    make(map[string]string),
	}

	// Store UUID mappings for masking
	if requestID > 0 {
		decodeCtx.RequestIDToUUID[requestID] = producerID
	}

	return req, nil
}

// extractCommandType extracts the command type from protobuf payload
// Protobuf field 1 (BaseCommand.type) is encoded as: 08 [varint command_type]
func extractCommandType(payload []byte) (uint32, error) {
	if len(payload) < 2 {
		return 0, fmt.Errorf("payload too short")
	}

	// Look for field tag 08 (field 1, wire type 0 = varint)
	if payload[0] != 0x08 {
		// Try to find it elsewhere in the payload (protobuf fields can be in any order)
		for i := 1; i < len(payload)-1; i++ {
			if payload[i] == 0x08 {
				// Found field tag, next byte is varint-encoded command type
				return uint32(payload[i+1]), nil
			}
		}
		return 0, fmt.Errorf("command type field not found")
	}

	// Command type is varint-encoded in the next byte(s)
	// For values < 128, it's a single byte
	if len(payload) < 2 {
		return 0, fmt.Errorf("payload too short for command type")
	}

	commandType := uint32(payload[1])
	return commandType, nil
}

// extractCommandFields extracts topic, producer ID, request ID from protobuf payload
// This is a simplified parser - full protobuf parsing would be more robust
func extractCommandFields(payload []byte, commandType uint32, logger *zap.Logger) (topic, producerID string, requestID uint64) {
	// For now, we'll extract basic fields by searching for string patterns
	// A full protobuf parser would be more accurate but is complex
	
	// Look for topic name patterns in the payload
	// Topic names typically appear as length-prefixed strings in protobuf
	// For CONNECT/PRODUCER commands, topic is often in field 2 or 3
	
	// This is a placeholder - full implementation would parse protobuf properly
	// For now, we'll extract what we can from the raw payload
	
	// Request ID is often in field 1 or 2, encoded as varint
	// Producer ID is typically a UUID string
	
	// TODO: Implement full protobuf parsing or use a protobuf library
	// For MVP, we can store the raw payload and parse it more fully later
	
	return "", "", 0
}

// DecodeResponse decodes a Pulsar response packet
func DecodeResponse(ctx context.Context, logger *zap.Logger, data []byte, decodeCtx *DecodeContext) (*pulsar.Response, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("packet too short: %d bytes", len(data))
	}

	totalLength := binary.BigEndian.Uint32(data[0:4])
	if len(data) < int(totalLength) {
		return nil, fmt.Errorf("packet incomplete: expected %d bytes, got %d", totalLength, len(data))
	}

	protobufPayload := data[4:totalLength]

	commandType, err := extractCommandType(protobufPayload)
	if err != nil {
		return nil, fmt.Errorf("failed to extract command type: %w", err)
	}

	_, producerID, requestID := extractCommandFields(protobufPayload, commandType, logger)

	resp := &pulsar.Response{
		CommandType: commandType,
		ProducerID:  producerID,
		RequestID:   requestID,
		Payload:     protobufPayload,
		RawPacket:   data[:totalLength],
		Metadata:    make(map[string]string),
	}

	return resp, nil
}
