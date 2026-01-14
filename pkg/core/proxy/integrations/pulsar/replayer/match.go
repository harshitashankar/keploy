//go:build linux

package replayer

import (
	"context"
	"fmt"
	"regexp"

	"go.keploy.io/server/v2/pkg/core/proxy/integrations"
	"go.keploy.io/server/v2/pkg/models"
	"go.keploy.io/server/v2/pkg/models/pulsar"
	"go.uber.org/zap"
)

// matchRequest finds a matching mock for the given request
func matchRequest(ctx context.Context, logger *zap.Logger, req *pulsar.Request, mocks []*models.Mock, mockDb integrations.MockMemDb) (*models.Mock, error) {
	if req == nil {
		return nil, fmt.Errorf("request is nil")
	}

	// First try exact match
	for _, mock := range mocks {
		if len(mock.Spec.PulsarRequests) == 0 {
			continue
		}

		mockReq := mock.Spec.PulsarRequests[0]

		// Match by command type
		if mockReq.CommandType != req.CommandType {
			continue
		}

		// Match by topic (if available)
		if mockReq.Topic != "" && req.Topic != "" {
			if mockReq.Topic != req.Topic {
				continue
			}
		}

		// Match by masked UUIDs (producer ID, etc.)
		// Since UUIDs are masked during recording, we compare masked versions
		maskedMockProducerID := maskUUID(mockReq.ProducerID)
		maskedReqProducerID := maskUUID(req.ProducerID)
		if maskedMockProducerID != "" && maskedReqProducerID != "" {
			if maskedMockProducerID != maskedReqProducerID {
				continue
			}
		}

		// For CONNECT commands, also match by payload similarity
		if req.CommandType == pulsar.CommandType_CONNECT {
			// Use fuzzy matching for CONNECT payloads since they contain dynamic data
			similarity := fuzzyMatchPayload(mockReq.Payload, req.Payload)
			if similarity < 0.7 { // 70% similarity threshold
				continue
			}
		} else {
			// For other commands, exact payload match
			if len(mockReq.Payload) != len(req.Payload) {
				continue
			}
			// Compare payloads (can be enhanced with better comparison)
			payloadMatch := true
			for i := 0; i < len(mockReq.Payload) && i < len(req.Payload); i++ {
				if mockReq.Payload[i] != req.Payload[i] {
					payloadMatch = false
					break
				}
			}
			if !payloadMatch {
				continue
			}
		}

		logger.Debug("Found matching Pulsar mock", zap.String("mock_name", mock.Name), zap.Uint32("command_type", req.CommandType))
		return mock, nil
	}

	return nil, nil
}

// maskUUID masks UUIDs by replacing them with a placeholder
func maskUUID(uuid string) string {
	if uuid == "" {
		return uuid
	}
	// UUID regex pattern
	uuidPattern := regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)
	return uuidPattern.ReplaceAllString(uuid, "00000000-0000-0000-0000-000000000000")
}

// fuzzyMatchPayload performs fuzzy matching on payloads
// Returns similarity score between 0.0 and 1.0
func fuzzyMatchPayload(mockPayload, reqPayload []byte) float64 {
	if len(mockPayload) == 0 && len(reqPayload) == 0 {
		return 1.0
	}
	if len(mockPayload) == 0 || len(reqPayload) == 0 {
		return 0.0
	}

	// Simple byte-by-byte comparison with tolerance for UUID differences
	// More sophisticated algorithms (like Jaccard similarity) can be used
	matches := 0
	minLen := len(mockPayload)
	if len(reqPayload) < minLen {
		minLen = len(reqPayload)
	}

	for i := 0; i < minLen; i++ {
		if mockPayload[i] == reqPayload[i] {
			matches++
		}
	}

	if minLen == 0 {
		return 0.0
	}

	return float64(matches) / float64(minLen)
}
