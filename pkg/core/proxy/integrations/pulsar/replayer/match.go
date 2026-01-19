//go:build linux

package replayer

import (
	"bytes"

	"go.keploy.io/server/v2/pkg/models/pulsar"
)

// matchRequest matches an incoming request with a mock
func matchRequest(req *pulsar.Request, mock *pulsar.Request) bool {
	// Simple exact match on raw packet
	if len(req.RawPacket) != len(mock.RawPacket) {
		return false
	}
	return bytes.Equal(req.RawPacket, mock.RawPacket)
}

// fuzzyMatchPayload performs fuzzy matching on Pulsar payloads
func fuzzyMatchPayload(reqData, mockData []byte) bool {
	// For now, use exact match
	// TODO: Implement fuzzy matching with UUID masking
	return bytes.Equal(reqData, mockData)
}
