//go:build linux

package wire

import (
	"encoding/base64"
	"fmt"

	"go.keploy.io/server/v2/pkg/models/pulsar"
)

// EncodePacket encodes a Pulsar Response into raw bytes
func EncodePacket(resp *pulsar.Response) ([]byte, error) {
	if resp.RawPacket != nil && len(resp.RawPacket) > 0 {
		return resp.RawPacket, nil
	}

	// If we have base64 encoded data in Message, decode it
	if msgMap, ok := resp.Message.(map[string]interface{}); ok {
		if rawBase64, ok := msgMap["raw_base64"].(string); ok {
			decoded, err := base64.StdEncoding.DecodeString(rawBase64)
			if err != nil {
				return nil, fmt.Errorf("failed to decode base64 packet: %w", err)
			}
			return decoded, nil
		}
	}

	return nil, fmt.Errorf("no raw packet data available in response")
}
