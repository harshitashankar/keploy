package pulsar

import (
	"time"
)

// Request represents a Pulsar protocol request
type Request struct {
	CommandType uint32            `json:"command_type" yaml:"command_type"`
	Topic       string            `json:"topic,omitempty" yaml:"topic,omitempty"`
	ProducerID  string            `json:"producer_id,omitempty" yaml:"producer_id,omitempty"`
	ConsumerID  string            `json:"consumer_id,omitempty" yaml:"consumer_id,omitempty"`
	RequestID   uint64            `json:"request_id,omitempty" yaml:"request_id,omitempty"`
	MessageID   string            `json:"message_id,omitempty" yaml:"message_id,omitempty"`
	Payload     []byte            `json:"payload,omitempty" yaml:"payload,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty" yaml:"metadata,omitempty"`
	RawPacket   []byte            `json:"raw_packet,omitempty" yaml:"raw_packet,omitempty"` // Full packet including length header
}

// Response represents a Pulsar protocol response
type Response struct {
	CommandType uint32            `json:"command_type" yaml:"command_type"`
	RequestID   uint64            `json:"request_id,omitempty" yaml:"request_id,omitempty"`
	ProducerID  string            `json:"producer_id,omitempty" yaml:"producer_id,omitempty"`
	ConsumerID  string            `json:"consumer_id,omitempty" yaml:"consumer_id,omitempty"`
	MessageID   string            `json:"message_id,omitempty" yaml:"message_id,omitempty"`
	Payload     []byte            `json:"payload,omitempty" yaml:"payload,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty" yaml:"metadata,omitempty"`
	RawPacket   []byte            `json:"raw_packet,omitempty" yaml:"raw_packet,omitempty"` // Full packet including length header
}

// RequestYaml is used for YAML serialization
type RequestYaml struct {
	CommandType uint32            `json:"command_type" yaml:"command_type"`
	Topic       string            `json:"topic,omitempty" yaml:"topic,omitempty"`
	ProducerID  string            `json:"producer_id,omitempty" yaml:"producer_id,omitempty"`
	ConsumerID  string            `json:"consumer_id,omitempty" yaml:"consumer_id,omitempty"`
	RequestID   uint64            `json:"request_id,omitempty" yaml:"request_id,omitempty"`
	MessageID   string            `json:"message_id,omitempty" yaml:"message_id,omitempty"`
	Payload     string            `json:"payload,omitempty" yaml:"payload,omitempty"` // Base64 encoded
	Metadata    map[string]string `json:"metadata,omitempty" yaml:"metadata,omitempty"`
	RawPacket   string            `json:"raw_packet,omitempty" yaml:"raw_packet,omitempty"` // Base64 encoded
}

// ResponseYaml is used for YAML serialization
type ResponseYaml struct {
	CommandType uint32            `json:"command_type" yaml:"command_type"`
	RequestID   uint64            `json:"request_id,omitempty" yaml:"request_id,omitempty"`
	ProducerID  string            `json:"producer_id,omitempty" yaml:"producer_id,omitempty"`
	ConsumerID  string            `json:"consumer_id,omitempty" yaml:"consumer_id,omitempty"`
	MessageID   string            `json:"message_id,omitempty" yaml:"message_id,omitempty"`
	Payload     string            `json:"payload,omitempty" yaml:"payload,omitempty"` // Base64 encoded
	Metadata    map[string]string `json:"metadata,omitempty" yaml:"metadata,omitempty"`
	RawPacket   string            `json:"raw_packet,omitempty" yaml:"raw_packet,omitempty"` // Base64 encoded
}

// Spec represents the Pulsar mock specification
type Spec struct {
	Metadata         map[string]string `json:"metadata" yaml:"metadata"`
	Requests         []RequestYaml    `json:"requests" yaml:"requests"`
	Responses        []ResponseYaml    `json:"responses" yaml:"responses"`
	CreatedAt        int64             `json:"created" yaml:"created,omitempty"`
	ReqTimestampMock time.Time         `json:"ReqTimestampMock,omitempty"`
	ResTimestampMock time.Time         `json:"ResTimestampMock,omitempty"`
}

// Pulsar Command Types (from Pulsar protocol)
const (
	CommandType_CONNECT              = 2
	CommandType_CONNECTED            = 3
	CommandType_SUBSCRIBE            = 4
	CommandType_PRODUCER             = 5
	CommandType_SEND                 = 6
	CommandType_SEND_RECEIPT         = 7
	CommandType_SEND_ERROR           = 8
	CommandType_MESSAGE              = 9
	CommandType_ACK                  = 10
	CommandType_FLOW                 = 11
	CommandType_UNSUBSCRIBE          = 12
	CommandType_SUCCESS              = 13
	CommandType_ERROR                = 14
	CommandType_CLOSE_PRODUCER       = 15
	CommandType_CLOSE_CONSUMER       = 16
	CommandType_PRODUCER_SUCCESS     = 17
	CommandType_PING                 = 18
	CommandType_PONG                 = 19
	CommandType_REDELIVER_UNACKNOWLEDGED_MESSAGES = 20
	CommandType_PARTITIONED_METADATA = 21
	CommandType_PARTITIONED_METADATA_RESPONSE = 22
	CommandType_LOOKUP               = 23
	CommandType_LOOKUP_RESPONSE      = 24
	CommandType_CONSUMER_STATS       = 25
	CommandType_CONSUMER_STATS_RESPONSE = 26
	CommandType_REACHED_END_OF_TOPIC = 27
	CommandType_SEEK                 = 28
	CommandType_GET_LAST_MESSAGE_ID  = 29
	CommandType_GET_LAST_MESSAGE_ID_RESPONSE = 30
	CommandType_ACTIVE_CONSUMER_CHANGE = 31
	CommandType_GET_TOPICS_OF_NAMESPACE = 32
	CommandType_GET_TOPICS_OF_NAMESPACE_RESPONSE = 33
	CommandType_GET_SCHEMA           = 34
	CommandType_GET_SCHEMA_RESPONSE  = 35
	CommandType_AUTH_CHALLENGE       = 36
	CommandType_AUTH_RESPONSE        = 37
	CommandType_ACK_RESPONSE         = 38
	CommandType_GET_OR_CREATE_SCHEMA = 39
	CommandType_GET_OR_CREATE_SCHEMA_RESPONSE = 40
)
