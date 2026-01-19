package pulsar

import (
	"time"

	"gopkg.in/yaml.v3"
)

// CommandType represents Pulsar command types
type CommandType int32

const (
	CommandType_CONNECT      CommandType = 2
	CommandType_CONNECTED    CommandType = 3
	CommandType_PRODUCER     CommandType = 5
	CommandType_SEND         CommandType = 5
	CommandType_SEND_RECEIPT CommandType = 6
	CommandType_SUBSCRIBE    CommandType = 11
	CommandType_SUCCESS      CommandType = 16
)

// Spec represents the YAML structure for Pulsar mocks
type Spec struct {
	Metadata         map[string]string `json:"metadata" yaml:"metadata"`
	Requests         []RequestYaml     `json:"requests" yaml:"requests"`
	Responses        []ResponseYaml    `json:"responses" yaml:"responses"`
	CreatedAt        int64             `json:"created" yaml:"created,omitempty"`
	ReqTimestampMock time.Time         `json:"ReqTimestampMock,omitempty"`
	ResTimestampMock time.Time         `json:"ResTimestampMock,omitempty"`
}

// RequestYaml represents a Pulsar request in YAML format
type RequestYaml struct {
	Header    map[string]string `json:"header,omitempty" yaml:"header,omitempty"`
	Message   yaml.Node         `json:"message,omitempty" yaml:"message"`
	Timestamp time.Time         `json:"timestamp,omitempty" yaml:"timestamp,omitempty"`
}

// ResponseYaml represents a Pulsar response in YAML format
type ResponseYaml struct {
	Header    map[string]string `json:"header,omitempty" yaml:"header,omitempty"`
	Message   yaml.Node         `json:"message,omitempty" yaml:"message"`
	Timestamp time.Time         `json:"timestamp,omitempty" yaml:"timestamp,omitempty"`
}

// Request represents a Pulsar request
type Request struct {
	Header    map[string]string `json:"header,omitempty" yaml:"header,omitempty"`
	Message   interface{}       `json:"message,omitempty" yaml:"message,omitempty"`
	Timestamp time.Time         `json:"timestamp,omitempty" yaml:"timestamp,omitempty"`
	RawPacket []byte            `json:"raw_packet,omitempty" yaml:"raw_packet,omitempty"` // Base64 encoded
}

// Response represents a Pulsar response
type Response struct {
	Header    map[string]string `json:"header,omitempty" yaml:"header,omitempty"`
	Message   interface{}       `json:"message,omitempty" yaml:"message,omitempty"`
	Timestamp time.Time         `json:"timestamp,omitempty" yaml:"timestamp,omitempty"`
	RawPacket []byte            `json:"raw_packet,omitempty" yaml:"raw_packet,omitempty"` // Base64 encoded
}

// BaseCommand represents the base Pulsar command structure
type BaseCommand struct {
	Type        CommandType         `json:"type" yaml:"type"`
	Connect     *ConnectCommand     `json:"connect,omitempty" yaml:"connect,omitempty"`
	Connected   *ConnectedCommand   `json:"connected,omitempty" yaml:"connected,omitempty"`
	Producer    *ProducerCommand    `json:"producer,omitempty" yaml:"producer,omitempty"`
	Send        *SendCommand        `json:"send,omitempty" yaml:"send,omitempty"`
	SendReceipt *SendReceiptCommand `json:"send_receipt,omitempty" yaml:"send_receipt,omitempty"`
	Subscribe   *SubscribeCommand   `json:"subscribe,omitempty" yaml:"subscribe,omitempty"`
	Success     *SuccessCommand     `json:"success,omitempty" yaml:"success,omitempty"`
}

// ConnectCommand represents CONNECT command
type ConnectCommand struct {
	ClientVersion   string `json:"client_version" yaml:"client_version"`
	ProtocolVersion int32  `json:"protocol_version" yaml:"protocol_version"`
}

// ConnectedCommand represents CONNECTED response
type ConnectedCommand struct {
	ServerVersion   string `json:"server_version" yaml:"server_version"`
	ProtocolVersion int32  `json:"protocol_version" yaml:"protocol_version"`
}

// ProducerCommand represents PRODUCER command
type ProducerCommand struct {
	Topic      string `json:"topic" yaml:"topic"`
	ProducerID uint64 `json:"producer_id" yaml:"producer_id"`
	RequestID  uint64 `json:"request_id" yaml:"request_id"`
}

// SendCommand represents SEND command
type SendCommand struct {
	ProducerID uint64     `json:"producer_id" yaml:"producer_id"`
	SequenceID uint64     `json:"sequence_id" yaml:"sequence_id"`
	MessageID  *MessageID `json:"message_id,omitempty" yaml:"message_id,omitempty"`
}

// SendReceiptCommand represents SEND_RECEIPT response
type SendReceiptCommand struct {
	ProducerID uint64     `json:"producer_id" yaml:"producer_id"`
	SequenceID uint64     `json:"sequence_id" yaml:"sequence_id"`
	MessageID  *MessageID `json:"message_id,omitempty" yaml:"message_id,omitempty"`
}

// SubscribeCommand represents SUBSCRIBE command
type SubscribeCommand struct {
	Topic        string `json:"topic" yaml:"topic"`
	Subscription string `json:"subscription" yaml:"subscription"`
	RequestID    uint64 `json:"request_id" yaml:"request_id"`
}

// SuccessCommand represents SUCCESS response
type SuccessCommand struct {
	RequestID uint64 `json:"request_id" yaml:"request_id"`
}

// MessageID represents a Pulsar message ID
type MessageID struct {
	LedgerID    int64 `json:"ledger_id" yaml:"ledger_id"`
	EntryID     int64 `json:"entry_id" yaml:"entry_id"`
	PartitionID int32 `json:"partition_id" yaml:"partition_id"`
	BatchIndex  int32 `json:"batch_index" yaml:"batch_index"`
}
