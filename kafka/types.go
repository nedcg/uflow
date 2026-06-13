package kafka

import (
	"context"
	"time"
)

// PartitionOffset represents a topic/partition/offset coordinate.
type PartitionOffset struct {
	Partition int32
	Offset    int64
}

// Session abstracts offset marking and request contexts.
type Session interface {
	MarkOffset(topic string, partition int32, offset int64, metadata string)
	Context() context.Context
}

// Producer abstracts publishing outgoing messages.
type Producer interface {
	SendMessages(msgs []OutgoingMessage) error
}

// TxProducer extends Producer to support atomic transaction lifecycles.
type TxProducer interface {
	Producer
	BeginTx() error
	CommitTx() error
	AbortTx() error
	AddOffsetsToTx(offsets map[string][]PartitionOffset, groupID string) error
}

// Message represents a pure-domain consumed Kafka message, decoupled from Sarama.
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string][]byte
	Timestamp time.Time
}

// FailedMessage holds a failed message and its processing error.
type FailedMessage struct {
	Message Message
	Catch   error
}

// OutgoingMessage represents a message to be published atomically.
type OutgoingMessage struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers map[string][]byte
}

// BatchContext is the generic context state processed by the Juzu pipeline.
type BatchContext struct {
	Messages         []Message
	FailedMessages   []FailedMessage
	OutgoingMessages []OutgoingMessage
	Payload          any
}

// Fail flags a single message in the batch as failed.
func (c *BatchContext) Fail(msg Message, err error) {
	c.FailedMessages = append(c.FailedMessages, FailedMessage{
		Message: msg,
		Catch:   err,
	})
}

// Send queues an outgoing message to be committed inside the Kafka transaction.
func (c *BatchContext) Send(topic string, key, value []byte) {
	c.OutgoingMessages = append(c.OutgoingMessages, OutgoingMessage{
		Topic: topic,
		Key:   key,
		Value: value,
	})
}

// SendWithHeaders queues an outgoing message with custom headers.
func (c *BatchContext) SendWithHeaders(topic string, key, value []byte, headers map[string][]byte) {
	c.OutgoingMessages = append(c.OutgoingMessages, OutgoingMessage{
		Topic:   topic,
		Key:     key,
		Value:   value,
		Headers: headers,
	})
}
