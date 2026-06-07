package kafka

import "time"

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
	Error   error
}

// OutgoingMessage represents a message to be published atomically.
type OutgoingMessage struct {
	Topic string
	Key   []byte
	Value []byte
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
		Error:   err,
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
