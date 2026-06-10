package saramax

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/nedcg/juzu/kafka"
)

// SaramaSession adapts sarama.ConsumerGroupSession to kafka.Session.
type SaramaSession struct {
	session sarama.ConsumerGroupSession
}

// NewSaramaSession creates a new SaramaSession adapter.
func NewSaramaSession(session sarama.ConsumerGroupSession) *SaramaSession {
	return &SaramaSession{session: session}
}

// MarkOffset marks the offset as processed in the Sarama session.
func (s *SaramaSession) MarkOffset(topic string, partition int32, offset int64, metadata string) {
	s.session.MarkOffset(topic, partition, offset, metadata)
}

// Context returns the session context.
func (s *SaramaSession) Context() context.Context {
	return s.session.Context()
}

// SaramaProducer adapts sarama.SyncProducer to kafka.TxProducer.
type SaramaProducer struct {
	producer sarama.SyncProducer
}

// NewSaramaProducer creates a new SaramaProducer adapter.
func NewSaramaProducer(producer sarama.SyncProducer) *SaramaProducer {
	return &SaramaProducer{producer: producer}
}

// SendMessages publishes outgoing messages to Kafka using the Sarama client.
func (p *SaramaProducer) SendMessages(msgs []kafka.OutgoingMessage) error {
	saramaMsgs := make([]*sarama.ProducerMessage, len(msgs))
	for i, m := range msgs {
		var hdrs []sarama.RecordHeader
		for k, v := range m.Headers {
			hdrs = append(hdrs, sarama.RecordHeader{Key: []byte(k), Value: v})
		}
		saramaMsgs[i] = &sarama.ProducerMessage{
			Topic:   m.Topic,
			Key:     sarama.ByteEncoder(m.Key),
			Value:   sarama.ByteEncoder(m.Value),
			Headers: hdrs,
		}
	}
	return p.producer.SendMessages(saramaMsgs)
}

// BeginTx starts a new Kafka transaction.
func (p *SaramaProducer) BeginTx() error {
	return p.producer.BeginTxn()
}

// CommitTx commits the current Kafka transaction.
func (p *SaramaProducer) CommitTx() error {
	return p.producer.CommitTxn()
}

// AbortTx aborts the current Kafka transaction.
func (p *SaramaProducer) AbortTx() error {
	return p.producer.AbortTxn()
}

// AddOffsetsToTx registers consumer group offsets within the active Kafka transaction.
func (p *SaramaProducer) AddOffsetsToTx(offsets map[string][]kafka.PartitionOffset, groupID string) error {
	saramaOffsets := make(map[string][]*sarama.PartitionOffsetMetadata)
	for topic, parts := range offsets {
		for _, pOffset := range parts {
			saramaOffsets[topic] = append(saramaOffsets[topic], &sarama.PartitionOffsetMetadata{
				Partition: pOffset.Partition,
				Offset:    pOffset.Offset,
			})
		}
	}
	return p.producer.AddOffsetsToTxn(saramaOffsets, groupID)
}
