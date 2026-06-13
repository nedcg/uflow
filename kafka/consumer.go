package kafka

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"

	"github.com/nedcg/uflow"
)

// BatchConsumer processes batches of messages using Juzu pipelines and generic interfaces.
type BatchConsumer struct {
	mu              sync.Mutex
	groupID         string
	producer        Producer
	pipeline        []uflow.Step[*BatchContext]
	stateTopic      string
	getGroupID      func(Message) string
	useTransactions bool
}

// BatchConsumerConfig holds configuration options for a BatchConsumer.
type BatchConsumerConfig struct {
	GroupID         string
	Producer        Producer
	Group        []uflow.Step[*BatchContext]
	StateTopic      string
	GetGroupID      func(Message) string
	UseTransactions bool
}

// NewBatchConsumer creates a new BatchConsumer.
// If cfg.GetGroupID is nil, it defaults to returning the Message's Key as the group ID.
func NewBatchConsumer(cfg BatchConsumerConfig) *BatchConsumer {
	getGroupID := cfg.GetGroupID
	if getGroupID == nil {
		getGroupID = func(msg Message) string {
			return string(msg.Key)
		}
	}

	return &BatchConsumer{
		groupID:         cfg.GroupID,
		producer:        cfg.Producer,
		pipeline:        cfg.Group,
		stateTopic:      cfg.StateTopic,
		getGroupID:      getGroupID,
		useTransactions: cfg.UseTransactions,
	}
}

// Process executes the pipeline on the batch and commits the results via transactions or non-transactional marking.
func (c *BatchConsumer) Process(ctx context.Context, session Session, batch []Message, maxOffsets map[string]map[int32]int64) error {
	batchCtx := &BatchContext{
		Messages: batch,
	}

	exec := uflow.NewRunner(ctx, c.pipeline, batchCtx)
	if err := exec.Execute(); err != nil {
		return err
	}

	if c.useTransactions && c.producer != nil {
		txProducer, ok := c.producer.(TxProducer)
		if !ok {
			return errors.New("producer does not support transactions")
		}
		c.mu.Lock()
		commitErr := c.commitTransaction(batchCtx, txProducer, maxOffsets)
		c.mu.Unlock()
		if commitErr != nil {
			return commitErr
		}
	} else {
		if err := c.commitNonTransactional(batchCtx, session, maxOffsets); err != nil {
			return err
		}
	}

	return nil
}

func (c *BatchConsumer) buildProducerMessages(ctx *BatchContext) []OutgoingMessage {
	var msgs []OutgoingMessage

	// 1. Business messages
	msgs = append(msgs, ctx.OutgoingMessages...)

	// 2. DLQ and state updates for failed messages
	for _, failed := range ctx.FailedMessages {
		dlqTopic := failed.Message.Topic + "-dlq"
		headers := make(map[string][]byte)
		maps.Copy(headers, failed.Message.Headers)
		headers["x-original-topic"] = []byte(failed.Message.Topic)
		headers["x-original-partition"] = fmt.Appendf(nil, "%d", failed.Message.Partition)
		headers["x-original-offset"] = fmt.Appendf(nil, "%d", failed.Message.Offset)
		headers["x-error"] = []byte(failed.Catch.Error())

		msgs = append(msgs, OutgoingMessage{
			Topic:   dlqTopic,
			Key:     failed.Message.Key,
			Value:   failed.Message.Value,
			Headers: headers,
		})

		if c.stateTopic != "" && c.getGroupID != nil {
			groupID := c.getGroupID(failed.Message)
			msgs = append(msgs, OutgoingMessage{
				Topic: c.stateTopic,
				Key:   []byte(groupID),
				Value: []byte(failed.Catch.Error()),
			})
		}
	}

	return msgs
}

func (c *BatchConsumer) commitTransaction(ctx *BatchContext, txProducer TxProducer, maxOffsets map[string]map[int32]int64) error {
	if err := txProducer.BeginTx(); err != nil {
		return err
	}

	msgs := c.buildProducerMessages(ctx)
	if len(msgs) > 0 {
		if err := txProducer.SendMessages(msgs); err != nil {
			_ = txProducer.AbortTx()
			return err
		}
	}

	// Map offsets for transaction commit
	mappedOffsets := make(map[string][]PartitionOffset)
	for topic, partitions := range maxOffsets {
		for partition, offset := range partitions {
			mappedOffsets[topic] = append(mappedOffsets[topic], PartitionOffset{
				Partition: partition,
				Offset:    offset + 1,
			})
		}
	}

	if err := txProducer.AddOffsetsToTx(mappedOffsets, c.groupID); err != nil {
		_ = txProducer.AbortTx()
		return err
	}

	if err := txProducer.CommitTx(); err != nil {
		_ = txProducer.AbortTx()
		return err
	}

	return nil
}

func (c *BatchConsumer) commitNonTransactional(ctx *BatchContext, session Session, maxOffsets map[string]map[int32]int64) error {
	msgs := c.buildProducerMessages(ctx)
	if len(msgs) > 0 && c.producer != nil {
		if err := c.producer.SendMessages(msgs); err != nil {
			return err
		}
	}

	for topic, partitions := range maxOffsets {
		for partition, offset := range partitions {
			session.MarkOffset(topic, partition, offset+1, "")
		}
	}

	return nil
}
