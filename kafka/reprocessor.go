package kafka

import (
	"context"
	"errors"
	"maps"
	"sync"

	"github.com/nedcg/juzu"
)

// Reprocessor processes batches of messages from the DLQ topic and decides to either cycle them or process them.
type Reprocessor struct {
	mu         sync.Mutex
	groupID    string
	getGroupID func(Message) string
	dlqTopic   string
	stateStore GroupStateStore
	producer   Producer
	pipeline   []juzu.Interceptor[*BatchContext]
}

// NewReprocessor creates a new Reprocessor.
func NewReprocessor(
	groupID string,
	dlqTopic string,
	stateStore GroupStateStore,
	getGroupID func(Message) string,
	producer Producer,
	pipeline []juzu.Interceptor[*BatchContext],
) *Reprocessor {
	return &Reprocessor{
		groupID:    groupID,
		dlqTopic:   dlqTopic,
		stateStore: stateStore,
		getGroupID: getGroupID,
		producer:   producer,
		pipeline:   pipeline,
	}
}

// Process processes the DLQ batch.
func (r *Reprocessor) Process(ctx context.Context, session Session, batch []Message, maxOffsets map[string]map[int32]int64) error {
	batchCtx := &BatchContext{}
	var cycleMsgs []Message

	for _, msg := range batch {
		groupID := r.getGroupID(msg)
		if r.stateStore.IsPoisoned(groupID) {
			// Group is still poisoned; cycle the message back to the tail of the DLQ
			cycleMsgs = append(cycleMsgs, msg)
		} else {
			// Group has been unpoisoned; add to active batch for reprocessing
			batchCtx.Messages = append(batchCtx.Messages, msg)
		}
	}

	var err error
	if len(batchCtx.Messages) > 0 {
		exec := juzu.NewExecution(ctx, r.pipeline, batchCtx)
		err = exec.Execute()
	}
	if err != nil {
		return err
	}

	txProducer, ok := r.producer.(TxProducer)
	if !ok {
		return errors.New("producer does not support transactions")
	}

	r.mu.Lock()
	commitErr := r.commitReprocessing(batchCtx, txProducer, cycleMsgs, maxOffsets)
	r.mu.Unlock()
	if commitErr != nil {
		return commitErr
	}

	for topic, partitions := range maxOffsets {
		for partition, offset := range partitions {
			session.MarkOffset(topic, partition, offset+1, "")
		}
	}

	return nil
}

func (r *Reprocessor) commitReprocessing(ctx *BatchContext, txProducer TxProducer, cycleMsgs []Message, maxOffsets map[string]map[int32]int64) error {
	if err := txProducer.BeginTx(); err != nil {
		return err
	}

	var msgs []OutgoingMessage

	// 1. Send new outgoing business messages from successful runs
	msgs = append(msgs, ctx.OutgoingMessages...)

	// 2. Re-enqueue (cycle) messages for groups that are still poisoned
	for _, msg := range cycleMsgs {
		msgs = append(msgs, OutgoingMessage{
			Topic:   r.dlqTopic,
			Key:     msg.Key,
			Value:   msg.Value,
			Headers: msg.Headers,
		})
	}

	// 3. Handle messages that failed processing *again*
	for _, failed := range ctx.FailedMessages {
		groupID := r.getGroupID(failed.Message)
		_ = r.stateStore.MarkPoisoned(groupID, failed.Error)

		headers := make(map[string][]byte)
		maps.Copy(headers, failed.Message.Headers)
		headers["x-reprocess-error"] = []byte(failed.Error.Error())

		msgs = append(msgs, OutgoingMessage{
			Topic:   r.dlqTopic,
			Key:     failed.Message.Key,
			Value:   failed.Message.Value,
			Headers: headers,
		})
	}

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

	if err := txProducer.AddOffsetsToTx(mappedOffsets, r.groupID); err != nil {
		_ = txProducer.AbortTx()
		return err
	}

	if err := txProducer.CommitTx(); err != nil {
		_ = txProducer.AbortTx()
		return err
	}

	return nil
}
