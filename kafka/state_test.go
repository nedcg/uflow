package kafka_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nedcg/uflow"
	"github.com/nedcg/uflow/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPoisonFilterAndCascade(t *testing.T) {
	// 1. Create a memory state store
	store := kafka.NewMemoryStateStore()

	// 2. Set up group extractor
	getGroupID := func(msg kafka.Message) string {
		return string(msg.Key)
	}

	// 3. Pre-poison GroupA
	errPoisoned := errors.New("prior failure")
	_ = store.MarkPoisoned("GroupA", errPoisoned)

	// 4. Create pipeline
	pipeline := []uflow.Step[*kafka.BatchContext]{
		kafka.PoisonFilter(store, getGroupID),
		kafka.WrapHandler(store, getGroupID, func(ctx *kafka.BatchContext, msg kafka.Message) error {
			if string(msg.Value) == "fail_me" {
				return errors.New("handler failed")
			}
			ctx.Send("output-topic", msg.Key, []byte("processed"))
			return nil
		}),
	}

	// 5. Run batch containing:
	// M1: Group A (already poisoned)
	// M2: Group B (healthy)
	// M3: Group B (will fail inside handler)
	// M4: Group B (healthy, but should be cascade-poisoned after M3 fails)
	// M5: Group C (healthy)
	batch := []kafka.Message{
		{Topic: "main", Key: []byte("GroupA"), Value: []byte("m1")},
		{Topic: "main", Key: []byte("GroupB"), Value: []byte("m2")},
		{Topic: "main", Key: []byte("GroupB"), Value: []byte("fail_me")}, // This fails
		{Topic: "main", Key: []byte("GroupB"), Value: []byte("m4")},      // Should cascade-fail
		{Topic: "main", Key: []byte("GroupC"), Value: []byte("m5")},      // Should succeed
	}

	ctx := &kafka.BatchContext{
		Messages: batch,
	}

	exec := uflow.NewRunner(context.Background(), pipeline, ctx)
	err := exec.Execute()
	require.NoError(t, err)

	// 6. Verify outputs
	// Successful messages: M2 (Group B before failure), M5 (Group C)
	assert.Len(t, ctx.Messages, 2)
	assert.Equal(t, "m2", string(ctx.Messages[0].Value))
	assert.Equal(t, "m5", string(ctx.Messages[1].Value))

	// Outgoing messages sent (only for M2 and M5)
	assert.Len(t, ctx.OutgoingMessages, 2)
	assert.Equal(t, "processed", string(ctx.OutgoingMessages[0].Value))
	assert.Equal(t, "processed", string(ctx.OutgoingMessages[1].Value))

	// Failed/Poisoned messages: M1, M3, M4
	assert.Len(t, ctx.FailedMessages, 3)

	// M1 (pre-filtered because Group A was poisoned)
	assert.Equal(t, "m1", string(ctx.FailedMessages[0].Message.Value))
	assert.Contains(t, ctx.FailedMessages[0].Catch.Error(), "is poisoned due to previous processing failure")

	// M3 (failed in handler)
	assert.Equal(t, "fail_me", string(ctx.FailedMessages[1].Message.Value))
	assert.Contains(t, ctx.FailedMessages[1].Catch.Error(), "handler failed")

	// M4 (cascade-poisoned in same batch)
	assert.Equal(t, "m4", string(ctx.FailedMessages[2].Message.Value))
	assert.Contains(t, ctx.FailedMessages[2].Catch.Error(), "poisoned due to prior batch failure")

	// 7. Verify Group B is now also marked as poisoned in the store
	assert.True(t, store.IsPoisoned("GroupB"))
	assert.True(t, store.IsPoisoned("GroupA"))
	assert.False(t, store.IsPoisoned("GroupC"))
}
