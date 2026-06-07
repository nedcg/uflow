package kafka

import (
	"fmt"

	"github.com/nedcg/juzu"
)

// PoisonFilter creates a Juzu interceptor that automatically filters and skips messages
// whose group IDs are already poisoned before any handlers execute.
func PoisonFilter(store GroupStateStore, getGroupID func(Message) string) juzu.Interceptor[*BatchContext] {
	return juzu.Enter("PoisonFilter", func(e *juzu.Execution[*BatchContext]) error {
		ctx := e.Data
		var activeMessages []Message

		for _, msg := range ctx.Messages {
			groupID := getGroupID(msg)
			if store.IsPoisoned(groupID) {
				ctx.Fail(msg, fmt.Errorf("group %s is poisoned due to previous processing failure", groupID))
			} else {
				activeMessages = append(activeMessages, msg)
			}
		}

		ctx.Messages = activeMessages
		return nil
	})
}

// MsgHandler represents an individual message handler.
type MsgHandler func(ctx *BatchContext, msg Message) error

// WrapHandler wraps a MsgHandler into a Juzu interceptor that processes messages sequentially.
// If a message group is poisoned (either from previous batches or earlier in the same batch),
// the message is skipped and routed to the DLQ. If a message fails processing, the group is
// immediately poisoned, causing all subsequent messages for that group in the batch to be skipped.
func WrapHandler(store GroupStateStore, getGroupID func(Message) string, h MsgHandler) juzu.Interceptor[*BatchContext] {
	return juzu.Enter("WrapHandler", func(e *juzu.Execution[*BatchContext]) error {
		ctx := e.Data
		var successfulMessages []Message

		for _, msg := range ctx.Messages {
			groupID := getGroupID(msg)

			// 1. Check if the group got poisoned earlier in this batch
			if store.IsPoisoned(groupID) {
				ctx.Fail(msg, fmt.Errorf("group %s is poisoned due to prior batch failure", groupID))
				continue
			}

			// 2. Process the message
			err := h(ctx, msg)
			if err != nil {
				// Poison the group immediately
				_ = store.MarkPoisoned(groupID, err)
				ctx.Fail(msg, err)
			} else {
				// Message succeeded
				successfulMessages = append(successfulMessages, msg)
			}
		}

		// Keep only the successfully processed messages in the active queue
		ctx.Messages = successfulMessages
		return nil
	})
}
