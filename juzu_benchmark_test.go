package juzu_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/nedcg/juzu"
)

type BenchState struct {
	Count int
}

func BenchmarkBaseline(b *testing.B) {
	enters := make([]func(*BenchState) error, 5)
	for i := 0; i < 5; i++ {
		enters[i] = func(s *BenchState) error {
			s.Count++
			return nil
		}
	}
	leaves := make([]func(*BenchState) error, 5)
	for i := 0; i < 5; i++ {
		leaves[i] = func(s *BenchState) error {
			s.Count++
			return nil
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		state := &BenchState{}
		// Execute enters
		for _, enter := range enters {
			_ = enter(state)
		}
		// Execute leaves
		for j := len(leaves) - 1; j >= 0; j-- {
			_ = leaves[j](state)
		}
	}
}

func BenchmarkTypedExecution(b *testing.B) {
	chain := make([]juzu.Interceptor[BenchState], 10)
	for i := 0; i < 5; i++ {
		chain[i] = juzu.Enter(fmt.Sprintf("enter-%d", i), func(e *juzu.Execution[BenchState]) error {
			e.Data.Count++
			return nil
		})
	}
	for i := 5; i < 10; i++ {
		chain[i] = juzu.Leave(fmt.Sprintf("leave-%d", i), func(e *juzu.Execution[BenchState]) error {
			e.Data.Count++
			return nil
		})
	}

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		state := BenchState{}
		exec := juzu.NewExecution(ctx, chain, state)
		_ = exec.Execute()
	}
}

func BenchmarkPooledTypedExecution(b *testing.B) {
	chain := make([]juzu.Interceptor[BenchState], 10)
	for i := 0; i < 5; i++ {
		chain[i] = juzu.Enter(fmt.Sprintf("enter-%d", i), func(e *juzu.Execution[BenchState]) error {
			e.Data.Count++
			return nil
		})
	}
	for i := 5; i < 10; i++ {
		chain[i] = juzu.Leave(fmt.Sprintf("leave-%d", i), func(e *juzu.Execution[BenchState]) error {
			e.Data.Count++
			return nil
		})
	}

	ctx := context.Background()
	pool := sync.Pool{
		New: func() any {
			return &juzu.Execution[BenchState]{}
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		state := BenchState{}
		exec := pool.Get().(*juzu.Execution[BenchState])
		exec.Reset(ctx, chain, state)
		_ = exec.Execute()
		pool.Put(exec)
	}
}

func BenchmarkMapExecution(b *testing.B) {
	chain := make([]juzu.Interceptor[map[string]any], 10)
	for i := 0; i < 5; i++ {
		chain[i] = juzu.Enter(fmt.Sprintf("enter-%d", i), func(e *juzu.Execution[map[string]any]) error {
			count, _ := e.Data["count"].(int)
			e.Data["count"] = count + 1
			return nil
		})
	}
	for i := 5; i < 10; i++ {
		chain[i] = juzu.Leave(fmt.Sprintf("leave-%d", i), func(e *juzu.Execution[map[string]any]) error {
			count, _ := e.Data["count"].(int)
			e.Data["count"] = count + 1
			return nil
		})
	}

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		state := make(map[string]any)
		exec := juzu.NewExecution(ctx, chain, state)
		_ = exec.Execute()
	}
}
