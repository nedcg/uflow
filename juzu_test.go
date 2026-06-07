package juzu_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nedcg/juzu"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestState struct {
	Trace []string
	Val   int
}

func TestExecution(t *testing.T) {
	errC := errors.New("error in C")

	tests := []struct {
		name          string
		chain         func(cancel context.CancelFunc) []juzu.Interceptor[TestState]
		setupCtx      func(parent context.Context) (context.Context, context.CancelFunc)
		expectedTrace []string
		expectedErr   error
	}{
		{
			name: "happy_path",
			chain: func(cancel context.CancelFunc) []juzu.Interceptor[TestState] {
				intA := &juzu.Func[TestState]{
					Id: "A",
					EnterFunc: func(e *juzu.Execution[TestState]) error {
						e.Data.Trace = append(e.Data.Trace, "enter_a")
						return nil
					},
					LeaveFunc: func(e *juzu.Execution[TestState]) error {
						e.Data.Trace = append(e.Data.Trace, "leave_a")
						return nil
					},
				}
				intB := juzu.Enter("B", func(e *juzu.Execution[TestState]) error {
					e.Data.Trace = append(e.Data.Trace, "enter_b")
					return nil
				})
				intC := juzu.Leave("C", func(e *juzu.Execution[TestState]) error {
					e.Data.Trace = append(e.Data.Trace, "leave_c")
					return nil
				})
				return []juzu.Interceptor[TestState]{intA, intB, intC}
			},
			expectedTrace: []string{"enter_a", "enter_b", "leave_c", "leave_a"},
		},
		{
			name: "error_recovery",
			chain: func(cancel context.CancelFunc) []juzu.Interceptor[TestState] {
				intA := &juzu.Func[TestState]{
					Id: "A",
					LeaveFunc: func(e *juzu.Execution[TestState]) error {
						e.Data.Trace = append(e.Data.Trace, "leave_a")
						return nil
					},
				}
				intB := &juzu.Func[TestState]{
					Id: "B",
					ErrorFunc: func(e *juzu.Execution[TestState], err error) error {
						e.Data.Trace = append(e.Data.Trace, "error_b_recovered")
						return nil // resolved!
					},
				}
				intC := &juzu.Func[TestState]{
					Id: "C",
					EnterFunc: func(e *juzu.Execution[TestState]) error {
						e.Data.Trace = append(e.Data.Trace, "enter_c")
						return errC
					},
				}
				return []juzu.Interceptor[TestState]{intA, intB, intC}
			},
			expectedTrace: []string{"enter_c", "error_b_recovered", "leave_a"},
		},
		{
			name: "unresolved_error",
			chain: func(cancel context.CancelFunc) []juzu.Interceptor[TestState] {
				intA := &juzu.Func[TestState]{
					Id: "A",
					ErrorFunc: func(e *juzu.Execution[TestState], err error) error {
						e.Data.Trace = append(e.Data.Trace, "error_a_propagate")
						return err
					},
				}
				intB := &juzu.Func[TestState]{
					Id: "B",
					LeaveFunc: func(e *juzu.Execution[TestState]) error {
						e.Data.Trace = append(e.Data.Trace, "leave_b")
						return nil
					},
				}
				intC := &juzu.Func[TestState]{
					Id: "C",
					EnterFunc: func(e *juzu.Execution[TestState]) error {
						e.Data.Trace = append(e.Data.Trace, "enter_c")
						return errC
					},
				}
				return []juzu.Interceptor[TestState]{intA, intB, intC}
			},
			expectedTrace: []string{"enter_c", "error_a_propagate"},
			expectedErr:   errC,
		},
		{
			name: "early_termination",
			chain: func(cancel context.CancelFunc) []juzu.Interceptor[TestState] {
				intA := &juzu.Func[TestState]{
					Id: "A",
					LeaveFunc: func(e *juzu.Execution[TestState]) error {
						e.Data.Trace = append(e.Data.Trace, "leave_a")
						return nil
					},
				}
				intB := &juzu.Func[TestState]{
					Id: "B",
					EnterFunc: func(e *juzu.Execution[TestState]) error {
						e.Data.Trace = append(e.Data.Trace, "enter_b")
						e.Terminate()
						return nil
					},
					LeaveFunc: func(e *juzu.Execution[TestState]) error {
						e.Data.Trace = append(e.Data.Trace, "leave_b")
						return nil
					},
				}
				intC := &juzu.Func[TestState]{
					Id: "C",
					EnterFunc: func(e *juzu.Execution[TestState]) error {
						e.Data.Trace = append(e.Data.Trace, "enter_c")
						return nil
					},
				}
				return []juzu.Interceptor[TestState]{intA, intB, intC}
			},
			expectedTrace: []string{"enter_b", "leave_b", "leave_a"},
		},
		{
			name: "dynamic_enqueue",
			chain: func(cancel context.CancelFunc) []juzu.Interceptor[TestState] {
				intDynamic := &juzu.Func[TestState]{
					Id: "Dynamic",
					EnterFunc: func(e *juzu.Execution[TestState]) error {
						e.Data.Trace = append(e.Data.Trace, "enter_dyn")
						return nil
					},
					LeaveFunc: func(e *juzu.Execution[TestState]) error {
						e.Data.Trace = append(e.Data.Trace, "leave_dyn")
						return nil
					},
				}
				intA := &juzu.Func[TestState]{
					Id: "A",
					EnterFunc: func(e *juzu.Execution[TestState]) error {
						e.Data.Trace = append(e.Data.Trace, "enter_a")
						e.Enqueue(intDynamic)
						return nil
					},
					LeaveFunc: func(e *juzu.Execution[TestState]) error {
						e.Data.Trace = append(e.Data.Trace, "leave_a")
						return nil
					},
				}
				return []juzu.Interceptor[TestState]{intA}
			},
			expectedTrace: []string{"enter_a", "enter_dyn", "leave_dyn", "leave_a"},
		},
		{
			name: "context_cancellation",
			setupCtx: func(parent context.Context) (context.Context, context.CancelFunc) {
				return context.WithCancel(parent)
			},
			chain: func(cancel context.CancelFunc) []juzu.Interceptor[TestState] {
				intA := &juzu.Func[TestState]{
					Id: "A",
					EnterFunc: func(e *juzu.Execution[TestState]) error {
						e.Data.Trace = append(e.Data.Trace, "enter_a")
						cancel() // cancel mid-pipeline
						return nil
					},
					ErrorFunc: func(e *juzu.Execution[TestState], err error) error {
						e.Data.Trace = append(e.Data.Trace, "error_a")
						return err
					},
				}
				intB := &juzu.Func[TestState]{
					Id: "B",
					EnterFunc: func(e *juzu.Execution[TestState]) error {
						e.Data.Trace = append(e.Data.Trace, "enter_b")
						return nil
					},
				}
				return []juzu.Interceptor[TestState]{intA, intB}
			},
			expectedTrace: []string{"enter_a", "error_a"},
			expectedErr:   context.Canceled,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var ctx context.Context
			var cancel context.CancelFunc

			// Use Go 1.24 t.Context() as parent
			if tc.setupCtx != nil {
				ctx, cancel = tc.setupCtx(t.Context())
			} else {
				ctx = t.Context()
				cancel = func() {}
			}
			defer cancel()

			chain := tc.chain(cancel)
			state := TestState{Trace: []string{}}

			exec := juzu.NewExecution(ctx, chain, state)
			err := exec.Execute()

			if tc.expectedErr != nil {
				require.ErrorIs(t, err, tc.expectedErr)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tc.expectedTrace, exec.Data.Trace)
		})
	}
}
