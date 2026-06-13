package uflow_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nedcg/uflow"
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
		chain         func(cancel context.CancelFunc) []uflow.Step[TestState]
		setupCtx      func(parent context.Context) (context.Context, context.CancelFunc)
		expectedTrace []string
		expectedErr   error
	}{
		{
			name: "happy_path",
			chain: func(cancel context.CancelFunc) []uflow.Step[TestState] {
				intA := &uflow.StepFunc[TestState]{
					Id: "A",
					InFunc: func(e *uflow.Runner[TestState]) error {
						e.Data.Trace = append(e.Data.Trace, "enter_a")
						return nil
					},
					OutFunc: func(e *uflow.Runner[TestState]) error {
						e.Data.Trace = append(e.Data.Trace, "leave_a")
						return nil
					},
				}
				intB := uflow.In("B", func(e *uflow.Runner[TestState]) error {
					e.Data.Trace = append(e.Data.Trace, "enter_b")
					return nil
				})
				intC := uflow.Out("C", func(e *uflow.Runner[TestState]) error {
					e.Data.Trace = append(e.Data.Trace, "leave_c")
					return nil
				})
				return []uflow.Step[TestState]{intA, intB, intC}
			},
			expectedTrace: []string{"enter_a", "enter_b", "leave_c", "leave_a"},
		},
		{
			name: "error_recovery",
			chain: func(cancel context.CancelFunc) []uflow.Step[TestState] {
				intA := &uflow.StepFunc[TestState]{
					Id: "A",
					OutFunc: func(e *uflow.Runner[TestState]) error {
						e.Data.Trace = append(e.Data.Trace, "leave_a")
						return nil
					},
				}
				intB := &uflow.StepFunc[TestState]{
					Id: "B",
					CatchFunc: func(e *uflow.Runner[TestState], err error) error {
						e.Data.Trace = append(e.Data.Trace, "error_b_recovered")
						return nil // resolved!
					},
				}
				intC := &uflow.StepFunc[TestState]{
					Id: "C",
					InFunc: func(e *uflow.Runner[TestState]) error {
						e.Data.Trace = append(e.Data.Trace, "enter_c")
						return errC
					},
				}
				return []uflow.Step[TestState]{intA, intB, intC}
			},
			expectedTrace: []string{"enter_c", "error_b_recovered", "leave_a"},
		},
		{
			name: "unresolved_error",
			chain: func(cancel context.CancelFunc) []uflow.Step[TestState] {
				intA := &uflow.StepFunc[TestState]{
					Id: "A",
					CatchFunc: func(e *uflow.Runner[TestState], err error) error {
						e.Data.Trace = append(e.Data.Trace, "error_a_propagate")
						return err
					},
				}
				intB := &uflow.StepFunc[TestState]{
					Id: "B",
					OutFunc: func(e *uflow.Runner[TestState]) error {
						e.Data.Trace = append(e.Data.Trace, "leave_b")
						return nil
					},
				}
				intC := &uflow.StepFunc[TestState]{
					Id: "C",
					InFunc: func(e *uflow.Runner[TestState]) error {
						e.Data.Trace = append(e.Data.Trace, "enter_c")
						return errC
					},
				}
				return []uflow.Step[TestState]{intA, intB, intC}
			},
			expectedTrace: []string{"enter_c", "error_a_propagate"},
			expectedErr:   errC,
		},
		{
			name: "early_termination",
			chain: func(cancel context.CancelFunc) []uflow.Step[TestState] {
				intA := &uflow.StepFunc[TestState]{
					Id: "A",
					OutFunc: func(e *uflow.Runner[TestState]) error {
						e.Data.Trace = append(e.Data.Trace, "leave_a")
						return nil
					},
				}
				intB := &uflow.StepFunc[TestState]{
					Id: "B",
					InFunc: func(e *uflow.Runner[TestState]) error {
						e.Data.Trace = append(e.Data.Trace, "enter_b")
						e.Terminate()
						return nil
					},
					OutFunc: func(e *uflow.Runner[TestState]) error {
						e.Data.Trace = append(e.Data.Trace, "leave_b")
						return nil
					},
				}
				intC := &uflow.StepFunc[TestState]{
					Id: "C",
					InFunc: func(e *uflow.Runner[TestState]) error {
						e.Data.Trace = append(e.Data.Trace, "enter_c")
						return nil
					},
				}
				return []uflow.Step[TestState]{intA, intB, intC}
			},
			expectedTrace: []string{"enter_b", "leave_b", "leave_a"},
		},
		{
			name: "dynamic_enqueue",
			chain: func(cancel context.CancelFunc) []uflow.Step[TestState] {
				intDynamic := &uflow.StepFunc[TestState]{
					Id: "Dynamic",
					InFunc: func(e *uflow.Runner[TestState]) error {
						e.Data.Trace = append(e.Data.Trace, "enter_dyn")
						return nil
					},
					OutFunc: func(e *uflow.Runner[TestState]) error {
						e.Data.Trace = append(e.Data.Trace, "leave_dyn")
						return nil
					},
				}
				intA := &uflow.StepFunc[TestState]{
					Id: "A",
					InFunc: func(e *uflow.Runner[TestState]) error {
						e.Data.Trace = append(e.Data.Trace, "enter_a")
						e.Enqueue(intDynamic)
						return nil
					},
					OutFunc: func(e *uflow.Runner[TestState]) error {
						e.Data.Trace = append(e.Data.Trace, "leave_a")
						return nil
					},
				}
				return []uflow.Step[TestState]{intA}
			},
			expectedTrace: []string{"enter_a", "enter_dyn", "leave_dyn", "leave_a"},
		},
		{
			name: "context_cancellation",
			setupCtx: func(parent context.Context) (context.Context, context.CancelFunc) {
				return context.WithCancel(parent)
			},
			chain: func(cancel context.CancelFunc) []uflow.Step[TestState] {
				intA := &uflow.StepFunc[TestState]{
					Id: "A",
					InFunc: func(e *uflow.Runner[TestState]) error {
						e.Data.Trace = append(e.Data.Trace, "enter_a")
						cancel() // cancel mid-pipeline
						return nil
					},
					CatchFunc: func(e *uflow.Runner[TestState], err error) error {
						e.Data.Trace = append(e.Data.Trace, "error_a")
						return err
					},
				}
				intB := &uflow.StepFunc[TestState]{
					Id: "B",
					InFunc: func(e *uflow.Runner[TestState]) error {
						e.Data.Trace = append(e.Data.Trace, "enter_b")
						return nil
					},
				}
				return []uflow.Step[TestState]{intA, intB}
			},
			expectedTrace: []string{"enter_a", "error_a"},
			expectedErr:   context.Canceled,
		},
		{
			name: "pipeline_flattening",
			chain: func(cancel context.CancelFunc) []uflow.Step[TestState] {
				intA := uflow.In("A", func(e *uflow.Runner[TestState]) error {
					e.Data.Trace = append(e.Data.Trace, "enter_a")
					return nil
				})
				intB := uflow.Out("B", func(e *uflow.Runner[TestState]) error {
					e.Data.Trace = append(e.Data.Trace, "leave_b")
					return nil
				})
				intC := uflow.In("C", func(e *uflow.Runner[TestState]) error {
					e.Data.Trace = append(e.Data.Trace, "enter_c")
					return nil
				})
				
				p1 := uflow.NewGroup(intA, intB)
				p2 := uflow.NewGroup(p1, intC) // nested pipeline

				return []uflow.Step[TestState]{p2}
			},
			expectedTrace: []string{"enter_a", "enter_c", "leave_b"}, // Onion model maintained across flatten
		},
		{
			name: "nested_enter",
			chain: func(cancel context.CancelFunc) []uflow.Step[TestState] {
				intA := uflow.In("A", func(e *uflow.Runner[TestState]) error {
					e.Data.Trace = append(e.Data.Trace, "enter_a")
					return nil
				})
				intB := uflow.Out("B", func(e *uflow.Runner[TestState]) error {
					e.Data.Trace = append(e.Data.Trace, "leave_b")
					return nil
				})
				intC := uflow.In("C", func(e *uflow.Runner[TestState]) error {
					e.Data.Trace = append(e.Data.Trace, "enter_c")
					return nil
				})
				
				nested := uflow.NestedIn("Nested", intA, intB)
				p1 := uflow.NewGroup(nested, intC)

				return []uflow.Step[TestState]{p1}
			},
			expectedTrace: []string{"enter_a", "leave_b", "enter_c"}, // Nested pipeline completes fully inside In phase
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

			exec := uflow.NewRunner(ctx, chain, state)
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
