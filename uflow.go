package uflow

import (
	"context"
)

// Step represents a stage in the execution pipeline for state T.
type Step[T any] interface {
	Name() string
	In(exec *Runner[T]) error
	Out(exec *Runner[T]) error
	Catch(exec *Runner[T], err error) error
}

// Runner holds the mutable pipeline state.
type Runner[T any] struct {
	ctx   context.Context
	queue []Step[T]
	index int
	Data  T
}

// NewRunner constructs a new Runner state manager.
func NewRunner[T any](ctx context.Context, queue []Step[T], data T) *Runner[T] {
	if ctx == nil {
		ctx = context.Background()
	}
	return &Runner[T]{
		ctx:   ctx,
		queue: flatten(queue),
		Data:  data,
	}
}

// Reset resets the Runner state manager in-place for reuse.
func (e *Runner[T]) Reset(ctx context.Context, queue []Step[T], data T) {
	if ctx == nil {
		ctx = context.Background()
	}
	e.ctx = ctx
	e.queue = flatten(queue)
	e.index = 0
	e.Data = data
}

// Context returns the standard context for cancellation and deadlines.
func (e *Runner[T]) Context() context.Context {
	return e.ctx
}

// SetContext sets a new standard context.
func (e *Runner[T]) SetContext(ctx context.Context) {
	e.ctx = ctx
}

// Terminate stops the execution of the remaining queue after the current interceptor.
func (e *Runner[T]) Terminate() {
	e.queue = e.queue[:e.index]
}

// Enqueue appends new interceptors to the end of the queue.
func (e *Runner[T]) Enqueue(interceptors ...Step[T]) {
	e.queue = append(e.queue, flatten(interceptors)...)
}

// StepFunc is a convenience struct implementing the Step interface.
// Perfect for simple, ad-hoc, or function-based interceptors.
type StepFunc[T any] struct {
	Id        string
	InFunc func(*Runner[T]) error
	OutFunc func(*Runner[T]) error
	CatchFunc func(*Runner[T], error) error
}

// Name returns the name of the interceptor.
func (f *StepFunc[T]) Name() string {
	if f.Id != "" {
		return f.Id
	}
	return "AnonymousFunc"
}

// In executes the InFunc if defined.
func (f *StepFunc[T]) In(exec *Runner[T]) error {
	if f.InFunc != nil {
		return f.InFunc(exec)
	}
	return nil
}

// Out executes the OutFunc if defined.
func (f *StepFunc[T]) Out(exec *Runner[T]) error {
	if f.OutFunc != nil {
		return f.OutFunc(exec)
	}
	return nil
}

// Catch executes the CatchFunc if defined, otherwise propagates the error.
func (f *StepFunc[T]) Catch(exec *Runner[T], err error) error {
	if f.CatchFunc != nil {
		return f.CatchFunc(exec, err)
	}
	return err
}

// In creates a simple enter-only interceptor.
func In[T any](name string, fn func(*Runner[T]) error) Step[T] {
	return &StepFunc[T]{Id: name, InFunc: fn}
}

// Out creates a simple leave-only interceptor.
func Out[T any](name string, fn func(*Runner[T]) error) Step[T] {
	return &StepFunc[T]{Id: name, OutFunc: fn}
}

// Catch creates a simple error-only interceptor.
func Catch[T any](name string, fn func(*Runner[T], error) error) Step[T] {
	return &StepFunc[T]{Id: name, CatchFunc: fn}
}

// flatten recursively flattens interceptors that expose their underlying lists.
func flatten[T any](interceptors []Step[T]) []Step[T] {
	var flat []Step[T]
	for _, intc := range interceptors {
		if p, ok := intc.(interface{ Expand() []Step[T] }); ok {
			flat = append(flat, flatten(p.Expand())...)
		} else {
			flat = append(flat, intc)
		}
	}
	return flat
}

// Group represents a logical grouping of interceptors that flattens into a single queue.
type Group[T any] struct {
	interceptors []Step[T]
}

// NewGroup creates a new Group from a list of interceptors.
func NewGroup[T any](interceptors ...Step[T]) Group[T] {
	return Group[T]{
		interceptors: interceptors,
	}
}

// Name implements Step.
func (p Group[T]) Name() string { return "Group" }

// In, Out, and Catch implement Step.
// In standard execution, these are never called because the Group is flattened.
func (p Group[T]) In(exec *Runner[T]) error { return nil }
func (p Group[T]) Out(exec *Runner[T]) error { return nil }
func (p Group[T]) Catch(exec *Runner[T], err error) error { return err }

// Expand returns the underlying interceptors.
func (p Group[T]) Expand() []Step[T] {
	return p.interceptors
}

// NestedIn creates an interceptor that executes a sub-pipeline entirely within the In phase.
func NestedIn[T any](name string, interceptors ...Step[T]) Step[T] {
	return &StepFunc[T]{
		Id: name,
		InFunc: func(exec *Runner[T]) error {
			subExec := NewRunner(exec.Context(), interceptors, exec.Data)
			err := subExec.Execute()
			exec.Data = subExec.Data
			exec.SetContext(subExec.Context())
			return err
		},
	}
}

// NestedOut creates an interceptor that executes a sub-pipeline entirely within the Out phase.
func NestedOut[T any](name string, interceptors ...Step[T]) Step[T] {
	return &StepFunc[T]{
		Id: name,
		OutFunc: func(exec *Runner[T]) error {
			subExec := NewRunner(exec.Context(), interceptors, exec.Data)
			err := subExec.Execute()
			exec.Data = subExec.Data
			exec.SetContext(subExec.Context())
			return err
		},
	}
}
