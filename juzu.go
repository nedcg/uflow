package juzu

import (
	"context"
)

// Interceptor represents a stage in the execution pipeline for state T.
type Interceptor[T any] interface {
	Name() string
	Enter(exec *Execution[T]) error
	Leave(exec *Execution[T]) error
	Error(exec *Execution[T], err error) error
}

// Execution holds the mutable pipeline state.
type Execution[T any] struct {
	ctx   context.Context
	queue []Interceptor[T]
	index int
	Data  T
}

// NewExecution constructs a new Execution state manager.
func NewExecution[T any](ctx context.Context, queue []Interceptor[T], data T) *Execution[T] {
	if ctx == nil {
		ctx = context.Background()
	}
	return &Execution[T]{
		ctx:   ctx,
		queue: queue,
		Data:  data,
	}
}

// Reset resets the Execution state manager in-place for reuse.
func (e *Execution[T]) Reset(ctx context.Context, queue []Interceptor[T], data T) {
	if ctx == nil {
		ctx = context.Background()
	}
	e.ctx = ctx
	e.queue = queue
	e.index = 0
	e.Data = data
}

// Context returns the standard context for cancellation and deadlines.
func (e *Execution[T]) Context() context.Context {
	return e.ctx
}

// SetContext sets a new standard context.
func (e *Execution[T]) SetContext(ctx context.Context) {
	e.ctx = ctx
}

// Terminate stops the execution of the remaining queue after the current interceptor.
func (e *Execution[T]) Terminate() {
	e.queue = e.queue[:e.index]
}

// Enqueue appends new interceptors to the end of the queue.
func (e *Execution[T]) Enqueue(interceptors ...Interceptor[T]) {
	e.queue = append(e.queue, interceptors...)
}

// Func is a convenience struct implementing the Interceptor interface.
// Perfect for simple, ad-hoc, or function-based interceptors.
type Func[T any] struct {
	Id        string
	EnterFunc func(*Execution[T]) error
	LeaveFunc func(*Execution[T]) error
	ErrorFunc func(*Execution[T], error) error
}

// Name returns the name of the interceptor.
func (f *Func[T]) Name() string {
	if f.Id != "" {
		return f.Id
	}
	return "AnonymousFunc"
}

// Enter executes the EnterFunc if defined.
func (f *Func[T]) Enter(exec *Execution[T]) error {
	if f.EnterFunc != nil {
		return f.EnterFunc(exec)
	}
	return nil
}

// Leave executes the LeaveFunc if defined.
func (f *Func[T]) Leave(exec *Execution[T]) error {
	if f.LeaveFunc != nil {
		return f.LeaveFunc(exec)
	}
	return nil
}

// Error executes the ErrorFunc if defined, otherwise propagates the error.
func (f *Func[T]) Error(exec *Execution[T], err error) error {
	if f.ErrorFunc != nil {
		return f.ErrorFunc(exec, err)
	}
	return err
}

// Enter creates a simple enter-only interceptor.
func Enter[T any](name string, fn func(*Execution[T]) error) Interceptor[T] {
	return &Func[T]{Id: name, EnterFunc: fn}
}

// Leave creates a simple leave-only interceptor.
func Leave[T any](name string, fn func(*Execution[T]) error) Interceptor[T] {
	return &Func[T]{Id: name, LeaveFunc: fn}
}

// Error creates a simple error-only interceptor.
func Error[T any](name string, fn func(*Execution[T], error) error) Interceptor[T] {
	return &Func[T]{Id: name, ErrorFunc: fn}
}
