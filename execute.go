package juzu

// Execute runs the execution pipeline.
func (e *Execution[T]) Execute() error {
	var err error
	var localStack [16]Interceptor[T]
	var stack []Interceptor[T]
	if len(e.queue) <= len(localStack) {
		stack = localStack[:0]
	} else {
		stack = make([]Interceptor[T], 0, len(e.queue))
	}

	// Enter phase
	for e.index < len(e.queue) {
		// Check for context cancellation/deadline before executing the next interceptor
		if err = e.ctx.Err(); err != nil {
			break
		}

		interceptor := e.queue[e.index]
		e.index++

		// Push to stack for leave/error phases
		stack = append(stack, interceptor)

		err = interceptor.Enter(e)
		if err != nil {
			break
		}
	}

	// If there was an error (either from Enter hook or from Context cancellation),
	// enter the Error phase. Otherwise, enter the Leave phase.
	if err != nil {
		return e.executeError(stack, err)
	}

	return e.executeLeave(stack)
}

// executeLeave runs the leave phase hooks in reverse order of entry (LIFO).
func (e *Execution[T]) executeLeave(stack []Interceptor[T]) error {
	var err error
	for len(stack) > 0 {
		// Check for context cancellation
		if err = e.ctx.Err(); err != nil {
			break
		}

		// Pop from stack
		n := len(stack)
		interceptor := stack[n-1]
		stack = stack[:n-1]

		err = interceptor.Leave(e)
		if err != nil {
			break
		}
	}

	if err != nil {
		return e.executeError(stack, err)
	}
	return nil
}

// executeError runs the error phase hooks in reverse order of entry (LIFO).
// If any Error hook handles (resolves) the error by returning nil,
// execution resumes in the Leave phase for the remaining stack.
func (e *Execution[T]) executeError(stack []Interceptor[T], err error) error {
	activeErr := err

	for len(stack) > 0 {
		// Pop from stack
		n := len(stack)
		interceptor := stack[n-1]
		stack = stack[:n-1]

		// Call the Error handler
		activeErr = interceptor.Error(e, activeErr)

		// If the error handler resolved the error (returned nil),
		// resume normal Leave phase for the rest of the stack.
		if activeErr == nil {
			return e.executeLeave(stack)
		}
	}

	// If we exhausted the stack and the error is still active, return it.
	return activeErr
}
