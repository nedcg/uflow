package uflow

// Execute runs the execution pipeline.
func (e *Runner[T]) Execute() error {
	var err error
	var localStack [16]Step[T]
	var stack []Step[T]
	if len(e.queue) <= len(localStack) {
		stack = localStack[:0]
	} else {
		stack = make([]Step[T], 0, len(e.queue))
	}

	// In phase
	for e.index < len(e.queue) {
		// Check for context cancellation/deadline before executing the next interceptor
		if err = e.ctx.Err(); err != nil {
			break
		}

		interceptor := e.queue[e.index]
		e.index++

		// Push to stack for leave/error phases
		stack = append(stack, interceptor)

		err = interceptor.In(e)
		if err != nil {
			break
		}
	}

	// If there was an error (either from In hook or from Context cancellation),
	// enter the Catch phase. Otherwise, enter the Out phase.
	if err != nil {
		return e.executeError(stack, err)
	}

	return e.executeLeave(stack)
}

// executeLeave runs the leave phase hooks in reverse order of entry (LIFO).
func (e *Runner[T]) executeLeave(stack []Step[T]) error {
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

		err = interceptor.Out(e)
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
// If any Catch hook handles (resolves) the error by returning nil,
// execution resumes in the Out phase for the remaining stack.
func (e *Runner[T]) executeError(stack []Step[T], err error) error {
	activeErr := err

	for len(stack) > 0 {
		// Pop from stack
		n := len(stack)
		interceptor := stack[n-1]
		stack = stack[:n-1]

		// Call the Catch handler
		activeErr = interceptor.Catch(e, activeErr)

		// If the error handler resolved the error (returned nil),
		// resume normal Out phase for the rest of the stack.
		if activeErr == nil {
			return e.executeLeave(stack)
		}
	}

	// If we exhausted the stack and the error is still active, return it.
	return activeErr
}
