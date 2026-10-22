package flezha

import (
	"errors"

	"github.com/flezha/flezha_go_sdk/internal/native"
)

// Error is anything Flezha refuses: the controller, the core or this package.
// Code is what a program branches on (errors.As, then Code), Class the error's
// class (invalid, not_found, denied, ...), Message one sentence for a reader,
// Details what the message is about, and Hint the next command to run.
type Error = native.Error

// refusal renders the catalogue's error for code, reading as the core's own.
func refusal(code string, details map[string]any) *Error { return native.Refusal(code, details) }

// NonRetryable marks err as one no retry can change: the item parks at once,
// with no further attempts.
func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return &nonRetryable{err}
}

type nonRetryable struct{ error }

func (e *nonRetryable) Unwrap() error { return e.error }

// isNonRetryable answers whether err, or an error it wraps, was marked.
func isNonRetryable(err error) bool {
	var marked *nonRetryable
	return errors.As(err, &marked)
}
