package operation

import "errors"

var (
	ErrMissingTable = errors.New("operation is missing a table")
	ErrRequestMissing = errors.New("operation is missing a request")
	ErrInvalidState = errors.New("operation is an invalid state")
	ErrPoolOperationDidNotRun = errors.New("pool operation did not run")
	ErrPoolNotRunning = errors.New("pool is not running")
)

