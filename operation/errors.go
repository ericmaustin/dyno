package operation

//ErrMissingTable is returned if a table is missing from the operation
type ErrMissingTable struct{}

func (e *ErrMissingTable) Error() string {
	return "operation is missing a table"
}

//ErrRequestMissing is returned if a request is missing from the oepration
type ErrRequestMissing struct{}

func (e *ErrRequestMissing) Error() string {
	return "operation is missing a request"
}

//ErrInvalidState is returned if the operation is an invalid state
type ErrInvalidState struct{}

func (e *ErrInvalidState) Error() string {
	return "operation is an invalid state"
}

//ErrPoolNotRunning returned by a Pool operation if Pool is not running
type ErrPoolNotRunning struct{}

func (e *ErrPoolNotRunning) Error() string {
	return "Pool is not running"
}

//ErrPoolOperationDidNotRun returned by a Pool operation if for some reason the operation never ran
type ErrPoolOperationDidNotRun struct{}

func (e *ErrPoolOperationDidNotRun) Error() string {
	return "Pool operation did not run"
}
