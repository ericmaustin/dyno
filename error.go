package dyno

import (
	"bytes"
	"fmt"
)

/*
Error is the Master Error type for all Dyno related errors
Required:
	: the int val for the
Optional:
	Message: the message for this specific error
*/
type Error struct {
	Code    int
	Message string
}

func (e Error) Error() string {
	if len(e.Message) > 0 {
		return e.Message
	}
	return fmt.Sprintf("DynoError  %v", e.Message)
}

// All  s used by Dyno are below
const (
	_ = iota
	//
	//	table errors
	//

	// ErrTableIsInInvalidState raised when a table is in an invalid state
	ErrTableIsInInvalidState

	// ErrTableMissingIndex raised when a non-existent index is attempted to be accessed
	ErrTableMissingIndex

	// ErrBackupInInvalidState raised when a backup is in an invalid state
	ErrBackupInInvalidState

	// ErrTableNotLoaded raised when a table is not loaded, e.g. the description wasn't loaded
	ErrTableNotLoaded

	//
	//	condition errors
	//

	// ErrOperatorNotFound raised when an Operator input is not found / is invalid
	ErrOperatorNotFound

	//
	// Lock Errors
	//

	// ErrLockFailedToAcquire is raised when a lock was unable to be acquired
	ErrLockFailedToAcquire

	// ErrLockTimeout raised when a lock attempt times out
	ErrLockTimeout

	// ErrLockFailedLeaseRenewal raised when a lock failed to renew a lease
	ErrLockFailedLeaseRenewal

	//
	// Index errors
	//

	ErrPartitionKeyTableIndexMismatch

	// ErrLsiNameAlreadyExists is raised if an LSI with this name already exists
	ErrLsiNameAlreadyExists

	// ErrGsiNameAlreadyExists is raised if an GSI with this name already exists
	ErrGsiNameAlreadyExists

	//
	// BuildOperation Errors
	//

	// ErrOperationNeverStarted raised when an operation never started
	ErrOperationNeverStarted

	// ErrOperationNeverFinished raised when an operation is not completed
	ErrOperationNeverFinished

	// ErrBatchOperationTimedOut raised when batch operation timed out before it finished
	ErrBatchOperationTimedOut

	// ErrBatchOutputContextCancelled raised when batch context was cancelled before it finished
	ErrBatchOutputContextCancelled

	//
	// Request Errors
	//

	// ErrRequestExecutionContextCancelled raised when context is cancelled before a request execution completes
	ErrRequestExecutionContextCancelled
)

//ErrSet is a generic collection of errors
type ErrSet struct {
	Errors []error
}

func (e *ErrSet) AddErr(errs ...error) {
	for _, err := range errs {
		if err == nil {
			continue
		}
		e.Errors = append(e.Errors, err)
	}
}

func (e *ErrSet) Error() string {
	var buf bytes.Buffer
	if len(e.Errors) < 1 {
		return ""
	}
	fmt.Fprintf(&buf, "%d errors: ", len(e.Errors))
	for _, err := range e.Errors {
		buf.WriteString(err.Error() + "; ")
	}
	buf.Truncate(buf.Len() - 2)
	return buf.String()
}

//Err returns itself only if the set contains any errors, otherwise it returns nil
func (e *ErrSet) Err() error {
	if len(e.Errors) == 0 {
		return nil
	}
	return e
}

func NewErrSet(errs []error) *ErrSet {
	e := new(ErrSet)
	for _, err := range errs {
		if err == nil {
			continue
		}
		e.Errors = append(e.Errors, err)
	}
	return e
}
