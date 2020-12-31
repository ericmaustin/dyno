package dyno

import "fmt"

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

	//
	// Encoding errors
	//

	// ErrEncodingEmbeddedStructUnmarshalFailed raised when unmarshalling an embedded struct fails
	ErrEncodingEmbeddedStructUnmarshalFailed

	// ErrEncodingEmbeddedStructMarshalFailed raised when marshalling an embedded struct fails
	ErrEncodingEmbeddedStructMarshalFailed

	// ErrEncodingEmbeddedMapUnmarshalFailed raised when unmarshalling an embedded map fails
	ErrEncodingEmbeddedMapUnmarshalFailed

	// ErrEncodingEmbeddedMapMarshalFailed raised when marshalling an embedded map fails
	ErrEncodingEmbeddedMapMarshalFailed

	// ErrEncodingEmbeddedBadKind raised when unmarshalling a bad value kind
	ErrEncodingEmbeddedBadKind

	// ErrEncodingBadKind raised when an input is of an unexpected kind
	ErrEncodingBadKind

	// ErrEncodingBadValue raised when the value of an input is unexpected or incorrect
	ErrEncodingBadValue
)
