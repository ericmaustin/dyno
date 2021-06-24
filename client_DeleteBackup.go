package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// DeleteBackup executes a scan api call with a DeleteBackupInput
func (c *DefaultClient) DeleteBackup(ctx context.Context, input *ddb.DeleteBackupInput, optFns ...func(*DeleteBackupOptions)) (*ddb.DeleteBackupOutput, error) {
	op := NewDeleteBackup(input, optFns...)
	op.DynoInvoke(ctx, c.ddb)

	return op.Await()
}

// DeleteBackup executes a DeleteBackup operation with a DeleteBackupInput in this pool and returns the DeleteBackup for processing
func (p *Pool) DeleteBackup(input *ddb.DeleteBackupInput, optFns ...func(*DeleteBackupOptions)) *DeleteBackup {
	op := NewDeleteBackup(input, optFns...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// DeleteBackupInputCallback is a callback that is called on a given DeleteBackupInput before a DeleteBackup operation api call executes
type DeleteBackupInputCallback interface {
	DeleteBackupInputCallback(context.Context, *ddb.DeleteBackupInput) (*ddb.DeleteBackupOutput, error)
}

// DeleteBackupOutputCallback is a callback that is called on a given DeleteBackupOutput after a DeleteBackup operation api call executes
type DeleteBackupOutputCallback interface {
	DeleteBackupOutputCallback(context.Context, *ddb.DeleteBackupOutput) error
}

// DeleteBackupInputCallbackFunc is DeleteBackupOutputCallback function
type DeleteBackupInputCallbackFunc func(context.Context, *ddb.DeleteBackupInput) (*ddb.DeleteBackupOutput, error)

// DeleteBackupInputCallback implements the DeleteBackupOutputCallback interface
func (cb DeleteBackupInputCallbackFunc) DeleteBackupInputCallback(ctx context.Context, input *ddb.DeleteBackupInput) (*ddb.DeleteBackupOutput, error) {
	return cb(ctx, input)
}

// DeleteBackupOutputCallbackFunc is DeleteBackupOutputCallback function
type DeleteBackupOutputCallbackFunc func(context.Context, *ddb.DeleteBackupOutput) error

// DeleteBackupOutputCallback implements the DeleteBackupOutputCallback interface
func (cb DeleteBackupOutputCallbackFunc) DeleteBackupOutputCallback(ctx context.Context, input *ddb.DeleteBackupOutput) error {
	return cb(ctx, input)
}

// DeleteBackupOptions represents options passed to the DeleteBackup operation
type DeleteBackupOptions struct {
	// InputCallbacks are called before the DeleteBackup dynamodb api operation with the dynamodb.DeleteBackupInput
	InputCallbacks []DeleteBackupInputCallback
	// OutputCallbacks are called after the DeleteBackup dynamodb api operation with the dynamodb.DeleteBackupOutput
	OutputCallbacks []DeleteBackupOutputCallback
}

// DeleteBackupWithInputCallback adds a DeleteBackupInputCallbackFunc to the InputCallbacks
func DeleteBackupWithInputCallback(cb DeleteBackupInputCallbackFunc) func(*DeleteBackupOptions) {
	return func(opt *DeleteBackupOptions) {
		opt.InputCallbacks = append(opt.InputCallbacks, cb)
	}
}

// DeleteBackupWithOutputCallback adds a DeleteBackupOutputCallback to the OutputCallbacks
func DeleteBackupWithOutputCallback(cb DeleteBackupOutputCallback) func(*DeleteBackupOptions) {
	return func(opt *DeleteBackupOptions) {
		opt.OutputCallbacks = append(opt.OutputCallbacks, cb)
	}
}

// DeleteBackup represents a DeleteBackup operation
type DeleteBackup struct {
	*Promise
	input   *ddb.DeleteBackupInput
	options DeleteBackupOptions
}

// NewDeleteBackup creates a new DeleteBackup operation on the given client with a given DeleteBackupInput and options
func NewDeleteBackup(input *ddb.DeleteBackupInput, optFns ...func(*DeleteBackupOptions)) *DeleteBackup {
	opts := DeleteBackupOptions{}

	for _, opt := range optFns {
		opt(&opts)
	}

	return &DeleteBackup{
		Promise: NewPromise(),
		input:   input,
		options: opts,
	}
}

// Await waits for the Operation to be complete and then returns a DeleteBackupOutput and error
func (op *DeleteBackup) Await() (*ddb.DeleteBackupOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}
	
	return out.(*ddb.DeleteBackupOutput), err
}

// Invoke invokes the DeleteBackup operation
func (op *DeleteBackup) Invoke(ctx context.Context, client *ddb.Client) *DeleteBackup {
	go op.DynoInvoke(ctx, client)
	return op
}

// DynoInvoke implements the Operation interface
func (op *DeleteBackup) DynoInvoke(ctx context.Context, client *ddb.Client) {
	var (
		out *ddb.DeleteBackupOutput
		err error
	)

	defer func() { op.SetResponse(out, err) }()

	for _, cb := range op.options.InputCallbacks {
		if out, err = cb.DeleteBackupInputCallback(ctx, op.input); out != nil || err != nil {
			return
		}
	}

	if out, err = client.DeleteBackup(ctx, op.input); err != nil {
		return
	}

	for _, cb := range op.options.OutputCallbacks {
		if err = cb.DeleteBackupOutputCallback(ctx, out); err != nil {
			return
		}
	}
}

// NewDeleteBackupInput creates a DeleteBackupInput with a given table name and key
func NewDeleteBackupInput(backupArn *string) *ddb.DeleteBackupInput {
	return &ddb.DeleteBackupInput{
		BackupArn: backupArn,
	}
}
