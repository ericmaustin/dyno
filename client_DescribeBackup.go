package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// DescribeBackup executes a scan api call with a DescribeBackupInput
func (c *Client) DescribeBackup(ctx context.Context, input *ddb.DescribeBackupInput, optFns ...func(*DescribeBackupOptions)) (*ddb.DescribeBackupOutput, error) {
	op := NewDescribeBackup(input, optFns...)
	op.DynoInvoke(ctx, c.ddb)
	
	return op.Await()
}

// DescribeBackupInputCallback is a callback that is called on a given DescribeBackupInput before a DescribeBackup operation api call executes
type DescribeBackupInputCallback interface {
	DescribeBackupInputCallback(context.Context, *ddb.DescribeBackupInput) (*ddb.DescribeBackupOutput, error)
}

// DescribeBackupOutputCallback is a callback that is called on a given DescribeBackupOutput after a DescribeBackup operation api call executes
type DescribeBackupOutputCallback interface {
	DescribeBackupOutputCallback(context.Context, *ddb.DescribeBackupOutput) error
}

// DescribeBackupInputCallbackFunc is DescribeBackupOutputCallback function
type DescribeBackupInputCallbackFunc func(context.Context, *ddb.DescribeBackupInput) (*ddb.DescribeBackupOutput, error)

// DescribeBackupInputCallback implements the DescribeBackupOutputCallback interface
func (cb DescribeBackupInputCallbackFunc) DescribeBackupInputCallback(ctx context.Context, input *ddb.DescribeBackupInput) (*ddb.DescribeBackupOutput, error) {
	return cb(ctx, input)
}

// DescribeBackupOutputCallbackFunc is DescribeBackupOutputCallback function
type DescribeBackupOutputCallbackFunc func(context.Context, *ddb.DescribeBackupOutput) error

// DescribeBackupOutputCallback implements the DescribeBackupOutputCallback interface
func (cb DescribeBackupOutputCallbackFunc) DescribeBackupOutputCallback(ctx context.Context, input *ddb.DescribeBackupOutput) error {
	return cb(ctx, input)
}

// DescribeBackupOptions represents options passed to the DescribeBackup operation
type DescribeBackupOptions struct {
	// InputCallbacks are called before the DescribeBackup dynamodb api operation with the dynamodb.DescribeBackupInput
	InputCallbacks []DescribeBackupInputCallback
	// OutputCallbacks are called after the DescribeBackup dynamodb api operation with the dynamodb.DescribeBackupOutput
	OutputCallbacks []DescribeBackupOutputCallback
}

// DescribeBackupWithInputCallback adds a DescribeBackupInputCallbackFunc to the InputCallbacks
func DescribeBackupWithInputCallback(cb DescribeBackupInputCallbackFunc) func(*DescribeBackupOptions) {
	return func(opt *DescribeBackupOptions) {
		opt.InputCallbacks = append(opt.InputCallbacks, cb)
	}
}

// DescribeBackupWithOutputCallback adds a DescribeBackupOutputCallback to the OutputCallbacks
func DescribeBackupWithOutputCallback(cb DescribeBackupOutputCallback) func(*DescribeBackupOptions) {
	return func(opt *DescribeBackupOptions) {
		opt.OutputCallbacks = append(opt.OutputCallbacks, cb)
	}
}

// DescribeBackup represents a DescribeBackup operation
type DescribeBackup struct {
	*Promise
	input   *ddb.DescribeBackupInput
	options DescribeBackupOptions
}

// NewDescribeBackup creates a new DescribeBackup operation on the given client with a given DescribeBackupInput and options
func NewDescribeBackup(input *ddb.DescribeBackupInput, optFns ...func(*DescribeBackupOptions)) *DescribeBackup {
	opts := DescribeBackupOptions{}

	for _, opt := range optFns {
		opt(&opts)
	}

	return &DescribeBackup{
		Promise: NewPromise(),
		input:   input,
		options: opts,
	}
}

// Await waits for the Operation to be complete and then returns a DescribeBackupOutput and error
func (op *DescribeBackup) Await() (*ddb.DescribeBackupOutput, error) {
	out, err := op.Promise.Await()

	if out == nil {
		return nil, err
	}

	return out.(*ddb.DescribeBackupOutput), err
}

// Invoke invokes the DescribeBackup operation
func (op *DescribeBackup) Invoke(ctx context.Context, client *ddb.Client) *DescribeBackup {
	go op.DynoInvoke(ctx, client)
	return op
}

// DynoInvoke implements the Operation interface
func (op *DescribeBackup) DynoInvoke(ctx context.Context, client *ddb.Client) {
	var (
		out *ddb.DescribeBackupOutput
		err error
	)

	defer func() { op.SetResponse(out, err) }()

	for _, cb := range op.options.InputCallbacks {
		if out, err = cb.DescribeBackupInputCallback(ctx, op.input); out != nil || err != nil {
			return
		}
	}

	if out, err = client.DescribeBackup(ctx, op.input); err != nil {
		return
	}

	for _, cb := range op.options.OutputCallbacks {
		if err = cb.DescribeBackupOutputCallback(ctx, out); err != nil {
			return
		}
	}
}

// NewDescribeBackupInput creates a new DescribeBackupInput
func NewDescribeBackupInput(backupArn *string) *ddb.DescribeBackupInput {
	return &ddb.DescribeBackupInput{BackupArn: backupArn}
}
