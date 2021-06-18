package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// NewRestoreTableFromBackup creates a new RestoreTableFromBackup with this Client
func (c *Client) NewRestoreTableFromBackup(input *ddb.RestoreTableFromBackupInput, optFns ...func(*RestoreTableFromBackupOptions)) *RestoreTableFromBackup {
	return NewRestoreTableFromBackup(c.ddb, input, optFns...)
}

// RestoreTableFromBackup executes a scan api call with a RestoreTableFromBackupInput
func (c *Client) RestoreTableFromBackup(ctx context.Context, input *ddb.RestoreTableFromBackupInput, optFns ...func(*RestoreTableFromBackupOptions)) (*ddb.RestoreTableFromBackupOutput, error) {
	scan := c.NewRestoreTableFromBackup(input, optFns...)
	scan.DynoInvoke(ctx)
	return scan.Await()
}

// RestoreTableFromBackupInputCallback is a callback that is called on a given RestoreTableFromBackupInput before a RestoreTableFromBackup operation api call executes
type RestoreTableFromBackupInputCallback interface {
	RestoreTableFromBackupInputCallback(context.Context, *ddb.RestoreTableFromBackupInput) (*ddb.RestoreTableFromBackupOutput, error)
}

// RestoreTableFromBackupOutputCallback is a callback that is called on a given RestoreTableFromBackupOutput after a RestoreTableFromBackup operation api call executes
type RestoreTableFromBackupOutputCallback interface {
	RestoreTableFromBackupOutputCallback(context.Context, *ddb.RestoreTableFromBackupOutput) error
}

// RestoreTableFromBackupInputCallbackFunc is RestoreTableFromBackupOutputCallback function
type RestoreTableFromBackupInputCallbackFunc func(context.Context, *ddb.RestoreTableFromBackupInput) (*ddb.RestoreTableFromBackupOutput, error)

// RestoreTableFromBackupInputCallback implements the RestoreTableFromBackupOutputCallback interface
func (cb RestoreTableFromBackupInputCallbackFunc) RestoreTableFromBackupInputCallback(ctx context.Context, input *ddb.RestoreTableFromBackupInput) (*ddb.RestoreTableFromBackupOutput, error) {
	return cb(ctx, input)
}

// RestoreTableFromBackupOutputCallbackFunc is RestoreTableFromBackupOutputCallback function
type RestoreTableFromBackupOutputCallbackFunc func(context.Context, *ddb.RestoreTableFromBackupOutput) error

// RestoreTableFromBackupOutputCallback implements the RestoreTableFromBackupOutputCallback interface
func (cb RestoreTableFromBackupOutputCallbackFunc) RestoreTableFromBackupOutputCallback(ctx context.Context, input *ddb.RestoreTableFromBackupOutput) error {
	return cb(ctx, input)
}

// RestoreTableFromBackupOptions represents options passed to the RestoreTableFromBackup operation
type RestoreTableFromBackupOptions struct {
	//InputCallbacks are called before the RestoreTableFromBackup dynamodb api operation with the dynamodb.RestoreTableFromBackupInput
	InputCallbacks []RestoreTableFromBackupInputCallback
	//OutputCallbacks are called after the RestoreTableFromBackup dynamodb api operation with the dynamodb.RestoreTableFromBackupOutput
	OutputCallbacks []RestoreTableFromBackupOutputCallback
}

// RestoreTableFromBackupWithInputCallback adds a RestoreTableFromBackupInputCallbackFunc to the InputCallbacks
func RestoreTableFromBackupWithInputCallback(cb RestoreTableFromBackupInputCallbackFunc) func(*RestoreTableFromBackupOptions) {
	return func(opt *RestoreTableFromBackupOptions) {
		opt.InputCallbacks = append(opt.InputCallbacks, cb)
	}
}

// RestoreTableFromBackupWithOutputCallback adds a RestoreTableFromBackupOutputCallback to the OutputCallbacks
func RestoreTableFromBackupWithOutputCallback(cb RestoreTableFromBackupOutputCallback) func(*RestoreTableFromBackupOptions) {
	return func(opt *RestoreTableFromBackupOptions) {
		opt.OutputCallbacks = append(opt.OutputCallbacks, cb)
	}
}

// RestoreTableFromBackup represents a RestoreTableFromBackup operation
type RestoreTableFromBackup struct {
	*Promise
	client  *ddb.Client
	input   *ddb.RestoreTableFromBackupInput
	options RestoreTableFromBackupOptions
}

// NewRestoreTableFromBackup creates a new RestoreTableFromBackup operation on the given client with a given RestoreTableFromBackupInput and options
func NewRestoreTableFromBackup(client *ddb.Client, input *ddb.RestoreTableFromBackupInput, optFns ...func(*RestoreTableFromBackupOptions)) *RestoreTableFromBackup {
	opts := RestoreTableFromBackupOptions{}
	for _, opt := range optFns {
		opt(&opts)
	}
	return &RestoreTableFromBackup{
		Promise: NewPromise(),
		client:  client,
		input:   input,
		options: opts,
	}
}

// Await waits for the Operation to be complete and then returns a RestoreTableFromBackupOutput and error
func (op *RestoreTableFromBackup) Await() (*ddb.RestoreTableFromBackupOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}
	return out.(*ddb.RestoreTableFromBackupOutput), err
}

// Invoke invokes the RestoreTableFromBackup operation
func (op *RestoreTableFromBackup) Invoke(ctx context.Context) *RestoreTableFromBackup {
	go op.DynoInvoke(ctx)
	return op
}

// DynoInvoke implements the Operation interface
func (op *RestoreTableFromBackup) DynoInvoke(ctx context.Context) {
	var (
		out *ddb.RestoreTableFromBackupOutput
		err error
	)
	defer op.SetResponse(out, err)
	for _, cb := range op.options.InputCallbacks {
		if out, err = cb.RestoreTableFromBackupInputCallback(ctx, op.input); out != nil || err != nil {
			return
		}
	}
	if out, err = op.client.RestoreTableFromBackup(ctx, op.input); err != nil {
		return
	}
	for _, cb := range op.options.OutputCallbacks {
		if err = cb.RestoreTableFromBackupOutputCallback(ctx, out); err != nil {
			return
		}
	}
	return
}

// NewRestoreTableFromBackupInput creates a RestoreTableFromBackupInput with a given table name and key
func NewRestoreTableFromBackupInput(tableName *string, backupArn *string) *ddb.RestoreTableFromBackupInput {
	return &ddb.RestoreTableFromBackupInput{
		TargetTableName: tableName,
		BackupArn:       backupArn,
	}
}
