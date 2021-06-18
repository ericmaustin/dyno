package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// NewCreateBackup creates a new CreateBackup with this Client
func (c *Client) NewCreateBackup(input *ddb.CreateBackupInput, optFns ...func(*CreateBackupOptions)) *CreateBackup {
	return NewCreateBackup(c.ddb, input, optFns...)
}

// CreateBackup executes a scan api call with a CreateBackupInput
func (c *Client) CreateBackup(ctx context.Context, input *ddb.CreateBackupInput, optFns ...func(*CreateBackupOptions)) (*ddb.CreateBackupOutput, error) {
	scan := c.NewCreateBackup(input, optFns...)
	scan.DynoInvoke(ctx)
	return scan.Await()
}

// CreateBackupInputCallback is a callback that is called on a given CreateBackupInput before a CreateBackup operation api call executes
type CreateBackupInputCallback interface {
	CreateBackupInputCallback(context.Context, *ddb.CreateBackupInput) (*ddb.CreateBackupOutput, error)
}

// CreateBackupOutputCallback is a callback that is called on a given CreateBackupOutput after a CreateBackup operation api call executes
type CreateBackupOutputCallback interface {
	CreateBackupOutputCallback(context.Context, *ddb.CreateBackupOutput) error
}

// CreateBackupInputCallbackFunc is CreateBackupOutputCallback function
type CreateBackupInputCallbackFunc func(context.Context, *ddb.CreateBackupInput) (*ddb.CreateBackupOutput, error)

// CreateBackupInputCallback implements the CreateBackupOutputCallback interface
func (cb CreateBackupInputCallbackFunc) CreateBackupInputCallback(ctx context.Context, input *ddb.CreateBackupInput) (*ddb.CreateBackupOutput, error) {
	return cb(ctx, input)
}

// CreateBackupOutputCallbackFunc is CreateBackupOutputCallback function
type CreateBackupOutputCallbackFunc func(context.Context, *ddb.CreateBackupOutput) error

// CreateBackupOutputCallback implements the CreateBackupOutputCallback interface
func (cb CreateBackupOutputCallbackFunc) CreateBackupOutputCallback(ctx context.Context, input *ddb.CreateBackupOutput) error {
	return cb(ctx, input)
}

// CreateBackupOptions represents options passed to the CreateBackup operation
type CreateBackupOptions struct {
	//InputCallbacks are called before the CreateBackup dynamodb api operation with the dynamodb.CreateBackupInput
	InputCallbacks []CreateBackupInputCallback
	//OutputCallbacks are called after the CreateBackup dynamodb api operation with the dynamodb.CreateBackupOutput
	OutputCallbacks []CreateBackupOutputCallback
}

// CreateBackupWithInputCallback adds a CreateBackupInputCallbackFunc to the InputCallbacks
func CreateBackupWithInputCallback(cb CreateBackupInputCallbackFunc) func(*CreateBackupOptions) {
	return func(opt *CreateBackupOptions) {
		opt.InputCallbacks = append(opt.InputCallbacks, cb)
	}
}

// CreateBackupWithOutputCallback adds a CreateBackupOutputCallback to the OutputCallbacks
func CreateBackupWithOutputCallback(cb CreateBackupOutputCallback) func(*CreateBackupOptions) {
	return func(opt *CreateBackupOptions) {
		opt.OutputCallbacks = append(opt.OutputCallbacks, cb)
	}
}

// CreateBackup represents a CreateBackup operation
type CreateBackup struct {
	*Promise
	client  *ddb.Client
	input   *ddb.CreateBackupInput
	options CreateBackupOptions
}

// NewCreateBackup creates a new CreateBackup operation on the given client with a given CreateBackupInput and options
func NewCreateBackup(client *ddb.Client, input *ddb.CreateBackupInput, optFns ...func(*CreateBackupOptions)) *CreateBackup {
	opts := CreateBackupOptions{}
	for _, opt := range optFns {
		opt(&opts)
	}
	return &CreateBackup{
		Promise: NewPromise(),
		client:  client,
		input:   input,
		options: opts,
	}
}

// Await waits for the Operation to be complete and then returns a CreateBackupOutput and error
func (op *CreateBackup) Await() (*ddb.CreateBackupOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}
	return out.(*ddb.CreateBackupOutput), err
}

// Invoke invokes the CreateBackup operation
func (op *CreateBackup) Invoke(ctx context.Context) *CreateBackup {
	go op.DynoInvoke(ctx)
	return op
}

// DynoInvoke implements the Operation interface
func (op *CreateBackup) DynoInvoke(ctx context.Context) {
	var (
		out *ddb.CreateBackupOutput
		err error
	)
	defer op.SetResponse(out, err)
	for _, cb := range op.options.InputCallbacks {
		if out, err = cb.CreateBackupInputCallback(ctx, op.input); out != nil || err != nil {
			return
		}
	}
	if out, err = op.client.CreateBackup(ctx, op.input); err != nil {
		return
	}
	for _, cb := range op.options.OutputCallbacks {
		if err = cb.CreateBackupOutputCallback(ctx, out); err != nil {
			return
		}
	}
	return
}

// NewCreateBackupInput creates a CreateBackupInput with a given table name and key
func NewCreateBackupInput(tableName *string, backupArn *string) *ddb.CreateBackupInput {
	return &ddb.CreateBackupInput{
		BackupName: backupArn,
		TableName:  tableName,
	}
}
