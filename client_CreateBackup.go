package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// CreateBackup executes a scan api call with a CreateBackupInput
func (c *DefaultClient) CreateBackup(ctx context.Context, input *ddb.CreateBackupInput, optFns ...func(*CreateBackupOptions)) (*ddb.CreateBackupOutput, error) {
	op := NewCreateBackup(input, optFns...)
	op.DynoInvoke(ctx, c.ddb)

	return op.Await()
}

// CreateBackup executes a CreateBackup operation with a CreateBackupInput in this pool and returns the CreateBackup for processing
func (p *Pool) CreateBackup(input *ddb.CreateBackupInput, optFns ...func(*CreateBackupOptions)) *CreateBackup {
	op := NewCreateBackup(input, optFns...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
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
	// InputCallbacks are called before the CreateBackup dynamodb api operation with the dynamodb.CreateBackupInput
	InputCallbacks []CreateBackupInputCallback
	// OutputCallbacks are called after the CreateBackup dynamodb api operation with the dynamodb.CreateBackupOutput
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
	//client  *ddb.DefaultClient
	input   *ddb.CreateBackupInput
	options CreateBackupOptions
}

// NewCreateBackup creates a new CreateBackup operation on the given client with a given CreateBackupInput and options
func NewCreateBackup(input *ddb.CreateBackupInput, optFns ...func(*CreateBackupOptions)) *CreateBackup {
	opts := CreateBackupOptions{}
	for _, opt := range optFns {
		opt(&opts)
	}
	
	return &CreateBackup{
		Promise: NewPromise(),
		//client:  client,
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
func (op *CreateBackup) Invoke(ctx context.Context, client *ddb.Client) *CreateBackup {
	go op.DynoInvoke(ctx, client)
	return op
}

// DynoInvoke implements the Operation interface
func (op *CreateBackup) DynoInvoke(ctx context.Context, client *ddb.Client) {
	var (
		out *ddb.CreateBackupOutput
		err error
	)

	defer func() { op.SetResponse(out, err) }()

	for _, cb := range op.options.InputCallbacks {
		if out, err = cb.CreateBackupInputCallback(ctx, op.input); out != nil || err != nil {
			return
		}
	}

	if out, err = client.CreateBackup(ctx, op.input); err != nil {
		return
	}

	for _, cb := range op.options.OutputCallbacks {
		if err = cb.CreateBackupOutputCallback(ctx, out); err != nil {
			return
		}
	}
}

// NewCreateBackupInput creates a CreateBackupInput with a given table name and key
func NewCreateBackupInput(tableName *string, backupArn *string) *ddb.CreateBackupInput {
	return &ddb.CreateBackupInput{
		BackupName: backupArn,
		TableName:  tableName,
	}
}
