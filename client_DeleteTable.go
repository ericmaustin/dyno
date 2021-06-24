package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// DeleteTable executes a scan api call with a DeleteTableInput
func (c *DefaultClient) DeleteTable(ctx context.Context, input *ddb.DeleteTableInput, optFns ...func(*DeleteTableOptions)) (*ddb.DeleteTableOutput, error) {
	op := NewDeleteTable(input, optFns...)
	op.DynoInvoke(ctx, c.ddb)
	
	return op.Await()
}

// DeleteTable executes a DeleteTable operation with a DeleteTableInput in this pool and returns the DeleteTable for processing
func (p *Pool) DeleteTable(input *ddb.DeleteTableInput, optFns ...func(*DeleteTableOptions)) *DeleteTable {
	op := NewDeleteTable(input, optFns...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// DeleteTableInputCallback is a callback that is called on a given DeleteTableInput before a DeleteTable operation api call executes
type DeleteTableInputCallback interface {
	DeleteTableInputCallback(context.Context, *ddb.DeleteTableInput) (*ddb.DeleteTableOutput, error)
}

// DeleteTableOutputCallback is a callback that is called on a given DeleteTableOutput after a DeleteTable operation api call executes
type DeleteTableOutputCallback interface {
	DeleteTableOutputCallback(context.Context, *ddb.DeleteTableOutput) error
}

// DeleteTableInputCallbackFunc is DeleteTableOutputCallback function
type DeleteTableInputCallbackFunc func(context.Context, *ddb.DeleteTableInput) (*ddb.DeleteTableOutput, error)

// DeleteTableInputCallback implements the DeleteTableOutputCallback interface
func (cb DeleteTableInputCallbackFunc) DeleteTableInputCallback(ctx context.Context, input *ddb.DeleteTableInput) (*ddb.DeleteTableOutput, error) {
	return cb(ctx, input)
}

// DeleteTableOutputCallbackFunc is DeleteTableOutputCallback function
type DeleteTableOutputCallbackFunc func(context.Context, *ddb.DeleteTableOutput) error

// DeleteTableOutputCallback implements the DeleteTableOutputCallback interface
func (cb DeleteTableOutputCallbackFunc) DeleteTableOutputCallback(ctx context.Context, input *ddb.DeleteTableOutput) error {
	return cb(ctx, input)
}

// DeleteTableOptions represents options passed to the DeleteTable operation
type DeleteTableOptions struct {
	// InputCallbacks are called before the DeleteTable dynamodb api operation with the dynamodb.DeleteTableInput
	InputCallbacks []DeleteTableInputCallback
	// OutputCallbacks are called after the DeleteTable dynamodb api operation with the dynamodb.DeleteTableOutput
	OutputCallbacks []DeleteTableOutputCallback
}

// DeleteTableWithInputCallback adds a DeleteTableInputCallbackFunc to the InputCallbacks
func DeleteTableWithInputCallback(cb DeleteTableInputCallbackFunc) func(*DeleteTableOptions) {
	return func(opt *DeleteTableOptions) {
		opt.InputCallbacks = append(opt.InputCallbacks, cb)
	}
}

// DeleteTableWithOutputCallback adds a DeleteTableOutputCallback to the OutputCallbacks
func DeleteTableWithOutputCallback(cb DeleteTableOutputCallback) func(*DeleteTableOptions) {
	return func(opt *DeleteTableOptions) {
		opt.OutputCallbacks = append(opt.OutputCallbacks, cb)
	}
}

// DeleteTable represents a DeleteTable operation
type DeleteTable struct {
	*Promise
	input   *ddb.DeleteTableInput
	options DeleteTableOptions
}

// NewDeleteTable creates a new DeleteTable operation on the given client with a given DeleteTableInput and options
func NewDeleteTable(input *ddb.DeleteTableInput, optFns ...func(*DeleteTableOptions)) *DeleteTable {
	opts := DeleteTableOptions{}

	for _, opt := range optFns {
		opt(&opts)
	}

	return &DeleteTable{
		Promise: NewPromise(),
		input:   input,
		options: opts,
	}
}

// Await waits for the Operation to be complete and then returns a DeleteTableOutput and error
func (op *DeleteTable) Await() (*ddb.DeleteTableOutput, error) {
	out, err := op.Promise.Await()

	if out == nil {
		return nil, err
	}

	return out.(*ddb.DeleteTableOutput), err
}

// Invoke invokes the DeleteTable operation
func (op *DeleteTable) Invoke(ctx context.Context, client *ddb.Client) *DeleteTable {
	go op.DynoInvoke(ctx, client)
	return op
}

// DynoInvoke implements the Operation interface
func (op *DeleteTable) DynoInvoke(ctx context.Context, client *ddb.Client) {
	var (
		out *ddb.DeleteTableOutput
		err error
	)

	defer func() { op.SetResponse(out, err) }()

	for _, cb := range op.options.InputCallbacks {
		if out, err = cb.DeleteTableInputCallback(ctx, op.input); out != nil || err != nil {
			return
		}
	}

	if out, err = client.DeleteTable(ctx, op.input); err != nil {
		return
	}

	for _, cb := range op.options.OutputCallbacks {
		if err = cb.DeleteTableOutputCallback(ctx, out); err != nil {
			return
		}
	}
}

// NewDeleteTableInput creates a new DeleteTableInput
func NewDeleteTableInput(tableName *string) *ddb.DeleteTableInput {
	return &ddb.DeleteTableInput{TableName: tableName}
}
