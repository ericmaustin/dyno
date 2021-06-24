package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// UpdateTable executes a scan api call with a UpdateTableInput
func (c *DefaultClient) UpdateTable(ctx context.Context, input *ddb.UpdateTableInput, optFns ...func(*UpdateTableOptions)) (*ddb.UpdateTableOutput, error) {
	op := NewUpdateTable(input, optFns...)
	op.DynoInvoke(ctx, c.ddb)

	return op.Await()
}

// UpdateTableInputCallback is a callback that is called on a given UpdateTableInput before a UpdateTable operation api call executes
type UpdateTableInputCallback interface {
	UpdateTableInputCallback(context.Context, *ddb.UpdateTableInput) (*ddb.UpdateTableOutput, error)
}

// UpdateTableOutputCallback is a callback that is called on a given UpdateTableOutput after a UpdateTable operation api call executes
type UpdateTableOutputCallback interface {
	UpdateTableOutputCallback(context.Context, *ddb.UpdateTableOutput) error
}

// UpdateTableInputCallbackFunc is UpdateTableOutputCallback function
type UpdateTableInputCallbackFunc func(context.Context, *ddb.UpdateTableInput) (*ddb.UpdateTableOutput, error)

// UpdateTableInputCallback implements the UpdateTableOutputCallback interface
func (cb UpdateTableInputCallbackFunc) UpdateTableInputCallback(ctx context.Context, input *ddb.UpdateTableInput) (*ddb.UpdateTableOutput, error) {
	return cb(ctx, input)
}

// UpdateTableOutputCallbackFunc is UpdateTableOutputCallback function
type UpdateTableOutputCallbackFunc func(context.Context, *ddb.UpdateTableOutput) error

// UpdateTableOutputCallback implements the UpdateTableOutputCallback interface
func (cb UpdateTableOutputCallbackFunc) UpdateTableOutputCallback(ctx context.Context, input *ddb.UpdateTableOutput) error {
	return cb(ctx, input)
}

// UpdateTableOptions represents options passed to the UpdateTable operation
type UpdateTableOptions struct {
	// InputCallbacks are called before the UpdateTable dynamodb api operation with the dynamodb.UpdateTableInput
	InputCallbacks []UpdateTableInputCallback
	// OutputCallbacks are called after the UpdateTable dynamodb api operation with the dynamodb.UpdateTableOutput
	OutputCallbacks []UpdateTableOutputCallback
}

// UpdateTableWithInputCallback adds a UpdateTableInputCallbackFunc to the InputCallbacks
func UpdateTableWithInputCallback(cb UpdateTableInputCallbackFunc) func(*UpdateTableOptions) {
	return func(opt *UpdateTableOptions) {
		opt.InputCallbacks = append(opt.InputCallbacks, cb)
	}
}

// UpdateTableWithOutputCallback adds a UpdateTableOutputCallback to the OutputCallbacks
func UpdateTableWithOutputCallback(cb UpdateTableOutputCallback) func(*UpdateTableOptions) {
	return func(opt *UpdateTableOptions) {
		opt.OutputCallbacks = append(opt.OutputCallbacks, cb)
	}
}

// UpdateTable represents a UpdateTable operation
type UpdateTable struct {
	*Promise
	input   *ddb.UpdateTableInput
	options UpdateTableOptions
}

// NewUpdateTable creates a new UpdateTable operation on the given client with a given UpdateTableInput and options
func NewUpdateTable(input *ddb.UpdateTableInput, optFns ...func(*UpdateTableOptions)) *UpdateTable {
	opts := UpdateTableOptions{}

	for _, opt := range optFns {
		opt(&opts)
	}

	return &UpdateTable{
		Promise: NewPromise(),
		input:   input,
		options: opts,
	}
}

// Await waits for the Operation to be complete and then returns a UpdateTableOutput and error
func (op *UpdateTable) Await() (*ddb.UpdateTableOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.UpdateTableOutput), err
}

// Invoke invokes the UpdateTable operation
func (op *UpdateTable) Invoke(ctx context.Context, client *ddb.Client) *UpdateTable {
	go op.DynoInvoke(ctx, client)
	return op
}

// DynoInvoke implements the Operation interface
func (op *UpdateTable) DynoInvoke(ctx context.Context, client *ddb.Client) {
	var (
		out *ddb.UpdateTableOutput
		err error
	)

	defer func() { op.SetResponse(out, err) }()

	for _, cb := range op.options.InputCallbacks {
		if out, err = cb.UpdateTableInputCallback(ctx, op.input); out != nil || err != nil {
			return
		}
	}

	if out, err = client.UpdateTable(ctx, op.input); err != nil {
		return
	}

	for _, cb := range op.options.OutputCallbacks {
		if err = cb.UpdateTableOutputCallback(ctx, out); err != nil {
			return
		}
	}
}

// NewUpdateTableInput creates a new UpdateTableInput
func NewUpdateTableInput(tableName *string) *ddb.UpdateTableInput {
	return &ddb.UpdateTableInput{
		TableName: tableName,
	}
}
