package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// NewListTables creates a new ListTables with this Client
func (c *Client) NewListTables(input *ddb.ListTablesInput, optFns ...func(*ListTablesOptions)) *ListTables {
	return NewListTables(c.ddb, input, optFns...)
}

// ListTables executes a scan api call with a ListTablesInput
func (c *Client) ListTables(ctx context.Context, input *ddb.ListTablesInput, optFns ...func(*ListTablesOptions)) (*ddb.ListTablesOutput, error) {
	scan := c.NewListTables(input, optFns...)
	scan.DynoInvoke(ctx)
	return scan.Await()
}

// ListTablesInputCallback is a callback that is called on a given ListTablesInput before a ListTables operation api call executes
type ListTablesInputCallback interface {
	ListTablesInputCallback(context.Context, *ddb.ListTablesInput) (*ddb.ListTablesOutput, error)
}

// ListTablesOutputCallback is a callback that is called on a given ListTablesOutput after a ListTables operation api call executes
type ListTablesOutputCallback interface {
	ListTablesOutputCallback(context.Context, *ddb.ListTablesOutput) error
}

// ListTablesInputCallbackFunc is ListTablesOutputCallback function
type ListTablesInputCallbackFunc func(context.Context, *ddb.ListTablesInput) (*ddb.ListTablesOutput, error)

// ListTablesInputCallback implements the ListTablesOutputCallback interface
func (cb ListTablesInputCallbackFunc) ListTablesInputCallback(ctx context.Context, input *ddb.ListTablesInput) (*ddb.ListTablesOutput, error) {
	return cb(ctx, input)
}

// ListTablesOutputCallbackFunc is ListTablesOutputCallback function
type ListTablesOutputCallbackFunc func(context.Context, *ddb.ListTablesOutput) error

// ListTablesOutputCallback implements the ListTablesOutputCallback interface
func (cb ListTablesOutputCallbackFunc) ListTablesOutputCallback(ctx context.Context, input *ddb.ListTablesOutput) error {
	return cb(ctx, input)
}

// ListTablesOptions represents options passed to the ListTables operation
type ListTablesOptions struct {
	//InputCallbacks are called before the ListTables dynamodb api operation with the dynamodb.ListTablesInput
	InputCallbacks []ListTablesInputCallback
	//OutputCallbacks are called after the ListTables dynamodb api operation with the dynamodb.ListTablesOutput
	OutputCallbacks []ListTablesOutputCallback
}

// ListTablesWithInputCallback adds a ListTablesInputCallbackFunc to the InputCallbacks
func ListTablesWithInputCallback(cb ListTablesInputCallbackFunc) func(*ListTablesOptions) {
	return func(opt *ListTablesOptions) {
		opt.InputCallbacks = append(opt.InputCallbacks, cb)
	}
}

// ListTablesWithOutputCallback adds a ListTablesOutputCallback to the OutputCallbacks
func ListTablesWithOutputCallback(cb ListTablesOutputCallback) func(*ListTablesOptions) {
	return func(opt *ListTablesOptions) {
		opt.OutputCallbacks = append(opt.OutputCallbacks, cb)
	}
}

// ListTables represents a ListTables operation
type ListTables struct {
	*Promise
	client  *ddb.Client
	input   *ddb.ListTablesInput
	options ListTablesOptions
}

// NewListTables creates a new ListTables operation on the given client with a given ListTablesInput and options
func NewListTables(client *ddb.Client, input *ddb.ListTablesInput, optFns ...func(*ListTablesOptions)) *ListTables {
	opts := ListTablesOptions{}
	for _, opt := range optFns {
		opt(&opts)
	}
	return &ListTables{
		Promise: NewPromise(),
		client:  client,
		input:   input,
		options: opts,
	}
}

// Await waits for the Operation to be complete and then returns a ListTablesOutput and error
func (op *ListTables) Await() (*ddb.ListTablesOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}
	return out.(*ddb.ListTablesOutput), err
}

// Invoke invokes the ListTables operation
func (op *ListTables) Invoke(ctx context.Context) *ListTables {
	go op.DynoInvoke(ctx)
	return op
}

// DynoInvoke implements the Operation interface
func (op *ListTables) DynoInvoke(ctx context.Context) {
	var (
		out *ddb.ListTablesOutput
		err error
	)
	defer op.SetResponse(out, err)
	for _, cb := range op.options.InputCallbacks {
		if out, err = cb.ListTablesInputCallback(ctx, op.input); out != nil || err != nil {
			return
		}
	}
	if out, err = op.client.ListTables(ctx, op.input); err != nil {
		return
	}
	for _, cb := range op.options.OutputCallbacks {
		if err = cb.ListTablesOutputCallback(ctx, out); err != nil {
			return
		}
	}
	return
}

// NewListTablesInput creates a new ListTablesInput
func NewListTablesInput() *ddb.ListTablesInput {
	return &ddb.ListTablesInput{}
}
