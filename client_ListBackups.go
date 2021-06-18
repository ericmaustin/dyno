package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// NewListBackups creates a new ListBackups with this Client
func (c *Client) NewListBackups(input *ddb.ListBackupsInput, optFns ...func(*ListBackupsOptions)) *ListBackups {
	return NewListBackups(c.ddb, input, optFns...)
}

// ListBackups executes a scan api call with a ListBackupsInput
func (c *Client) ListBackups(ctx context.Context, input *ddb.ListBackupsInput, optFns ...func(*ListBackupsOptions)) (*ddb.ListBackupsOutput, error) {
	scan := c.NewListBackups(input, optFns...)
	scan.DynoInvoke(ctx)
	return scan.Await()
}

// ListBackupsInputCallback is a callback that is called on a given ListBackupsInput before a ListBackups operation api call executes
type ListBackupsInputCallback interface {
	ListBackupsInputCallback(context.Context, *ddb.ListBackupsInput) (*ddb.ListBackupsOutput, error)
}

// ListBackupsOutputCallback is a callback that is called on a given ListBackupsOutput after a ListBackups operation api call executes
type ListBackupsOutputCallback interface {
	ListBackupsOutputCallback(context.Context, *ddb.ListBackupsOutput) error
}

// ListBackupsInputCallbackFunc is ListBackupsOutputCallback function
type ListBackupsInputCallbackFunc func(context.Context, *ddb.ListBackupsInput) (*ddb.ListBackupsOutput, error)

// ListBackupsInputCallback implements the ListBackupsOutputCallback interface
func (cb ListBackupsInputCallbackFunc) ListBackupsInputCallback(ctx context.Context, input *ddb.ListBackupsInput) (*ddb.ListBackupsOutput, error) {
	return cb(ctx, input)
}

// ListBackupsOutputCallbackFunc is ListBackupsOutputCallback function
type ListBackupsOutputCallbackFunc func(context.Context, *ddb.ListBackupsOutput) error

// ListBackupsOutputCallback implements the ListBackupsOutputCallback interface
func (cb ListBackupsOutputCallbackFunc) ListBackupsOutputCallback(ctx context.Context, input *ddb.ListBackupsOutput) error {
	return cb(ctx, input)
}

// ListBackupsOptions represents options passed to the ListBackups operation
type ListBackupsOptions struct {
	//InputCallbacks are called before the ListBackups dynamodb api operation with the dynamodb.ListBackupsInput
	InputCallbacks []ListBackupsInputCallback
	//OutputCallbacks are called after the ListBackups dynamodb api operation with the dynamodb.ListBackupsOutput
	OutputCallbacks []ListBackupsOutputCallback
}

// ListBackupsWithInputCallback adds a ListBackupsInputCallbackFunc to the InputCallbacks
func ListBackupsWithInputCallback(cb ListBackupsInputCallbackFunc) func(*ListBackupsOptions) {
	return func(opt *ListBackupsOptions) {
		opt.InputCallbacks = append(opt.InputCallbacks, cb)
	}
}

// ListBackupsWithOutputCallback adds a ListBackupsOutputCallback to the OutputCallbacks
func ListBackupsWithOutputCallback(cb ListBackupsOutputCallback) func(*ListBackupsOptions) {
	return func(opt *ListBackupsOptions) {
		opt.OutputCallbacks = append(opt.OutputCallbacks, cb)
	}
}

// ListBackups represents a ListBackups operation
type ListBackups struct {
	*Promise
	client  *ddb.Client
	input   *ddb.ListBackupsInput
	options ListBackupsOptions
}

// NewListBackups creates a new ListBackups operation on the given client with a given ListBackupsInput and options
func NewListBackups(client *ddb.Client, input *ddb.ListBackupsInput, optFns ...func(*ListBackupsOptions)) *ListBackups {
	opts := ListBackupsOptions{}
	for _, opt := range optFns {
		opt(&opts)
	}
	return &ListBackups{
		Promise: NewPromise(),
		client:  client,
		input:   input,
		options: opts,
	}
}

// Await waits for the Operation to be complete and then returns a ListBackupsOutput and error
func (op *ListBackups) Await() (*ddb.ListBackupsOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}
	return out.(*ddb.ListBackupsOutput), err
}

// Invoke invokes the ListBackups operation
func (op *ListBackups) Invoke(ctx context.Context) *ListBackups {
	go op.DynoInvoke(ctx)
	return op
}

// DynoInvoke implements the Operation interface
func (op *ListBackups) DynoInvoke(ctx context.Context) {
	var (
		out *ddb.ListBackupsOutput
		err error
	)
	defer op.SetResponse(out, err)
	for _, cb := range op.options.InputCallbacks {
		if out, err = cb.ListBackupsInputCallback(ctx, op.input); out != nil || err != nil {
			return
		}
	}
	if out, err = op.client.ListBackups(ctx, op.input); err != nil {
		return
	}
	for _, cb := range op.options.OutputCallbacks {
		if err = cb.ListBackupsOutputCallback(ctx, out); err != nil {
			return
		}
	}
	return
}

// NewListBackupsInput creates a new ListBackupsInput
func NewListBackupsInput() *ddb.ListBackupsInput {
	return &ddb.ListBackupsInput{}
}
