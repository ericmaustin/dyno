package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// ListBackups creates a new ListBackups operation, invokes and returns it
func (c *Client) ListBackups(ctx context.Context, input *ddb.ListBackupsInput, mw ...ListBackupsMiddleWare) *ListBackups {
	return NewListBackups(input, mw...).Invoke(ctx, c.ddb)
}

// ListBackups creates a new ListBackups operation, passes it to the pool where it is evoked and returns it
func (p *Pool) ListBackups(input *ddb.ListBackupsInput, mw ...ListBackupsMiddleWare) *ListBackups {
	op := NewListBackups(input, mw...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// ListBackupsContext represents an exhaustive ListBackups operation request context
type ListBackupsContext struct {
	context.Context
	Input  *ddb.ListBackupsInput
	Client *ddb.Client
}

// ListBackupsOutput represents the output for the ListBackups opration
type ListBackupsOutput struct {
	out *ddb.ListBackupsOutput
	err error
	mu sync.RWMutex
}

// Set sets the output
func (o *ListBackupsOutput) Set(out *ddb.ListBackupsOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *ListBackupsOutput) Get() (out *ddb.ListBackupsOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()
	return
}

// ListBackupsHandler represents a handler for ListBackups requests
type ListBackupsHandler interface {
	HandleListBackups(ctx *ListBackupsContext, output *ListBackupsOutput)
}

// ListBackupsHandlerFunc is a ListBackupsHandler function
type ListBackupsHandlerFunc func(ctx *ListBackupsContext, output *ListBackupsOutput)

// HandleListBackups implements ListBackupsHandler
func (h ListBackupsHandlerFunc) HandleListBackups(ctx *ListBackupsContext, output *ListBackupsOutput) {
	h(ctx, output)
}

// ListBackupsFinalHandler is the final ListBackupsHandler that executes a dynamodb ListBackups operation
type ListBackupsFinalHandler struct {}

// HandleListBackups implements the ListBackupsHandler
func (h *ListBackupsFinalHandler) HandleListBackups(ctx *ListBackupsContext, output *ListBackupsOutput) {
	output.Set(ctx.Client.ListBackups(ctx, ctx.Input))
}

// ListBackupsMiddleWare is a middleware function use for wrapping ListBackupsHandler requests
type ListBackupsMiddleWare interface {
	ListBackupsMiddleWare(next ListBackupsHandler) ListBackupsHandler
}

// ListBackupsMiddleWareFunc is a functional ListBackupsMiddleWare
type ListBackupsMiddleWareFunc func(next ListBackupsHandler) ListBackupsHandler

// ListBackupsMiddleWare implements the ListBackupsMiddleWare interface
func (mw ListBackupsMiddleWareFunc) ListBackupsMiddleWare(next ListBackupsHandler) ListBackupsHandler {
	return mw(next)
}

// ListBackups represents a ListBackups operation
type ListBackups struct {
	*Promise
	input       *ddb.ListBackupsInput
	middleWares []ListBackupsMiddleWare
}

// NewListBackups creates a new ListBackups
func NewListBackups(input *ddb.ListBackupsInput, mws ...ListBackupsMiddleWare) *ListBackups {
	return &ListBackups{
		Promise: NewPromise(),
		input:       input,
		middleWares: mws,
	}
}

// DynoInvoke invokes the ListBackups operation and returns a ListBackupsPromise
func (op *ListBackups) Invoke(ctx context.Context, client *ddb.Client) *ListBackups {
	go op.Invoke(ctx, client)

	return op
}

// DynoInvoke implements the Operation interface
func (op *ListBackups) Invoke(ctx context.Context, client *ddb.Client) {

	output := new(ListBackupsOutput)

	defer func() { op.SetResponse(output.Get())}()

	requestCtx := &ListBackupsContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	var h ListBackupsHandler

	h = new(ListBackupsFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].ListBackupsMiddleWare(h)
		}
	}

	h.HandleListBackups(requestCtx, output)
}

// Await waits for the ListBackupsPromise to be fulfilled and then returns a ListBackupsOutput and error
func (op *ListBackups) Await() (*ddb.ListBackupsOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.ListBackupsOutput), err
}

// NewListBackupsInput creates a new ListBackupsInput
func NewListBackupsInput() *ddb.ListBackupsInput {
	return &ddb.ListBackupsInput{}
}

// todo: ListAllBackups operation