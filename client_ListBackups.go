package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// ListBackups executes ListBackups operation and returns a ListBackupsPromise
func (c *Client) ListBackups(ctx context.Context, input *ddb.ListBackupsInput, mw ...ListBackupsMiddleWare) *ListBackupsPromise {
	return NewListBackups(input, mw...).Invoke(ctx, c.ddb)
}

// ListBackups executes a ListBackups operation with a ListBackupsInput in this pool and returns the ListBackupsPromise
func (p *Pool) ListBackups(input *ddb.ListBackupsInput, mw ...ListBackupsMiddleWare) *ListBackupsPromise {
	op := NewListBackups(input, mw...)

	if err := p.Do(op); err != nil {
		op.promise.SetResponse(nil, err)
	}

	return op.promise
}

// ListBackupsContext represents an exhaustive ListBackups operation request context
type ListBackupsContext struct {
	context.Context
	input  *ddb.ListBackupsInput
	client *ddb.Client
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

// ListBackupsPromise represents a promise for the ListBackups
type ListBackupsPromise struct {
	*Promise
}

// Await waits for the ListBackupsPromise to be fulfilled and then returns a ListBackupsOutput and error
func (p *ListBackupsPromise) Await() (*ddb.ListBackupsOutput, error) {
	out, err := p.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.ListBackupsOutput), err
}

// newListBackupsPromise returns a new ListBackupsPromise
func newListBackupsPromise() *ListBackupsPromise {
	return &ListBackupsPromise{NewPromise()}
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
	output.Set(ctx.client.ListBackups(ctx, ctx.input))
}

// ListBackupsMiddleWare is a middleware function use for wrapping ListBackupsHandler requests
type ListBackupsMiddleWare interface {
	ListBackupsMiddleWare(h ListBackupsHandler) ListBackupsHandler
}

// ListBackupsMiddleWareFunc is a functional ListBackupsMiddleWare
type ListBackupsMiddleWareFunc func(handler ListBackupsHandler) ListBackupsHandler

// ListBackupsMiddleWare implements the ListBackupsMiddleWare interface
func (mw ListBackupsMiddleWareFunc) ListBackupsMiddleWare(h ListBackupsHandler) ListBackupsHandler {
	return mw(h)
}

// ListBackups represents a ListBackups operation
type ListBackups struct {
	promise     *ListBackupsPromise
	input       *ddb.ListBackupsInput
	middleWares []ListBackupsMiddleWare
}

// NewListBackups creates a new ListBackups
func NewListBackups(input *ddb.ListBackupsInput, mws ...ListBackupsMiddleWare) *ListBackups {
	return &ListBackups{
		input:       input,
		middleWares: mws,
		promise:     newListBackupsPromise(),
	}
}

// Invoke invokes the ListBackups operation and returns a ListBackupsPromise
func (op *ListBackups) Invoke(ctx context.Context, client *ddb.Client) *ListBackupsPromise {
	go op.DynoInvoke(ctx, client)

	return op.promise
}

// DynoInvoke implements the Operation interface
func (op *ListBackups) DynoInvoke(ctx context.Context, client *ddb.Client) {

	output := new(ListBackupsOutput)

	defer func() { op.promise.SetResponse(output.Get())}()

	requestCtx := &ListBackupsContext{
		Context: ctx,
		client:  client,
		input:   op.input,
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

// NewListBackupsInput creates a new ListBackupsInput
func NewListBackupsInput() *ddb.ListBackupsInput {
	return &ddb.ListBackupsInput{}
}
