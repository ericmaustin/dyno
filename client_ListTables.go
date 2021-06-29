package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// ListTables executes ListTables operation and returns a ListTablesPromise
func (c *Client) ListTables(ctx context.Context, input *ddb.ListTablesInput, mw ...ListTablesMiddleWare) *ListTablesPromise {
	return NewListTables(input, mw...).Invoke(ctx, c.ddb)
}

// ListTables executes a ListTables operation with a ListTablesInput in this pool and returns the ListTablesPromise
func (p *Pool) ListTables(input *ddb.ListTablesInput, mw ...ListTablesMiddleWare) *ListTablesPromise {
	op := NewListTables(input, mw...)

	if err := p.Do(op); err != nil {
		op.promise.SetResponse(nil, err)
	}

	return op.promise
}

// ListTablesContext represents an exhaustive ListTables operation request context
type ListTablesContext struct {
	context.Context
	input  *ddb.ListTablesInput
	client *ddb.Client
}

// ListTablesOutput represents the output for the ListTables opration
type ListTablesOutput struct {
	out *ddb.ListTablesOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *ListTablesOutput) Set(out *ddb.ListTablesOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *ListTablesOutput) Get() (out *ddb.ListTablesOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()
	return
}

// ListTablesPromise represents a promise for the ListTables
type ListTablesPromise struct {
	*Promise
}

// Await waits for the ListTablesPromise to be fulfilled and then returns a ListTablesOutput and error
func (p *ListTablesPromise) Await() (*ddb.ListTablesOutput, error) {
	out, err := p.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.ListTablesOutput), err
}

// newListTablesPromise returns a new ListTablesPromise
func newListTablesPromise() *ListTablesPromise {
	return &ListTablesPromise{NewPromise()}
}

// ListTablesHandler represents a handler for ListTables requests
type ListTablesHandler interface {
	HandleListTables(ctx *ListTablesContext, output *ListTablesOutput)
}

// ListTablesHandlerFunc is a ListTablesHandler function
type ListTablesHandlerFunc func(ctx *ListTablesContext, output *ListTablesOutput)

// HandleListTables implements ListTablesHandler
func (h ListTablesHandlerFunc) HandleListTables(ctx *ListTablesContext, output *ListTablesOutput) {
	h(ctx, output)
}

// ListTablesFinalHandler is the final ListTablesHandler that executes a dynamodb ListTables operation
type ListTablesFinalHandler struct{}

// HandleListTables implements the ListTablesHandler
func (h *ListTablesFinalHandler) HandleListTables(ctx *ListTablesContext, output *ListTablesOutput) {
	output.Set(ctx.client.ListTables(ctx, ctx.input))
}

// ListTablesMiddleWare is a middleware function use for wrapping ListTablesHandler requests
type ListTablesMiddleWare interface {
	ListTablesMiddleWare(next ListTablesHandler) ListTablesHandler
}

// ListTablesMiddleWareFunc is a functional ListTablesMiddleWare
type ListTablesMiddleWareFunc func(next ListTablesHandler) ListTablesHandler

// ListTablesMiddleWare implements the ListTablesMiddleWare interface
func (mw ListTablesMiddleWareFunc) ListTablesMiddleWare(next ListTablesHandler) ListTablesHandler {
	return mw(next)
}

// ListTables represents a ListTables operation
type ListTables struct {
	promise     *ListTablesPromise
	input       *ddb.ListTablesInput
	middleWares []ListTablesMiddleWare
}

// NewListTables creates a new ListTables
func NewListTables(input *ddb.ListTablesInput, mws ...ListTablesMiddleWare) *ListTables {
	return &ListTables{
		input:       input,
		middleWares: mws,
		promise:     newListTablesPromise(),
	}
}

// Invoke invokes the ListTables operation and returns a ListTablesPromise
func (op *ListTables) Invoke(ctx context.Context, client *ddb.Client) *ListTablesPromise {
	go op.DynoInvoke(ctx, client)

	return op.promise
}

// DynoInvoke implements the Operation interface
func (op *ListTables) DynoInvoke(ctx context.Context, client *ddb.Client) {

	output := new(ListTablesOutput)

	defer func() { op.promise.SetResponse(output.Get()) }()

	requestCtx := &ListTablesContext{
		Context: ctx,
		client:  client,
		input:   op.input,
	}

	var h ListTablesHandler

	h = new(ListTablesFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].ListTablesMiddleWare(h)
		}
	}

	h.HandleListTables(requestCtx, output)
}

// NewListTablesInput creates a new ListTablesInput
func NewListTablesInput() *ddb.ListTablesInput {
	return &ddb.ListTablesInput{}
}
