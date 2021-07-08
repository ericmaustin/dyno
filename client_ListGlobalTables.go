package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// ListGlobalTables executes ListGlobalTables operation and returns a ListGlobalTables
func (c *Client) ListGlobalTables(ctx context.Context, input *ddb.ListGlobalTablesInput, mw ...ListGlobalTablesMiddleWare) *ListGlobalTables {
	return NewListGlobalTables(input, mw...).Invoke(ctx, c.ddb)
}

// ListGlobalTables executes a ListGlobalTables operation with a ListGlobalTablesInput in this pool and returns it
func (p *Pool) ListGlobalTables(input *ddb.ListGlobalTablesInput, mw ...ListGlobalTablesMiddleWare) *ListGlobalTables {
	op := NewListGlobalTables(input, mw...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// ListGlobalTablesContext represents an exhaustive ListGlobalTables operation request context
type ListGlobalTablesContext struct {
	context.Context
	Input  *ddb.ListGlobalTablesInput
	Client *ddb.Client
}

// ListGlobalTablesOutput represents the output for the ListGlobalTables operation
type ListGlobalTablesOutput struct {
	out *ddb.ListGlobalTablesOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *ListGlobalTablesOutput) Set(out *ddb.ListGlobalTablesOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *ListGlobalTablesOutput) Get() (out *ddb.ListGlobalTablesOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()
	return
}

// ListGlobalTablesHandler represents a handler for ListGlobalTables requests
type ListGlobalTablesHandler interface {
	HandleListGlobalTables(ctx *ListGlobalTablesContext, output *ListGlobalTablesOutput)
}

// ListGlobalTablesHandlerFunc is a ListGlobalTablesHandler function
type ListGlobalTablesHandlerFunc func(ctx *ListGlobalTablesContext, output *ListGlobalTablesOutput)

// HandleListGlobalTables implements ListGlobalTablesHandler
func (h ListGlobalTablesHandlerFunc) HandleListGlobalTables(ctx *ListGlobalTablesContext, output *ListGlobalTablesOutput) {
	h(ctx, output)
}

// ListGlobalTablesFinalHandler is the final ListGlobalTablesHandler that executes a dynamodb ListGlobalTables operation
type ListGlobalTablesFinalHandler struct{}

// HandleListGlobalTables implements the ListGlobalTablesHandler
func (h *ListGlobalTablesFinalHandler) HandleListGlobalTables(ctx *ListGlobalTablesContext, output *ListGlobalTablesOutput) {
	output.Set(ctx.Client.ListGlobalTables(ctx, ctx.Input))
}

// ListGlobalTablesMiddleWare is a middleware function use for wrapping ListGlobalTablesHandler requests
type ListGlobalTablesMiddleWare interface {
	ListGlobalTablesMiddleWare(next ListGlobalTablesHandler) ListGlobalTablesHandler
}

// ListGlobalTablesMiddleWareFunc is a functional ListGlobalTablesMiddleWare
type ListGlobalTablesMiddleWareFunc func(next ListGlobalTablesHandler) ListGlobalTablesHandler

// ListGlobalTablesMiddleWare implements the ListGlobalTablesMiddleWare interface
func (mw ListGlobalTablesMiddleWareFunc) ListGlobalTablesMiddleWare(next ListGlobalTablesHandler) ListGlobalTablesHandler {
	return mw(next)
}

// ListGlobalTables represents a ListGlobalTables operation
type ListGlobalTables struct {
	*Promise
	input       *ddb.ListGlobalTablesInput
	middleWares []ListGlobalTablesMiddleWare
}

// NewListGlobalTables creates a new ListGlobalTables
func NewListGlobalTables(input *ddb.ListGlobalTablesInput, mws ...ListGlobalTablesMiddleWare) *ListGlobalTables {
	return &ListGlobalTables{
		Promise: NewPromise(),
		input:       input,
		middleWares: mws,
	}
}

// Invoke invokes the ListGlobalTables operation in a goroutine and returns a BatchGetItemAllPromise
func (op *ListGlobalTables) Invoke(ctx context.Context, client *ddb.Client) *ListGlobalTables {
	op.SetWaiting() // promise now waiting for a response
	go op.invoke(ctx, client)

	return op
}

// DynoInvoke implements the Operation interface
func (op *ListGlobalTables) DynoInvoke(ctx context.Context, client *ddb.Client) {
	op.SetWaiting() // promise ≈now waiting for a response
	op.invoke(ctx, client)
}

// invoke invokes the ListGlobalTables operation
func (op *ListGlobalTables) invoke(ctx context.Context, client *ddb.Client) {
	output := new(ListGlobalTablesOutput)

	defer func() { op.SetResponse(output.Get()) }()

	requestCtx := &ListGlobalTablesContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	var h ListGlobalTablesHandler

	h = new(ListGlobalTablesFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].ListGlobalTablesMiddleWare(h)
		}
	}

	h.HandleListGlobalTables(requestCtx, output)
}

// Await waits for the ListGlobalTablesPromise to be fulfilled and then returns a ListGlobalTablesOutput and error
func (op *ListGlobalTables) Await() (*ddb.ListGlobalTablesOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.ListGlobalTablesOutput), err
}

// NewListGlobalTablesInput creates a new ListTablesInput
func NewListGlobalTablesInput() *ddb.ListGlobalTablesInput {
	return &ddb.ListGlobalTablesInput{}
}

// todo: ListAllTables operation