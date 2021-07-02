package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// DescribeGlobalTable executes DescribeGlobalTable operation and returns a DescribeGlobalTablePromise
func (c *Client) DescribeGlobalTable(ctx context.Context, input *ddb.DescribeGlobalTableInput, mw ...DescribeGlobalTableMiddleWare) *DescribeGlobalTable {
	return NewDescribeGlobalTable(input, mw...).Invoke(ctx, c.ddb)
}

// DescribeGlobalTable executes a DescribeGlobalTable operation with a DescribeGlobalTableInput in this pool and returns the DescribeGlobalTablePromise
func (p *Pool) DescribeGlobalTable(input *ddb.DescribeGlobalTableInput, mw ...DescribeGlobalTableMiddleWare) *DescribeGlobalTable {
	op := NewDescribeGlobalTable(input, mw...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// DescribeGlobalTableContext represents an exhaustive DescribeGlobalTable operation request context
type DescribeGlobalTableContext struct {
	context.Context
	Input  *ddb.DescribeGlobalTableInput
	Client *ddb.Client
}

// DescribeGlobalTableOutput represents the output for the DescribeGlobalTable opration
type DescribeGlobalTableOutput struct {
	out *ddb.DescribeGlobalTableOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *DescribeGlobalTableOutput) Set(out *ddb.DescribeGlobalTableOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *DescribeGlobalTableOutput) Get() (out *ddb.DescribeGlobalTableOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()
	return
}

// DescribeGlobalTableHandler represents a handler for DescribeGlobalTable requests
type DescribeGlobalTableHandler interface {
	HandleDescribeGlobalTable(ctx *DescribeGlobalTableContext, output *DescribeGlobalTableOutput)
}

// DescribeGlobalTableHandlerFunc is a DescribeGlobalTableHandler function
type DescribeGlobalTableHandlerFunc func(ctx *DescribeGlobalTableContext, output *DescribeGlobalTableOutput)

// HandleDescribeGlobalTable implements DescribeGlobalTableHandler
func (h DescribeGlobalTableHandlerFunc) HandleDescribeGlobalTable(ctx *DescribeGlobalTableContext, output *DescribeGlobalTableOutput) {
	h(ctx, output)
}

// DescribeGlobalTableFinalHandler is the final DescribeGlobalTableHandler that executes a dynamodb DescribeGlobalTable operation
type DescribeGlobalTableFinalHandler struct{}

// HandleDescribeGlobalTable implements the DescribeGlobalTableHandler
func (h *DescribeGlobalTableFinalHandler) HandleDescribeGlobalTable(ctx *DescribeGlobalTableContext, output *DescribeGlobalTableOutput) {
	output.Set(ctx.Client.DescribeGlobalTable(ctx, ctx.Input))
}

// DescribeGlobalTableMiddleWare is a middleware function use for wrapping DescribeGlobalTableHandler requests
type DescribeGlobalTableMiddleWare interface {
	DescribeGlobalTableMiddleWare(next DescribeGlobalTableHandler) DescribeGlobalTableHandler
}

// DescribeGlobalTableMiddleWareFunc is a functional DescribeGlobalTableMiddleWare
type DescribeGlobalTableMiddleWareFunc func(next DescribeGlobalTableHandler) DescribeGlobalTableHandler

// DescribeGlobalTableMiddleWare implements the DescribeGlobalTableMiddleWare interface
func (mw DescribeGlobalTableMiddleWareFunc) DescribeGlobalTableMiddleWare(next DescribeGlobalTableHandler) DescribeGlobalTableHandler {
	return mw(next)
}

// DescribeGlobalTable represents a DescribeGlobalTable operation
type DescribeGlobalTable struct {
	*Promise
	input       *ddb.DescribeGlobalTableInput
	middleWares []DescribeGlobalTableMiddleWare
}

// NewDescribeGlobalTable creates a new DescribeGlobalTable
func NewDescribeGlobalTable(input *ddb.DescribeGlobalTableInput, mws ...DescribeGlobalTableMiddleWare) *DescribeGlobalTable {
	return &DescribeGlobalTable{
		Promise: NewPromise(),
		input:       input,
		middleWares: mws,
	}
}

// Invoke invokes the DescribeGlobalTable operation and returns a DescribeGlobalTablePromise
func (op *DescribeGlobalTable) Invoke(ctx context.Context, client *ddb.Client) *DescribeGlobalTable {
	go op.DynoInvoke(ctx, client)

	return op
}

// DynoInvoke implements the Operation interface
func (op *DescribeGlobalTable) DynoInvoke(ctx context.Context, client *ddb.Client) {
	output := new(DescribeGlobalTableOutput)

	defer func() { op.SetResponse(output.Get()) }()

	requestCtx := &DescribeGlobalTableContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	var h DescribeGlobalTableHandler

	h = new(DescribeGlobalTableFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].DescribeGlobalTableMiddleWare(h)
		}
	}

	h.HandleDescribeGlobalTable(requestCtx, output)
}

// Await waits for the DescribeGlobalTablePromise to be fulfilled and then returns a DescribeGlobalTableOutput and error
func (op *DescribeGlobalTable) Await() (*ddb.DescribeGlobalTableOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.DescribeGlobalTableOutput), err
}

// NewDescribeGlobalTableInput creates a new DescribeGlobalTableInput
func NewDescribeGlobalTableInput(tableName *string) *ddb.DescribeGlobalTableInput {
	return &ddb.DescribeGlobalTableInput{
		GlobalTableName: tableName,
	}
}
