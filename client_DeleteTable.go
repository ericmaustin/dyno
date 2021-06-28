package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// DeleteTable executes DeleteTable operation and returns a DeleteTablePromise
func (c *Client) DeleteTable(ctx context.Context, input *ddb.DeleteTableInput, mw ...DeleteTableMiddleWare) *DeleteTablePromise {
	return NewDeleteTable(input, mw...).Invoke(ctx, c.ddb)
}

// DeleteTable executes a DeleteTable operation with a DeleteTableInput in this pool and returns the DeleteTablePromise
func (p *Pool) DeleteTable(input *ddb.DeleteTableInput, mw ...DeleteTableMiddleWare) *DeleteTablePromise {
	op := NewDeleteTable(input, mw...)

	if err := p.Do(op); err != nil {
		op.promise.SetResponse(nil, err)
	}

	return op.promise
}

// DeleteTableContext represents an exhaustive DeleteTable operation request context
type DeleteTableContext struct {
	context.Context
	input  *ddb.DeleteTableInput
	client *ddb.Client
}

// DeleteTableOutput represents the output for the DeleteTable opration
type DeleteTableOutput struct {
	out *ddb.DeleteTableOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *DeleteTableOutput) Set(out *ddb.DeleteTableOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *DeleteTableOutput) Get() (out *ddb.DeleteTableOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()
	return
}

// DeleteTablePromise represents a promise for the DeleteTable
type DeleteTablePromise struct {
	*Promise
}

// Await waits for the DeleteTablePromise to be fulfilled and then returns a DeleteTableOutput and error
func (p *DeleteTablePromise) Await() (*ddb.DeleteTableOutput, error) {
	out, err := p.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.DeleteTableOutput), err
}

// newDeleteTablePromise returns a new DeleteTablePromise
func newDeleteTablePromise() *DeleteTablePromise {
	return &DeleteTablePromise{NewPromise()}
}

// DeleteTableHandler represents a handler for DeleteTable requests
type DeleteTableHandler interface {
	HandleDeleteTable(ctx *DeleteTableContext, output *DeleteTableOutput)
}

// DeleteTableHandlerFunc is a DeleteTableHandler function
type DeleteTableHandlerFunc func(ctx *DeleteTableContext, output *DeleteTableOutput)

// HandleDeleteTable implements DeleteTableHandler
func (h DeleteTableHandlerFunc) HandleDeleteTable(ctx *DeleteTableContext, output *DeleteTableOutput) {
	h(ctx, output)
}

// DeleteTableFinalHandler is the final DeleteTableHandler that executes a dynamodb DeleteTable operation
type DeleteTableFinalHandler struct{}

// HandleDeleteTable implements the DeleteTableHandler
func (h *DeleteTableFinalHandler) HandleDeleteTable(ctx *DeleteTableContext, output *DeleteTableOutput) {
	output.Set(ctx.client.DeleteTable(ctx, ctx.input))
}

// DeleteTableMiddleWare is a middleware function use for wrapping DeleteTableHandler requests
type DeleteTableMiddleWare interface {
	DeleteTableMiddleWare(h DeleteTableHandler) DeleteTableHandler
}

// DeleteTableMiddleWareFunc is a functional DeleteTableMiddleWare
type DeleteTableMiddleWareFunc func(handler DeleteTableHandler) DeleteTableHandler

// DeleteTableMiddleWare implements the DeleteTableMiddleWare interface
func (mw DeleteTableMiddleWareFunc) DeleteTableMiddleWare(h DeleteTableHandler) DeleteTableHandler {
	return mw(h)
}

// DeleteTable represents a DeleteTable operation
type DeleteTable struct {
	promise     *DeleteTablePromise
	input       *ddb.DeleteTableInput
	middleWares []DeleteTableMiddleWare
}

// NewDeleteTable creates a new DeleteTable
func NewDeleteTable(input *ddb.DeleteTableInput, mws ...DeleteTableMiddleWare) *DeleteTable {
	return &DeleteTable{
		input:       input,
		middleWares: mws,
		promise:     newDeleteTablePromise(),
	}
}

// Invoke invokes the DeleteTable operation and returns a DeleteTablePromise
func (op *DeleteTable) Invoke(ctx context.Context, client *ddb.Client) *DeleteTablePromise {
	go op.DynoInvoke(ctx, client)

	return op.promise
}

// DynoInvoke implements the Operation interface
func (op *DeleteTable) DynoInvoke(ctx context.Context, client *ddb.Client) {

	output := new(DeleteTableOutput)

	defer func() { op.promise.SetResponse(output.Get()) }()

	requestCtx := &DeleteTableContext{
		Context: ctx,
		client:  client,
		input:   op.input,
	}

	var h DeleteTableHandler

	h = new(DeleteTableFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].DeleteTableMiddleWare(h)
		}
	}

	h.HandleDeleteTable(requestCtx, output)
}

// NewDeleteTableInput creates a new DeleteTableInput
func NewDeleteTableInput(tableName *string) *ddb.DeleteTableInput {
	return &ddb.DeleteTableInput{TableName: tableName}
}
