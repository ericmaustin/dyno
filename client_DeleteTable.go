package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
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
	HandleDeleteTable(ctx *DeleteTableContext, promise *DeleteTablePromise)
}

// DeleteTableHandlerFunc is a DeleteTableHandler function
type DeleteTableHandlerFunc func(ctx *DeleteTableContext, promise *DeleteTablePromise)

// HandleDeleteTable implements DeleteTableHandler
func (h DeleteTableHandlerFunc) HandleDeleteTable(ctx *DeleteTableContext, promise *DeleteTablePromise) {
	h(ctx, promise)
}

// DeleteTableMiddleWare is a middleware function use for wrapping DeleteTableHandler requests
type DeleteTableMiddleWare func(handler DeleteTableHandler) DeleteTableHandler

// DeleteTableFinalHandler returns the final DeleteTableHandler that executes a dynamodb DeleteTable operation
func DeleteTableFinalHandler() DeleteTableHandler {
	return DeleteTableHandlerFunc(func(ctx *DeleteTableContext, promise *DeleteTablePromise) {
		promise.SetResponse(ctx.client.DeleteTable(ctx, ctx.input))
	})
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

	requestCtx := &DeleteTableContext{
		Context: ctx,
		client:  client,
		input:   op.input,
	}

	h := DeleteTableFinalHandler()

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i](h)
		}
	}

	h.HandleDeleteTable(requestCtx, op.promise)
}

// NewDeleteTableInput creates a new DeleteTableInput
func NewDeleteTableInput(tableName *string) *ddb.DeleteTableInput {
	return &ddb.DeleteTableInput{TableName: tableName}
}
