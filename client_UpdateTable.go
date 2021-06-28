package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)


// UpdateTable executes UpdateTable operation and returns a UpdateTablePromise
func (c *Client) UpdateTable(ctx context.Context, input *ddb.UpdateTableInput, mw ...UpdateTableMiddleWare) *UpdateTablePromise {
	return NewUpdateTable(input, mw...).Invoke(ctx, c.ddb)
}

// UpdateTable executes a UpdateTable operation with a UpdateTableInput in this pool and returns the UpdateTablePromise
func (p *Pool) UpdateTable(input *ddb.UpdateTableInput, mw ...UpdateTableMiddleWare) *UpdateTablePromise {
	op := NewUpdateTable(input, mw...)

	if err := p.Do(op); err != nil {
		op.promise.SetResponse(nil, err)
	}

	return op.promise
}

// UpdateTableContext represents an exhaustive UpdateTable operation request context
type UpdateTableContext struct {
	context.Context
	input  *ddb.UpdateTableInput
	client *ddb.Client
}

// UpdateTablePromise represents a promise for the UpdateTable
type UpdateTablePromise struct {
	*Promise
}

// GetResponse returns the GetResponse output and error
// if Output has not been set yet nil is returned
func (p *UpdateTablePromise) GetResponse() (*ddb.UpdateTableOutput, error) {
	out, err := p.Promise.GetResponse()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.UpdateTableOutput), err
}

// Await waits for the UpdateTablePromise to be fulfilled and then returns a UpdateTableOutput and error
func (p *UpdateTablePromise) Await() (*ddb.UpdateTableOutput, error) {
	out, err := p.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.UpdateTableOutput), err
}

// newUpdateTablePromise returns a new UpdateTablePromise
func newUpdateTablePromise() *UpdateTablePromise {
	return &UpdateTablePromise{NewPromise()}
}

// UpdateTableHandler represents a handler for UpdateTable requests
type UpdateTableHandler interface {
	HandleUpdateTable(ctx *UpdateTableContext, promise *UpdateTablePromise)
}

// UpdateTableHandlerFunc is a UpdateTableHandler function
type UpdateTableHandlerFunc func(ctx *UpdateTableContext, promise *UpdateTablePromise)

// HandleUpdateTable implements UpdateTableHandler
func (h UpdateTableHandlerFunc) HandleUpdateTable(ctx *UpdateTableContext, promise *UpdateTablePromise) {
	h(ctx, promise)
}

// UpdateTableFinalHandler is the final UpdateTableHandler that executes a dynamodb UpdateTable operation
type UpdateTableFinalHandler struct {}

// HandleUpdateTable implements the UpdateTableHandler
func (h *UpdateTableFinalHandler) HandleUpdateTable(ctx *UpdateTableContext, promise *UpdateTablePromise) {
	promise.SetResponse(ctx.client.UpdateTable(ctx, ctx.input))
}

// UpdateTableMiddleWare is a middleware function use for wrapping UpdateTableHandler requests
type UpdateTableMiddleWare interface {
	UpdateTableMiddleWare(h UpdateTableHandler) UpdateTableHandler
}

// UpdateTableMiddleWareFunc is a functional UpdateTableMiddleWare
type UpdateTableMiddleWareFunc func(handler UpdateTableHandler) UpdateTableHandler

// UpdateTableMiddleWare implements the UpdateTableMiddleWare interface
func (mw UpdateTableMiddleWareFunc) UpdateTableMiddleWare(h UpdateTableHandler) UpdateTableHandler {
	return mw(h)
}

// UpdateTable represents a UpdateTable operation
type UpdateTable struct {
	promise     *UpdateTablePromise
	input       *ddb.UpdateTableInput
	middleWares []UpdateTableMiddleWare
}

// NewUpdateTable creates a new UpdateTable
func NewUpdateTable(input *ddb.UpdateTableInput, mws ...UpdateTableMiddleWare) *UpdateTable {
	return &UpdateTable{
		input:       input,
		middleWares: mws,
		promise:     newUpdateTablePromise(),
	}
}

// Invoke invokes the UpdateTable operation and returns a UpdateTablePromise
func (op *UpdateTable) Invoke(ctx context.Context, client *ddb.Client) *UpdateTablePromise {
	go op.DynoInvoke(ctx, client)

	return op.promise
}

// DynoInvoke implements the Operation interface
func (op *UpdateTable) DynoInvoke(ctx context.Context, client *ddb.Client) {

	requestCtx := &UpdateTableContext{
		Context: ctx,
		client:  client,
		input:   op.input,
	}

	var h UpdateTableHandler

	h = new(UpdateTableFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].UpdateTableMiddleWare(h)
		}
	}

	h.HandleUpdateTable(requestCtx, op.promise)
}

// NewUpdateTableInput creates a new UpdateTableInput
func NewUpdateTableInput(tableName *string) *ddb.UpdateTableInput {
	return &ddb.UpdateTableInput{
		TableName: tableName,
	}
}
