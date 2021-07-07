package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// UpdateTable creates a new UpdateTable, invokes and returns it
func (c *Client) UpdateTable(ctx context.Context, input *ddb.UpdateTableInput, mw ...UpdateTableMiddleWare) *UpdateTable {
	return NewUpdateTable(input, mw...).Invoke(ctx, c.ddb)
}

// UpdateTable creates a new UpdateTable, passes it to the Pool and then returns the UpdateTable
func (p *Pool) UpdateTable(input *ddb.UpdateTableInput, mw ...UpdateTableMiddleWare) *UpdateTable {
	op := NewUpdateTable(input, mw...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// UpdateTableContext represents an exhaustive UpdateTable operation request context
type UpdateTableContext struct {
	context.Context
	Input  *ddb.UpdateTableInput
	Client *ddb.Client
}

// UpdateTableOutput represents the output for the UpdateTable opration
type UpdateTableOutput struct {
	out *ddb.UpdateTableOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *UpdateTableOutput) Set(out *ddb.UpdateTableOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *UpdateTableOutput) Get() (out *ddb.UpdateTableOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()

	return
}

// UpdateTableHandler represents a handler for UpdateTable requests
type UpdateTableHandler interface {
	HandleUpdateTable(ctx *UpdateTableContext, output *UpdateTableOutput)
}

// UpdateTableHandlerFunc is a UpdateTableHandler function
type UpdateTableHandlerFunc func(ctx *UpdateTableContext, output *UpdateTableOutput)

// HandleUpdateTable implements UpdateTableHandler
func (h UpdateTableHandlerFunc) HandleUpdateTable(ctx *UpdateTableContext, output *UpdateTableOutput) {
	h(ctx, output)
}

// UpdateTableFinalHandler is the final UpdateTableHandler that executes a dynamodb UpdateTable operation
type UpdateTableFinalHandler struct{}

// HandleUpdateTable implements the UpdateTableHandler
func (h *UpdateTableFinalHandler) HandleUpdateTable(ctx *UpdateTableContext, output *UpdateTableOutput) {
	output.Set(ctx.Client.UpdateTable(ctx, ctx.Input))
}

// UpdateTableMiddleWare is a middleware function use for wrapping UpdateTableHandler requests
type UpdateTableMiddleWare interface {
	UpdateTableMiddleWare(next UpdateTableHandler) UpdateTableHandler
}

// UpdateTableMiddleWareFunc is a functional UpdateTableMiddleWare
type UpdateTableMiddleWareFunc func(next UpdateTableHandler) UpdateTableHandler

// UpdateTableMiddleWare implements the UpdateTableMiddleWare interface
func (mw UpdateTableMiddleWareFunc) UpdateTableMiddleWare(next UpdateTableHandler) UpdateTableHandler {
	return mw(next)
}

// UpdateTable represents a UpdateTable operation
type UpdateTable struct {
	*Promise
	input       *ddb.UpdateTableInput
	middleWares []UpdateTableMiddleWare
}

// NewUpdateTable creates a new UpdateTable
func NewUpdateTable(input *ddb.UpdateTableInput, mws ...UpdateTableMiddleWare) *UpdateTable {
	return &UpdateTable{
		Promise: NewPromise(),
		input:       input,
		middleWares: mws,
	}
}

// DynoInvoke invokes the UpdateTable operation and returns a UpdateTablePromise
func (op *UpdateTable) Invoke(ctx context.Context, client *ddb.Client) *UpdateTable {
	go op.Invoke(ctx, client)

	return op
}

// DynoInvoke implements the Operation interface
func (op *UpdateTable) Invoke(ctx context.Context, client *ddb.Client) {
	output := new(UpdateTableOutput)

	defer func() { op.SetResponse(output.Get()) }()

	requestCtx := &UpdateTableContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
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

	h.HandleUpdateTable(requestCtx, output)
}

// Await waits for the UpdateTablePromise to be fulfilled and then returns a UpdateTableOutput and error
func (op *UpdateTable) Await() (*ddb.UpdateTableOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.UpdateTableOutput), err
}

// NewUpdateTableInput creates a new UpdateTableInput
func NewUpdateTableInput(tableName *string) *ddb.UpdateTableInput {
	return &ddb.UpdateTableInput{
		TableName: tableName,
	}
}
