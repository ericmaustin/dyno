package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// UpdateTableReplicaAutoScaling creates a new UpdateTableReplicaAutoScaling, invokes and returns it
func (c *Client) UpdateTableReplicaAutoScaling(ctx context.Context, input *ddb.UpdateTableReplicaAutoScalingInput, mw ...UpdateTableReplicaAutoScalingMiddleWare) *UpdateTableReplicaAutoScaling {
	return NewUpdateTableReplicaAutoScaling(input, mw...).Invoke(ctx, c.ddb)
}

// UpdateTableReplicaAutoScaling creates a new UpdateTableReplicaAutoScaling, passes it to the Pool and then returns the UpdateTableReplicaAutoScaling
func (p *Pool) UpdateTableReplicaAutoScaling(input *ddb.UpdateTableReplicaAutoScalingInput, mw ...UpdateTableReplicaAutoScalingMiddleWare) *UpdateTableReplicaAutoScaling {
	op := NewUpdateTableReplicaAutoScaling(input, mw...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// UpdateTableReplicaAutoScalingContext represents an exhaustive UpdateTableReplicaAutoScaling operation request context
type UpdateTableReplicaAutoScalingContext struct {
	context.Context
	input  *ddb.UpdateTableReplicaAutoScalingInput
	client *ddb.Client
}

// UpdateTableReplicaAutoScalingOutput represents the output for the UpdateTableReplicaAutoScaling opration
type UpdateTableReplicaAutoScalingOutput struct {
	out *ddb.UpdateTableReplicaAutoScalingOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *UpdateTableReplicaAutoScalingOutput) Set(out *ddb.UpdateTableReplicaAutoScalingOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *UpdateTableReplicaAutoScalingOutput) Get() (out *ddb.UpdateTableReplicaAutoScalingOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()

	return
}

// UpdateTableReplicaAutoScalingHandler represents a handler for UpdateTableReplicaAutoScaling requests
type UpdateTableReplicaAutoScalingHandler interface {
	HandleUpdateTableReplicaAutoScaling(ctx *UpdateTableReplicaAutoScalingContext, output *UpdateTableReplicaAutoScalingOutput)
}

// UpdateTableReplicaAutoScalingHandlerFunc is a UpdateTableReplicaAutoScalingHandler function
type UpdateTableReplicaAutoScalingHandlerFunc func(ctx *UpdateTableReplicaAutoScalingContext, output *UpdateTableReplicaAutoScalingOutput)

// HandleUpdateTableReplicaAutoScaling implements UpdateTableReplicaAutoScalingHandler
func (h UpdateTableReplicaAutoScalingHandlerFunc) HandleUpdateTableReplicaAutoScaling(ctx *UpdateTableReplicaAutoScalingContext, output *UpdateTableReplicaAutoScalingOutput) {
	h(ctx, output)
}

// UpdateTableReplicaAutoScalingFinalHandler is the final UpdateTableReplicaAutoScalingHandler that executes a dynamodb UpdateTableReplicaAutoScaling operation
type UpdateTableReplicaAutoScalingFinalHandler struct{}

// HandleUpdateTableReplicaAutoScaling implements the UpdateTableReplicaAutoScalingHandler
func (h *UpdateTableReplicaAutoScalingFinalHandler) HandleUpdateTableReplicaAutoScaling(ctx *UpdateTableReplicaAutoScalingContext, output *UpdateTableReplicaAutoScalingOutput) {
	output.Set(ctx.client.UpdateTableReplicaAutoScaling(ctx, ctx.input))
}

// UpdateTableReplicaAutoScalingMiddleWare is a middleware function use for wrapping UpdateTableReplicaAutoScalingHandler requests
type UpdateTableReplicaAutoScalingMiddleWare interface {
	UpdateTableReplicaAutoScalingMiddleWare(next UpdateTableReplicaAutoScalingHandler) UpdateTableReplicaAutoScalingHandler
}

// UpdateTableReplicaAutoScalingMiddleWareFunc is a functional UpdateTableReplicaAutoScalingMiddleWare
type UpdateTableReplicaAutoScalingMiddleWareFunc func(next UpdateTableReplicaAutoScalingHandler) UpdateTableReplicaAutoScalingHandler

// UpdateTableReplicaAutoScalingMiddleWare implements the UpdateTableReplicaAutoScalingMiddleWare interface
func (mw UpdateTableReplicaAutoScalingMiddleWareFunc) UpdateTableReplicaAutoScalingMiddleWare(next UpdateTableReplicaAutoScalingHandler) UpdateTableReplicaAutoScalingHandler {
	return mw(next)
}

// UpdateTableReplicaAutoScaling represents a UpdateTableReplicaAutoScaling operation
type UpdateTableReplicaAutoScaling struct {
	*Promise
	input       *ddb.UpdateTableReplicaAutoScalingInput
	middleWares []UpdateTableReplicaAutoScalingMiddleWare
}

// NewUpdateTableReplicaAutoScaling creates a new UpdateTableReplicaAutoScaling
func NewUpdateTableReplicaAutoScaling(input *ddb.UpdateTableReplicaAutoScalingInput, mws ...UpdateTableReplicaAutoScalingMiddleWare) *UpdateTableReplicaAutoScaling {
	return &UpdateTableReplicaAutoScaling{
		Promise: NewPromise(),
		input:       input,
		middleWares: mws,
	}
}

// Invoke invokes the UpdateTableReplicaAutoScaling operation and returns a UpdateTableReplicaAutoScalingPromise
func (op *UpdateTableReplicaAutoScaling) Invoke(ctx context.Context, client *ddb.Client) *UpdateTableReplicaAutoScaling {
	go op.DynoInvoke(ctx, client)

	return op
}

// DynoInvoke implements the Operation interface
func (op *UpdateTableReplicaAutoScaling) DynoInvoke(ctx context.Context, client *ddb.Client) {
	output := new(UpdateTableReplicaAutoScalingOutput)

	defer func() { op.SetResponse(output.Get()) }()

	requestCtx := &UpdateTableReplicaAutoScalingContext{
		Context: ctx,
		client:  client,
		input:   op.input,
	}

	var h UpdateTableReplicaAutoScalingHandler

	h = new(UpdateTableReplicaAutoScalingFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].UpdateTableReplicaAutoScalingMiddleWare(h)
		}
	}

	h.HandleUpdateTableReplicaAutoScaling(requestCtx, output)
}

// Await waits for the UpdateTableReplicaAutoScalingPromise to be fulfilled and then returns a UpdateTableReplicaAutoScalingOutput and error
func (op *UpdateTableReplicaAutoScaling) Await() (*ddb.UpdateTableReplicaAutoScalingOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.UpdateTableReplicaAutoScalingOutput), err
}

// NewUpdateTableReplicaAutoScalingInput creates a new UpdateTableReplicaAutoScalingInput
func NewUpdateTableReplicaAutoScalingInput(tableName *string) *ddb.UpdateTableReplicaAutoScalingInput {
	return &ddb.UpdateTableReplicaAutoScalingInput{
		TableName: tableName,
	}
}

// todo: add UpdateTableReplicaAutoScalingBuilder
