package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"sync"
)

// UpdateTimeToLive creates a new UpdateTimeToLive, invokes and returns it
func (c *Client) UpdateTimeToLive(ctx context.Context, input *ddb.UpdateTimeToLiveInput, mw ...UpdateTimeToLiveMiddleWare) *UpdateTimeToLive {
	return NewUpdateTimeToLive(input, mw...).Invoke(ctx, c.ddb)
}

// UpdateTimeToLive creates a new UpdateTimeToLive, passes it to the Pool and then returns the UpdateTimeToLive
func (p *Pool) UpdateTimeToLive(input *ddb.UpdateTimeToLiveInput, mw ...UpdateTimeToLiveMiddleWare) *UpdateTimeToLive {
	op := NewUpdateTimeToLive(input, mw...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// UpdateTimeToLiveContext represents an exhaustive UpdateTimeToLive operation request context
type UpdateTimeToLiveContext struct {
	context.Context
	Input  *ddb.UpdateTimeToLiveInput
	Client *ddb.Client
}

// UpdateTimeToLiveOutput represents the output for the UpdateTimeToLive opration
type UpdateTimeToLiveOutput struct {
	out *ddb.UpdateTimeToLiveOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *UpdateTimeToLiveOutput) Set(out *ddb.UpdateTimeToLiveOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *UpdateTimeToLiveOutput) Get() (out *ddb.UpdateTimeToLiveOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()

	return
}

// UpdateTimeToLiveHandler represents a handler for UpdateTimeToLive requests
type UpdateTimeToLiveHandler interface {
	HandleUpdateTimeToLive(ctx *UpdateTimeToLiveContext, output *UpdateTimeToLiveOutput)
}

// UpdateTimeToLiveHandlerFunc is a UpdateTimeToLiveHandler function
type UpdateTimeToLiveHandlerFunc func(ctx *UpdateTimeToLiveContext, output *UpdateTimeToLiveOutput)

// HandleUpdateTimeToLive implements UpdateTimeToLiveHandler
func (h UpdateTimeToLiveHandlerFunc) HandleUpdateTimeToLive(ctx *UpdateTimeToLiveContext, output *UpdateTimeToLiveOutput) {
	h(ctx, output)
}

// UpdateTimeToLiveFinalHandler is the final UpdateTimeToLiveHandler that executes a dynamodb UpdateTimeToLive operation
type UpdateTimeToLiveFinalHandler struct{}

// HandleUpdateTimeToLive implements the UpdateTimeToLiveHandler
func (h *UpdateTimeToLiveFinalHandler) HandleUpdateTimeToLive(ctx *UpdateTimeToLiveContext, output *UpdateTimeToLiveOutput) {
	output.Set(ctx.Client.UpdateTimeToLive(ctx, ctx.Input))
}

// UpdateTimeToLiveMiddleWare is a middleware function use for wrapping UpdateTimeToLiveHandler requests
type UpdateTimeToLiveMiddleWare interface {
	UpdateTimeToLiveMiddleWare(next UpdateTimeToLiveHandler) UpdateTimeToLiveHandler
}

// UpdateTimeToLiveMiddleWareFunc is a functional UpdateTimeToLiveMiddleWare
type UpdateTimeToLiveMiddleWareFunc func(next UpdateTimeToLiveHandler) UpdateTimeToLiveHandler

// UpdateTimeToLiveMiddleWare implements the UpdateTimeToLiveMiddleWare interface
func (mw UpdateTimeToLiveMiddleWareFunc) UpdateTimeToLiveMiddleWare(next UpdateTimeToLiveHandler) UpdateTimeToLiveHandler {
	return mw(next)
}

// UpdateTimeToLive represents a UpdateTimeToLive operation
type UpdateTimeToLive struct {
	*Promise
	input       *ddb.UpdateTimeToLiveInput
	middleWares []UpdateTimeToLiveMiddleWare
}

// NewUpdateTimeToLive creates a new UpdateTimeToLive
func NewUpdateTimeToLive(input *ddb.UpdateTimeToLiveInput, mws ...UpdateTimeToLiveMiddleWare) *UpdateTimeToLive {
	return &UpdateTimeToLive{
		Promise: NewPromise(),
		input:       input,
		middleWares: mws,
	}
}

// Invoke invokes the UpdateTimeToLive operation and returns a UpdateTimeToLivePromise
func (op *UpdateTimeToLive) Invoke(ctx context.Context, client *ddb.Client) *UpdateTimeToLive {
	go op.DynoInvoke(ctx, client)

	return op
}

// DynoInvoke implements the Operation interface
func (op *UpdateTimeToLive) DynoInvoke(ctx context.Context, client *ddb.Client) {
	output := new(UpdateTimeToLiveOutput)

	defer func() { op.SetResponse(output.Get()) }()

	requestCtx := &UpdateTimeToLiveContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	var h UpdateTimeToLiveHandler

	h = new(UpdateTimeToLiveFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].UpdateTimeToLiveMiddleWare(h)
		}
	}

	h.HandleUpdateTimeToLive(requestCtx, output)
}

// Await waits for the UpdateTimeToLivePromise to be fulfilled and then returns a UpdateTimeToLiveOutput and error
func (op *UpdateTimeToLive) Await() (*ddb.UpdateTimeToLiveOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.UpdateTimeToLiveOutput), err
}

// NewUpdateTimeToLiveInput creates a new UpdateTimeToLiveInput
func NewUpdateTimeToLiveInput(tableName *string, specification *types.TimeToLiveSpecification) *ddb.UpdateTimeToLiveInput {
	return &ddb.UpdateTimeToLiveInput{
		TableName: tableName,
		TimeToLiveSpecification: specification,
	}
}

// todo: add UpdateTimeToLiveBuilder
