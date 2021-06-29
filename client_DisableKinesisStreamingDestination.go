package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// DisableKinesisStreamingDestination executes DisableKinesisStreamingDestination operation and returns a DisableKinesisStreamingDestinationPromise
func (c *Client) DisableKinesisStreamingDestination(ctx context.Context, input *ddb.DisableKinesisStreamingDestinationInput, mw ...DisableKinesisStreamingDestinationMiddleWare) *DisableKinesisStreamingDestination {
	return NewDisableKinesisStreamingDestination(input, mw...).Invoke(ctx, c.ddb)
}

// DisableKinesisStreamingDestination executes a DisableKinesisStreamingDestination operation with a DisableKinesisStreamingDestinationInput in this pool and returns the DisableKinesisStreamingDestinationPromise
func (p *Pool) DisableKinesisStreamingDestination(input *ddb.DisableKinesisStreamingDestinationInput, mw ...DisableKinesisStreamingDestinationMiddleWare) *DisableKinesisStreamingDestination {
	op := NewDisableKinesisStreamingDestination(input, mw...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// DisableKinesisStreamingDestinationContext represents an exhaustive DisableKinesisStreamingDestination operation request context
type DisableKinesisStreamingDestinationContext struct {
	context.Context
	input  *ddb.DisableKinesisStreamingDestinationInput
	client *ddb.Client
}

// DisableKinesisStreamingDestinationOutput represents the output for the DisableKinesisStreamingDestination opration
type DisableKinesisStreamingDestinationOutput struct {
	out *ddb.DisableKinesisStreamingDestinationOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *DisableKinesisStreamingDestinationOutput) Set(out *ddb.DisableKinesisStreamingDestinationOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *DisableKinesisStreamingDestinationOutput) Get() (out *ddb.DisableKinesisStreamingDestinationOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()
	return
}

// DisableKinesisStreamingDestinationHandler represents a handler for DisableKinesisStreamingDestination requests
type DisableKinesisStreamingDestinationHandler interface {
	HandleDisableKinesisStreamingDestination(ctx *DisableKinesisStreamingDestinationContext, output *DisableKinesisStreamingDestinationOutput)
}

// DisableKinesisStreamingDestinationHandlerFunc is a DisableKinesisStreamingDestinationHandler function
type DisableKinesisStreamingDestinationHandlerFunc func(ctx *DisableKinesisStreamingDestinationContext, output *DisableKinesisStreamingDestinationOutput)

// HandleDisableKinesisStreamingDestination implements DisableKinesisStreamingDestinationHandler
func (h DisableKinesisStreamingDestinationHandlerFunc) HandleDisableKinesisStreamingDestination(ctx *DisableKinesisStreamingDestinationContext, output *DisableKinesisStreamingDestinationOutput) {
	h(ctx, output)
}

// DisableKinesisStreamingDestinationFinalHandler is the final DisableKinesisStreamingDestinationHandler that executes a dynamodb DisableKinesisStreamingDestination operation
type DisableKinesisStreamingDestinationFinalHandler struct{}

// HandleDisableKinesisStreamingDestination implements the DisableKinesisStreamingDestinationHandler
func (h *DisableKinesisStreamingDestinationFinalHandler) HandleDisableKinesisStreamingDestination(ctx *DisableKinesisStreamingDestinationContext, output *DisableKinesisStreamingDestinationOutput) {
	output.Set(ctx.client.DisableKinesisStreamingDestination(ctx, ctx.input))
}

// DisableKinesisStreamingDestinationMiddleWare is a middleware function use for wrapping DisableKinesisStreamingDestinationHandler requests
type DisableKinesisStreamingDestinationMiddleWare interface {
	DisableKinesisStreamingDestinationMiddleWare(next DisableKinesisStreamingDestinationHandler) DisableKinesisStreamingDestinationHandler
}

// DisableKinesisStreamingDestinationMiddleWareFunc is a functional DisableKinesisStreamingDestinationMiddleWare
type DisableKinesisStreamingDestinationMiddleWareFunc func(next DisableKinesisStreamingDestinationHandler) DisableKinesisStreamingDestinationHandler

// DisableKinesisStreamingDestinationMiddleWare implements the DisableKinesisStreamingDestinationMiddleWare interface
func (mw DisableKinesisStreamingDestinationMiddleWareFunc) DisableKinesisStreamingDestinationMiddleWare(next DisableKinesisStreamingDestinationHandler) DisableKinesisStreamingDestinationHandler {
	return mw(next)
}

// DisableKinesisStreamingDestination represents a DisableKinesisStreamingDestination operation
type DisableKinesisStreamingDestination struct {
	*Promise
	input       *ddb.DisableKinesisStreamingDestinationInput
	middleWares []DisableKinesisStreamingDestinationMiddleWare
}

// NewDisableKinesisStreamingDestination creates a new DisableKinesisStreamingDestination
func NewDisableKinesisStreamingDestination(input *ddb.DisableKinesisStreamingDestinationInput, mws ...DisableKinesisStreamingDestinationMiddleWare) *DisableKinesisStreamingDestination {
	return &DisableKinesisStreamingDestination{
		Promise: NewPromise(),
		input:       input,
		middleWares: mws,
	}
}

// Invoke invokes the DisableKinesisStreamingDestination operation and returns a DisableKinesisStreamingDestinationPromise
func (op *DisableKinesisStreamingDestination) Invoke(ctx context.Context, client *ddb.Client) *DisableKinesisStreamingDestination {
	go op.DynoInvoke(ctx, client)

	return op
}

// DynoInvoke implements the Operation interface
func (op *DisableKinesisStreamingDestination) DynoInvoke(ctx context.Context, client *ddb.Client) {
	output := new(DisableKinesisStreamingDestinationOutput)

	defer func() { op.SetResponse(output.Get()) }()

	requestCtx := &DisableKinesisStreamingDestinationContext{
		Context: ctx,
		client:  client,
		input:   op.input,
	}

	var h DisableKinesisStreamingDestinationHandler

	h = new(DisableKinesisStreamingDestinationFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].DisableKinesisStreamingDestinationMiddleWare(h)
		}
	}

	h.HandleDisableKinesisStreamingDestination(requestCtx, output)
}

// Await waits for the DisableKinesisStreamingDestinationPromise to be fulfilled and then returns a DisableKinesisStreamingDestinationOutput and error
func (op *DisableKinesisStreamingDestination) Await() (*ddb.DisableKinesisStreamingDestinationOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.DisableKinesisStreamingDestinationOutput), err
}

// NewDisableKinesisStreamingDestinationInput creates a new DisableKinesisStreamingDestinationInput
func NewDisableKinesisStreamingDestinationInput(tableName, streamArn *string) *ddb.DisableKinesisStreamingDestinationInput {
	return &ddb.DisableKinesisStreamingDestinationInput{
		StreamArn: streamArn,
		TableName: tableName,
	}
}
