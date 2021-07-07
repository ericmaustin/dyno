package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// DescribeTimeToLive executes DescribeTimeToLive operation and returns a DescribeTimeToLivePromise
func (c *Client) DescribeTimeToLive(ctx context.Context, input *ddb.DescribeTimeToLiveInput, mw ...DescribeTimeToLiveMiddleWare) *DescribeTimeToLive {
	return NewDescribeTimeToLive(input, mw...).Invoke(ctx, c.ddb)
}

// DescribeTimeToLive executes a DescribeTimeToLive operation with a DescribeTimeToLiveInput in this pool and returns the DescribeTimeToLivePromise
func (p *Pool) DescribeTimeToLive(input *ddb.DescribeTimeToLiveInput, mw ...DescribeTimeToLiveMiddleWare) *DescribeTimeToLive {
	op := NewDescribeTimeToLive(input, mw...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// DescribeTimeToLiveContext represents an exhaustive DescribeTimeToLive operation request context
type DescribeTimeToLiveContext struct {
	context.Context
	Input  *ddb.DescribeTimeToLiveInput
	Client *ddb.Client
}

// DescribeTimeToLiveOutput represents the output for the DescribeTimeToLive opration
type DescribeTimeToLiveOutput struct {
	out *ddb.DescribeTimeToLiveOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *DescribeTimeToLiveOutput) Set(out *ddb.DescribeTimeToLiveOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *DescribeTimeToLiveOutput) Get() (out *ddb.DescribeTimeToLiveOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()
	return
}

// DescribeTimeToLiveHandler represents a handler for DescribeTimeToLive requests
type DescribeTimeToLiveHandler interface {
	HandleDescribeTimeToLive(ctx *DescribeTimeToLiveContext, output *DescribeTimeToLiveOutput)
}

// DescribeTimeToLiveHandlerFunc is a DescribeTimeToLiveHandler function
type DescribeTimeToLiveHandlerFunc func(ctx *DescribeTimeToLiveContext, output *DescribeTimeToLiveOutput)

// HandleDescribeTimeToLive implements DescribeTimeToLiveHandler
func (h DescribeTimeToLiveHandlerFunc) HandleDescribeTimeToLive(ctx *DescribeTimeToLiveContext, output *DescribeTimeToLiveOutput) {
	h(ctx, output)
}

// DescribeTimeToLiveFinalHandler is the final DescribeTimeToLiveHandler that executes a dynamodb DescribeTimeToLive operation
type DescribeTimeToLiveFinalHandler struct{}

// HandleDescribeTimeToLive implements the DescribeTimeToLiveHandler
func (h *DescribeTimeToLiveFinalHandler) HandleDescribeTimeToLive(ctx *DescribeTimeToLiveContext, output *DescribeTimeToLiveOutput) {
	output.Set(ctx.Client.DescribeTimeToLive(ctx, ctx.Input))
}

// DescribeTimeToLiveMiddleWare is a middleware function use for wrapping DescribeTimeToLiveHandler requests
type DescribeTimeToLiveMiddleWare interface {
	DescribeTimeToLiveMiddleWare(next DescribeTimeToLiveHandler) DescribeTimeToLiveHandler
}

// DescribeTimeToLiveMiddleWareFunc is a functional DescribeTimeToLiveMiddleWare
type DescribeTimeToLiveMiddleWareFunc func(next DescribeTimeToLiveHandler) DescribeTimeToLiveHandler

// DescribeTimeToLiveMiddleWare implements the DescribeTimeToLiveMiddleWare interface
func (mw DescribeTimeToLiveMiddleWareFunc) DescribeTimeToLiveMiddleWare(next DescribeTimeToLiveHandler) DescribeTimeToLiveHandler {
	return mw(next)
}

// DescribeTimeToLive represents a DescribeTimeToLive operation
type DescribeTimeToLive struct {
	*Promise
	input       *ddb.DescribeTimeToLiveInput
	middleWares []DescribeTimeToLiveMiddleWare
}

// NewDescribeTimeToLive creates a new DescribeTimeToLive
func NewDescribeTimeToLive(input *ddb.DescribeTimeToLiveInput, mws ...DescribeTimeToLiveMiddleWare) *DescribeTimeToLive {
	return &DescribeTimeToLive{
		Promise: NewPromise(),
		input:       input,
		middleWares: mws,
	}
}

// DynoInvoke invokes the DescribeTimeToLive operation and returns a DescribeTimeToLivePromise
func (op *DescribeTimeToLive) Invoke(ctx context.Context, client *ddb.Client) *DescribeTimeToLive {
	go op.Invoke(ctx, client)

	return op
}

// DynoInvoke implements the Operation interface
func (op *DescribeTimeToLive) Invoke(ctx context.Context, client *ddb.Client) {
	output := new(DescribeTimeToLiveOutput)

	defer func() { op.SetResponse(output.Get()) }()

	requestCtx := &DescribeTimeToLiveContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	var h DescribeTimeToLiveHandler

	h = new(DescribeTimeToLiveFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].DescribeTimeToLiveMiddleWare(h)
		}
	}

	h.HandleDescribeTimeToLive(requestCtx, output)
}

// Await waits for the DescribeTimeToLivePromise to be fulfilled and then returns a DescribeTimeToLiveOutput and error
func (op *DescribeTimeToLive) Await() (*ddb.DescribeTimeToLiveOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.DescribeTimeToLiveOutput), err
}

// NewDescribeTimeToLiveInput creates a new DescribeTimeToLiveInput
func NewDescribeTimeToLiveInput(tableName *string) *ddb.DescribeTimeToLiveInput {
	return &ddb.DescribeTimeToLiveInput{
		TableName: tableName,
	}
}
