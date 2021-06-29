package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// DescribeEndpoints executes DescribeEndpoints operation and returns a DescribeEndpointsPromise
func (c *Client) DescribeEndpoints(ctx context.Context, input *ddb.DescribeEndpointsInput, mw ...DescribeEndpointsMiddleWare) *DescribeEndpoints {
	return NewDescribeEndpoints(input, mw...).Invoke(ctx, c.ddb)
}

// DescribeEndpoints executes a DescribeEndpoints operation with a DescribeEndpointsInput in this pool and returns the DescribeEndpointsPromise
func (p *Pool) DescribeEndpoints(input *ddb.DescribeEndpointsInput, mw ...DescribeEndpointsMiddleWare) *DescribeEndpoints {
	op := NewDescribeEndpoints(input, mw...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// DescribeEndpointsContext represents an exhaustive DescribeEndpoints operation request context
type DescribeEndpointsContext struct {
	context.Context
	input  *ddb.DescribeEndpointsInput
	client *ddb.Client
}

// DescribeEndpointsOutput represents the output for the DescribeEndpoints opration
type DescribeEndpointsOutput struct {
	out *ddb.DescribeEndpointsOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *DescribeEndpointsOutput) Set(out *ddb.DescribeEndpointsOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *DescribeEndpointsOutput) Get() (out *ddb.DescribeEndpointsOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()
	return
}

// DescribeEndpointsHandler represents a handler for DescribeEndpoints requests
type DescribeEndpointsHandler interface {
	HandleDescribeEndpoints(ctx *DescribeEndpointsContext, output *DescribeEndpointsOutput)
}

// DescribeEndpointsHandlerFunc is a DescribeEndpointsHandler function
type DescribeEndpointsHandlerFunc func(ctx *DescribeEndpointsContext, output *DescribeEndpointsOutput)

// HandleDescribeEndpoints implements DescribeEndpointsHandler
func (h DescribeEndpointsHandlerFunc) HandleDescribeEndpoints(ctx *DescribeEndpointsContext, output *DescribeEndpointsOutput) {
	h(ctx, output)
}

// DescribeEndpointsFinalHandler is the final DescribeEndpointsHandler that executes a dynamodb DescribeEndpoints operation
type DescribeEndpointsFinalHandler struct{}

// HandleDescribeEndpoints implements the DescribeEndpointsHandler
func (h *DescribeEndpointsFinalHandler) HandleDescribeEndpoints(ctx *DescribeEndpointsContext, output *DescribeEndpointsOutput) {
	output.Set(ctx.client.DescribeEndpoints(ctx, ctx.input))
}

// DescribeEndpointsMiddleWare is a middleware function use for wrapping DescribeEndpointsHandler requests
type DescribeEndpointsMiddleWare interface {
	DescribeEndpointsMiddleWare(next DescribeEndpointsHandler) DescribeEndpointsHandler
}

// DescribeEndpointsMiddleWareFunc is a functional DescribeEndpointsMiddleWare
type DescribeEndpointsMiddleWareFunc func(next DescribeEndpointsHandler) DescribeEndpointsHandler

// DescribeEndpointsMiddleWare implements the DescribeEndpointsMiddleWare interface
func (mw DescribeEndpointsMiddleWareFunc) DescribeEndpointsMiddleWare(next DescribeEndpointsHandler) DescribeEndpointsHandler {
	return mw(next)
}

// DescribeEndpoints represents a DescribeEndpoints operation
type DescribeEndpoints struct {
	*Promise
	input       *ddb.DescribeEndpointsInput
	middleWares []DescribeEndpointsMiddleWare
}

// NewDescribeEndpoints creates a new DescribeEndpoints
func NewDescribeEndpoints(input *ddb.DescribeEndpointsInput, mws ...DescribeEndpointsMiddleWare) *DescribeEndpoints {
	return &DescribeEndpoints{
		Promise: NewPromise(),
		input:       input,
		middleWares: mws,
	}
}

// Invoke invokes the DescribeEndpoints operation and returns a DescribeEndpointsPromise
func (op *DescribeEndpoints) Invoke(ctx context.Context, client *ddb.Client) *DescribeEndpoints {
	go op.DynoInvoke(ctx, client)

	return op
}

// DynoInvoke implements the Operation interface
func (op *DescribeEndpoints) DynoInvoke(ctx context.Context, client *ddb.Client) {
	output := new(DescribeEndpointsOutput)

	defer func() { op.SetResponse(output.Get()) }()

	requestCtx := &DescribeEndpointsContext{
		Context: ctx,
		client:  client,
		input:   op.input,
	}

	var h DescribeEndpointsHandler

	h = new(DescribeEndpointsFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].DescribeEndpointsMiddleWare(h)
		}
	}

	h.HandleDescribeEndpoints(requestCtx, output)
}

// Await waits for the DescribeEndpointsPromise to be fulfilled and then returns a DescribeEndpointsOutput and error
func (op *DescribeEndpoints) Await() (*ddb.DescribeEndpointsOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.DescribeEndpointsOutput), err
}

// NewDescribeEndpointsInput creates a new DescribeEndpointsInput
func NewDescribeEndpointsInput() *ddb.DescribeEndpointsInput {
	return &ddb.DescribeEndpointsInput{}
}
