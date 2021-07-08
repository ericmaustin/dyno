package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// DescribeEndpoints executes DescribeEndpoints operation and returns a DescribeEndpoints
func (s *Session) DescribeEndpoints(input *ddb.DescribeEndpointsInput, mw ...DescribeEndpointsMiddleWare) *DescribeEndpoints {
	return NewDescribeEndpoints(input, mw...).Invoke(s.ctx, s.ddb)
}

// DescribeEndpoints executes a DescribeEndpoints operation with a DescribeEndpointsInput in this pool and returns the DescribeEndpoints
func (p *Pool) DescribeEndpoints(input *ddb.DescribeEndpointsInput, mw ...DescribeEndpointsMiddleWare) *DescribeEndpoints {
	op := NewDescribeEndpoints(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// DescribeEndpointsContext represents an exhaustive DescribeEndpoints operation request context
type DescribeEndpointsContext struct {
	context.Context
	Input  *ddb.DescribeEndpointsInput
	Client *ddb.Client
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
	output.Set(ctx.Client.DescribeEndpoints(ctx, ctx.Input))
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
	*BaseOperation
	input       *ddb.DescribeEndpointsInput
	middleWares []DescribeEndpointsMiddleWare
}

// NewDescribeEndpoints creates a new DescribeEndpoints
func NewDescribeEndpoints(input *ddb.DescribeEndpointsInput, mws ...DescribeEndpointsMiddleWare) *DescribeEndpoints {
	return &DescribeEndpoints{
		BaseOperation: NewOperation(),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the DescribeEndpoints operation in a goroutine and returns a BatchGetItemAll
func (op *DescribeEndpoints) Invoke(ctx context.Context, client *ddb.Client) *DescribeEndpoints {
	op.SetRunning() // operation now waiting for a response

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the DescribeEndpoints operation
func (op *DescribeEndpoints) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	output := new(DescribeEndpointsOutput)

	defer func() { op.SetResponse(output.Get()) }()

	var h DescribeEndpointsHandler

	h = new(DescribeEndpointsFinalHandler)
	
	// loop in reverse to preserve middleware order
	for i := len(op.middleWares) - 1; i >= 0; i-- {
		h = op.middleWares[i].DescribeEndpointsMiddleWare(h)
	}
	
	requestCtx := &DescribeEndpointsContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}
	
	h.HandleDescribeEndpoints(requestCtx, output)
}

// Await waits for the DescribeEndpoints operation to be fulfilled and then returns a DescribeEndpointsOutput and error
func (op *DescribeEndpoints) Await() (*ddb.DescribeEndpointsOutput, error) {
	out, err := op.BaseOperation.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.DescribeEndpointsOutput), err
}

// NewDescribeEndpointsInput creates a new DescribeEndpointsInput
func NewDescribeEndpointsInput() *ddb.DescribeEndpointsInput {
	return &ddb.DescribeEndpointsInput{}
}
