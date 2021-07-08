package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// DescribeLimits executes DescribeLimits operation and returns a DescribeLimits
func (s *Session) DescribeLimits(input *ddb.DescribeLimitsInput, mw ...DescribeLimitsMiddleWare) *DescribeLimits {
	return NewDescribeLimits(input, mw...).Invoke(s.ctx, s.ddb)
}

// DescribeLimits executes a DescribeLimits operation with a DescribeLimitsInput in this pool and returns the DescribeLimits
func (p *Pool) DescribeLimits(input *ddb.DescribeLimitsInput, mw ...DescribeLimitsMiddleWare) *DescribeLimits {
	op := NewDescribeLimits(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// DescribeLimitsContext represents an exhaustive DescribeLimits operation request context
type DescribeLimitsContext struct {
	context.Context
	Input  *ddb.DescribeLimitsInput
	Client *ddb.Client
}

// DescribeLimitsOutput represents the output for the DescribeLimits operation
type DescribeLimitsOutput struct {
	out *ddb.DescribeLimitsOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *DescribeLimitsOutput) Set(out *ddb.DescribeLimitsOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *DescribeLimitsOutput) Get() (out *ddb.DescribeLimitsOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()

	return
}

// DescribeLimitsHandler represents a handler for DescribeLimits requests
type DescribeLimitsHandler interface {
	HandleDescribeLimits(ctx *DescribeLimitsContext, output *DescribeLimitsOutput)
}

// DescribeLimitsHandlerFunc is a DescribeLimitsHandler function
type DescribeLimitsHandlerFunc func(ctx *DescribeLimitsContext, output *DescribeLimitsOutput)

// HandleDescribeLimits implements DescribeLimitsHandler
func (h DescribeLimitsHandlerFunc) HandleDescribeLimits(ctx *DescribeLimitsContext, output *DescribeLimitsOutput) {
	h(ctx, output)
}

// DescribeLimitsFinalHandler is the final DescribeLimitsHandler that executes a dynamodb DescribeLimits operation
type DescribeLimitsFinalHandler struct{}

// HandleDescribeLimits implements the DescribeLimitsHandler
func (h *DescribeLimitsFinalHandler) HandleDescribeLimits(ctx *DescribeLimitsContext, output *DescribeLimitsOutput) {
	output.Set(ctx.Client.DescribeLimits(ctx, ctx.Input))
}

// DescribeLimitsMiddleWare is a middleware function use for wrapping DescribeLimitsHandler requests
type DescribeLimitsMiddleWare interface {
	DescribeLimitsMiddleWare(next DescribeLimitsHandler) DescribeLimitsHandler
}

// DescribeLimitsMiddleWareFunc is a functional DescribeLimitsMiddleWare
type DescribeLimitsMiddleWareFunc func(next DescribeLimitsHandler) DescribeLimitsHandler

// DescribeLimitsMiddleWare implements the DescribeLimitsMiddleWare interface
func (mw DescribeLimitsMiddleWareFunc) DescribeLimitsMiddleWare(next DescribeLimitsHandler) DescribeLimitsHandler {
	return mw(next)
}

// DescribeLimits represents a DescribeLimits operation
type DescribeLimits struct {
	*BaseOperation
	input       *ddb.DescribeLimitsInput
	middleWares []DescribeLimitsMiddleWare
}

// NewDescribeLimits creates a new DescribeLimits
func NewDescribeLimits(input *ddb.DescribeLimitsInput, mws ...DescribeLimitsMiddleWare) *DescribeLimits {
	return &DescribeLimits{
		BaseOperation: NewOperation(),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the DescribeLimits operation in a goroutine and returns a DescribeLimits operation
func (op *DescribeLimits) Invoke(ctx context.Context, client *ddb.Client) *DescribeLimits {
	op.SetRunning() // operation now waiting for a response

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the DescribeLimits operation
func (op *DescribeLimits) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	output := new(DescribeLimitsOutput)

	defer func() { op.SetResponse(output.Get()) }()

	var h DescribeLimitsHandler

	h = new(DescribeLimitsFinalHandler)

	// loop in reverse to preserve middleware order
	for i := len(op.middleWares) - 1; i >= 0; i-- {
		h = op.middleWares[i].DescribeLimitsMiddleWare(h)
	}

	requestCtx := &DescribeLimitsContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	h.HandleDescribeLimits(requestCtx, output)
}

// Await waits for the DescribeLimits to be fulfilled and then returns a DescribeLimitsOutput and error
func (op *DescribeLimits) Await() (*ddb.DescribeLimitsOutput, error) {
	out, err := op.BaseOperation.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.DescribeLimitsOutput), err
}

// NewDescribeLimitsInput creates a new DescribeLimitsInput
func NewDescribeLimitsInput() *ddb.DescribeLimitsInput {
	return &ddb.DescribeLimitsInput{}
}
