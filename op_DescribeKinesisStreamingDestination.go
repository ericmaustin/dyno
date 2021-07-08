package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// DescribeKinesisStreamingDestination executes DescribeKinesisStreamingDestination operation and returns a DescribeKinesisStreamingDestination
func (s *Session) DescribeKinesisStreamingDestination(input *ddb.DescribeKinesisStreamingDestinationInput, mw ...DescribeKinesisStreamingDestinationMiddleWare) *DescribeKinesisStreamingDestination {
	return NewDescribeKinesisStreamingDestination(input, mw...).Invoke(s.ctx, s.ddb)
}

// DescribeKinesisStreamingDestination executes a DescribeKinesisStreamingDestination operation with a DescribeKinesisStreamingDestinationInput in this pool and returns the DescribeKinesisStreamingDestination
func (p *Pool) DescribeKinesisStreamingDestination(input *ddb.DescribeKinesisStreamingDestinationInput, mw ...DescribeKinesisStreamingDestinationMiddleWare) *DescribeKinesisStreamingDestination {
	op := NewDescribeKinesisStreamingDestination(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// DescribeKinesisStreamingDestinationContext represents an exhaustive DescribeKinesisStreamingDestination operation request context
type DescribeKinesisStreamingDestinationContext struct {
	context.Context
	Input  *ddb.DescribeKinesisStreamingDestinationInput
	Client *ddb.Client
}

// DescribeKinesisStreamingDestinationOutput represents the output for the DescribeKinesisStreamingDestination operation
type DescribeKinesisStreamingDestinationOutput struct {
	out *ddb.DescribeKinesisStreamingDestinationOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *DescribeKinesisStreamingDestinationOutput) Set(out *ddb.DescribeKinesisStreamingDestinationOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *DescribeKinesisStreamingDestinationOutput) Get() (out *ddb.DescribeKinesisStreamingDestinationOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()

	return
}

// DescribeKinesisStreamingDestinationHandler represents a handler for DescribeKinesisStreamingDestination requests
type DescribeKinesisStreamingDestinationHandler interface {
	HandleDescribeKinesisStreamingDestination(ctx *DescribeKinesisStreamingDestinationContext, output *DescribeKinesisStreamingDestinationOutput)
}

// DescribeKinesisStreamingDestinationHandlerFunc is a DescribeKinesisStreamingDestinationHandler function
type DescribeKinesisStreamingDestinationHandlerFunc func(ctx *DescribeKinesisStreamingDestinationContext, output *DescribeKinesisStreamingDestinationOutput)

// HandleDescribeKinesisStreamingDestination implements DescribeKinesisStreamingDestinationHandler
func (h DescribeKinesisStreamingDestinationHandlerFunc) HandleDescribeKinesisStreamingDestination(ctx *DescribeKinesisStreamingDestinationContext, output *DescribeKinesisStreamingDestinationOutput) {
	h(ctx, output)
}

// DescribeKinesisStreamingDestinationFinalHandler is the final DescribeKinesisStreamingDestinationHandler that executes a dynamodb DescribeKinesisStreamingDestination operation
type DescribeKinesisStreamingDestinationFinalHandler struct{}

// HandleDescribeKinesisStreamingDestination implements the DescribeKinesisStreamingDestinationHandler
func (h *DescribeKinesisStreamingDestinationFinalHandler) HandleDescribeKinesisStreamingDestination(ctx *DescribeKinesisStreamingDestinationContext, output *DescribeKinesisStreamingDestinationOutput) {
	output.Set(ctx.Client.DescribeKinesisStreamingDestination(ctx, ctx.Input))
}

// DescribeKinesisStreamingDestinationMiddleWare is a middleware function use for wrapping DescribeKinesisStreamingDestinationHandler requests
type DescribeKinesisStreamingDestinationMiddleWare interface {
	DescribeKinesisStreamingDestinationMiddleWare(next DescribeKinesisStreamingDestinationHandler) DescribeKinesisStreamingDestinationHandler
}

// DescribeKinesisStreamingDestinationMiddleWareFunc is a functional DescribeKinesisStreamingDestinationMiddleWare
type DescribeKinesisStreamingDestinationMiddleWareFunc func(next DescribeKinesisStreamingDestinationHandler) DescribeKinesisStreamingDestinationHandler

// DescribeKinesisStreamingDestinationMiddleWare implements the DescribeKinesisStreamingDestinationMiddleWare interface
func (mw DescribeKinesisStreamingDestinationMiddleWareFunc) DescribeKinesisStreamingDestinationMiddleWare(next DescribeKinesisStreamingDestinationHandler) DescribeKinesisStreamingDestinationHandler {
	return mw(next)
}

// DescribeKinesisStreamingDestination represents a DescribeKinesisStreamingDestination operation
type DescribeKinesisStreamingDestination struct {
	*BaseOperation
	input       *ddb.DescribeKinesisStreamingDestinationInput
	middleWares []DescribeKinesisStreamingDestinationMiddleWare
}

// NewDescribeKinesisStreamingDestination creates a new DescribeKinesisStreamingDestination
func NewDescribeKinesisStreamingDestination(input *ddb.DescribeKinesisStreamingDestinationInput, mws ...DescribeKinesisStreamingDestinationMiddleWare) *DescribeKinesisStreamingDestination {
	return &DescribeKinesisStreamingDestination{
		BaseOperation: NewOperation(),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the DescribeKinesisStreamingDestination operation in a goroutine and returns a DescribeKinesisStreamingDestination operation
func (op *DescribeKinesisStreamingDestination) Invoke(ctx context.Context, client *ddb.Client) *DescribeKinesisStreamingDestination {
	op.SetRunning() // operation now waiting for a response

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the DescribeKinesisStreamingDestination operation
func (op *DescribeKinesisStreamingDestination) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	output := new(DescribeKinesisStreamingDestinationOutput)

	defer func() { op.SetResponse(output.Get()) }()

	var h DescribeKinesisStreamingDestinationHandler

	h = new(DescribeKinesisStreamingDestinationFinalHandler)


	// loop in reverse to preserve middleware order
	for i := len(op.middleWares) - 1; i >= 0; i-- {
		h = op.middleWares[i].DescribeKinesisStreamingDestinationMiddleWare(h)
	}

	requestCtx := &DescribeKinesisStreamingDestinationContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	h.HandleDescribeKinesisStreamingDestination(requestCtx, output)
}

// Await waits for the DescribeKinesisStreamingDestination operation to be fulfilled and then returns a DescribeKinesisStreamingDestinationOutput and error
func (op *DescribeKinesisStreamingDestination) Await() (*ddb.DescribeKinesisStreamingDestinationOutput, error) {
	out, err := op.BaseOperation.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.DescribeKinesisStreamingDestinationOutput), err
}

// NewDescribeKinesisStreamingDestinationInput creates a new DescribeKinesisStreamingDestinationInput
func NewDescribeKinesisStreamingDestinationInput(tableName *string) *ddb.DescribeKinesisStreamingDestinationInput {
	return &ddb.DescribeKinesisStreamingDestinationInput{
		TableName: tableName,
	}
}
