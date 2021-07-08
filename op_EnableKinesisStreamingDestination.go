package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// EnableKinesisStreamingDestination executes EnableKinesisStreamingDestination operation and returns a EnableKinesisStreamingDestination operation
func (s *Session) EnableKinesisStreamingDestination(input *ddb.EnableKinesisStreamingDestinationInput, mw ...EnableKinesisStreamingDestinationMiddleWare) *EnableKinesisStreamingDestination {
	return NewEnableKinesisStreamingDestination(input, mw...).Invoke(s.ctx, s.ddb)
}

// EnableKinesisStreamingDestination executes a EnableKinesisStreamingDestination operation with a EnableKinesisStreamingDestinationInput in this pool and returns the EnableKinesisStreamingDestination operation
func (p *Pool) EnableKinesisStreamingDestination(input *ddb.EnableKinesisStreamingDestinationInput, mw ...EnableKinesisStreamingDestinationMiddleWare) *EnableKinesisStreamingDestination {
	op := NewEnableKinesisStreamingDestination(input, mw...)
	
	p.Do(op) // run the operation in the pool

	return op
}

// EnableKinesisStreamingDestinationContext represents an exhaustive EnableKinesisStreamingDestination operation request context
type EnableKinesisStreamingDestinationContext struct {
	context.Context
	Input  *ddb.EnableKinesisStreamingDestinationInput
	Client *ddb.Client
}

// EnableKinesisStreamingDestinationOutput represents the output for the EnableKinesisStreamingDestination operation
type EnableKinesisStreamingDestinationOutput struct {
	out *ddb.EnableKinesisStreamingDestinationOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *EnableKinesisStreamingDestinationOutput) Set(out *ddb.EnableKinesisStreamingDestinationOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *EnableKinesisStreamingDestinationOutput) Get() (out *ddb.EnableKinesisStreamingDestinationOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()
	
	return
}

// EnableKinesisStreamingDestinationHandler represents a handler for EnableKinesisStreamingDestination requests
type EnableKinesisStreamingDestinationHandler interface {
	HandleEnableKinesisStreamingDestination(ctx *EnableKinesisStreamingDestinationContext, output *EnableKinesisStreamingDestinationOutput)
}

// EnableKinesisStreamingDestinationHandlerFunc is a EnableKinesisStreamingDestinationHandler function
type EnableKinesisStreamingDestinationHandlerFunc func(ctx *EnableKinesisStreamingDestinationContext, output *EnableKinesisStreamingDestinationOutput)

// HandleEnableKinesisStreamingDestination implements EnableKinesisStreamingDestinationHandler
func (h EnableKinesisStreamingDestinationHandlerFunc) HandleEnableKinesisStreamingDestination(ctx *EnableKinesisStreamingDestinationContext, output *EnableKinesisStreamingDestinationOutput) {
	h(ctx, output)
}

// EnableKinesisStreamingDestinationFinalHandler is the final EnableKinesisStreamingDestinationHandler that executes a dynamodb EnableKinesisStreamingDestination operation
type EnableKinesisStreamingDestinationFinalHandler struct{}

// HandleEnableKinesisStreamingDestination implements the EnableKinesisStreamingDestinationHandler
func (h *EnableKinesisStreamingDestinationFinalHandler) HandleEnableKinesisStreamingDestination(ctx *EnableKinesisStreamingDestinationContext, output *EnableKinesisStreamingDestinationOutput) {
	output.Set(ctx.Client.EnableKinesisStreamingDestination(ctx, ctx.Input))
}

// EnableKinesisStreamingDestinationMiddleWare is a middleware function use for wrapping EnableKinesisStreamingDestinationHandler requests
type EnableKinesisStreamingDestinationMiddleWare interface {
	EnableKinesisStreamingDestinationMiddleWare(next EnableKinesisStreamingDestinationHandler) EnableKinesisStreamingDestinationHandler
}

// EnableKinesisStreamingDestinationMiddleWareFunc is a functional EnableKinesisStreamingDestinationMiddleWare
type EnableKinesisStreamingDestinationMiddleWareFunc func(next EnableKinesisStreamingDestinationHandler) EnableKinesisStreamingDestinationHandler

// EnableKinesisStreamingDestinationMiddleWare implements the EnableKinesisStreamingDestinationMiddleWare interface
func (mw EnableKinesisStreamingDestinationMiddleWareFunc) EnableKinesisStreamingDestinationMiddleWare(next EnableKinesisStreamingDestinationHandler) EnableKinesisStreamingDestinationHandler {
	return mw(next)
}

// EnableKinesisStreamingDestination represents a EnableKinesisStreamingDestination operation
type EnableKinesisStreamingDestination struct {
	*BaseOperation
	input       *ddb.EnableKinesisStreamingDestinationInput
	middleWares []EnableKinesisStreamingDestinationMiddleWare
}

// NewEnableKinesisStreamingDestination creates a new EnableKinesisStreamingDestination operation
func NewEnableKinesisStreamingDestination(input *ddb.EnableKinesisStreamingDestinationInput, mws ...EnableKinesisStreamingDestinationMiddleWare) *EnableKinesisStreamingDestination {
	return &EnableKinesisStreamingDestination{
		BaseOperation: NewOperation(),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the EnableKinesisStreamingDestination operation in a goroutine and returns a EnableKinesisStreamingDestination operation
func (op *EnableKinesisStreamingDestination) Invoke(ctx context.Context, client *ddb.Client) *EnableKinesisStreamingDestination {
	op.SetRunning() // operation now waiting for a response
	
	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the EnableKinesisStreamingDestination operation
func (op *EnableKinesisStreamingDestination) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	output := new(EnableKinesisStreamingDestinationOutput)

	defer func() { op.SetResponse(output.Get()) }()

	var h EnableKinesisStreamingDestinationHandler

	h = new(EnableKinesisStreamingDestinationFinalHandler)

	// loop in reverse to preserve middleware order
	for i := len(op.middleWares) - 1; i >= 0; i-- {
		h = op.middleWares[i].EnableKinesisStreamingDestinationMiddleWare(h)
	}

	requestCtx := &EnableKinesisStreamingDestinationContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	h.HandleEnableKinesisStreamingDestination(requestCtx, output)
}

// Await waits for the EnableKinesisStreamingDestination operation to be fulfilled and then returns a EnableKinesisStreamingDestinationOutput and error
func (op *EnableKinesisStreamingDestination) Await() (*ddb.EnableKinesisStreamingDestinationOutput, error) {
	out, err := op.BaseOperation.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.EnableKinesisStreamingDestinationOutput), err
}

// NewEnableKinesisStreamingDestinationInput creates a new EnableKinesisStreamingDestinationInput
func NewEnableKinesisStreamingDestinationInput(tableName, streamArn *string) *ddb.EnableKinesisStreamingDestinationInput {
	return &ddb.EnableKinesisStreamingDestinationInput{
		StreamArn: streamArn,
		TableName: tableName,
	}
}
