package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// DescribeTableReplicaAutoScaling executes DescribeTableReplicaAutoScaling operation and returns a DescribeTableReplicaAutoScaling operation
func (s *Session) DescribeTableReplicaAutoScaling(input *ddb.DescribeTableReplicaAutoScalingInput, mw ...DescribeTableReplicaAutoScalingMiddleWare) *DescribeTableReplicaAutoScaling {
	return NewDescribeTableReplicaAutoScaling(input, mw...).Invoke(s.ctx, s.ddb)
}

// DescribeTableReplicaAutoScaling executes a DescribeTableReplicaAutoScaling operation with a DescribeTableReplicaAutoScalingInput in this pool and returns the DescribeTableReplicaAutoScaling operation
func (p *Pool) DescribeTableReplicaAutoScaling(input *ddb.DescribeTableReplicaAutoScalingInput, mw ...DescribeTableReplicaAutoScalingMiddleWare) *DescribeTableReplicaAutoScaling {
	op := NewDescribeTableReplicaAutoScaling(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// DescribeTableReplicaAutoScalingContext represents an exhaustive DescribeTableReplicaAutoScaling operation request context
type DescribeTableReplicaAutoScalingContext struct {
	context.Context
	Input  *ddb.DescribeTableReplicaAutoScalingInput
	Client *ddb.Client
}

// DescribeTableReplicaAutoScalingOutput represents the output for the DescribeTableReplicaAutoScaling opration
type DescribeTableReplicaAutoScalingOutput struct {
	out *ddb.DescribeTableReplicaAutoScalingOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *DescribeTableReplicaAutoScalingOutput) Set(out *ddb.DescribeTableReplicaAutoScalingOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *DescribeTableReplicaAutoScalingOutput) Get() (out *ddb.DescribeTableReplicaAutoScalingOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()

	return
}

// DescribeTableReplicaAutoScalingHandler represents a handler for DescribeTableReplicaAutoScaling requests
type DescribeTableReplicaAutoScalingHandler interface {
	HandleDescribeTableReplicaAutoScaling(ctx *DescribeTableReplicaAutoScalingContext, output *DescribeTableReplicaAutoScalingOutput)
}

// DescribeTableReplicaAutoScalingHandlerFunc is a DescribeTableReplicaAutoScalingHandler function
type DescribeTableReplicaAutoScalingHandlerFunc func(ctx *DescribeTableReplicaAutoScalingContext, output *DescribeTableReplicaAutoScalingOutput)

// HandleDescribeTableReplicaAutoScaling implements DescribeTableReplicaAutoScalingHandler
func (h DescribeTableReplicaAutoScalingHandlerFunc) HandleDescribeTableReplicaAutoScaling(ctx *DescribeTableReplicaAutoScalingContext, output *DescribeTableReplicaAutoScalingOutput) {
	h(ctx, output)
}

// DescribeTableReplicaAutoScalingFinalHandler is the final DescribeTableReplicaAutoScalingHandler that executes a dynamodb DescribeTableReplicaAutoScaling operation
type DescribeTableReplicaAutoScalingFinalHandler struct{}

// HandleDescribeTableReplicaAutoScaling implements the DescribeTableReplicaAutoScalingHandler
func (h *DescribeTableReplicaAutoScalingFinalHandler) HandleDescribeTableReplicaAutoScaling(ctx *DescribeTableReplicaAutoScalingContext, output *DescribeTableReplicaAutoScalingOutput) {
	output.Set(ctx.Client.DescribeTableReplicaAutoScaling(ctx, ctx.Input))
}

// DescribeTableReplicaAutoScalingMiddleWare is a middleware function use for wrapping DescribeTableReplicaAutoScalingHandler requests
type DescribeTableReplicaAutoScalingMiddleWare interface {
	DescribeTableReplicaAutoScalingMiddleWare(next DescribeTableReplicaAutoScalingHandler) DescribeTableReplicaAutoScalingHandler
}

// DescribeTableReplicaAutoScalingMiddleWareFunc is a functional DescribeTableReplicaAutoScalingMiddleWare
type DescribeTableReplicaAutoScalingMiddleWareFunc func(next DescribeTableReplicaAutoScalingHandler) DescribeTableReplicaAutoScalingHandler

// DescribeTableReplicaAutoScalingMiddleWare implements the DescribeTableReplicaAutoScalingMiddleWare interface
func (mw DescribeTableReplicaAutoScalingMiddleWareFunc) DescribeTableReplicaAutoScalingMiddleWare(next DescribeTableReplicaAutoScalingHandler) DescribeTableReplicaAutoScalingHandler {
	return mw(next)
}

// DescribeTableReplicaAutoScaling represents a DescribeTableReplicaAutoScaling operation
type DescribeTableReplicaAutoScaling struct {
	*BaseOperation
	input       *ddb.DescribeTableReplicaAutoScalingInput
	middleWares []DescribeTableReplicaAutoScalingMiddleWare
}

// NewDescribeTableReplicaAutoScaling creates a new DescribeTableReplicaAutoScaling
func NewDescribeTableReplicaAutoScaling(input *ddb.DescribeTableReplicaAutoScalingInput, mws ...DescribeTableReplicaAutoScalingMiddleWare) *DescribeTableReplicaAutoScaling {
	return &DescribeTableReplicaAutoScaling{
		BaseOperation: NewOperation(),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the DescribeTableReplicaAutoScaling operation in a goroutine and returns a DescribeTableReplicaAutoScaling operation
func (op *DescribeTableReplicaAutoScaling) Invoke(ctx context.Context, client *ddb.Client) *DescribeTableReplicaAutoScaling {
	op.SetRunning() // operation now waiting for a response

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the DescribeTableReplicaAutoScaling operation
func (op *DescribeTableReplicaAutoScaling) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	output := new(DescribeTableReplicaAutoScalingOutput)

	defer func() { op.SetResponse(output.Get()) }()



	var h DescribeTableReplicaAutoScalingHandler

	h = new(DescribeTableReplicaAutoScalingFinalHandler)

	// loop in reverse to preserve middleware order
	for i := len(op.middleWares) - 1; i >= 0; i-- {
		h = op.middleWares[i].DescribeTableReplicaAutoScalingMiddleWare(h)
	}

	requestCtx := &DescribeTableReplicaAutoScalingContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	h.HandleDescribeTableReplicaAutoScaling(requestCtx, output)
}

// Await waits for the DescribeTableReplicaAutoScaling operation to be fulfilled and then returns a DescribeTableReplicaAutoScalingOutput and error
func (op *DescribeTableReplicaAutoScaling) Await() (*ddb.DescribeTableReplicaAutoScalingOutput, error) {
	out, err := op.BaseOperation.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.DescribeTableReplicaAutoScalingOutput), err
}

// NewDescribeTableReplicaAutoScalingInput creates a new DescribeTableReplicaAutoScalingInput
func NewDescribeTableReplicaAutoScalingInput(tableName *string) *ddb.DescribeTableReplicaAutoScalingInput {
	return &ddb.DescribeTableReplicaAutoScalingInput{
		TableName: tableName,
	}
}
