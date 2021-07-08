package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// UpdateTableReplicaAutoScaling executes UpdateTableReplicaAutoScaling operation and returns a UpdateTableReplicaAutoScaling operation
func (s *Session) UpdateTableReplicaAutoScaling(input *ddb.UpdateTableReplicaAutoScalingInput, mw ...UpdateTableReplicaAutoScalingMiddleWare) *UpdateTableReplicaAutoScaling {
	return NewUpdateTableReplicaAutoScaling(input, mw...).Invoke(s.ctx, s.ddb)
}

// UpdateTableReplicaAutoScaling executes a UpdateTableReplicaAutoScaling operation with a UpdateTableReplicaAutoScalingInput in this pool and returns the UpdateTableReplicaAutoScaling operation
func (p *Pool) UpdateTableReplicaAutoScaling(input *ddb.UpdateTableReplicaAutoScalingInput, mw ...UpdateTableReplicaAutoScalingMiddleWare) *UpdateTableReplicaAutoScaling {
	op := NewUpdateTableReplicaAutoScaling(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// UpdateTableReplicaAutoScalingContext represents an exhaustive UpdateTableReplicaAutoScaling operation request context
type UpdateTableReplicaAutoScalingContext struct {
	context.Context
	Input  *ddb.UpdateTableReplicaAutoScalingInput
	Client *ddb.Client
}

// UpdateTableReplicaAutoScalingOutput represents the output for the UpdateTableReplicaAutoScaling operation
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
	output.Set(ctx.Client.UpdateTableReplicaAutoScaling(ctx, ctx.Input))
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
	*BaseOperation
	input       *ddb.UpdateTableReplicaAutoScalingInput
	middleWares []UpdateTableReplicaAutoScalingMiddleWare
}

// NewUpdateTableReplicaAutoScaling creates a new UpdateTableReplicaAutoScaling operation
func NewUpdateTableReplicaAutoScaling(input *ddb.UpdateTableReplicaAutoScalingInput, mws ...UpdateTableReplicaAutoScalingMiddleWare) *UpdateTableReplicaAutoScaling {
	return &UpdateTableReplicaAutoScaling{
		BaseOperation: NewOperation(),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the UpdateTableReplicaAutoScaling operation in a goroutine and returns a UpdateTableReplicaAutoScaling operation
func (op *UpdateTableReplicaAutoScaling) Invoke(ctx context.Context, client *ddb.Client) *UpdateTableReplicaAutoScaling {
	op.SetRunning() // operation now waiting for a response

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the UpdateTableReplicaAutoScaling operation
func (op *UpdateTableReplicaAutoScaling) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	output := new(UpdateTableReplicaAutoScalingOutput)

	defer func() { op.SetResponse(output.Get()) }()

	var h UpdateTableReplicaAutoScalingHandler

	h = new(UpdateTableReplicaAutoScalingFinalHandler)

	// loop in reverse to preserve middleware order
	for i := len(op.middleWares) - 1; i >= 0; i-- {
		h = op.middleWares[i].UpdateTableReplicaAutoScalingMiddleWare(h)
	}

	requestCtx := &UpdateTableReplicaAutoScalingContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	h.HandleUpdateTableReplicaAutoScaling(requestCtx, output)
}

// Await waits for the UpdateTableReplicaAutoScaling operation to be fulfilled and then returns a UpdateTableReplicaAutoScalingOutput and error
func (op *UpdateTableReplicaAutoScaling) Await() (*ddb.UpdateTableReplicaAutoScalingOutput, error) {
	out, err := op.BaseOperation.Await()
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
