package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"sync"
)

// UpdateTimeToLive executes UpdateTimeToLive operation and returns a UpdateTimeToLive operation
func (s *Session) UpdateTimeToLive(input *ddb.UpdateTimeToLiveInput, mw ...UpdateTimeToLiveMiddleWare) *UpdateTimeToLive {
	return NewUpdateTimeToLive(input, mw...).Invoke(s.ctx, s.ddb)
}

// UpdateTimeToLive executes a UpdateTimeToLive operation with a UpdateTimeToLiveInput in this pool and returns the UpdateTimeToLive operation
func (p *Pool) UpdateTimeToLive(input *ddb.UpdateTimeToLiveInput, mw ...UpdateTimeToLiveMiddleWare) *UpdateTimeToLive {
	op := NewUpdateTimeToLive(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// UpdateTimeToLiveContext represents an exhaustive UpdateTimeToLive operation request context
type UpdateTimeToLiveContext struct {
	context.Context
	Input  *ddb.UpdateTimeToLiveInput
	Client *ddb.Client
}

// UpdateTimeToLiveOutput represents the output for the UpdateTimeToLive operation
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
	*BaseOperation
	input       *ddb.UpdateTimeToLiveInput
	middleWares []UpdateTimeToLiveMiddleWare
}

// NewUpdateTimeToLive creates a new UpdateTimeToLive operation
func NewUpdateTimeToLive(input *ddb.UpdateTimeToLiveInput, mws ...UpdateTimeToLiveMiddleWare) *UpdateTimeToLive {
	return &UpdateTimeToLive{
		BaseOperation: NewOperation(),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the UpdateTimeToLive operation in a goroutine and returns a UpdateTimeToLive operation
func (op *UpdateTimeToLive) Invoke(ctx context.Context, client *ddb.Client) *UpdateTimeToLive {
	op.SetRunning() // operation now waiting for a response

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the UpdateTimeToLive operation
func (op *UpdateTimeToLive) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	output := new(UpdateTimeToLiveOutput)

	defer func() { op.SetResponse(output.Get()) }()

	var h UpdateTimeToLiveHandler

	h = new(UpdateTimeToLiveFinalHandler)

	// loop in reverse to preserve middleware order
	for i := len(op.middleWares) - 1; i >= 0; i-- {
		h = op.middleWares[i].UpdateTimeToLiveMiddleWare(h)
	}

	requestCtx := &UpdateTimeToLiveContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	h.HandleUpdateTimeToLive(requestCtx, output)
}

// Await waits for the UpdateTimeToLive operation to be fulfilled and then returns a UpdateTimeToLiveOutput and error
func (op *UpdateTimeToLive) Await() (*ddb.UpdateTimeToLiveOutput, error) {
	out, err := op.BaseOperation.Await()
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
