package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// UpdateGlobalTable executes UpdateGlobalTable operation and returns a UpdateGlobalTable operation
func (s *Session) UpdateGlobalTable(input *ddb.UpdateGlobalTableInput, mw ...UpdateGlobalTableMiddleWare) *UpdateGlobalTable {
	return NewUpdateGlobalTable(input, mw...).Invoke(s.ctx, s.ddb)
}

// UpdateGlobalTable executes a UpdateGlobalTable operation with a UpdateGlobalTableInput in this pool and returns the UpdateGlobalTable operation
func (p *Pool) UpdateGlobalTable(input *ddb.UpdateGlobalTableInput, mw ...UpdateGlobalTableMiddleWare) *UpdateGlobalTable {
	op := NewUpdateGlobalTable(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// UpdateGlobalTableContext represents an exhaustive UpdateGlobalTable operation request context
type UpdateGlobalTableContext struct {
	context.Context
	Input  *ddb.UpdateGlobalTableInput
	Client *ddb.Client
}

// UpdateGlobalTableOutput represents the output for the UpdateGlobalTable operation
type UpdateGlobalTableOutput struct {
	out *ddb.UpdateGlobalTableOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *UpdateGlobalTableOutput) Set(out *ddb.UpdateGlobalTableOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *UpdateGlobalTableOutput) Get() (out *ddb.UpdateGlobalTableOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()

	return
}

// UpdateGlobalTableHandler represents a handler for UpdateGlobalTable requests
type UpdateGlobalTableHandler interface {
	HandleUpdateGlobalTable(ctx *UpdateGlobalTableContext, output *UpdateGlobalTableOutput)
}

// UpdateGlobalTableHandlerFunc is a UpdateGlobalTableHandler function
type UpdateGlobalTableHandlerFunc func(ctx *UpdateGlobalTableContext, output *UpdateGlobalTableOutput)

// HandleUpdateGlobalTable implements UpdateGlobalTableHandler
func (h UpdateGlobalTableHandlerFunc) HandleUpdateGlobalTable(ctx *UpdateGlobalTableContext, output *UpdateGlobalTableOutput) {
	h(ctx, output)
}

// UpdateGlobalTableFinalHandler is the final UpdateGlobalTableHandler that executes a dynamodb UpdateGlobalTable operation
type UpdateGlobalTableFinalHandler struct{}

// HandleUpdateGlobalTable implements the UpdateGlobalTableHandler
func (h *UpdateGlobalTableFinalHandler) HandleUpdateGlobalTable(ctx *UpdateGlobalTableContext, output *UpdateGlobalTableOutput) {
	output.Set(ctx.Client.UpdateGlobalTable(ctx, ctx.Input))
}

// UpdateGlobalTableMiddleWare is a middleware function use for wrapping UpdateGlobalTableHandler requests
type UpdateGlobalTableMiddleWare interface {
	UpdateGlobalTableMiddleWare(next UpdateGlobalTableHandler) UpdateGlobalTableHandler
}

// UpdateGlobalTableMiddleWareFunc is a functional UpdateGlobalTableMiddleWare
type UpdateGlobalTableMiddleWareFunc func(next UpdateGlobalTableHandler) UpdateGlobalTableHandler

// UpdateGlobalTableMiddleWare implements the UpdateGlobalTableMiddleWare interface
func (mw UpdateGlobalTableMiddleWareFunc) UpdateGlobalTableMiddleWare(next UpdateGlobalTableHandler) UpdateGlobalTableHandler {
	return mw(next)
}

// UpdateGlobalTable represents a UpdateGlobalTable operation
type UpdateGlobalTable struct {
	*BaseOperation
	input       *ddb.UpdateGlobalTableInput
	middleWares []UpdateGlobalTableMiddleWare
}

// NewUpdateGlobalTable creates a new UpdateGlobalTable operation
func NewUpdateGlobalTable(input *ddb.UpdateGlobalTableInput, mws ...UpdateGlobalTableMiddleWare) *UpdateGlobalTable {
	return &UpdateGlobalTable{
		BaseOperation: NewOperation(),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the UpdateGlobalTable operation in a goroutine and returns a UpdateGlobalTable operation
func (op *UpdateGlobalTable) Invoke(ctx context.Context, client *ddb.Client) *UpdateGlobalTable {
	op.SetRunning() // operation now waiting for a response

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the UpdateGlobalTable operation
func (op *UpdateGlobalTable) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	output := new(UpdateGlobalTableOutput)

	defer func() { op.SetResponse(output.Get()) }()

	var h UpdateGlobalTableHandler

	h = new(UpdateGlobalTableFinalHandler)

	// loop in reverse to preserve middleware order
	for i := len(op.middleWares) - 1; i >= 0; i-- {
		h = op.middleWares[i].UpdateGlobalTableMiddleWare(h)
	}

	requestCtx := &UpdateGlobalTableContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	h.HandleUpdateGlobalTable(requestCtx, output)
}

// Await waits for the UpdateGlobalTable operation to be fulfilled and then returns a UpdateGlobalTableOutput and error
func (op *UpdateGlobalTable) Await() (*ddb.UpdateGlobalTableOutput, error) {
	out, err := op.BaseOperation.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.UpdateGlobalTableOutput), err
}

// NewUpdateGlobalTableInput creates a new UpdateGlobalTableInput
func NewUpdateGlobalTableInput(tableName *string) *ddb.UpdateGlobalTableInput {
	return &ddb.UpdateGlobalTableInput{
		GlobalTableName: tableName,
	}
}

// todo: add UpdateGlobalTableInputBuilder