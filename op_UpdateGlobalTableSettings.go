package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// UpdateGlobalTableSettings executes UpdateGlobalTableSettings operation and returns a UpdateGlobalTableSettings operation
func (s *Session) UpdateGlobalTableSettings(input *ddb.UpdateGlobalTableSettingsInput, mw ...UpdateGlobalTableSettingsMiddleWare) *UpdateGlobalTableSettings {
	return NewUpdateGlobalTableSettings(input, mw...).Invoke(s.ctx, s.ddb)
}

// UpdateGlobalTableSettings executes a UpdateGlobalTableSettings operation with a UpdateGlobalTableSettingsInput in this pool and returns the UpdateGlobalTableSettings operation
func (p *Pool) UpdateGlobalTableSettings(input *ddb.UpdateGlobalTableSettingsInput, mw ...UpdateGlobalTableSettingsMiddleWare) *UpdateGlobalTableSettings {
	op := NewUpdateGlobalTableSettings(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// UpdateGlobalTableSettingsContext represents an exhaustive UpdateGlobalTableSettings operation request context
type UpdateGlobalTableSettingsContext struct {
	context.Context
	Input  *ddb.UpdateGlobalTableSettingsInput
	Client *ddb.Client
}

// UpdateGlobalTableSettingsOutput represents the output for the UpdateGlobalTableSettings operation
type UpdateGlobalTableSettingsOutput struct {
	out *ddb.UpdateGlobalTableSettingsOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *UpdateGlobalTableSettingsOutput) Set(out *ddb.UpdateGlobalTableSettingsOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *UpdateGlobalTableSettingsOutput) Get() (out *ddb.UpdateGlobalTableSettingsOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()

	return
}

// UpdateGlobalTableSettingsHandler represents a handler for UpdateGlobalTableSettings requests
type UpdateGlobalTableSettingsHandler interface {
	HandleUpdateGlobalTableSettings(ctx *UpdateGlobalTableSettingsContext, output *UpdateGlobalTableSettingsOutput)
}

// UpdateGlobalTableSettingsHandlerFunc is a UpdateGlobalTableSettingsHandler function
type UpdateGlobalTableSettingsHandlerFunc func(ctx *UpdateGlobalTableSettingsContext, output *UpdateGlobalTableSettingsOutput)

// HandleUpdateGlobalTableSettings implements UpdateGlobalTableSettingsHandler
func (h UpdateGlobalTableSettingsHandlerFunc) HandleUpdateGlobalTableSettings(ctx *UpdateGlobalTableSettingsContext, output *UpdateGlobalTableSettingsOutput) {
	h(ctx, output)
}

// UpdateGlobalTableSettingsFinalHandler is the final UpdateGlobalTableSettingsHandler that executes a dynamodb UpdateGlobalTableSettings operation
type UpdateGlobalTableSettingsFinalHandler struct{}

// HandleUpdateGlobalTableSettings implements the UpdateGlobalTableSettingsHandler
func (h *UpdateGlobalTableSettingsFinalHandler) HandleUpdateGlobalTableSettings(ctx *UpdateGlobalTableSettingsContext, output *UpdateGlobalTableSettingsOutput) {
	output.Set(ctx.Client.UpdateGlobalTableSettings(ctx, ctx.Input))
}

// UpdateGlobalTableSettingsMiddleWare is a middleware function use for wrapping UpdateGlobalTableSettingsHandler requests
type UpdateGlobalTableSettingsMiddleWare interface {
	UpdateGlobalTableSettingsMiddleWare(next UpdateGlobalTableSettingsHandler) UpdateGlobalTableSettingsHandler
}

// UpdateGlobalTableSettingsMiddleWareFunc is a functional UpdateGlobalTableSettingsMiddleWare
type UpdateGlobalTableSettingsMiddleWareFunc func(next UpdateGlobalTableSettingsHandler) UpdateGlobalTableSettingsHandler

// UpdateGlobalTableSettingsMiddleWare implements the UpdateGlobalTableSettingsMiddleWare interface
func (mw UpdateGlobalTableSettingsMiddleWareFunc) UpdateGlobalTableSettingsMiddleWare(next UpdateGlobalTableSettingsHandler) UpdateGlobalTableSettingsHandler {
	return mw(next)
}

// UpdateGlobalTableSettings represents a UpdateGlobalTableSettings operation
type UpdateGlobalTableSettings struct {
	*BaseOperation
	input       *ddb.UpdateGlobalTableSettingsInput
	middleWares []UpdateGlobalTableSettingsMiddleWare
}

// NewUpdateGlobalTableSettings creates a new UpdateGlobalTableSettings operation
func NewUpdateGlobalTableSettings(input *ddb.UpdateGlobalTableSettingsInput, mws ...UpdateGlobalTableSettingsMiddleWare) *UpdateGlobalTableSettings {
	return &UpdateGlobalTableSettings{
		BaseOperation: NewOperation(),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the UpdateGlobalTableSettings operation in a goroutine and returns a UpdateGlobalTableSettings operation
func (op *UpdateGlobalTableSettings) Invoke(ctx context.Context, client *ddb.Client) *UpdateGlobalTableSettings {
	op.SetRunning() // operation now waiting for a response

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the UpdateGlobalTableSettings operation
func (op *UpdateGlobalTableSettings) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	output := new(UpdateGlobalTableSettingsOutput)

	defer func() { op.SetResponse(output.Get()) }()

	var h UpdateGlobalTableSettingsHandler

	h = new(UpdateGlobalTableSettingsFinalHandler)

	// loop in reverse to preserve middleware order
	for i := len(op.middleWares) - 1; i >= 0; i-- {
		h = op.middleWares[i].UpdateGlobalTableSettingsMiddleWare(h)
	}

	requestCtx := &UpdateGlobalTableSettingsContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	h.HandleUpdateGlobalTableSettings(requestCtx, output)
}

// Await waits for the UpdateGlobalTableSettings operation to be fulfilled and then returns a UpdateGlobalTableSettingsOutput and error
func (op *UpdateGlobalTableSettings) Await() (*ddb.UpdateGlobalTableSettingsOutput, error) {
	out, err := op.BaseOperation.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.UpdateGlobalTableSettingsOutput), err
}

// NewUpdateGlobalTableSettingsInput creates a new UpdateGlobalTableSettingsInput
func NewUpdateGlobalTableSettingsInput(tableName *string) *ddb.UpdateGlobalTableSettingsInput {
	return &ddb.UpdateGlobalTableSettingsInput{
		GlobalTableName: tableName,
	}
}

// todo: add UpdateGlobalTableSettingsInputBuilder
