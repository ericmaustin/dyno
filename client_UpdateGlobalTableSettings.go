package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// UpdateGlobalTableSettings creates a new UpdateGlobalTableSettings, invokes and returns it
func (c *Client) UpdateGlobalTableSettings(ctx context.Context, input *ddb.UpdateGlobalTableSettingsInput, mw ...UpdateGlobalTableSettingsMiddleWare) *UpdateGlobalTableSettings {
	return NewUpdateGlobalTableSettings(input, mw...).Invoke(ctx, c.ddb)
}

// UpdateGlobalTableSettings creates a new UpdateGlobalTableSettings, passes it to the Pool and then returns the UpdateGlobalTableSettings
func (p *Pool) UpdateGlobalTableSettings(input *ddb.UpdateGlobalTableSettingsInput, mw ...UpdateGlobalTableSettingsMiddleWare) *UpdateGlobalTableSettings {
	op := NewUpdateGlobalTableSettings(input, mw...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// UpdateGlobalTableSettingsContext represents an exhaustive UpdateGlobalTableSettings operation request context
type UpdateGlobalTableSettingsContext struct {
	context.Context
	input  *ddb.UpdateGlobalTableSettingsInput
	client *ddb.Client
}

// UpdateGlobalTableSettingsOutput represents the output for the UpdateGlobalTableSettings opration
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
	output.Set(ctx.client.UpdateGlobalTableSettings(ctx, ctx.input))
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
	*Promise
	input       *ddb.UpdateGlobalTableSettingsInput
	middleWares []UpdateGlobalTableSettingsMiddleWare
}

// NewUpdateGlobalTableSettings creates a new UpdateGlobalTableSettings
func NewUpdateGlobalTableSettings(input *ddb.UpdateGlobalTableSettingsInput, mws ...UpdateGlobalTableSettingsMiddleWare) *UpdateGlobalTableSettings {
	return &UpdateGlobalTableSettings{
		Promise: NewPromise(),
		input:       input,
		middleWares: mws,
	}
}

// Invoke invokes the UpdateGlobalTableSettings operation and returns a UpdateGlobalTableSettingsPromise
func (op *UpdateGlobalTableSettings) Invoke(ctx context.Context, client *ddb.Client) *UpdateGlobalTableSettings {
	go op.DynoInvoke(ctx, client)

	return op
}

// DynoInvoke implements the Operation interface
func (op *UpdateGlobalTableSettings) DynoInvoke(ctx context.Context, client *ddb.Client) {
	output := new(UpdateGlobalTableSettingsOutput)

	defer func() { op.SetResponse(output.Get()) }()

	requestCtx := &UpdateGlobalTableSettingsContext{
		Context: ctx,
		client:  client,
		input:   op.input,
	}

	var h UpdateGlobalTableSettingsHandler

	h = new(UpdateGlobalTableSettingsFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].UpdateGlobalTableSettingsMiddleWare(h)
		}
	}

	h.HandleUpdateGlobalTableSettings(requestCtx, output)
}

// Await waits for the UpdateGlobalTableSettingsPromise to be fulfilled and then returns a UpdateGlobalTableSettingsOutput and error
func (op *UpdateGlobalTableSettings) Await() (*ddb.UpdateGlobalTableSettingsOutput, error) {
	out, err := op.Promise.Await()
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
