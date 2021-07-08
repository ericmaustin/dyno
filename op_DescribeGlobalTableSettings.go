package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// DescribeGlobalTableSettings executes DescribeGlobalTableSettings operation and returns a DescribeGlobalTableSettings
func (s *Session) DescribeGlobalTableSettings(input *ddb.DescribeGlobalTableSettingsInput, mw ...DescribeGlobalTableSettingsMiddleWare) *DescribeGlobalTableSettings {
	return NewDescribeGlobalTableSettings(input, mw...).Invoke(s.ctx, s.ddb)
}

// DescribeGlobalTableSettings executes a DescribeGlobalTableSettings operation with a DescribeGlobalTableSettingsInput in this pool and returns the DescribeGlobalTableSettings
func (p *Pool) DescribeGlobalTableSettings(input *ddb.DescribeGlobalTableSettingsInput, mw ...DescribeGlobalTableSettingsMiddleWare) *DescribeGlobalTableSettings {
	op := NewDescribeGlobalTableSettings(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// DescribeGlobalTableSettingsContext represents an exhaustive DescribeGlobalTableSettings operation request context
type DescribeGlobalTableSettingsContext struct {
	context.Context
	Input  *ddb.DescribeGlobalTableSettingsInput
	Client *ddb.Client
}

// DescribeGlobalTableSettingsOutput represents the output for the DescribeGlobalTableSettings operation
type DescribeGlobalTableSettingsOutput struct {
	out *ddb.DescribeGlobalTableSettingsOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *DescribeGlobalTableSettingsOutput) Set(out *ddb.DescribeGlobalTableSettingsOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *DescribeGlobalTableSettingsOutput) Get() (out *ddb.DescribeGlobalTableSettingsOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()

	return
}

// DescribeGlobalTableSettingsHandler represents a handler for DescribeGlobalTableSettings requests
type DescribeGlobalTableSettingsHandler interface {
	HandleDescribeGlobalTableSettings(ctx *DescribeGlobalTableSettingsContext, output *DescribeGlobalTableSettingsOutput)
}

// DescribeGlobalTableSettingsHandlerFunc is a DescribeGlobalTableSettingsHandler function
type DescribeGlobalTableSettingsHandlerFunc func(ctx *DescribeGlobalTableSettingsContext, output *DescribeGlobalTableSettingsOutput)

// HandleDescribeGlobalTableSettings implements DescribeGlobalTableSettingsHandler
func (h DescribeGlobalTableSettingsHandlerFunc) HandleDescribeGlobalTableSettings(ctx *DescribeGlobalTableSettingsContext, output *DescribeGlobalTableSettingsOutput) {
	h(ctx, output)
}

// DescribeGlobalTableSettingsFinalHandler is the final DescribeGlobalTableSettingsHandler that executes a dynamodb DescribeGlobalTableSettings operation
type DescribeGlobalTableSettingsFinalHandler struct{}

// HandleDescribeGlobalTableSettings implements the DescribeGlobalTableSettingsHandler
func (h *DescribeGlobalTableSettingsFinalHandler) HandleDescribeGlobalTableSettings(ctx *DescribeGlobalTableSettingsContext, output *DescribeGlobalTableSettingsOutput) {
	output.Set(ctx.Client.DescribeGlobalTableSettings(ctx, ctx.Input))
}

// DescribeGlobalTableSettingsMiddleWare is a middleware function use for wrapping DescribeGlobalTableSettingsHandler requests
type DescribeGlobalTableSettingsMiddleWare interface {
	DescribeGlobalTableSettingsMiddleWare(next DescribeGlobalTableSettingsHandler) DescribeGlobalTableSettingsHandler
}

// DescribeGlobalTableSettingsMiddleWareFunc is a functional DescribeGlobalTableSettingsMiddleWare
type DescribeGlobalTableSettingsMiddleWareFunc func(next DescribeGlobalTableSettingsHandler) DescribeGlobalTableSettingsHandler

// DescribeGlobalTableSettingsMiddleWare implements the DescribeGlobalTableSettingsMiddleWare interface
func (mw DescribeGlobalTableSettingsMiddleWareFunc) DescribeGlobalTableSettingsMiddleWare(next DescribeGlobalTableSettingsHandler) DescribeGlobalTableSettingsHandler {
	return mw(next)
}

// DescribeGlobalTableSettings represents a DescribeGlobalTableSettings operation
type DescribeGlobalTableSettings struct {
	*BaseOperation
	input       *ddb.DescribeGlobalTableSettingsInput
	middleWares []DescribeGlobalTableSettingsMiddleWare
}

// NewDescribeGlobalTableSettings creates a new DescribeGlobalTableSettings
func NewDescribeGlobalTableSettings(input *ddb.DescribeGlobalTableSettingsInput, mws ...DescribeGlobalTableSettingsMiddleWare) *DescribeGlobalTableSettings {
	return &DescribeGlobalTableSettings{
		BaseOperation: NewOperation(),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the DescribeGlobalTableSettings operation in a goroutine and returns a DescribeGlobalTableSettings operation
func (op *DescribeGlobalTableSettings) Invoke(ctx context.Context, client *ddb.Client) *DescribeGlobalTableSettings {
	op.SetRunning() // operation now waiting for a response

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the DescribeGlobalTableSettings operation
func (op *DescribeGlobalTableSettings) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	output := new(DescribeGlobalTableSettingsOutput)

	defer func() { op.SetResponse(output.Get()) }()

	var h DescribeGlobalTableSettingsHandler

	h = new(DescribeGlobalTableSettingsFinalHandler)

	// loop in reverse to preserve middleware order
	for i := len(op.middleWares) - 1; i >= 0; i-- {
		h = op.middleWares[i].DescribeGlobalTableSettingsMiddleWare(h)
	}

	requestCtx := &DescribeGlobalTableSettingsContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	h.HandleDescribeGlobalTableSettings(requestCtx, output)
}

// Await waits for the DescribeGlobalTableSettings to be fulfilled and then returns a DescribeGlobalTableSettingsOutput and error
func (op *DescribeGlobalTableSettings) Await() (*ddb.DescribeGlobalTableSettingsOutput, error) {
	out, err := op.BaseOperation.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.DescribeGlobalTableSettingsOutput), err
}

// NewDescribeGlobalTableSettingsInput creates a new DescribeGlobalTableSettingsInput
func NewDescribeGlobalTableSettingsInput(tableName *string) *ddb.DescribeGlobalTableSettingsInput {
	return &ddb.DescribeGlobalTableSettingsInput{
		GlobalTableName: tableName,
	}
}
