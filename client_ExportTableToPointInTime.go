package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// ExportTableToPointInTime executes ExportTableToPointInTime operation and returns a ExportTableToPointInTimePromise
func (c *Client) ExportTableToPointInTime(ctx context.Context, input *ddb.ExportTableToPointInTimeInput, mw ...ExportTableToPointInTimeMiddleWare) *ExportTableToPointInTime {
	return NewExportTableToPointInTime(input, mw...).Invoke(ctx, c.ddb)
}

// ExportTableToPointInTime executes a ExportTableToPointInTime operation with a ExportTableToPointInTimeInput in this pool and returns the ExportTableToPointInTimePromise
func (p *Pool) ExportTableToPointInTime(input *ddb.ExportTableToPointInTimeInput, mw ...ExportTableToPointInTimeMiddleWare) *ExportTableToPointInTime {
	op := NewExportTableToPointInTime(input, mw...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// ExportTableToPointInTimeContext represents an exhaustive ExportTableToPointInTime operation request context
type ExportTableToPointInTimeContext struct {
	context.Context
	Input  *ddb.ExportTableToPointInTimeInput
	Client *ddb.Client
}

// ExportTableToPointInTimeOutput represents the output for the ExportTableToPointInTime opration
type ExportTableToPointInTimeOutput struct {
	out *ddb.ExportTableToPointInTimeOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *ExportTableToPointInTimeOutput) Set(out *ddb.ExportTableToPointInTimeOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *ExportTableToPointInTimeOutput) Get() (out *ddb.ExportTableToPointInTimeOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()
	return
}

// ExportTableToPointInTimeHandler represents a handler for ExportTableToPointInTime requests
type ExportTableToPointInTimeHandler interface {
	HandleExportTableToPointInTime(ctx *ExportTableToPointInTimeContext, output *ExportTableToPointInTimeOutput)
}

// ExportTableToPointInTimeHandlerFunc is a ExportTableToPointInTimeHandler function
type ExportTableToPointInTimeHandlerFunc func(ctx *ExportTableToPointInTimeContext, output *ExportTableToPointInTimeOutput)

// HandleExportTableToPointInTime implements ExportTableToPointInTimeHandler
func (h ExportTableToPointInTimeHandlerFunc) HandleExportTableToPointInTime(ctx *ExportTableToPointInTimeContext, output *ExportTableToPointInTimeOutput) {
	h(ctx, output)
}

// ExportTableToPointInTimeFinalHandler is the final ExportTableToPointInTimeHandler that executes a dynamodb ExportTableToPointInTime operation
type ExportTableToPointInTimeFinalHandler struct{}

// HandleExportTableToPointInTime implements the ExportTableToPointInTimeHandler
func (h *ExportTableToPointInTimeFinalHandler) HandleExportTableToPointInTime(ctx *ExportTableToPointInTimeContext, output *ExportTableToPointInTimeOutput) {
	output.Set(ctx.Client.ExportTableToPointInTime(ctx, ctx.Input))
}

// ExportTableToPointInTimeMiddleWare is a middleware function use for wrapping ExportTableToPointInTimeHandler requests
type ExportTableToPointInTimeMiddleWare interface {
	ExportTableToPointInTimeMiddleWare(next ExportTableToPointInTimeHandler) ExportTableToPointInTimeHandler
}

// ExportTableToPointInTimeMiddleWareFunc is a functional ExportTableToPointInTimeMiddleWare
type ExportTableToPointInTimeMiddleWareFunc func(next ExportTableToPointInTimeHandler) ExportTableToPointInTimeHandler

// ExportTableToPointInTimeMiddleWare implements the ExportTableToPointInTimeMiddleWare interface
func (mw ExportTableToPointInTimeMiddleWareFunc) ExportTableToPointInTimeMiddleWare(next ExportTableToPointInTimeHandler) ExportTableToPointInTimeHandler {
	return mw(next)
}

// ExportTableToPointInTime represents a ExportTableToPointInTime operation
type ExportTableToPointInTime struct {
	*Promise
	input       *ddb.ExportTableToPointInTimeInput
	middleWares []ExportTableToPointInTimeMiddleWare
}

// NewExportTableToPointInTime creates a new ExportTableToPointInTime
func NewExportTableToPointInTime(input *ddb.ExportTableToPointInTimeInput, mws ...ExportTableToPointInTimeMiddleWare) *ExportTableToPointInTime {
	return &ExportTableToPointInTime{
		Promise: NewPromise(),
		input:       input,
		middleWares: mws,
	}
}

// Invoke invokes the ExportTableToPointInTime operation and returns a ExportTableToPointInTimePromise
func (op *ExportTableToPointInTime) Invoke(ctx context.Context, client *ddb.Client) *ExportTableToPointInTime {
	go op.DynoInvoke(ctx, client)

	return op
}

// DynoInvoke implements the Operation interface
func (op *ExportTableToPointInTime) DynoInvoke(ctx context.Context, client *ddb.Client) {
	output := new(ExportTableToPointInTimeOutput)

	defer func() { op.SetResponse(output.Get()) }()

	requestCtx := &ExportTableToPointInTimeContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	var h ExportTableToPointInTimeHandler

	h = new(ExportTableToPointInTimeFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].ExportTableToPointInTimeMiddleWare(h)
		}
	}

	h.HandleExportTableToPointInTime(requestCtx, output)
}

// Await waits for the ExportTableToPointInTimePromise to be fulfilled and then returns a ExportTableToPointInTimeOutput and error
func (op *ExportTableToPointInTime) Await() (*ddb.ExportTableToPointInTimeOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.ExportTableToPointInTimeOutput), err
}