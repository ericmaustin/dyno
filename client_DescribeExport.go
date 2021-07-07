package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// DescribeExport executes DescribeExport operation and returns a DescribeExportPromise
func (c *Client) DescribeExport(ctx context.Context, input *ddb.DescribeExportInput, mw ...DescribeExportMiddleWare) *DescribeExport {
	return NewDescribeExport(input, mw...).Invoke(ctx, c.ddb)
}

// DescribeExport executes a DescribeExport operation with a DescribeExportInput in this pool and returns the DescribeExportPromise
func (p *Pool) DescribeExport(input *ddb.DescribeExportInput, mw ...DescribeExportMiddleWare) *DescribeExport {
	op := NewDescribeExport(input, mw...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// DescribeExportContext represents an exhaustive DescribeExport operation request context
type DescribeExportContext struct {
	context.Context
	Input  *ddb.DescribeExportInput
	Client *ddb.Client
}

// DescribeExportOutput represents the output for the DescribeExport opration
type DescribeExportOutput struct {
	out *ddb.DescribeExportOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *DescribeExportOutput) Set(out *ddb.DescribeExportOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *DescribeExportOutput) Get() (out *ddb.DescribeExportOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()
	return
}

// DescribeExportHandler represents a handler for DescribeExport requests
type DescribeExportHandler interface {
	HandleDescribeExport(ctx *DescribeExportContext, output *DescribeExportOutput)
}

// DescribeExportHandlerFunc is a DescribeExportHandler function
type DescribeExportHandlerFunc func(ctx *DescribeExportContext, output *DescribeExportOutput)

// HandleDescribeExport implements DescribeExportHandler
func (h DescribeExportHandlerFunc) HandleDescribeExport(ctx *DescribeExportContext, output *DescribeExportOutput) {
	h(ctx, output)
}

// DescribeExportFinalHandler is the final DescribeExportHandler that executes a dynamodb DescribeExport operation
type DescribeExportFinalHandler struct{}

// HandleDescribeExport implements the DescribeExportHandler
func (h *DescribeExportFinalHandler) HandleDescribeExport(ctx *DescribeExportContext, output *DescribeExportOutput) {
	output.Set(ctx.Client.DescribeExport(ctx, ctx.Input))
}

// DescribeExportMiddleWare is a middleware function use for wrapping DescribeExportHandler requests
type DescribeExportMiddleWare interface {
	DescribeExportMiddleWare(next DescribeExportHandler) DescribeExportHandler
}

// DescribeExportMiddleWareFunc is a functional DescribeExportMiddleWare
type DescribeExportMiddleWareFunc func(next DescribeExportHandler) DescribeExportHandler

// DescribeExportMiddleWare implements the DescribeExportMiddleWare interface
func (mw DescribeExportMiddleWareFunc) DescribeExportMiddleWare(next DescribeExportHandler) DescribeExportHandler {
	return mw(next)
}

// DescribeExport represents a DescribeExport operation
type DescribeExport struct {
	*Promise
	input       *ddb.DescribeExportInput
	middleWares []DescribeExportMiddleWare
}

// NewDescribeExport creates a new DescribeExport
func NewDescribeExport(input *ddb.DescribeExportInput, mws ...DescribeExportMiddleWare) *DescribeExport {
	return &DescribeExport{
		Promise: NewPromise(),
		input:       input,
		middleWares: mws,
	}
}

// DynoInvoke invokes the DescribeExport operation and returns a DescribeExportPromise
func (op *DescribeExport) Invoke(ctx context.Context, client *ddb.Client) *DescribeExport {
	go op.Invoke(ctx, client)

	return op
}

// DynoInvoke implements the Operation interface
func (op *DescribeExport) Invoke(ctx context.Context, client *ddb.Client) {
	output := new(DescribeExportOutput)

	defer func() { op.SetResponse(output.Get()) }()

	requestCtx := &DescribeExportContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	var h DescribeExportHandler

	h = new(DescribeExportFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].DescribeExportMiddleWare(h)
		}
	}

	h.HandleDescribeExport(requestCtx, output)
}

// Await waits for the DescribeExportPromise to be fulfilled and then returns a DescribeExportOutput and error
func (op *DescribeExport) Await() (*ddb.DescribeExportOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.DescribeExportOutput), err
}

// NewDescribeExportInput creates a new DescribeExportInput
func NewDescribeExportInput(exportArn *string) *ddb.DescribeExportInput {
	return &ddb.DescribeExportInput{
		ExportArn: exportArn,
	}
}
