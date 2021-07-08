package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// DescribeExport executes DescribeExport operation and returns a DescribeExport
func (s *Session) DescribeExport(input *ddb.DescribeExportInput, mw ...DescribeExportMiddleWare) *DescribeExport {
	return NewDescribeExport(input, mw...).Invoke(s.ctx, s.ddb)
}

// DescribeExport executes a DescribeExport operation with a DescribeExportInput in this pool and returns the DescribeExport
func (p *Pool) DescribeExport(input *ddb.DescribeExportInput, mw ...DescribeExportMiddleWare) *DescribeExport {
	op := NewDescribeExport(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// DescribeExportContext represents an exhaustive DescribeExport operation request context
type DescribeExportContext struct {
	context.Context
	Input  *ddb.DescribeExportInput
	Client *ddb.Client
}

// DescribeExportOutput represents the output for the DescribeExport operation
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
	*BaseOperation
	input       *ddb.DescribeExportInput
	middleWares []DescribeExportMiddleWare
}

// NewDescribeExport creates a new DescribeExport
func NewDescribeExport(input *ddb.DescribeExportInput, mws ...DescribeExportMiddleWare) *DescribeExport {
	return &DescribeExport{
		BaseOperation: NewOperation(),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the DescribeExport operation in a goroutine and returns the DescribeExport operation
func (op *DescribeExport) Invoke(ctx context.Context, client *ddb.Client) *DescribeExport {
	op.SetRunning() // operation now waiting for a response

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the DescribeExport operation
func (op *DescribeExport) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	output := new(DescribeExportOutput)

	defer func() { op.SetResponse(output.Get()) }()

	var h DescribeExportHandler

	h = new(DescribeExportFinalHandler)

	// loop in reverse to preserve middleware order
	for i := len(op.middleWares) - 1; i >= 0; i-- {
		h = op.middleWares[i].DescribeExportMiddleWare(h)
	}

	requestCtx := &DescribeExportContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	h.HandleDescribeExport(requestCtx, output)
}

// Await waits for the DescribeExport operation to be fulfilled and then returns a DescribeExportOutput and error
func (op *DescribeExport) Await() (*ddb.DescribeExportOutput, error) {
	out, err := op.BaseOperation.Await()
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
