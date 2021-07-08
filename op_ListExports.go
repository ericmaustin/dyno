package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// ListExports executes ListExports operation and returns a ListExports operation
func (s *Session) ListExports(input *ddb.ListExportsInput, mw ...ListExportsMiddleWare) *ListExports {
	return NewListExports(input, mw...).Invoke(s.ctx, s.ddb)
}

// ListExports executes a ListExports operation with a ListExportsInput in this pool and returns the ListExports operation
func (p *Pool) ListExports(input *ddb.ListExportsInput, mw ...ListExportsMiddleWare) *ListExports {
	op := NewListExports(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// ListExportsContext represents an exhaustive ListExports operation request context
type ListExportsContext struct {
	context.Context
	Input  *ddb.ListExportsInput
	Client *ddb.Client
}

// ListExportsOutput represents the output for the ListExports operation
type ListExportsOutput struct {
	out *ddb.ListExportsOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *ListExportsOutput) Set(out *ddb.ListExportsOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *ListExportsOutput) Get() (out *ddb.ListExportsOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()

	return
}

// ListExportsHandler represents a handler for ListExports requests
type ListExportsHandler interface {
	HandleListExports(ctx *ListExportsContext, output *ListExportsOutput)
}

// ListExportsHandlerFunc is a ListExportsHandler function
type ListExportsHandlerFunc func(ctx *ListExportsContext, output *ListExportsOutput)

// HandleListExports implements ListExportsHandler
func (h ListExportsHandlerFunc) HandleListExports(ctx *ListExportsContext, output *ListExportsOutput) {
	h(ctx, output)
}

// ListExportsFinalHandler is the final ListExportsHandler that executes a dynamodb ListExports operation
type ListExportsFinalHandler struct{}

// HandleListExports implements the ListExportsHandler
func (h *ListExportsFinalHandler) HandleListExports(ctx *ListExportsContext, output *ListExportsOutput) {
	output.Set(ctx.Client.ListExports(ctx, ctx.Input))
}

// ListExportsMiddleWare is a middleware function use for wrapping ListExportsHandler requests
type ListExportsMiddleWare interface {
	ListExportsMiddleWare(next ListExportsHandler) ListExportsHandler
}

// ListExportsMiddleWareFunc is a functional ListExportsMiddleWare
type ListExportsMiddleWareFunc func(next ListExportsHandler) ListExportsHandler

// ListExportsMiddleWare implements the ListExportsMiddleWare interface
func (mw ListExportsMiddleWareFunc) ListExportsMiddleWare(next ListExportsHandler) ListExportsHandler {
	return mw(next)
}

// ListExports represents a ListExports operation
type ListExports struct {
	*BaseOperation
	input       *ddb.ListExportsInput
	middleWares []ListExportsMiddleWare
}

// NewListExports creates a new ListExports operation
func NewListExports(input *ddb.ListExportsInput, mws ...ListExportsMiddleWare) *ListExports {
	return &ListExports{
		BaseOperation: NewOperation(),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the ListExports operation in a goroutine and returns a ListExports operation
func (op *ListExports) Invoke(ctx context.Context, client *ddb.Client) *ListExports {
	op.SetRunning() // operation now waiting for a response

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the ListExports operation
func (op *ListExports) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	output := new(ListExportsOutput)

	defer func() { op.SetResponse(output.Get()) }()

	var h ListExportsHandler

	h = new(ListExportsFinalHandler)

	// loop in reverse to preserve middleware order
	for i := len(op.middleWares) - 1; i >= 0; i-- {
		h = op.middleWares[i].ListExportsMiddleWare(h)
	}

	requestCtx := &ListExportsContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	h.HandleListExports(requestCtx, output)
}

// Await waits for the ListExports operation to be fulfilled and then returns a ListExportsOutput and error
func (op *ListExports) Await() (*ddb.ListExportsOutput, error) {
	out, err := op.BaseOperation.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.ListExportsOutput), err
}

// NewListExportsInput creates a new ListExportsInput
func NewListExportsInput() *ddb.ListExportsInput {
	return &ddb.ListExportsInput{}
}


// todo: ListAllExports operation