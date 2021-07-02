package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// ListTagsOfResource executes ListTagsOfResource operation and returns a ListTagsOfResource
func (c *Client) ListTagsOfResource(ctx context.Context, input *ddb.ListTagsOfResourceInput, mw ...ListTagsOfResourceMiddleWare) *ListTagsOfResource {
	return NewListTagsOfResource(input, mw...).Invoke(ctx, c.ddb)
}

// ListTagsOfResource executes a ListTagsOfResource operation with a ListTagsOfResourceInput in this pool and returns it
func (p *Pool) ListTagsOfResource(input *ddb.ListTagsOfResourceInput, mw ...ListTagsOfResourceMiddleWare) *ListTagsOfResource {
	op := NewListTagsOfResource(input, mw...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// ListTagsOfResourceContext represents an exhaustive ListTagsOfResource operation request context
type ListTagsOfResourceContext struct {
	context.Context
	Input  *ddb.ListTagsOfResourceInput
	Client *ddb.Client
}

// ListTagsOfResourceOutput represents the output for the ListTagsOfResource operation
type ListTagsOfResourceOutput struct {
	out *ddb.ListTagsOfResourceOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *ListTagsOfResourceOutput) Set(out *ddb.ListTagsOfResourceOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *ListTagsOfResourceOutput) Get() (out *ddb.ListTagsOfResourceOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()
	return
}

// ListTagsOfResourceHandler represents a handler for ListTagsOfResource requests
type ListTagsOfResourceHandler interface {
	HandleListTagsOfResource(ctx *ListTagsOfResourceContext, output *ListTagsOfResourceOutput)
}

// ListTagsOfResourceHandlerFunc is a ListTagsOfResourceHandler function
type ListTagsOfResourceHandlerFunc func(ctx *ListTagsOfResourceContext, output *ListTagsOfResourceOutput)

// HandleListTagsOfResource implements ListTagsOfResourceHandler
func (h ListTagsOfResourceHandlerFunc) HandleListTagsOfResource(ctx *ListTagsOfResourceContext, output *ListTagsOfResourceOutput) {
	h(ctx, output)
}

// ListTagsOfResourceFinalHandler is the final ListTagsOfResourceHandler that executes a dynamodb ListTagsOfResource operation
type ListTagsOfResourceFinalHandler struct{}

// HandleListTagsOfResource implements the ListTagsOfResourceHandler
func (h *ListTagsOfResourceFinalHandler) HandleListTagsOfResource(ctx *ListTagsOfResourceContext, output *ListTagsOfResourceOutput) {
	output.Set(ctx.Client.ListTagsOfResource(ctx, ctx.Input))
}

// ListTagsOfResourceMiddleWare is a middleware function use for wrapping ListTagsOfResourceHandler requests
type ListTagsOfResourceMiddleWare interface {
	ListTagsOfResourceMiddleWare(next ListTagsOfResourceHandler) ListTagsOfResourceHandler
}

// ListTagsOfResourceMiddleWareFunc is a functional ListTagsOfResourceMiddleWare
type ListTagsOfResourceMiddleWareFunc func(next ListTagsOfResourceHandler) ListTagsOfResourceHandler

// ListTagsOfResourceMiddleWare implements the ListTagsOfResourceMiddleWare interface
func (mw ListTagsOfResourceMiddleWareFunc) ListTagsOfResourceMiddleWare(next ListTagsOfResourceHandler) ListTagsOfResourceHandler {
	return mw(next)
}

// ListTagsOfResource represents a ListTagsOfResource operation
type ListTagsOfResource struct {
	*Promise
	input       *ddb.ListTagsOfResourceInput
	middleWares []ListTagsOfResourceMiddleWare
}

// NewListTagsOfResource creates a new ListTagsOfResource
func NewListTagsOfResource(input *ddb.ListTagsOfResourceInput, mws ...ListTagsOfResourceMiddleWare) *ListTagsOfResource {
	return &ListTagsOfResource{
		Promise: NewPromise(),
		input:       input,
		middleWares: mws,
	}
}

// Invoke invokes the ListTagsOfResource operation and returns it
func (op *ListTagsOfResource) Invoke(ctx context.Context, client *ddb.Client) *ListTagsOfResource {
	go op.DynoInvoke(ctx, client)

	return op
}

// DynoInvoke implements the Operation interface
func (op *ListTagsOfResource) DynoInvoke(ctx context.Context, client *ddb.Client) {

	output := new(ListTagsOfResourceOutput)

	defer func() { op.SetResponse(output.Get()) }()

	requestCtx := &ListTagsOfResourceContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	var h ListTagsOfResourceHandler

	h = new(ListTagsOfResourceFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].ListTagsOfResourceMiddleWare(h)
		}
	}

	h.HandleListTagsOfResource(requestCtx, output)
}

// Await waits for the ListTagsOfResourcePromise to be fulfilled and then returns a ListTagsOfResourceOutput and error
func (op *ListTagsOfResource) Await() (*ddb.ListTagsOfResourceOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.ListTagsOfResourceOutput), err
}

// NewListTagsOfResourceInput creates a new ListTagsOfResourceInput
func NewListTagsOfResourceInput(resourceArn *string) *ddb.ListTagsOfResourceInput {
	return &ddb.ListTagsOfResourceInput{
		ResourceArn: resourceArn,
	}
}

// todo: ListAllTagsOfResource operation