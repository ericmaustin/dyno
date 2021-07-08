package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// ListTagsOfResource executes ListTagsOfResource operation and returns a ListTagsOfResource operation
func (s *Session) ListTagsOfResource(input *ddb.ListTagsOfResourceInput, mw ...ListTagsOfResourceMiddleWare) *ListTagsOfResource {
	return NewListTagsOfResource(input, mw...).Invoke(s.ctx, s.ddb)
}

// ListTagsOfResource executes a ListTagsOfResource operation with a ListTagsOfResourceInput in this pool and returns the ListTagsOfResource operation
func (p *Pool) ListTagsOfResource(input *ddb.ListTagsOfResourceInput, mw ...ListTagsOfResourceMiddleWare) *ListTagsOfResource {
	op := NewListTagsOfResource(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// ListAllTagsOfResource executes ListTagsOfResource operation and returns a ListTagsOfResource operation
func (s *Session) ListAllTagsOfResource(input *ddb.ListTagsOfResourceInput, mw ...ListTagsOfResourceMiddleWare) *ListTagsOfResource {
	return NewListAllTagsOfResource(input, mw...).Invoke(s.ctx, s.ddb)
}

// ListAllTagsOfResource executes a ListTagsOfResource operation with a ListTablesInput in this pool and returns the ListTagsOfResource operation
func (p *Pool) ListAllTagsOfResource(input *ddb.ListTagsOfResourceInput, mw ...ListTagsOfResourceMiddleWare) *ListTagsOfResource {
	op := NewListAllTagsOfResource(input, mw...)

	p.Do(op) // run the operation in the pool

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

// ListAllTagsOfResourceFinalHandler is the final ListTagsOfResourceHandler that executes a dynamodb ListTagsOfResource operation
type ListAllTagsOfResourceFinalHandler struct{}

// HandleListTagsOfResource implements the ListTagsOfResourceHandler
func (h *ListAllTagsOfResourceFinalHandler) HandleListTagsOfResource(ctx *ListTagsOfResourceContext, output *ListTagsOfResourceOutput) {
	var (
		err error
		out, finalOutput *ddb.ListTagsOfResourceOutput
	)

	input := CopyListTagsOfResourceInput(ctx.Input)
	finalOutput = new(ddb.ListTagsOfResourceOutput)

	defer func() { output.Set(finalOutput, err) }()

	for {
		out, err = ctx.Client.ListTagsOfResource(ctx, input)
		if err != nil {
			output.Set(nil, err)
		}

		finalOutput.Tags = append(finalOutput.Tags, out.Tags...)

		if out.NextToken == nil || len(*out.NextToken) == 0 {
			return
		}

		*input.NextToken = *out.NextToken
	}
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
	*BaseOperation
	Handler     ListTagsOfResourceHandler
	input       *ddb.ListTagsOfResourceInput
	middleWares []ListTagsOfResourceMiddleWare
}

// NewListTagsOfResource creates a new ListTagsOfResource operation
func NewListTagsOfResource(input *ddb.ListTagsOfResourceInput, mws ...ListTagsOfResourceMiddleWare) *ListTagsOfResource {
	return &ListTagsOfResource{
		BaseOperation: NewOperation(),
		Handler:       new(ListTagsOfResourceFinalHandler),
		input:         input,
		middleWares:   mws,
	}
}

// NewListAllTagsOfResource creates a new ListTagsOfResource operation
func NewListAllTagsOfResource(input *ddb.ListTagsOfResourceInput, mws ...ListTagsOfResourceMiddleWare) *ListTagsOfResource {
	return &ListTagsOfResource{
		BaseOperation: NewOperation(),
		Handler:       new(ListAllTagsOfResourceFinalHandler),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the ListTagsOfResource operation in a goroutine and returns a ListTagsOfResource operation
func (op *ListTagsOfResource) Invoke(ctx context.Context, client *ddb.Client) *ListTagsOfResource {
	op.SetRunning() // operation now waiting for a response

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the ListTagsOfResource operation
func (op *ListTagsOfResource) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	output := new(ListTagsOfResourceOutput)

	defer func() { op.SetResponse(output.Get()) }()

	var h ListTagsOfResourceHandler

	h = op.Handler

	// loop in reverse to preserve middleware order
	for i := len(op.middleWares) - 1; i >= 0; i-- {
		h = op.middleWares[i].ListTagsOfResourceMiddleWare(h)
	}

	requestCtx := &ListTagsOfResourceContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	h.HandleListTagsOfResource(requestCtx, output)
}

// Await waits for the ListTagsOfResource operation to be fulfilled and then returns a ListTagsOfResourceOutput and error
func (op *ListTagsOfResource) Await() (*ddb.ListTagsOfResourceOutput, error) {
	out, err := op.BaseOperation.Await()
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

// CopyListTagsOfResourceInput creates a copy of a ListTagsOfResourceInput
func CopyListTagsOfResourceInput(input *ddb.ListTagsOfResourceInput) *ddb.ListTagsOfResourceInput {
	out := &ddb.ListTagsOfResourceInput{}

	if input.ResourceArn != nil {
		out.ResourceArn = new(string)
		*out.ResourceArn = *input.ResourceArn
	}

	if input.NextToken != nil {
		out.NextToken = new(string)
		*out.NextToken = *input.NextToken
	}

	return out
}