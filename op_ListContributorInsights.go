package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// ListContributorInsights executes ListContributorInsights operation and returns a ListContributorInsights operation
func (s *Session) ListContributorInsights(input *ddb.ListContributorInsightsInput, mw ...ListContributorInsightsMiddleWare) *ListContributorInsights {
	return NewListContributorInsights(input, mw...).Invoke(s.ctx, s.ddb)
}

// ListContributorInsights executes a ListContributorInsights operation with a ListContributorInsightsInput in this pool and returns the ListContributorInsights operation
func (p *Pool) ListContributorInsights(input *ddb.ListContributorInsightsInput, mw ...ListContributorInsightsMiddleWare) *ListContributorInsights {
	op := NewListContributorInsights(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// ListContributorInsightsContext represents an exhaustive ListContributorInsights operation request context
type ListContributorInsightsContext struct {
	context.Context
	Input  *ddb.ListContributorInsightsInput
	Client *ddb.Client
}

// ListContributorInsightsOutput represents the output for the ListContributorInsights operation
type ListContributorInsightsOutput struct {
	out *ddb.ListContributorInsightsOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *ListContributorInsightsOutput) Set(out *ddb.ListContributorInsightsOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *ListContributorInsightsOutput) Get() (out *ddb.ListContributorInsightsOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()

	return
}

// ListContributorInsightsHandler represents a handler for ListContributorInsights requests
type ListContributorInsightsHandler interface {
	HandleListContributorInsights(ctx *ListContributorInsightsContext, output *ListContributorInsightsOutput)
}

// ListContributorInsightsHandlerFunc is a ListContributorInsightsHandler function
type ListContributorInsightsHandlerFunc func(ctx *ListContributorInsightsContext, output *ListContributorInsightsOutput)

// HandleListContributorInsights implements ListContributorInsightsHandler
func (h ListContributorInsightsHandlerFunc) HandleListContributorInsights(ctx *ListContributorInsightsContext, output *ListContributorInsightsOutput) {
	h(ctx, output)
}

// ListContributorInsightsFinalHandler is the final ListContributorInsightsHandler that executes a dynamodb ListContributorInsights operation
type ListContributorInsightsFinalHandler struct{}

// HandleListContributorInsights implements the ListContributorInsightsHandler
func (h *ListContributorInsightsFinalHandler) HandleListContributorInsights(ctx *ListContributorInsightsContext, output *ListContributorInsightsOutput) {
	output.Set(ctx.Client.ListContributorInsights(ctx, ctx.Input))
}

// ListContributorInsightsMiddleWare is a middleware function use for wrapping ListContributorInsightsHandler requests
type ListContributorInsightsMiddleWare interface {
	ListContributorInsightsMiddleWare(next ListContributorInsightsHandler) ListContributorInsightsHandler
}

// ListContributorInsightsMiddleWareFunc is a functional ListContributorInsightsMiddleWare
type ListContributorInsightsMiddleWareFunc func(next ListContributorInsightsHandler) ListContributorInsightsHandler

// ListContributorInsightsMiddleWare implements the ListContributorInsightsMiddleWare interface
func (mw ListContributorInsightsMiddleWareFunc) ListContributorInsightsMiddleWare(next ListContributorInsightsHandler) ListContributorInsightsHandler {
	return mw(next)
}

// ListContributorInsights represents a ListContributorInsights operation
type ListContributorInsights struct {
	*BaseOperation
	input       *ddb.ListContributorInsightsInput
	middleWares []ListContributorInsightsMiddleWare
}

// NewListContributorInsights creates a new ListContributorInsights operation
func NewListContributorInsights(input *ddb.ListContributorInsightsInput, mws ...ListContributorInsightsMiddleWare) *ListContributorInsights {
	return &ListContributorInsights{
		BaseOperation: NewOperation(),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the ListContributorInsights operation in a goroutine and returns a ListContributorInsights operation
func (op *ListContributorInsights) Invoke(ctx context.Context, client *ddb.Client) *ListContributorInsights {
	op.SetRunning() // operation now waiting for a response

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the ListContributorInsights operation
func (op *ListContributorInsights) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	output := new(ListContributorInsightsOutput)

	defer func() { op.SetResponse(output.Get()) }()

	var h ListContributorInsightsHandler

	h = new(ListContributorInsightsFinalHandler)

	// loop in reverse to preserve middleware order
	for i := len(op.middleWares) - 1; i >= 0; i-- {
		h = op.middleWares[i].ListContributorInsightsMiddleWare(h)
	}

	requestCtx := &ListContributorInsightsContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	h.HandleListContributorInsights(requestCtx, output)
}

// Await waits for the ListContributorInsights operation to be fulfilled and then returns a ListContributorInsightsOutput and error
func (op *ListContributorInsights) Await() (*ddb.ListContributorInsightsOutput, error) {
	out, err := op.BaseOperation.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.ListContributorInsightsOutput), err
}

// NewListContributorInsightsInput creates a new ListContributorInsightsInput
func NewListContributorInsightsInput() *ddb.ListContributorInsightsInput {
	return &ddb.ListContributorInsightsInput{}
}

// todo: ListAllContributorInsights operation