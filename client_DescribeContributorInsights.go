package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// DescribeContributorInsights executes DescribeContributorInsights operation and returns a DescribeContributorInsightsPromise
func (c *Client) DescribeContributorInsights(ctx context.Context, input *ddb.DescribeContributorInsightsInput, mw ...DescribeContributorInsightsMiddleWare) *DescribeContributorInsights {
	return NewDescribeContributorInsights(input, mw...).Invoke(ctx, c.ddb)
}

// DescribeContributorInsights executes a DescribeContributorInsights operation with a DescribeContributorInsightsInput in this pool and returns the DescribeContributorInsightsPromise
func (p *Pool) DescribeContributorInsights(input *ddb.DescribeContributorInsightsInput, mw ...DescribeContributorInsightsMiddleWare) *DescribeContributorInsights {
	op := NewDescribeContributorInsights(input, mw...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// DescribeContributorInsightsContext represents an exhaustive DescribeContributorInsights operation request context
type DescribeContributorInsightsContext struct {
	context.Context
	Input  *ddb.DescribeContributorInsightsInput
	Client *ddb.Client
}

// DescribeContributorInsightsOutput represents the output for the DescribeContributorInsights opration
type DescribeContributorInsightsOutput struct {
	out *ddb.DescribeContributorInsightsOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *DescribeContributorInsightsOutput) Set(out *ddb.DescribeContributorInsightsOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *DescribeContributorInsightsOutput) Get() (out *ddb.DescribeContributorInsightsOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()
	return
}

// DescribeContributorInsightsHandler represents a handler for DescribeContributorInsights requests
type DescribeContributorInsightsHandler interface {
	HandleDescribeContributorInsights(ctx *DescribeContributorInsightsContext, output *DescribeContributorInsightsOutput)
}

// DescribeContributorInsightsHandlerFunc is a DescribeContributorInsightsHandler function
type DescribeContributorInsightsHandlerFunc func(ctx *DescribeContributorInsightsContext, output *DescribeContributorInsightsOutput)

// HandleDescribeContributorInsights implements DescribeContributorInsightsHandler
func (h DescribeContributorInsightsHandlerFunc) HandleDescribeContributorInsights(ctx *DescribeContributorInsightsContext, output *DescribeContributorInsightsOutput) {
	h(ctx, output)
}

// DescribeContributorInsightsFinalHandler is the final DescribeContributorInsightsHandler that executes a dynamodb DescribeContributorInsights operation
type DescribeContributorInsightsFinalHandler struct{}

// HandleDescribeContributorInsights implements the DescribeContributorInsightsHandler
func (h *DescribeContributorInsightsFinalHandler) HandleDescribeContributorInsights(ctx *DescribeContributorInsightsContext, output *DescribeContributorInsightsOutput) {
	output.Set(ctx.Client.DescribeContributorInsights(ctx, ctx.Input))
}

// DescribeContributorInsightsMiddleWare is a middleware function use for wrapping DescribeContributorInsightsHandler requests
type DescribeContributorInsightsMiddleWare interface {
	DescribeContributorInsightsMiddleWare(next DescribeContributorInsightsHandler) DescribeContributorInsightsHandler
}

// DescribeContributorInsightsMiddleWareFunc is a functional DescribeContributorInsightsMiddleWare
type DescribeContributorInsightsMiddleWareFunc func(next DescribeContributorInsightsHandler) DescribeContributorInsightsHandler

// DescribeContributorInsightsMiddleWare implements the DescribeContributorInsightsMiddleWare interface
func (mw DescribeContributorInsightsMiddleWareFunc) DescribeContributorInsightsMiddleWare(next DescribeContributorInsightsHandler) DescribeContributorInsightsHandler {
	return mw(next)
}

// DescribeContributorInsights represents a DescribeContributorInsights operation
type DescribeContributorInsights struct {
	*Promise
	input       *ddb.DescribeContributorInsightsInput
	middleWares []DescribeContributorInsightsMiddleWare
}

// NewDescribeContributorInsights creates a new DescribeContributorInsights
func NewDescribeContributorInsights(input *ddb.DescribeContributorInsightsInput, mws ...DescribeContributorInsightsMiddleWare) *DescribeContributorInsights {
	return &DescribeContributorInsights{
		Promise: NewPromise(),
		input:       input,
		middleWares: mws,
	}
}

// DynoInvoke invokes the DescribeContributorInsights operation and returns a DescribeContributorInsightsPromise
func (op *DescribeContributorInsights) Invoke(ctx context.Context, client *ddb.Client) *DescribeContributorInsights {
	go op.Invoke(ctx, client)

	return op
}

// DynoInvoke implements the Operation interface
func (op *DescribeContributorInsights) Invoke(ctx context.Context, client *ddb.Client) {
	output := new(DescribeContributorInsightsOutput)

	defer func() { op.SetResponse(output.Get()) }()

	requestCtx := &DescribeContributorInsightsContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	var h DescribeContributorInsightsHandler

	h = new(DescribeContributorInsightsFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].DescribeContributorInsightsMiddleWare(h)
		}
	}

	h.HandleDescribeContributorInsights(requestCtx, output)
}

// Await waits for the DescribeContributorInsightsPromise to be fulfilled and then returns a DescribeContributorInsightsOutput and error
func (op *DescribeContributorInsights) Await() (*ddb.DescribeContributorInsightsOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.DescribeContributorInsightsOutput), err
}

// NewDescribeContributorInsightsInput creates a new DescribeContributorInsightsInput
func NewDescribeContributorInsightsInput(tableName *string, indexName *string) *ddb.DescribeContributorInsightsInput {
	return &ddb.DescribeContributorInsightsInput{
		TableName: tableName,
		IndexName: indexName,
	}
}
