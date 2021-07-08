package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// DescribeContributorInsights executes DescribeContributorInsights operation and returns a DescribeContributorInsights
func (s *Session) DescribeContributorInsights(input *ddb.DescribeContributorInsightsInput, mw ...DescribeContributorInsightsMiddleWare) *DescribeContributorInsights {
	return NewDescribeContributorInsights(input, mw...).Invoke(s.ctx, s.ddb)
}

// DescribeContributorInsights executes a DescribeContributorInsights operation with a DescribeContributorInsightsInput in this pool and returns the DescribeContributorInsights
func (p *Pool) DescribeContributorInsights(input *ddb.DescribeContributorInsightsInput, mw ...DescribeContributorInsightsMiddleWare) *DescribeContributorInsights {
	op := NewDescribeContributorInsights(input, mw...)

	p.Do(op) // run the operation in the pool

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
	*BaseOperation
	input       *ddb.DescribeContributorInsightsInput
	middleWares []DescribeContributorInsightsMiddleWare
}

// NewDescribeContributorInsights creates a new DescribeContributorInsights
func NewDescribeContributorInsights(input *ddb.DescribeContributorInsightsInput, mws ...DescribeContributorInsightsMiddleWare) *DescribeContributorInsights {
	return &DescribeContributorInsights{
		BaseOperation: NewOperation(),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the DescribeContributorInsights operation in a goroutine and returns a BatchGetItemAll
func (op *DescribeContributorInsights) Invoke(ctx context.Context, client *ddb.Client) *DescribeContributorInsights {
	op.SetRunning() // operation now waiting for a response

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the DescribeContributorInsights operation
func (op *DescribeContributorInsights) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	output := new(DescribeContributorInsightsOutput)

	defer func() { op.SetResponse(output.Get()) }()

	var h DescribeContributorInsightsHandler

	h = new(DescribeContributorInsightsFinalHandler)

	// loop in reverse to preserve middleware order
	for i := len(op.middleWares) - 1; i >= 0; i-- {
		h = op.middleWares[i].DescribeContributorInsightsMiddleWare(h)
	}
	
	requestCtx := &DescribeContributorInsightsContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}
	
	h.HandleDescribeContributorInsights(requestCtx, output)
}

// Await waits for the DescribeContributorInsights operation to be fulfilled and then returns a DescribeContributorInsightsOutput and error
func (op *DescribeContributorInsights) Await() (*ddb.DescribeContributorInsightsOutput, error) {
	out, err := op.BaseOperation.Await()
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
