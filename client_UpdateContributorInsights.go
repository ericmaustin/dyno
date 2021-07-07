package dyno

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"sync"
)

// UpdateContributorInsights executes UpdateContributorInsights operation and returns a UpdateContributorInsightsPromise
func (c *Client) UpdateContributorInsights(ctx context.Context, input *ddb.UpdateContributorInsightsInput, mw ...UpdateContributorInsightsMiddleWare) *UpdateContributorInsights {
	return NewUpdateContributorInsights(input, mw...).Invoke(ctx, c.ddb)
}

// UpdateContributorInsights executes a UpdateContributorInsights operation with a UpdateContributorInsightsInput in this pool and returns the UpdateContributorInsightsPromise
func (p *Pool) UpdateContributorInsights(input *ddb.UpdateContributorInsightsInput, mw ...UpdateContributorInsightsMiddleWare) *UpdateContributorInsights {
	op := NewUpdateContributorInsights(input, mw...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// UpdateContributorInsightsContext represents an exhaustive UpdateContributorInsights operation request context
type UpdateContributorInsightsContext struct {
	context.Context
	Input  *ddb.UpdateContributorInsightsInput
	Client *ddb.Client
}

// UpdateContributorInsightsOutput represents the output for the UpdateContributorInsights operation
type UpdateContributorInsightsOutput struct {
	out *ddb.UpdateContributorInsightsOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *UpdateContributorInsightsOutput) Set(out *ddb.UpdateContributorInsightsOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *UpdateContributorInsightsOutput) Get() (out *ddb.UpdateContributorInsightsOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()
	return
}

// UpdateContributorInsightsHandler represents a handler for UpdateContributorInsights requests
type UpdateContributorInsightsHandler interface {
	HandleUpdateContributorInsights(ctx *UpdateContributorInsightsContext, output *UpdateContributorInsightsOutput)
}

// UpdateContributorInsightsHandlerFunc is a UpdateContributorInsightsHandler function
type UpdateContributorInsightsHandlerFunc func(ctx *UpdateContributorInsightsContext, output *UpdateContributorInsightsOutput)

// HandleUpdateContributorInsights implements UpdateContributorInsightsHandler
func (h UpdateContributorInsightsHandlerFunc) HandleUpdateContributorInsights(ctx *UpdateContributorInsightsContext, output *UpdateContributorInsightsOutput) {
	h(ctx, output)
}

// UpdateContributorInsightsFinalHandler is the final UpdateContributorInsightsHandler that executes a dynamodb UpdateContributorInsights operation
type UpdateContributorInsightsFinalHandler struct{}

// HandleUpdateContributorInsights implements the UpdateContributorInsightsHandler
func (h *UpdateContributorInsightsFinalHandler) HandleUpdateContributorInsights(ctx *UpdateContributorInsightsContext, output *UpdateContributorInsightsOutput) {
	output.Set(ctx.Client.UpdateContributorInsights(ctx, ctx.Input))
}

// UpdateContributorInsightsMiddleWare is a middleware function use for wrapping UpdateContributorInsightsHandler requests
type UpdateContributorInsightsMiddleWare interface {
	UpdateContributorInsightsMiddleWare(next UpdateContributorInsightsHandler) UpdateContributorInsightsHandler
}

// UpdateContributorInsightsMiddleWareFunc is a functional UpdateContributorInsightsMiddleWare
type UpdateContributorInsightsMiddleWareFunc func(next UpdateContributorInsightsHandler) UpdateContributorInsightsHandler

// UpdateContributorInsightsMiddleWare implements the UpdateContributorInsightsMiddleWare interface
func (mw UpdateContributorInsightsMiddleWareFunc) UpdateContributorInsightsMiddleWare(next UpdateContributorInsightsHandler) UpdateContributorInsightsHandler {
	return mw(next)
}

// UpdateContributorInsights represents a UpdateContributorInsights operation
type UpdateContributorInsights struct {
	*Promise
	input       *ddb.UpdateContributorInsightsInput
	middleWares []UpdateContributorInsightsMiddleWare
}

// NewUpdateContributorInsights creates a new UpdateContributorInsights
func NewUpdateContributorInsights(input *ddb.UpdateContributorInsightsInput, mws ...UpdateContributorInsightsMiddleWare) *UpdateContributorInsights {
	return &UpdateContributorInsights{
		Promise:     NewPromise(),
		input:       input,
		middleWares: mws,
	}
}

// DynoInvoke invokes the UpdateContributorInsights operation and returns a UpdateContributorInsightsPromise
func (op *UpdateContributorInsights) Invoke(ctx context.Context, client *ddb.Client) *UpdateContributorInsights {
	go op.Invoke(ctx, client)

	return op
}

// DynoInvoke implements the Operation interface
func (op *UpdateContributorInsights) Invoke(ctx context.Context, client *ddb.Client) {

	output := new(UpdateContributorInsightsOutput)

	defer func() { op.SetResponse(output.Get()) }()

	requestCtx := &UpdateContributorInsightsContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	var h UpdateContributorInsightsHandler

	h = new(UpdateContributorInsightsFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].UpdateContributorInsightsMiddleWare(h)
		}
	}

	h.HandleUpdateContributorInsights(requestCtx, output)
}

// Await waits for the UpdateContributorInsightsPromise to be fulfilled and then returns a UpdateContributorInsightsOutput and error
func (op *UpdateContributorInsights) Await() (*ddb.UpdateContributorInsightsOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.UpdateContributorInsightsOutput), err
}

// UpdateContributorInsightsBuilder is used to dynamically build a UpdateContributorInsightsInput request
type UpdateContributorInsightsBuilder struct {
	*ddb.UpdateContributorInsightsInput
	projection *expression.ProjectionBuilder
}

// NewUpdateContributorInsightsInput creates a new UpdateContributorInsightsInput with a table name and key
func NewUpdateContributorInsightsInput(tableName, indexName *string, action ddbTypes.ContributorInsightsAction) *ddb.UpdateContributorInsightsInput {
	return &ddb.UpdateContributorInsightsInput{
		TableName: tableName,
		IndexName: indexName,
		ContributorInsightsAction: action,
	}
}
