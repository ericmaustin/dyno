package dyno

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"sync"
)

// TagResource executes TagResource operation and returns a TagResourcePromise
func (c *Client) TagResource(ctx context.Context, input *ddb.TagResourceInput, mw ...TagResourceMiddleWare) *TagResource {
	return NewTagResource(input, mw...).Invoke(ctx, c.ddb)
}

// TagResource executes a TagResource operation with a TagResourceInput in this pool and returns the TagResourcePromise
func (p *Pool) TagResource(input *ddb.TagResourceInput, mw ...TagResourceMiddleWare) *TagResource {
	op := NewTagResource(input, mw...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// TagResourceContext represents an exhaustive TagResource operation request context
type TagResourceContext struct {
	context.Context
	input  *ddb.TagResourceInput
	client *ddb.Client
}

// TagResourceOutput represents the output for the TagResource operation
type TagResourceOutput struct {
	out *ddb.TagResourceOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *TagResourceOutput) Set(out *ddb.TagResourceOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *TagResourceOutput) Get() (out *ddb.TagResourceOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()
	return
}

// TagResourceHandler represents a handler for TagResource requests
type TagResourceHandler interface {
	HandleTagResource(ctx *TagResourceContext, output *TagResourceOutput)
}

// TagResourceHandlerFunc is a TagResourceHandler function
type TagResourceHandlerFunc func(ctx *TagResourceContext, output *TagResourceOutput)

// HandleTagResource implements TagResourceHandler
func (h TagResourceHandlerFunc) HandleTagResource(ctx *TagResourceContext, output *TagResourceOutput) {
	h(ctx, output)
}

// TagResourceFinalHandler is the final TagResourceHandler that executes a dynamodb TagResource operation
type TagResourceFinalHandler struct{}

// HandleTagResource implements the TagResourceHandler
func (h *TagResourceFinalHandler) HandleTagResource(ctx *TagResourceContext, output *TagResourceOutput) {
	output.Set(ctx.client.TagResource(ctx, ctx.input))
}

// TagResourceMiddleWare is a middleware function use for wrapping TagResourceHandler requests
type TagResourceMiddleWare interface {
	TagResourceMiddleWare(next TagResourceHandler) TagResourceHandler
}

// TagResourceMiddleWareFunc is a functional TagResourceMiddleWare
type TagResourceMiddleWareFunc func(next TagResourceHandler) TagResourceHandler

// TagResourceMiddleWare implements the TagResourceMiddleWare interface
func (mw TagResourceMiddleWareFunc) TagResourceMiddleWare(next TagResourceHandler) TagResourceHandler {
	return mw(next)
}

// TagResource represents a TagResource operation
type TagResource struct {
	*Promise
	input       *ddb.TagResourceInput
	middleWares []TagResourceMiddleWare
}

// NewTagResource creates a new TagResource
func NewTagResource(input *ddb.TagResourceInput, mws ...TagResourceMiddleWare) *TagResource {
	return &TagResource{
		Promise:     NewPromise(),
		input:       input,
		middleWares: mws,
	}
}

// Invoke invokes the TagResource operation and returns a TagResourcePromise
func (op *TagResource) Invoke(ctx context.Context, client *ddb.Client) *TagResource {
	go op.DynoInvoke(ctx, client)

	return op
}

// DynoInvoke implements the Operation interface
func (op *TagResource) DynoInvoke(ctx context.Context, client *ddb.Client) {

	output := new(TagResourceOutput)

	defer func() { op.SetResponse(output.Get()) }()

	requestCtx := &TagResourceContext{
		Context: ctx,
		client:  client,
		input:   op.input,
	}

	var h TagResourceHandler

	h = new(TagResourceFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].TagResourceMiddleWare(h)
		}
	}

	h.HandleTagResource(requestCtx, output)
}

// Await waits for the TagResourcePromise to be fulfilled and then returns a TagResourceOutput and error
func (op *TagResource) Await() (*ddb.TagResourceOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.TagResourceOutput), err
}

// TagResourceBuilder is used to dynamically build a TagResourceInput request
type TagResourceBuilder struct {
	*ddb.TagResourceInput
	projection *expression.ProjectionBuilder
}

// NewTagResourceInput creates a new TagResourceInput with a table name and key
func NewTagResourceInput(resourceArn *string, tags []ddbTypes.Tag) *ddb.TagResourceInput {
	return &ddb.TagResourceInput{
		ResourceArn: resourceArn,
		Tags:        tags,
	}
}
