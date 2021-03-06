package dyno

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"sync"
)

// TagResource executes TagResource operation and returns a TagResource operation
func (s *Session) TagResource(input *ddb.TagResourceInput, mw ...TagResourceMiddleWare) *TagResource {
	return NewTagResource(input, mw...).Invoke(s.ctx, s.ddb)
}

// TagResource executes a TagResource operation with a TagResourceInput in this pool and returns the TagResource operation
func (p *Pool) TagResource(input *ddb.TagResourceInput, mw ...TagResourceMiddleWare) *TagResource {
	op := NewTagResource(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// TagResourceContext represents an exhaustive TagResource operation request context
type TagResourceContext struct {
	context.Context
	Input  *ddb.TagResourceInput
	Client *ddb.Client
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
	output.Set(ctx.Client.TagResource(ctx, ctx.Input))
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
	*BaseOperation
	input       *ddb.TagResourceInput
	middleWares []TagResourceMiddleWare
}

// NewTagResource creates a new TagResource operation
func NewTagResource(input *ddb.TagResourceInput, mws ...TagResourceMiddleWare) *TagResource {
	return &TagResource{
		BaseOperation: NewOperation(),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the TagResource operation in a goroutine and returns a TagResource operation
func (op *TagResource) Invoke(ctx context.Context, client *ddb.Client) *TagResource {
	op.SetRunning() // operation now waiting for a response

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the TagResource operation
func (op *TagResource) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	output := new(TagResourceOutput)

	defer func() { op.SetResponse(output.Get()) }()

	var h TagResourceHandler

	h = new(TagResourceFinalHandler)

	// loop in reverse to preserve middleware order
	for i := len(op.middleWares) - 1; i >= 0; i-- {
		h = op.middleWares[i].TagResourceMiddleWare(h)
	}

	requestCtx := &TagResourceContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	h.HandleTagResource(requestCtx, output)
}

// Await waits for the TagResource operation to be fulfilled and then returns a TagResourceOutput and error
func (op *TagResource) Await() (*ddb.TagResourceOutput, error) {
	out, err := op.BaseOperation.Await()
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
