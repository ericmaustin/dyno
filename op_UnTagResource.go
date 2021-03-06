package dyno

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// UntagResource executes UntagResource operation and returns a UntagResource operation
func (s *Session) UntagResource(input *ddb.UntagResourceInput, mw ...UntagResourceMiddleWare) *UntagResource {
	return NewUntagResource(input, mw...).Invoke(s.ctx, s.ddb)
}

// UntagResource executes a UntagResource operation with a UntagResourceInput in this pool and returns the UntagResource operation
func (p *Pool) UntagResource(input *ddb.UntagResourceInput, mw ...UntagResourceMiddleWare) *UntagResource {
	op := NewUntagResource(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// UntagResourceContext represents an exhaustive UntagResource operation request context
type UntagResourceContext struct {
	context.Context
	Input  *ddb.UntagResourceInput
	Client *ddb.Client
}

// UntagResourceOutput represents the output for the UntagResource operation
type UntagResourceOutput struct {
	out *ddb.UntagResourceOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *UntagResourceOutput) Set(out *ddb.UntagResourceOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *UntagResourceOutput) Get() (out *ddb.UntagResourceOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()

	return
}

// UntagResourceHandler represents a handler for UntagResource requests
type UntagResourceHandler interface {
	HandleUntagResource(ctx *UntagResourceContext, output *UntagResourceOutput)
}

// UntagResourceHandlerFunc is a UntagResourceHandler function
type UntagResourceHandlerFunc func(ctx *UntagResourceContext, output *UntagResourceOutput)

// HandleUntagResource implements UntagResourceHandler
func (h UntagResourceHandlerFunc) HandleUntagResource(ctx *UntagResourceContext, output *UntagResourceOutput) {
	h(ctx, output)
}

// UntagResourceFinalHandler is the final UntagResourceHandler that executes a dynamodb UntagResource operation
type UntagResourceFinalHandler struct{}

// HandleUntagResource implements the UntagResourceHandler
func (h *UntagResourceFinalHandler) HandleUntagResource(ctx *UntagResourceContext, output *UntagResourceOutput) {
	output.Set(ctx.Client.UntagResource(ctx, ctx.Input))
}

// UntagResourceMiddleWare is a middleware function use for wrapping UntagResourceHandler requests
type UntagResourceMiddleWare interface {
	UntagResourceMiddleWare(next UntagResourceHandler) UntagResourceHandler
}

// UntagResourceMiddleWareFunc is a functional UntagResourceMiddleWare
type UntagResourceMiddleWareFunc func(next UntagResourceHandler) UntagResourceHandler

// UntagResourceMiddleWare implements the UntagResourceMiddleWare interface
func (mw UntagResourceMiddleWareFunc) UntagResourceMiddleWare(next UntagResourceHandler) UntagResourceHandler {
	return mw(next)
}

// UntagResource represents a UntagResource operation
type UntagResource struct {
	*BaseOperation
	input       *ddb.UntagResourceInput
	middleWares []UntagResourceMiddleWare
}

// NewUntagResource creates a new UntagResource operation
func NewUntagResource(input *ddb.UntagResourceInput, mws ...UntagResourceMiddleWare) *UntagResource {
	return &UntagResource{
		BaseOperation: NewOperation(),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the UntagResource operation in a goroutine and returns a UntagResource operation
func (op *UntagResource) Invoke(ctx context.Context, client *ddb.Client) *UntagResource {
	op.SetRunning() // operation now waiting for a response

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the UntagResource operation
func (op *UntagResource) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	output := new(UntagResourceOutput)

	defer func() { op.SetResponse(output.Get()) }()

	var h UntagResourceHandler

	h = new(UntagResourceFinalHandler)

	// loop in reverse to preserve middleware order
	for i := len(op.middleWares) - 1; i >= 0; i-- {
		h = op.middleWares[i].UntagResourceMiddleWare(h)
	}

	requestCtx := &UntagResourceContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	h.HandleUntagResource(requestCtx, output)
}

// Await waits for the UntagResource operation to be fulfilled and then returns a UntagResourceOutput and error
func (op *UntagResource) Await() (*ddb.UntagResourceOutput, error) {
	out, err := op.BaseOperation.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.UntagResourceOutput), err
}

// UntagResourceBuilder is used to dynamically build a UntagResourceInput request
type UntagResourceBuilder struct {
	*ddb.UntagResourceInput
	projection *expression.ProjectionBuilder
}

// NewUntagResourceInput creates a new UntagResourceInput with a table name and key
func NewUntagResourceInput(resourceArn *string, tagKeys []string) *ddb.UntagResourceInput {
	return &ddb.UntagResourceInput{
		ResourceArn: resourceArn,
		TagKeys:     tagKeys,
	}
}
