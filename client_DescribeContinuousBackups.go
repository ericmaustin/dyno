package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// DescribeContinuousBackups executes DescribeContinuousBackups operation and returns a DescribeContinuousBackupsPromise
func (c *Client) DescribeContinuousBackups(ctx context.Context, input *ddb.DescribeContinuousBackupsInput, mw ...DescribeContinuousBackupsMiddleWare) *DescribeContinuousBackups {
	return NewDescribeContinuousBackups(input, mw...).Invoke(ctx, c.ddb)
}

// DescribeContinuousBackups executes a DescribeContinuousBackups operation with a DescribeContinuousBackupsInput in this pool and returns the DescribeContinuousBackupsPromise
func (p *Pool) DescribeContinuousBackups(input *ddb.DescribeContinuousBackupsInput, mw ...DescribeContinuousBackupsMiddleWare) *DescribeContinuousBackups {
	op := NewDescribeContinuousBackups(input, mw...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// DescribeContinuousBackupsContext represents an exhaustive DescribeContinuousBackups operation request context
type DescribeContinuousBackupsContext struct {
	context.Context
	input  *ddb.DescribeContinuousBackupsInput
	client *ddb.Client
}

// DescribeContinuousBackupsOutput represents the output for the DescribeContinuousBackups opration
type DescribeContinuousBackupsOutput struct {
	out *ddb.DescribeContinuousBackupsOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *DescribeContinuousBackupsOutput) Set(out *ddb.DescribeContinuousBackupsOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *DescribeContinuousBackupsOutput) Get() (out *ddb.DescribeContinuousBackupsOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()
	return
}

// DescribeContinuousBackupsHandler represents a handler for DescribeContinuousBackups requests
type DescribeContinuousBackupsHandler interface {
	HandleDescribeContinuousBackups(ctx *DescribeContinuousBackupsContext, output *DescribeContinuousBackupsOutput)
}

// DescribeContinuousBackupsHandlerFunc is a DescribeContinuousBackupsHandler function
type DescribeContinuousBackupsHandlerFunc func(ctx *DescribeContinuousBackupsContext, output *DescribeContinuousBackupsOutput)

// HandleDescribeContinuousBackups implements DescribeContinuousBackupsHandler
func (h DescribeContinuousBackupsHandlerFunc) HandleDescribeContinuousBackups(ctx *DescribeContinuousBackupsContext, output *DescribeContinuousBackupsOutput) {
	h(ctx, output)
}

// DescribeContinuousBackupsFinalHandler is the final DescribeContinuousBackupsHandler that executes a dynamodb DescribeContinuousBackups operation
type DescribeContinuousBackupsFinalHandler struct{}

// HandleDescribeContinuousBackups implements the DescribeContinuousBackupsHandler
func (h *DescribeContinuousBackupsFinalHandler) HandleDescribeContinuousBackups(ctx *DescribeContinuousBackupsContext, output *DescribeContinuousBackupsOutput) {
	output.Set(ctx.client.DescribeContinuousBackups(ctx, ctx.input))
}

// DescribeContinuousBackupsMiddleWare is a middleware function use for wrapping DescribeContinuousBackupsHandler requests
type DescribeContinuousBackupsMiddleWare interface {
	DescribeContinuousBackupsMiddleWare(next DescribeContinuousBackupsHandler) DescribeContinuousBackupsHandler
}

// DescribeContinuousBackupsMiddleWareFunc is a functional DescribeContinuousBackupsMiddleWare
type DescribeContinuousBackupsMiddleWareFunc func(next DescribeContinuousBackupsHandler) DescribeContinuousBackupsHandler

// DescribeContinuousBackupsMiddleWare implements the DescribeContinuousBackupsMiddleWare interface
func (mw DescribeContinuousBackupsMiddleWareFunc) DescribeContinuousBackupsMiddleWare(next DescribeContinuousBackupsHandler) DescribeContinuousBackupsHandler {
	return mw(next)
}

// DescribeContinuousBackups represents a DescribeContinuousBackups operation
type DescribeContinuousBackups struct {
	*Promise
	input       *ddb.DescribeContinuousBackupsInput
	middleWares []DescribeContinuousBackupsMiddleWare
}

// NewDescribeContinuousBackups creates a new DescribeContinuousBackups
func NewDescribeContinuousBackups(input *ddb.DescribeContinuousBackupsInput, mws ...DescribeContinuousBackupsMiddleWare) *DescribeContinuousBackups {
	return &DescribeContinuousBackups{
		Promise: NewPromise(),
		input:       input,
		middleWares: mws,
	}
}

// Invoke invokes the DescribeContinuousBackups operation and returns a DescribeContinuousBackupsPromise
func (op *DescribeContinuousBackups) Invoke(ctx context.Context, client *ddb.Client) *DescribeContinuousBackups {
	go op.DynoInvoke(ctx, client)

	return op
}

// DynoInvoke implements the Operation interface
func (op *DescribeContinuousBackups) DynoInvoke(ctx context.Context, client *ddb.Client) {
	output := new(DescribeContinuousBackupsOutput)

	defer func() { op.SetResponse(output.Get()) }()

	requestCtx := &DescribeContinuousBackupsContext{
		Context: ctx,
		client:  client,
		input:   op.input,
	}

	var h DescribeContinuousBackupsHandler

	h = new(DescribeContinuousBackupsFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].DescribeContinuousBackupsMiddleWare(h)
		}
	}

	h.HandleDescribeContinuousBackups(requestCtx, output)
}

// Await waits for the DescribeContinuousBackupsPromise to be fulfilled and then returns a DescribeContinuousBackupsOutput and error
func (op *DescribeContinuousBackups) Await() (*ddb.DescribeContinuousBackupsOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.DescribeContinuousBackupsOutput), err
}

// NewDescribeContinuousBackupsInput creates a new DescribeContinuousBackupsInput
func NewDescribeContinuousBackupsInput(tableName *string) *ddb.DescribeContinuousBackupsInput {
	return &ddb.DescribeContinuousBackupsInput{
		TableName: tableName,
	}
}
