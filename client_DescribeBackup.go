package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// DescribeBackup executes DescribeBackup operation and returns a DescribeBackupPromise
func (c *Client) DescribeBackup(ctx context.Context, input *ddb.DescribeBackupInput, mw ...DescribeBackupMiddleWare) *DescribeBackupPromise {
	return NewDescribeBackup(input, mw...).Invoke(ctx, c.ddb)
}

// DescribeBackup executes a DescribeBackup operation with a DescribeBackupInput in this pool and returns the DescribeBackupPromise
func (p *Pool) DescribeBackup(input *ddb.DescribeBackupInput, mw ...DescribeBackupMiddleWare) *DescribeBackupPromise {
	op := NewDescribeBackup(input, mw...)

	if err := p.Do(op); err != nil {
		op.promise.SetResponse(nil, err)
	}

	return op.promise
}

// DescribeBackupContext represents an exhaustive DescribeBackup operation request context
type DescribeBackupContext struct {
	context.Context
	input  *ddb.DescribeBackupInput
	client *ddb.Client
}

// DescribeBackupOutput represents the output for the DescribeBackup opration
type DescribeBackupOutput struct {
	out *ddb.DescribeBackupOutput
	err error
	mu sync.RWMutex
}

// Set sets the output
func (o *DescribeBackupOutput) Set(out *ddb.DescribeBackupOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *DescribeBackupOutput) Get() (out *ddb.DescribeBackupOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()
	return
}

// DescribeBackupPromise represents a promise for the DescribeBackup
type DescribeBackupPromise struct {
	*Promise
}

// Await waits for the DescribeBackupPromise to be fulfilled and then returns a DescribeBackupOutput and error
func (p *DescribeBackupPromise) Await() (*ddb.DescribeBackupOutput, error) {
	out, err := p.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.DescribeBackupOutput), err
}

// newDescribeBackupPromise returns a new DescribeBackupPromise
func newDescribeBackupPromise() *DescribeBackupPromise {
	return &DescribeBackupPromise{NewPromise()}
}

// DescribeBackupHandler represents a handler for DescribeBackup requests
type DescribeBackupHandler interface {
	HandleDescribeBackup(ctx *DescribeBackupContext, output *DescribeBackupOutput)
}

// DescribeBackupHandlerFunc is a DescribeBackupHandler function
type DescribeBackupHandlerFunc func(ctx *DescribeBackupContext, output *DescribeBackupOutput)

// HandleDescribeBackup implements DescribeBackupHandler
func (h DescribeBackupHandlerFunc) HandleDescribeBackup(ctx *DescribeBackupContext, output *DescribeBackupOutput) {
	h(ctx, output)
}

// DescribeBackupFinalHandler is the final DescribeBackupHandler that executes a dynamodb DescribeBackup operation
type DescribeBackupFinalHandler struct {}

// HandleDescribeBackup implements the DescribeBackupHandler
func (h *DescribeBackupFinalHandler) HandleDescribeBackup(ctx *DescribeBackupContext, output *DescribeBackupOutput) {
	output.Set(ctx.client.DescribeBackup(ctx, ctx.input))
}

// DescribeBackupMiddleWare is a middleware function use for wrapping DescribeBackupHandler requests
type DescribeBackupMiddleWare interface {
	DescribeBackupMiddleWare(h DescribeBackupHandler) DescribeBackupHandler
}

// DescribeBackupMiddleWareFunc is a functional DescribeBackupMiddleWare
type DescribeBackupMiddleWareFunc func(handler DescribeBackupHandler) DescribeBackupHandler

// DescribeBackupMiddleWare implements the DescribeBackupMiddleWare interface
func (mw DescribeBackupMiddleWareFunc) DescribeBackupMiddleWare(h DescribeBackupHandler) DescribeBackupHandler {
	return mw(h)
}

// DescribeBackup represents a DescribeBackup operation
type DescribeBackup struct {
	promise     *DescribeBackupPromise
	input       *ddb.DescribeBackupInput
	middleWares []DescribeBackupMiddleWare
}

// NewDescribeBackup creates a new DescribeBackup
func NewDescribeBackup(input *ddb.DescribeBackupInput, mws ...DescribeBackupMiddleWare) *DescribeBackup {
	return &DescribeBackup{
		input:       input,
		middleWares: mws,
		promise:     newDescribeBackupPromise(),
	}
}

// Invoke invokes the DescribeBackup operation and returns a DescribeBackupPromise
func (op *DescribeBackup) Invoke(ctx context.Context, client *ddb.Client) *DescribeBackupPromise {
	go op.DynoInvoke(ctx, client)

	return op.promise
}

// DynoInvoke implements the Operation interface
func (op *DescribeBackup) DynoInvoke(ctx context.Context, client *ddb.Client) {
	output := new(DescribeBackupOutput)

	defer func() {op.promise.SetResponse(output.Get())}()

	requestCtx := &DescribeBackupContext{
		Context: ctx,
		client:  client,
		input:   op.input,
	}

	var h DescribeBackupHandler

	h = new(DescribeBackupFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].DescribeBackupMiddleWare(h)
		}
	}

	h.HandleDescribeBackup(requestCtx, output)
}


// NewDescribeBackupInput creates a new DescribeBackupInput
func NewDescribeBackupInput(backupArn *string) *ddb.DescribeBackupInput {
	return &ddb.DescribeBackupInput{BackupArn: backupArn}
}
