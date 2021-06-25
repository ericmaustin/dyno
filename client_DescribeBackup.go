package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
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
	HandleDescribeBackup(ctx *DescribeBackupContext, promise *DescribeBackupPromise)
}

// DescribeBackupHandlerFunc is a DescribeBackupHandler function
type DescribeBackupHandlerFunc func(ctx *DescribeBackupContext, promise *DescribeBackupPromise)

// HandleDescribeBackup implements DescribeBackupHandler
func (h DescribeBackupHandlerFunc) HandleDescribeBackup(ctx *DescribeBackupContext, promise *DescribeBackupPromise) {
	h(ctx, promise)
}

// DescribeBackupMiddleWare is a middleware function use for wrapping DescribeBackupHandler requests
type DescribeBackupMiddleWare func(handler DescribeBackupHandler) DescribeBackupHandler

// DescribeBackupFinalHandler returns the final DescribeBackupHandler that executes a dynamodb DescribeBackup operation
func DescribeBackupFinalHandler() DescribeBackupHandler {
	return DescribeBackupHandlerFunc(func(ctx *DescribeBackupContext, promise *DescribeBackupPromise) {
		promise.SetResponse(ctx.client.DescribeBackup(ctx, ctx.input))
	})
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
	requestCtx := &DescribeBackupContext{
		Context: ctx,
		client:  client,
		input:   op.input,
	}

	h := DescribeBackupFinalHandler()

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i](h)
		}
	}

	h.HandleDescribeBackup(requestCtx, op.promise)
}


// NewDescribeBackupInput creates a new DescribeBackupInput
func NewDescribeBackupInput(backupArn *string) *ddb.DescribeBackupInput {
	return &ddb.DescribeBackupInput{BackupArn: backupArn}
}
