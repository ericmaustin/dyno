package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// DeleteBackup executes DeleteBackup operation and returns a DeleteBackupPromise
func (c *Client) DeleteBackup(ctx context.Context, input *ddb.DeleteBackupInput, mw ...DeleteBackupMiddleWare) *DeleteBackupPromise {
	return NewDeleteBackup(input, mw...).Invoke(ctx, c.ddb)
}

// DeleteBackup executes a DeleteBackup operation with a DeleteBackupInput in this pool and returns the DeleteBackupPromise
func (p *Pool) DeleteBackup(input *ddb.DeleteBackupInput, mw ...DeleteBackupMiddleWare) *DeleteBackupPromise {
	op := NewDeleteBackup(input, mw...)

	if err := p.Do(op); err != nil {
		op.promise.SetResponse(nil, err)
	}

	return op.promise
}

// DeleteBackupContext represents an exhaustive DeleteBackup operation request context
type DeleteBackupContext struct {
	context.Context
	input  *ddb.DeleteBackupInput
	client *ddb.Client
}

// DeleteBackupPromise represents a promise for the DeleteBackup
type DeleteBackupPromise struct {
	*Promise
}

// Await waits for the DeleteBackupPromise to be fulfilled and then returns a DeleteBackupOutput and error
func (p *DeleteBackupPromise) Await() (*ddb.DeleteBackupOutput, error) {
	out, err := p.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.DeleteBackupOutput), err
}

// newDeleteBackupPromise returns a new DeleteBackupPromise
func newDeleteBackupPromise() *DeleteBackupPromise {
	return &DeleteBackupPromise{NewPromise()}
}

// DeleteBackupHandler represents a handler for DeleteBackup requests
type DeleteBackupHandler interface {
	HandleDeleteBackup(ctx *DeleteBackupContext, promise *DeleteBackupPromise)
}

// DeleteBackupHandlerFunc is a DeleteBackupHandler function
type DeleteBackupHandlerFunc func(ctx *DeleteBackupContext, promise *DeleteBackupPromise)

// HandleDeleteBackup implements DeleteBackupHandler
func (h DeleteBackupHandlerFunc) HandleDeleteBackup(ctx *DeleteBackupContext, promise *DeleteBackupPromise) {
	h(ctx, promise)
}

// DeleteBackupMiddleWare is a middleware function use for wrapping DeleteBackupHandler requests
type DeleteBackupMiddleWare func(handler DeleteBackupHandler) DeleteBackupHandler

// DeleteBackupFinalHandler returns the final DeleteBackupHandler that executes a dynamodb DeleteBackup operation
func DeleteBackupFinalHandler() DeleteBackupHandler {
	return DeleteBackupHandlerFunc(func(ctx *DeleteBackupContext, promise *DeleteBackupPromise) {
		promise.SetResponse(ctx.client.DeleteBackup(ctx, ctx.input))
	})
}

// DeleteBackup represents a DeleteBackup operation
type DeleteBackup struct {
	promise     *DeleteBackupPromise
	input       *ddb.DeleteBackupInput
	middleWares []DeleteBackupMiddleWare
}

// NewDeleteBackup creates a new DeleteBackup
func NewDeleteBackup(input *ddb.DeleteBackupInput, mws ...DeleteBackupMiddleWare) *DeleteBackup {
	return &DeleteBackup{
		input:       input,
		middleWares: mws,
		promise:     newDeleteBackupPromise(),
	}
}

// Invoke invokes the DeleteBackup operation and returns a DeleteBackupPromise
func (op *DeleteBackup) Invoke(ctx context.Context, client *ddb.Client) *DeleteBackupPromise {
	go op.DynoInvoke(ctx, client)

	return op.promise
}

// DynoInvoke implements the Operation interface
func (op *DeleteBackup) DynoInvoke(ctx context.Context, client *ddb.Client) {

	requestCtx := &DeleteBackupContext{
		Context: ctx,
		client:  client,
		input:   op.input,
	}

	h := DeleteBackupFinalHandler()

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i](h)
		}
	}

	h.HandleDeleteBackup(requestCtx, op.promise)
}

// NewDeleteBackupInput creates a DeleteBackupInput with a given table name and key
func NewDeleteBackupInput(backupArn *string) *ddb.DeleteBackupInput {
	return &ddb.DeleteBackupInput{
		BackupArn: backupArn,
	}
}
