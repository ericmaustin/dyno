package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// DeleteBackup creates a new DeleteBackup, invokes and returns it
func (c *Client) DeleteBackup(ctx context.Context, input *ddb.DeleteBackupInput, mw ...DeleteBackupMiddleWare) *DeleteBackup {
	return NewDeleteBackup(input, mw...).Invoke(ctx, c.ddb)
}

// DeleteBackup creates a new DeleteBackup, passes it to the Pool and then returns the DeleteBackup
func (p *Pool) DeleteBackup(input *ddb.DeleteBackupInput, mw ...DeleteBackupMiddleWare) *DeleteBackup {
	op := NewDeleteBackup(input, mw...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// DeleteBackupContext represents an exhaustive DeleteBackup operation request context
type DeleteBackupContext struct {
	context.Context
	Input  *ddb.DeleteBackupInput
	Client *ddb.Client
}

// DeleteBackupOutput represents the output for the DeleteBackup operation
type DeleteBackupOutput struct {
	out *ddb.DeleteBackupOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *DeleteBackupOutput) Set(out *ddb.DeleteBackupOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *DeleteBackupOutput) Get() (out *ddb.DeleteBackupOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()
	return
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
	HandleDeleteBackup(ctx *DeleteBackupContext, output *DeleteBackupOutput)
}

// DeleteBackupHandlerFunc is a DeleteBackupHandler function
type DeleteBackupHandlerFunc func(ctx *DeleteBackupContext, output *DeleteBackupOutput)

// HandleDeleteBackup implements DeleteBackupHandler
func (h DeleteBackupHandlerFunc) HandleDeleteBackup(ctx *DeleteBackupContext, output *DeleteBackupOutput) {
	h(ctx, output)
}

// DeleteBackupFinalHandler is the final DeleteBackupHandler that executes a dynamodb DeleteBackup operation
type DeleteBackupFinalHandler struct{}

// HandleDeleteBackup implements the DeleteBackupHandler
func (h *DeleteBackupFinalHandler) HandleDeleteBackup(ctx *DeleteBackupContext, output *DeleteBackupOutput) {
	output.Set(ctx.Client.DeleteBackup(ctx, ctx.Input))
}

// DeleteBackupMiddleWare is a middleware function use for wrapping DeleteBackupHandler requests
type DeleteBackupMiddleWare interface {
	DeleteBackupMiddleWare(next DeleteBackupHandler) DeleteBackupHandler
}

// DeleteBackupMiddleWareFunc is a functional DeleteBackupMiddleWare
type DeleteBackupMiddleWareFunc func(next DeleteBackupHandler) DeleteBackupHandler

// DeleteBackupMiddleWare implements the DeleteBackupMiddleWare interface
func (mw DeleteBackupMiddleWareFunc) DeleteBackupMiddleWare(next DeleteBackupHandler) DeleteBackupHandler {
	return mw(next)
}

// DeleteBackup represents a DeleteBackup operation
type DeleteBackup struct {
	*Promise
	input       *ddb.DeleteBackupInput
	middleWares []DeleteBackupMiddleWare
}

// NewDeleteBackup creates a new DeleteBackup
func NewDeleteBackup(input *ddb.DeleteBackupInput, mws ...DeleteBackupMiddleWare) *DeleteBackup {
	return &DeleteBackup{
		Promise:     NewPromise(),
		input:       input,
		middleWares: mws,
	}
}

// Invoke invokes the DeleteBackup operation and returns a DeleteBackupPromise
func (op *DeleteBackup) Invoke(ctx context.Context, client *ddb.Client) *DeleteBackup {
	go op.DynoInvoke(ctx, client)

	return op
}

// DynoInvoke implements the Operation interface
func (op *DeleteBackup) DynoInvoke(ctx context.Context, client *ddb.Client) {

	output := new(DeleteBackupOutput)

	defer func() { op.SetResponse(output.Get()) }()

	requestCtx := &DeleteBackupContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	var h DeleteBackupHandler

	h = new(DeleteBackupFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].DeleteBackupMiddleWare(h)
		}
	}

	h.HandleDeleteBackup(requestCtx, output)
}

// Await waits for the DeleteBackupPromise to be fulfilled and then returns a DeleteBackupOutput and error
func (op *DeleteBackup) Await() (*ddb.DeleteBackupOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.DeleteBackupOutput), err
}

// NewDeleteBackupInput creates a DeleteBackupInput with a given table name and key
func NewDeleteBackupInput(backupArn *string) *ddb.DeleteBackupInput {
	return &ddb.DeleteBackupInput{
		BackupArn: backupArn,
	}
}
