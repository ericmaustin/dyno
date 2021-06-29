package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// RestoreTableFromBackup executes RestoreTableFromBackup operation and returns a RestoreTableFromBackupPromise
func (c *Client) RestoreTableFromBackup(ctx context.Context, input *ddb.RestoreTableFromBackupInput, mw ...RestoreTableFromBackupMiddleWare) *RestoreTableFromBackupPromise {
	return NewRestoreTableFromBackup(input, mw...).Invoke(ctx, c.ddb)
}

// RestoreTableFromBackup executes a RestoreTableFromBackup operation with a RestoreTableFromBackupInput in this pool and returns the RestoreTableFromBackupPromise
func (p *Pool) RestoreTableFromBackup(input *ddb.RestoreTableFromBackupInput, mw ...RestoreTableFromBackupMiddleWare) *RestoreTableFromBackupPromise {
	op := NewRestoreTableFromBackup(input, mw...)

	if err := p.Do(op); err != nil {
		op.promise.SetResponse(nil, err)
	}

	return op.promise
}

// RestoreTableFromBackupContext represents an exhaustive RestoreTableFromBackup operation request context
type RestoreTableFromBackupContext struct {
	context.Context
	input  *ddb.RestoreTableFromBackupInput
	client *ddb.Client
}

// RestoreTableFromBackupOutput represents the output for the RestoreTableFromBackup opration
type RestoreTableFromBackupOutput struct {
	out *ddb.RestoreTableFromBackupOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *RestoreTableFromBackupOutput) Set(out *ddb.RestoreTableFromBackupOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *RestoreTableFromBackupOutput) Get() (out *ddb.RestoreTableFromBackupOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()
	return
}

// RestoreTableFromBackupPromise represents a promise for the RestoreTableFromBackup
type RestoreTableFromBackupPromise struct {
	*Promise
}

// GetResponse returns the GetResponse output and error
// if Output has not been set yet nil is returned
func (p *RestoreTableFromBackupPromise) GetResponse() (*ddb.RestoreTableFromBackupOutput, error) {
	out, err := p.Promise.GetResponse()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.RestoreTableFromBackupOutput), err
}

// Await waits for the RestoreTableFromBackupPromise to be fulfilled and then returns a RestoreTableFromBackupOutput and error
func (p *RestoreTableFromBackupPromise) Await() (*ddb.RestoreTableFromBackupOutput, error) {
	out, err := p.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.RestoreTableFromBackupOutput), err
}

// newRestoreTableFromBackupPromise returns a new RestoreTableFromBackupPromise
func newRestoreTableFromBackupPromise() *RestoreTableFromBackupPromise {
	return &RestoreTableFromBackupPromise{NewPromise()}
}

// RestoreTableFromBackupHandler represents a handler for RestoreTableFromBackup requests
type RestoreTableFromBackupHandler interface {
	HandleRestoreTableFromBackup(ctx *RestoreTableFromBackupContext, output *RestoreTableFromBackupOutput)
}

// RestoreTableFromBackupHandlerFunc is a RestoreTableFromBackupHandler function
type RestoreTableFromBackupHandlerFunc func(ctx *RestoreTableFromBackupContext, output *RestoreTableFromBackupOutput)

// HandleRestoreTableFromBackup implements RestoreTableFromBackupHandler
func (h RestoreTableFromBackupHandlerFunc) HandleRestoreTableFromBackup(ctx *RestoreTableFromBackupContext, output *RestoreTableFromBackupOutput) {
	h(ctx, output)
}

// RestoreTableFromBackupFinalHandler is the final RestoreTableFromBackupHandler that executes a dynamodb RestoreTableFromBackup operation
type RestoreTableFromBackupFinalHandler struct{}

// HandleRestoreTableFromBackup implements the RestoreTableFromBackupHandler
func (h *RestoreTableFromBackupFinalHandler) HandleRestoreTableFromBackup(ctx *RestoreTableFromBackupContext, output *RestoreTableFromBackupOutput) {
	output.Set(ctx.client.RestoreTableFromBackup(ctx, ctx.input))
}

// RestoreTableFromBackupMiddleWare is a middleware function use for wrapping RestoreTableFromBackupHandler requests
type RestoreTableFromBackupMiddleWare interface {
	RestoreTableFromBackupMiddleWare(next RestoreTableFromBackupHandler) RestoreTableFromBackupHandler
}

// RestoreTableFromBackupMiddleWareFunc is a functional RestoreTableFromBackupMiddleWare
type RestoreTableFromBackupMiddleWareFunc func(next RestoreTableFromBackupHandler) RestoreTableFromBackupHandler

// RestoreTableFromBackupMiddleWare implements the RestoreTableFromBackupMiddleWare interface
func (mw RestoreTableFromBackupMiddleWareFunc) RestoreTableFromBackupMiddleWare(next RestoreTableFromBackupHandler) RestoreTableFromBackupHandler {
	return mw(next)
}

// RestoreTableFromBackup represents a RestoreTableFromBackup operation
type RestoreTableFromBackup struct {
	promise     *RestoreTableFromBackupPromise
	input       *ddb.RestoreTableFromBackupInput
	middleWares []RestoreTableFromBackupMiddleWare
}

// NewRestoreTableFromBackup creates a new RestoreTableFromBackup
func NewRestoreTableFromBackup(input *ddb.RestoreTableFromBackupInput, mws ...RestoreTableFromBackupMiddleWare) *RestoreTableFromBackup {
	return &RestoreTableFromBackup{
		input:       input,
		middleWares: mws,
		promise:     newRestoreTableFromBackupPromise(),
	}
}

// Invoke invokes the RestoreTableFromBackup operation and returns a RestoreTableFromBackupPromise
func (op *RestoreTableFromBackup) Invoke(ctx context.Context, client *ddb.Client) *RestoreTableFromBackupPromise {
	go op.DynoInvoke(ctx, client)

	return op.promise
}

// DynoInvoke implements the Operation interface
func (op *RestoreTableFromBackup) DynoInvoke(ctx context.Context, client *ddb.Client) {
	output := new(RestoreTableFromBackupOutput)

	defer func() { op.promise.SetResponse(output.Get()) }()

	requestCtx := &RestoreTableFromBackupContext{
		Context: ctx,
		client:  client,
		input:   op.input,
	}

	var h RestoreTableFromBackupHandler

	h = new(RestoreTableFromBackupFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].RestoreTableFromBackupMiddleWare(h)
		}
	}

	h.HandleRestoreTableFromBackup(requestCtx, output)
}

// NewRestoreTableFromBackupInput creates a RestoreTableFromBackupInput with a given table name and key
func NewRestoreTableFromBackupInput(tableName *string, backupArn *string) *ddb.RestoreTableFromBackupInput {
	return &ddb.RestoreTableFromBackupInput{
		TargetTableName: tableName,
		BackupArn:       backupArn,
	}
}
