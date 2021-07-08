package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// RestoreTableFromBackup executes RestoreTableFromBackup operation and returns a RestoreTableFromBackup operation
func (s *Session) RestoreTableFromBackup(input *ddb.RestoreTableFromBackupInput, mw ...RestoreTableFromBackupMiddleWare) *RestoreTableFromBackup {
	return NewRestoreTableFromBackup(input, mw...).Invoke(s.ctx, s.ddb)
}

// RestoreTableFromBackup executes a RestoreTableFromBackup operation with a RestoreTableFromBackupInput in this pool and returns the RestoreTableFromBackup operation
func (p *Pool) RestoreTableFromBackup(input *ddb.RestoreTableFromBackupInput, mw ...RestoreTableFromBackupMiddleWare) *RestoreTableFromBackup {
	op := NewRestoreTableFromBackup(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// RestoreTableFromBackupContext represents an exhaustive RestoreTableFromBackup operation request context
type RestoreTableFromBackupContext struct {
	context.Context
	Input  *ddb.RestoreTableFromBackupInput
	Client *ddb.Client
}

// RestoreTableFromBackupOutput represents the output for the RestoreTableFromBackup operation
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
	output.Set(ctx.Client.RestoreTableFromBackup(ctx, ctx.Input))
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
	*BaseOperation
	input       *ddb.RestoreTableFromBackupInput
	middleWares []RestoreTableFromBackupMiddleWare
}

// NewRestoreTableFromBackup creates a new RestoreTableFromBackup operation
func NewRestoreTableFromBackup(input *ddb.RestoreTableFromBackupInput, mws ...RestoreTableFromBackupMiddleWare) *RestoreTableFromBackup {
	return &RestoreTableFromBackup{
		BaseOperation: NewOperation(),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the RestoreTableFromBackup operation in a goroutine and returns a RestoreTableFromBackup operation
func (op *RestoreTableFromBackup) Invoke(ctx context.Context, client *ddb.Client) *RestoreTableFromBackup {
	op.SetRunning() // operation now waiting for a response

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the RestoreTableFromBackup operation
func (op *RestoreTableFromBackup) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	output := new(RestoreTableFromBackupOutput)

	defer func() { op.SetResponse(output.Get()) }()

	var h RestoreTableFromBackupHandler

	h = new(RestoreTableFromBackupFinalHandler)

	// loop in reverse to preserve middleware order
	for i := len(op.middleWares) - 1; i >= 0; i-- {
		h = op.middleWares[i].RestoreTableFromBackupMiddleWare(h)
	}

	requestCtx := &RestoreTableFromBackupContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	h.HandleRestoreTableFromBackup(requestCtx, output)
}

// Await waits for the RestoreTableFromBackup operation to be fulfilled and then returns a RestoreTableFromBackupOutput and error
func (op *RestoreTableFromBackup) Await() (*ddb.RestoreTableFromBackupOutput, error) {
	out, err := op.BaseOperation.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.RestoreTableFromBackupOutput), err
}

// NewRestoreTableFromBackupInput creates a RestoreTableFromBackupInput with a given table name and key
func NewRestoreTableFromBackupInput(tableName *string, backupArn *string) *ddb.RestoreTableFromBackupInput {
	return &ddb.RestoreTableFromBackupInput{
		TargetTableName: tableName,
		BackupArn:       backupArn,
	}
}
