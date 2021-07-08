package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// DescribeBackup executes DescribeBackup operation and returns a DescribeBackup
func (s *Session) DescribeBackup(input *ddb.DescribeBackupInput, mw ...DescribeBackupMiddleWare) *DescribeBackup {
	return NewDescribeBackup(input, mw...).Invoke(s.ctx, s.ddb)
}

// DescribeBackup executes a DescribeBackup operation with a DescribeBackupInput in this pool and returns the DescribeBackup
func (p *Pool) DescribeBackup(input *ddb.DescribeBackupInput, mw ...DescribeBackupMiddleWare) *DescribeBackup {
	op := NewDescribeBackup(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// DescribeBackupContext represents an exhaustive DescribeBackup operation request context
type DescribeBackupContext struct {
	context.Context
	Input  *ddb.DescribeBackupInput
	Client *ddb.Client
}

// DescribeBackupOutput represents the output for the DescribeBackup opration
type DescribeBackupOutput struct {
	out *ddb.DescribeBackupOutput
	err error
	mu  sync.RWMutex
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
type DescribeBackupFinalHandler struct{}

// HandleDescribeBackup implements the DescribeBackupHandler
func (h *DescribeBackupFinalHandler) HandleDescribeBackup(ctx *DescribeBackupContext, output *DescribeBackupOutput) {
	output.Set(ctx.Client.DescribeBackup(ctx, ctx.Input))
}

// DescribeBackupMiddleWare is a middleware function use for wrapping DescribeBackupHandler requests
type DescribeBackupMiddleWare interface {
	DescribeBackupMiddleWare(next DescribeBackupHandler) DescribeBackupHandler
}

// DescribeBackupMiddleWareFunc is a functional DescribeBackupMiddleWare
type DescribeBackupMiddleWareFunc func(next DescribeBackupHandler) DescribeBackupHandler

// DescribeBackupMiddleWare implements the DescribeBackupMiddleWare interface
func (mw DescribeBackupMiddleWareFunc) DescribeBackupMiddleWare(next DescribeBackupHandler) DescribeBackupHandler {
	return mw(next)
}

// DescribeBackup represents a DescribeBackup operation
type DescribeBackup struct {
	*BaseOperation
	input       *ddb.DescribeBackupInput
	middleWares []DescribeBackupMiddleWare
}

// NewDescribeBackup creates a new DescribeBackup
func NewDescribeBackup(input *ddb.DescribeBackupInput, mws ...DescribeBackupMiddleWare) *DescribeBackup {
	return &DescribeBackup{
		BaseOperation: NewOperation(),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the DescribeBackup operation in a goroutine and returns a BatchGetItemAll
func (op *DescribeBackup) Invoke(ctx context.Context, client *ddb.Client) *DescribeBackup {
	op.SetRunning() // operation now waiting for a response

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the DescribeBackup operation
func (op *DescribeBackup) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	output := new(DescribeBackupOutput)

	defer func() { op.SetResponse(output.Get()) }()
	
	var h DescribeBackupHandler

	h = new(DescribeBackupFinalHandler)

	// loop in reverse to preserve middleware order
	for i := len(op.middleWares) - 1; i >= 0; i-- {
		h = op.middleWares[i].DescribeBackupMiddleWare(h)
	}
	
	requestCtx := &DescribeBackupContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}
	
	h.HandleDescribeBackup(requestCtx, output)
}

// Await waits for the DescribeBackup to be fulfilled and then returns a DescribeBackupOutput and error
func (op *DescribeBackup) Await() (*ddb.DescribeBackupOutput, error) {
	out, err := op.BaseOperation.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.DescribeBackupOutput), err
}

// NewDescribeBackupInput creates a new DescribeBackupInput
func NewDescribeBackupInput(backupArn *string) *ddb.DescribeBackupInput {
	return &ddb.DescribeBackupInput{BackupArn: backupArn}
}
