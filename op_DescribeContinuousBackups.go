package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// DescribeContinuousBackups executes DescribeContinuousBackups operation and returns a DescribeContinuousBackups
func (s *Session) DescribeContinuousBackups(input *ddb.DescribeContinuousBackupsInput, mw ...DescribeContinuousBackupsMiddleWare) *DescribeContinuousBackups {
	return NewDescribeContinuousBackups(input, mw...).Invoke(s.ctx, s.ddb)
}

// DescribeContinuousBackups executes a DescribeContinuousBackups operation with a DescribeContinuousBackupsInput in this pool and returns the DescribeContinuousBackups
func (p *Pool) DescribeContinuousBackups(input *ddb.DescribeContinuousBackupsInput, mw ...DescribeContinuousBackupsMiddleWare) *DescribeContinuousBackups {
	op := NewDescribeContinuousBackups(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// DescribeContinuousBackupsContext represents an exhaustive DescribeContinuousBackups operation request context
type DescribeContinuousBackupsContext struct {
	context.Context
	Input  *ddb.DescribeContinuousBackupsInput
	Client *ddb.Client
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
	output.Set(ctx.Client.DescribeContinuousBackups(ctx, ctx.Input))
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
	*BaseOperation
	input       *ddb.DescribeContinuousBackupsInput
	middleWares []DescribeContinuousBackupsMiddleWare
}

// NewDescribeContinuousBackups creates a new DescribeContinuousBackups
func NewDescribeContinuousBackups(input *ddb.DescribeContinuousBackupsInput, mws ...DescribeContinuousBackupsMiddleWare) *DescribeContinuousBackups {
	return &DescribeContinuousBackups{
		BaseOperation: NewOperation(),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the DescribeContinuousBackups operation in a goroutine and returns a BatchGetItemAll
func (op *DescribeContinuousBackups) Invoke(ctx context.Context, client *ddb.Client) *DescribeContinuousBackups {
	op.SetRunning() // operation now waiting for a response

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the DescribeContinuousBackups operation
func (op *DescribeContinuousBackups) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	output := new(DescribeContinuousBackupsOutput)

	defer func() { op.SetResponse(output.Get()) }()

	var h DescribeContinuousBackupsHandler

	h = new(DescribeContinuousBackupsFinalHandler)

	// loop in reverse to preserve middleware order
	for i := len(op.middleWares) - 1; i >= 0; i-- {
		h = op.middleWares[i].DescribeContinuousBackupsMiddleWare(h)
	}

	requestCtx := &DescribeContinuousBackupsContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}
	
	h.HandleDescribeContinuousBackups(requestCtx, output)
}

// Await waits for the DescribeContinuousBackups operation to be fulfilled and then returns a DescribeContinuousBackupsOutput and error
func (op *DescribeContinuousBackups) Await() (*ddb.DescribeContinuousBackupsOutput, error) {
	out, err := op.BaseOperation.Await()
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
