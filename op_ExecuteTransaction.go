package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// ExecuteTransaction executes ExecuteTransaction operation and returns a ExecuteTransaction operation
func (s *Session) ExecuteTransaction(input *ddb.ExecuteTransactionInput, mw ...ExecuteTransactionMiddleWare) *ExecuteTransaction {
	return NewExecuteTransaction(input, mw...).Invoke(s.ctx, s.ddb)
}

// ExecuteTransaction executes a ExecuteTransaction operation with a ExecuteTransactionInput in this pool and returns the ExecuteTransaction operation
func (p *Pool) ExecuteTransaction(input *ddb.ExecuteTransactionInput, mw ...ExecuteTransactionMiddleWare) *ExecuteTransaction {
	op := NewExecuteTransaction(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// ExecuteTransactionContext represents an exhaustive ExecuteTransaction operation request context
type ExecuteTransactionContext struct {
	context.Context
	Input  *ddb.ExecuteTransactionInput
	Client *ddb.Client
}

// ExecuteTransactionOutput represents the output for the ExecuteTransaction operation
type ExecuteTransactionOutput struct {
	out *ddb.ExecuteTransactionOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *ExecuteTransactionOutput) Set(out *ddb.ExecuteTransactionOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *ExecuteTransactionOutput) Get() (out *ddb.ExecuteTransactionOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()

	return
}

// ExecuteTransactionHandler represents a handler for ExecuteTransaction requests
type ExecuteTransactionHandler interface {
	HandleExecuteTransaction(ctx *ExecuteTransactionContext, output *ExecuteTransactionOutput)
}

// ExecuteTransactionHandlerFunc is a ExecuteTransactionHandler function
type ExecuteTransactionHandlerFunc func(ctx *ExecuteTransactionContext, output *ExecuteTransactionOutput)

// HandleExecuteTransaction implements ExecuteTransactionHandler
func (h ExecuteTransactionHandlerFunc) HandleExecuteTransaction(ctx *ExecuteTransactionContext, output *ExecuteTransactionOutput) {
	h(ctx, output)
}

// ExecuteTransactionFinalHandler is the final ExecuteTransactionHandler that executes a dynamodb ExecuteTransaction operation
type ExecuteTransactionFinalHandler struct{}

// HandleExecuteTransaction implements the ExecuteTransactionHandler
func (h *ExecuteTransactionFinalHandler) HandleExecuteTransaction(ctx *ExecuteTransactionContext, output *ExecuteTransactionOutput) {
	output.Set(ctx.Client.ExecuteTransaction(ctx, ctx.Input))
}

// ExecuteTransactionMiddleWare is a middleware function use for wrapping ExecuteTransactionHandler requests
type ExecuteTransactionMiddleWare interface {
	ExecuteTransactionMiddleWare(next ExecuteTransactionHandler) ExecuteTransactionHandler
}

// ExecuteTransactionMiddleWareFunc is a functional ExecuteTransactionMiddleWare
type ExecuteTransactionMiddleWareFunc func(next ExecuteTransactionHandler) ExecuteTransactionHandler

// ExecuteTransactionMiddleWare implements the ExecuteTransactionMiddleWare interface
func (mw ExecuteTransactionMiddleWareFunc) ExecuteTransactionMiddleWare(next ExecuteTransactionHandler) ExecuteTransactionHandler {
	return mw(next)
}

// ExecuteTransaction represents a ExecuteTransaction operation
type ExecuteTransaction struct {
	*BaseOperation
	input       *ddb.ExecuteTransactionInput
	middleWares []ExecuteTransactionMiddleWare
}

// NewExecuteTransaction creates a new ExecuteTransaction operation
func NewExecuteTransaction(input *ddb.ExecuteTransactionInput, mws ...ExecuteTransactionMiddleWare) *ExecuteTransaction {
	return &ExecuteTransaction{
		BaseOperation: NewOperation(),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the ExecuteTransaction operation in a goroutine and returns a ExecuteTransaction operation
func (op *ExecuteTransaction) Invoke(ctx context.Context, client *ddb.Client) *ExecuteTransaction {
	op.SetRunning() // operation now waiting for a response

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the ExecuteTransaction operation
func (op *ExecuteTransaction) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	output := new(ExecuteTransactionOutput)

	defer func() { op.SetResponse(output.Get()) }()

	var h ExecuteTransactionHandler

	h = new(ExecuteTransactionFinalHandler)

	// loop in reverse to preserve middleware order
	for i := len(op.middleWares) - 1; i >= 0; i-- {
		h = op.middleWares[i].ExecuteTransactionMiddleWare(h)
	}

	requestCtx := &ExecuteTransactionContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	h.HandleExecuteTransaction(requestCtx, output)
}

// Await waits for the ExecuteTransaction operation to be fulfilled and then returns a ExecuteTransactionOutput and error
func (op *ExecuteTransaction) Await() (*ddb.ExecuteTransactionOutput, error) {
	out, err := op.BaseOperation.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.ExecuteTransactionOutput), err
}