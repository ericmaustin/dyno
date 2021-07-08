package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// ExecuteStatement executes ExecuteStatement operation and returns a ExecuteStatement operation
func (s *Session) ExecuteStatement(input *ddb.ExecuteStatementInput, mw ...ExecuteStatementMiddleWare) *ExecuteStatement {
	return NewExecuteStatement(input, mw...).Invoke(s.ctx, s.ddb)
}

// ExecuteStatement executes a ExecuteStatement operation with a ExecuteStatementInput in this pool and returns the ExecuteStatement operation
func (p *Pool) ExecuteStatement(input *ddb.ExecuteStatementInput, mw ...ExecuteStatementMiddleWare) *ExecuteStatement {
	op := NewExecuteStatement(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// ExecuteStatementContext represents an exhaustive ExecuteStatement operation request context
type ExecuteStatementContext struct {
	context.Context
	Input  *ddb.ExecuteStatementInput
	Client *ddb.Client
}

// ExecuteStatementOutput represents the output for the ExecuteStatement operation
type ExecuteStatementOutput struct {
	out *ddb.ExecuteStatementOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *ExecuteStatementOutput) Set(out *ddb.ExecuteStatementOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *ExecuteStatementOutput) Get() (out *ddb.ExecuteStatementOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()

	return
}

// ExecuteStatementHandler represents a handler for ExecuteStatement requests
type ExecuteStatementHandler interface {
	HandleExecuteStatement(ctx *ExecuteStatementContext, output *ExecuteStatementOutput)
}

// ExecuteStatementHandlerFunc is a ExecuteStatementHandler function
type ExecuteStatementHandlerFunc func(ctx *ExecuteStatementContext, output *ExecuteStatementOutput)

// HandleExecuteStatement implements ExecuteStatementHandler
func (h ExecuteStatementHandlerFunc) HandleExecuteStatement(ctx *ExecuteStatementContext, output *ExecuteStatementOutput) {
	h(ctx, output)
}

// ExecuteStatementFinalHandler is the final ExecuteStatementHandler that executes a dynamodb ExecuteStatement operation
type ExecuteStatementFinalHandler struct{}

// HandleExecuteStatement implements the ExecuteStatementHandler
func (h *ExecuteStatementFinalHandler) HandleExecuteStatement(ctx *ExecuteStatementContext, output *ExecuteStatementOutput) {
	output.Set(ctx.Client.ExecuteStatement(ctx, ctx.Input))
}

// ExecuteStatementMiddleWare is a middleware function use for wrapping ExecuteStatementHandler requests
type ExecuteStatementMiddleWare interface {
	ExecuteStatementMiddleWare(next ExecuteStatementHandler) ExecuteStatementHandler
}

// ExecuteStatementMiddleWareFunc is a functional ExecuteStatementMiddleWare
type ExecuteStatementMiddleWareFunc func(next ExecuteStatementHandler) ExecuteStatementHandler

// ExecuteStatementMiddleWare implements the ExecuteStatementMiddleWare interface
func (mw ExecuteStatementMiddleWareFunc) ExecuteStatementMiddleWare(next ExecuteStatementHandler) ExecuteStatementHandler {
	return mw(next)
}

// ExecuteStatement represents a ExecuteStatement operation
type ExecuteStatement struct {
	*BaseOperation
	input       *ddb.ExecuteStatementInput
	middleWares []ExecuteStatementMiddleWare
}

// NewExecuteStatement creates a new ExecuteStatement operation
func NewExecuteStatement(input *ddb.ExecuteStatementInput, mws ...ExecuteStatementMiddleWare) *ExecuteStatement {
	return &ExecuteStatement{
		BaseOperation: NewOperation(),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the ExecuteStatement operation in a goroutine and returns a ExecuteStatement operation
func (op *ExecuteStatement) Invoke(ctx context.Context, client *ddb.Client) *ExecuteStatement {
	op.SetRunning() // operation now waiting for a response

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the ExecuteStatement operation
func (op *ExecuteStatement) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	output := new(ExecuteStatementOutput)

	defer func() { op.SetResponse(output.Get()) }()

	var h ExecuteStatementHandler

	h = new(ExecuteStatementFinalHandler)

	// loop in reverse to preserve middleware order
	for i := len(op.middleWares) - 1; i >= 0; i-- {
		h = op.middleWares[i].ExecuteStatementMiddleWare(h)
	}

	requestCtx := &ExecuteStatementContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	h.HandleExecuteStatement(requestCtx, output)
}

// Await waits for the ExecuteStatement operation to be fulfilled and then returns a ExecuteStatementOutput and error
func (op *ExecuteStatement) Await() (*ddb.ExecuteStatementOutput, error) {
	out, err := op.BaseOperation.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.ExecuteStatementOutput), err
}