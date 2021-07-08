package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// BatchExecuteStatement executes BatchExecuteStatement operation and returns a BatchExecuteStatementPromise
func (c *Client) BatchExecuteStatement(ctx context.Context, input *ddb.BatchExecuteStatementInput, mw ...BatchExecuteStatementMiddleWare) *BatchExecuteStatement {
	return NewBatchExecuteStatement(input, mw...).Invoke(ctx, c.ddb)
}

// BatchExecuteStatement executes a BatchExecuteStatement operation with a BatchExecuteStatementInput in this pool and returns the BatchExecuteStatementPromise
func (p *Pool) BatchExecuteStatement(input *ddb.BatchExecuteStatementInput, mw ...BatchExecuteStatementMiddleWare) *BatchExecuteStatement {
	op := NewBatchExecuteStatement(input, mw...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// BatchExecuteStatementContext represents an exhaustive BatchExecuteStatement operation request context
type BatchExecuteStatementContext struct {
	context.Context
	Input  *ddb.BatchExecuteStatementInput
	Client *ddb.Client
}

// BatchExecuteStatementOutput represents the output for the BatchExecuteStatement operation
type BatchExecuteStatementOutput struct {
	out *ddb.BatchExecuteStatementOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *BatchExecuteStatementOutput) Set(out *ddb.BatchExecuteStatementOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *BatchExecuteStatementOutput) Get() (out *ddb.BatchExecuteStatementOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()

	return
}

// BatchExecuteStatementHandler represents a handler for BatchExecuteStatement requests
type BatchExecuteStatementHandler interface {
	HandleBatchExecuteStatement(ctx *BatchExecuteStatementContext, output *BatchExecuteStatementOutput)
}

// BatchExecuteStatementHandlerFunc is a BatchExecuteStatementHandler function
type BatchExecuteStatementHandlerFunc func(ctx *BatchExecuteStatementContext, output *BatchExecuteStatementOutput)

// HandleBatchExecuteStatement implements BatchExecuteStatementHandler
func (h BatchExecuteStatementHandlerFunc) HandleBatchExecuteStatement(ctx *BatchExecuteStatementContext, output *BatchExecuteStatementOutput) {
	h(ctx, output)
}

// BatchExecuteStatementFinalHandler is the final BatchExecuteStatementHandler that executes a dynamodb BatchExecuteStatement operation
type BatchExecuteStatementFinalHandler struct{}

// HandleBatchExecuteStatement implements the BatchExecuteStatementHandler
func (h *BatchExecuteStatementFinalHandler) HandleBatchExecuteStatement(ctx *BatchExecuteStatementContext, output *BatchExecuteStatementOutput) {
	output.Set(ctx.Client.BatchExecuteStatement(ctx, ctx.Input))
}

// BatchExecuteStatementMiddleWare is a middleware function use for wrapping BatchExecuteStatementHandler requests
type BatchExecuteStatementMiddleWare interface {
	BatchExecuteStatementMiddleWare(next BatchExecuteStatementHandler) BatchExecuteStatementHandler
}

// BatchExecuteStatementMiddleWareFunc is a functional BatchExecuteStatementMiddleWare
type BatchExecuteStatementMiddleWareFunc func(next BatchExecuteStatementHandler) BatchExecuteStatementHandler

// BatchExecuteStatementMiddleWare implements the BatchExecuteStatementMiddleWare interface
func (mw BatchExecuteStatementMiddleWareFunc) BatchExecuteStatementMiddleWare(next BatchExecuteStatementHandler) BatchExecuteStatementHandler {
	return mw(next)
}

// BatchExecuteStatement represents a BatchExecuteStatement operation
type BatchExecuteStatement struct {
	*Promise
	input       *ddb.BatchExecuteStatementInput
	middleWares []BatchExecuteStatementMiddleWare
}

// NewBatchExecuteStatement creates a new BatchExecuteStatement
func NewBatchExecuteStatement(input *ddb.BatchExecuteStatementInput, mws ...BatchExecuteStatementMiddleWare) *BatchExecuteStatement {
	return &BatchExecuteStatement{
		Promise:     NewPromise(),
		input:       input,
		middleWares: mws,
	}
}

// Invoke invokes the BatchExecuteStatement operation in a goroutine and returns a BatchExecuteStatementPromise
func (op *BatchExecuteStatement) Invoke(ctx context.Context, client *ddb.Client) *BatchExecuteStatement {
	op.SetWaiting()

	go op.invoke(ctx, client)

	return op
}

// DynoInvoke implements the Operation interface
func (op *BatchExecuteStatement) DynoInvoke(ctx context.Context, client *ddb.Client) {
	op.SetWaiting()
	op.invoke(ctx, client)
}

// invoke invokes the BatchExecuteStatement
func (op *BatchExecuteStatement) invoke(ctx context.Context, client *ddb.Client) {
	output := new(BatchExecuteStatementOutput)

	defer func() { op.SetResponse(output.Get()) }()

	requestCtx := &BatchExecuteStatementContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	var h BatchExecuteStatementHandler

	h = new(BatchExecuteStatementFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].BatchExecuteStatementMiddleWare(h)
		}
	}

	h.HandleBatchExecuteStatement(requestCtx, output)
}

// Await waits for the BatchExecuteStatementPromise to be fulfilled and then returns a BatchExecuteStatementOutput and error
func (op *BatchExecuteStatement) Await() (*ddb.BatchExecuteStatementOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.BatchExecuteStatementOutput), err
}
