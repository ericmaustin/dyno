package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// DeleteTable executes DeleteTable operation and returns a DeleteTable
func (s *Session) DeleteTable(input *ddb.DeleteTableInput, mw ...DeleteTableMiddleWare) *DeleteTable {
	return NewDeleteTable(input, mw...).Invoke(s.ctx, s.ddb)
}

// DeleteTable executes a DeleteTable operation with a DeleteTableInput in this pool and returns the DeleteTable
func (p *Pool) DeleteTable(input *ddb.DeleteTableInput, mw ...DeleteTableMiddleWare) *DeleteTable {
	op := NewDeleteTable(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// DeleteTableContext represents an exhaustive DeleteTable operation request context
type DeleteTableContext struct {
	context.Context
	Input  *ddb.DeleteTableInput
	Client *ddb.Client
}

// DeleteTableOutput represents the output for the DeleteTable opration
type DeleteTableOutput struct {
	out *ddb.DeleteTableOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *DeleteTableOutput) Set(out *ddb.DeleteTableOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *DeleteTableOutput) Get() (out *ddb.DeleteTableOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()

	return
}

// DeleteTableHandler represents a handler for DeleteTable requests
type DeleteTableHandler interface {
	HandleDeleteTable(ctx *DeleteTableContext, output *DeleteTableOutput)
}

// DeleteTableHandlerFunc is a DeleteTableHandler function
type DeleteTableHandlerFunc func(ctx *DeleteTableContext, output *DeleteTableOutput)

// HandleDeleteTable implements DeleteTableHandler
func (h DeleteTableHandlerFunc) HandleDeleteTable(ctx *DeleteTableContext, output *DeleteTableOutput) {
	h(ctx, output)
}

// DeleteTableFinalHandler is the final DeleteTableHandler that executes a dynamodb DeleteTable operation
type DeleteTableFinalHandler struct{}

// HandleDeleteTable implements the DeleteTableHandler
func (h *DeleteTableFinalHandler) HandleDeleteTable(ctx *DeleteTableContext, output *DeleteTableOutput) {
	output.Set(ctx.Client.DeleteTable(ctx, ctx.Input))
}

// DeleteTableMiddleWare is a middleware function use for wrapping DeleteTableHandler requests
type DeleteTableMiddleWare interface {
	DeleteTableMiddleWare(next DeleteTableHandler) DeleteTableHandler
}

// DeleteTableMiddleWareFunc is a functional DeleteTableMiddleWare
type DeleteTableMiddleWareFunc func(next DeleteTableHandler) DeleteTableHandler

// DeleteTableMiddleWare implements the DeleteTableMiddleWare interface
func (mw DeleteTableMiddleWareFunc) DeleteTableMiddleWare(next DeleteTableHandler) DeleteTableHandler {
	return mw(next)
}

// DeleteTable represents a DeleteTable operation
type DeleteTable struct {
	*BaseOperation
	input       *ddb.DeleteTableInput
	middleWares []DeleteTableMiddleWare
}

// NewDeleteTable creates a new DeleteTable
func NewDeleteTable(input *ddb.DeleteTableInput, mws ...DeleteTableMiddleWare) *DeleteTable {
	return &DeleteTable{
		BaseOperation: NewOperation(),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the DeleteTable operation in a goroutine and returns a BatchGetItemAll
func (op *DeleteTable) Invoke(ctx context.Context, client *ddb.Client) *DeleteTable {
	op.SetRunning() // operation now waiting for a response

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the DeleteTable operation
func (op *DeleteTable) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	output := new(DeleteTableOutput)

	defer func() { op.SetResponse(output.Get()) }()

	var h DeleteTableHandler

	h = new(DeleteTableFinalHandler)

	// loop in reverse to preserve middleware order
	for i := len(op.middleWares) - 1; i >= 0; i-- {
		h = op.middleWares[i].DeleteTableMiddleWare(h)
	}

	requestCtx := &DeleteTableContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	h.HandleDeleteTable(requestCtx, output)
}

// Await waits for the DeleteTable to be fulfilled and then returns a DeleteTableOutput and error
func (op *DeleteTable) Await() (*ddb.DeleteTableOutput, error) {
	out, err := op.BaseOperation.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.DeleteTableOutput), err
}

// NewDeleteTableInput creates a new DeleteTableInput
func NewDeleteTableInput(tableName *string) *ddb.DeleteTableInput {
	return &ddb.DeleteTableInput{TableName: tableName}
}
