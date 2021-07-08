package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// ListGlobalTables executes ListGlobalTables operation and returns a ListGlobalTables operation
func (s *Session) ListGlobalTables(input *ddb.ListGlobalTablesInput, mw ...ListGlobalTablesMiddleWare) *ListGlobalTables {
	return NewListGlobalTables(input, mw...).Invoke(s.ctx, s.ddb)
}

// ListGlobalTables executes a ListGlobalTables operation with a ListGlobalTablesInput in this pool and returns the ListGlobalTables operation
func (p *Pool) ListGlobalTables(input *ddb.ListGlobalTablesInput, mw ...ListGlobalTablesMiddleWare) *ListGlobalTables {
	op := NewListGlobalTables(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// ListAllGlobalTables executes ListGlobalTables operation and returns a ListGlobalTables operation
func (s *Session) ListAllGlobalTables(input *ddb.ListGlobalTablesInput, mw ...ListGlobalTablesMiddleWare) *ListGlobalTables {
	return NewListAllGlobalTables(input, mw...).Invoke(s.ctx, s.ddb)
}

// ListAllGlobalTables executes a ListGlobalTables operation with a ListGlobalTablesInput in this pool and returns the ListGlobalTables operation
func (p *Pool) ListAllGlobalTables(input *ddb.ListGlobalTablesInput, mw ...ListGlobalTablesMiddleWare) *ListGlobalTables {
	op := NewListAllGlobalTables(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// ListGlobalTablesContext represents an exhaustive ListGlobalTables operation request context
type ListGlobalTablesContext struct {
	context.Context
	Input  *ddb.ListGlobalTablesInput
	Client *ddb.Client
}

// ListGlobalTablesOutput represents the output for the ListGlobalTables operation
type ListGlobalTablesOutput struct {
	out *ddb.ListGlobalTablesOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *ListGlobalTablesOutput) Set(out *ddb.ListGlobalTablesOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *ListGlobalTablesOutput) Get() (out *ddb.ListGlobalTablesOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()

	return
}

// ListGlobalTablesHandler represents a handler for ListGlobalTables requests
type ListGlobalTablesHandler interface {
	HandleListGlobalTables(ctx *ListGlobalTablesContext, output *ListGlobalTablesOutput)
}

// ListGlobalTablesHandlerFunc is a ListGlobalTablesHandler function
type ListGlobalTablesHandlerFunc func(ctx *ListGlobalTablesContext, output *ListGlobalTablesOutput)

// HandleListGlobalTables implements ListGlobalTablesHandler
func (h ListGlobalTablesHandlerFunc) HandleListGlobalTables(ctx *ListGlobalTablesContext, output *ListGlobalTablesOutput) {
	h(ctx, output)
}

// ListGlobalTablesFinalHandler is the final ListGlobalTablesHandler that executes a dynamodb ListGlobalTables operation
type ListGlobalTablesFinalHandler struct{}

// HandleListGlobalTables implements the ListGlobalTablesHandler
func (h *ListGlobalTablesFinalHandler) HandleListGlobalTables(ctx *ListGlobalTablesContext, output *ListGlobalTablesOutput) {
	output.Set(ctx.Client.ListGlobalTables(ctx, ctx.Input))
}

// ListAllGlobalTablesFinalHandler is the final ListGlobalTablesHandler that executes a dynamodb ListGlobalTables operation
type ListAllGlobalTablesFinalHandler struct{}

// HandleListGlobalTables implements the ListGlobalTablesHandler
func (h *ListAllGlobalTablesFinalHandler) HandleListGlobalTables(ctx *ListGlobalTablesContext, output *ListGlobalTablesOutput) {
	var (
		err error
		out, finalOutput *ddb.ListGlobalTablesOutput
	)

	input := CopyListGlobalTablesInput(ctx.Input)
	finalOutput = new(ddb.ListGlobalTablesOutput)

	defer func() { output.Set(finalOutput, err) }()

	for {
		out, err = ctx.Client.ListGlobalTables(ctx, input)
		if err != nil {
			output.Set(nil, err)
		}

		finalOutput.GlobalTables = append(finalOutput.GlobalTables, out.GlobalTables...)

		if out.LastEvaluatedGlobalTableName == nil || len(*out.LastEvaluatedGlobalTableName) == 0 {
			return
		}

		*input.ExclusiveStartGlobalTableName = *out.LastEvaluatedGlobalTableName
	}
}

// ListGlobalTablesMiddleWare is a middleware function use for wrapping ListGlobalTablesHandler requests
type ListGlobalTablesMiddleWare interface {
	ListGlobalTablesMiddleWare(next ListGlobalTablesHandler) ListGlobalTablesHandler
}

// ListGlobalTablesMiddleWareFunc is a functional ListGlobalTablesMiddleWare
type ListGlobalTablesMiddleWareFunc func(next ListGlobalTablesHandler) ListGlobalTablesHandler

// ListGlobalTablesMiddleWare implements the ListGlobalTablesMiddleWare interface
func (mw ListGlobalTablesMiddleWareFunc) ListGlobalTablesMiddleWare(next ListGlobalTablesHandler) ListGlobalTablesHandler {
	return mw(next)
}

// ListGlobalTables represents a ListGlobalTables operation
type ListGlobalTables struct {
	*BaseOperation
	Handler     ListGlobalTablesHandler
	input       *ddb.ListGlobalTablesInput
	middleWares []ListGlobalTablesMiddleWare
}

// NewListGlobalTables creates a new ListGlobalTables operation
func NewListGlobalTables(input *ddb.ListGlobalTablesInput, mws ...ListGlobalTablesMiddleWare) *ListGlobalTables {
	return &ListGlobalTables{
		BaseOperation: NewOperation(),
		Handler:       new(ListGlobalTablesFinalHandler),
		input:         input,
		middleWares:   mws,
	}
}

// NewListAllGlobalTables creates a new ListGlobalTables operation that lists ALL tables and combines their outputs
func NewListAllGlobalTables(input *ddb.ListGlobalTablesInput, mws ...ListGlobalTablesMiddleWare) *ListGlobalTables {
	return &ListGlobalTables{
		BaseOperation: NewOperation(),
		Handler:       new(ListAllGlobalTablesFinalHandler),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the ListGlobalTables operation in a goroutine and returns a ListGlobalTables operation
func (op *ListGlobalTables) Invoke(ctx context.Context, client *ddb.Client) *ListGlobalTables {
	op.SetRunning() // operation now waiting for a response

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the ListGlobalTables operation
func (op *ListGlobalTables) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	output := new(ListGlobalTablesOutput)

	defer func() { op.SetResponse(output.Get()) }()

	var h ListGlobalTablesHandler

	h = op.Handler

	// loop in reverse to preserve middleware order
	for i := len(op.middleWares) - 1; i >= 0; i-- {
		h = op.middleWares[i].ListGlobalTablesMiddleWare(h)
	}

	requestCtx := &ListGlobalTablesContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	h.HandleListGlobalTables(requestCtx, output)
}

// Await waits for the ListGlobalTables operation to be fulfilled and then returns a ListGlobalTablesOutput and error
func (op *ListGlobalTables) Await() (*ddb.ListGlobalTablesOutput, error) {
	out, err := op.BaseOperation.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.ListGlobalTablesOutput), err
}

// NewListGlobalTablesInput creates a new ListTablesInput
func NewListGlobalTablesInput() *ddb.ListGlobalTablesInput {
	return &ddb.ListGlobalTablesInput{}
}

// CopyListGlobalTablesInput creates a copy of a ListGlobalTablesInput
func CopyListGlobalTablesInput(input *ddb.ListGlobalTablesInput) *ddb.ListGlobalTablesInput {
	out := &ddb.ListGlobalTablesInput{}

	if input.Limit != nil {
		out.Limit = new(int32)
		*out.Limit = *input.Limit
	}

	if input.ExclusiveStartGlobalTableName != nil {
		out.ExclusiveStartGlobalTableName = new(string)
		*out.ExclusiveStartGlobalTableName = *input.ExclusiveStartGlobalTableName
	}

	if input.RegionName != nil {
		out.RegionName = new(string)
		*out.RegionName = *input.RegionName
	}

	return out
}
