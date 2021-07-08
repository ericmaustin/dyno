package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// ListTables executes ListTables operation and returns a ListTables operation
func (s *Session) ListTables(input *ddb.ListTablesInput, mw ...ListTablesMiddleWare) *ListTables {
	return NewListTables(input, mw...).Invoke(s.ctx, s.ddb)
}

// ListTables executes a ListTables operation with a ListTablesInput in this pool and returns the ListTables operation
func (p *Pool) ListTables(input *ddb.ListTablesInput, mw ...ListTablesMiddleWare) *ListTables {
	op := NewListTables(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// ListAllTables executes ListTables operation and returns a ListTables operation
func (s *Session) ListAllTables(input *ddb.ListTablesInput, mw ...ListTablesMiddleWare) *ListTables {
	return NewListAllTables(input, mw...).Invoke(s.ctx, s.ddb)
}

// ListAllTables executes a ListTables operation with a ListTablesInput in this pool and returns the ListTables operation
func (p *Pool) ListAllTables(input *ddb.ListTablesInput, mw ...ListTablesMiddleWare) *ListTables {
	op := NewListAllTables(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// ListTablesContext represents an exhaustive ListTables operation request context
type ListTablesContext struct {
	context.Context
	Input  *ddb.ListTablesInput
	Client *ddb.Client
}

// ListTablesOutput represents the output for the ListTables operation
type ListTablesOutput struct {
	out *ddb.ListTablesOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *ListTablesOutput) Set(out *ddb.ListTablesOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *ListTablesOutput) Get() (out *ddb.ListTablesOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()

	return
}

// ListTablesHandler represents a handler for ListTables requests
type ListTablesHandler interface {
	HandleListTables(ctx *ListTablesContext, output *ListTablesOutput)
}

// ListTablesHandlerFunc is a ListTablesHandler function
type ListTablesHandlerFunc func(ctx *ListTablesContext, output *ListTablesOutput)

// HandleListTables implements ListTablesHandler
func (h ListTablesHandlerFunc) HandleListTables(ctx *ListTablesContext, output *ListTablesOutput) {
	h(ctx, output)
}

// ListTablesFinalHandler is the final ListTablesHandler that executes a dynamodb ListTables operation
type ListTablesFinalHandler struct{}

// HandleListTables implements the ListTablesHandler
func (h *ListTablesFinalHandler) HandleListTables(ctx *ListTablesContext, output *ListTablesOutput) {
	output.Set(ctx.Client.ListTables(ctx, ctx.Input))
}

// ListAllTablesFinalHandler is the final ListTablesHandler that executes a dynamodb ListTables operation
type ListAllTablesFinalHandler struct{}

// HandleListTables implements the ListTablesHandler
func (h *ListAllTablesFinalHandler) HandleListTables(ctx *ListTablesContext, output *ListTablesOutput) {
	var (
		err error
		out, finalOutput *ddb.ListTablesOutput
	)

	input := CopyListTablesInput(ctx.Input)
	finalOutput = new(ddb.ListTablesOutput)

	defer func() { output.Set(finalOutput, err) }()

	for {
		out, err = ctx.Client.ListTables(ctx, input)
		if err != nil {
			output.Set(nil, err)
		}

		finalOutput.TableNames = append(finalOutput.TableNames, out.TableNames...)

		if out.LastEvaluatedTableName == nil || len(*out.LastEvaluatedTableName) == 0 {
			return
		}

		*input.ExclusiveStartTableName = *out.LastEvaluatedTableName
	}
}

// ListTablesMiddleWare is a middleware function use for wrapping ListTablesHandler requests
type ListTablesMiddleWare interface {
	ListTablesMiddleWare(next ListTablesHandler) ListTablesHandler
}

// ListTablesMiddleWareFunc is a functional ListTablesMiddleWare
type ListTablesMiddleWareFunc func(next ListTablesHandler) ListTablesHandler

// ListTablesMiddleWare implements the ListTablesMiddleWare interface
func (mw ListTablesMiddleWareFunc) ListTablesMiddleWare(next ListTablesHandler) ListTablesHandler {
	return mw(next)
}

// ListTables represents a ListTables operation
type ListTables struct {
	*BaseOperation
	Handler     ListTablesHandler
	input       *ddb.ListTablesInput
	middleWares []ListTablesMiddleWare
}

// NewListTables creates a new ListTables operation
func NewListTables(input *ddb.ListTablesInput, mws ...ListTablesMiddleWare) *ListTables {
	return &ListTables{
		BaseOperation: NewOperation(),
		Handler:       new(ListTablesFinalHandler),
		input:         input,
		middleWares:   mws,
	}
}

// NewListAllTables creates a new ListTables operation that lists ALL tables and combines their outputs
func NewListAllTables(input *ddb.ListTablesInput, mws ...ListTablesMiddleWare) *ListTables {
	return &ListTables{
		BaseOperation: NewOperation(),
		Handler:       new(ListAllTablesFinalHandler),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the ListTables operation in a goroutine and returns a ListTables operation
func (op *ListTables) Invoke(ctx context.Context, client *ddb.Client) *ListTables {
	op.SetRunning() // operation now waiting for a response

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the ListTables operation
func (op *ListTables) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	output := new(ListTablesOutput)

	defer func() { op.SetResponse(output.Get()) }()

	var h ListTablesHandler

	h = op.Handler

	// loop in reverse to preserve middleware order
	for i := len(op.middleWares) - 1; i >= 0; i-- {
		h = op.middleWares[i].ListTablesMiddleWare(h)
	}

	requestCtx := &ListTablesContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	h.HandleListTables(requestCtx, output)
}

// Await waits for the ListTables operation to be fulfilled and then returns a ListTablesOutput and error
func (op *ListTables) Await() (*ddb.ListTablesOutput, error) {
	out, err := op.BaseOperation.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.ListTablesOutput), err
}

// NewListTablesInput creates a new ListTablesInput
func NewListTablesInput() *ddb.ListTablesInput {
	return &ddb.ListTablesInput{}
}

// CopyListTablesInput creates a copy of a ListGlobalTablesInput
func CopyListTablesInput(input *ddb.ListTablesInput) *ddb.ListTablesInput {
	out := &ddb.ListTablesInput{}

	if input.Limit != nil {
		out.Limit = new(int32)
		*out.Limit = *input.Limit
	}

	if input.ExclusiveStartTableName != nil {
		out.ExclusiveStartTableName = new(string)
		*out.ExclusiveStartTableName = *input.ExclusiveStartTableName
	}

	return out
}