package dyno

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"sync"
)

// TransactWriteItems executes TransactWriteItems operation and returns a TransactWriteItems operation
func (s *Session) TransactWriteItems(input *ddb.TransactWriteItemsInput, mw ...TransactWriteItemsMiddleWare) *TransactWriteItems {
	return NewTransactWriteItems(input, mw...).Invoke(s.ctx, s.ddb)
}

// TransactWriteItems executes a TransactWriteItems operation with a TransactWriteItemsInput in this pool and returns the TransactWriteItems operation
func (p *Pool) TransactWriteItems(input *ddb.TransactWriteItemsInput, mw ...TransactWriteItemsMiddleWare) *TransactWriteItems {
	op := NewTransactWriteItems(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// TransactWriteItemsContext represents an exhaustive TransactWriteItems operation request context
type TransactWriteItemsContext struct {
	context.Context
	Input  *ddb.TransactWriteItemsInput
	Client *ddb.Client
}

// TransactWriteItemsOutput represents the output for the TransactWriteItems operation
type TransactWriteItemsOutput struct {
	out *ddb.TransactWriteItemsOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *TransactWriteItemsOutput) Set(out *ddb.TransactWriteItemsOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *TransactWriteItemsOutput) Get() (out *ddb.TransactWriteItemsOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()

	return
}

// TransactWriteItemsHandler represents a handler for TransactWriteItems requests
type TransactWriteItemsHandler interface {
	HandleTransactWriteItems(ctx *TransactWriteItemsContext, output *TransactWriteItemsOutput)
}

// TransactWriteItemsHandlerFunc is a TransactWriteItemsHandler function
type TransactWriteItemsHandlerFunc func(ctx *TransactWriteItemsContext, output *TransactWriteItemsOutput)

// HandleTransactWriteItems implements TransactWriteItemsHandler
func (h TransactWriteItemsHandlerFunc) HandleTransactWriteItems(ctx *TransactWriteItemsContext, output *TransactWriteItemsOutput) {
	h(ctx, output)
}

// TransactWriteItemsFinalHandler is the final TransactWriteItemsHandler that executes a dynamodb TransactWriteItems operation
type TransactWriteItemsFinalHandler struct{}

// HandleTransactWriteItems implements the TransactWriteItemsHandler
func (h *TransactWriteItemsFinalHandler) HandleTransactWriteItems(ctx *TransactWriteItemsContext, output *TransactWriteItemsOutput) {
	output.Set(ctx.Client.TransactWriteItems(ctx, ctx.Input))
}

// TransactWriteItemsMiddleWare is a middleware function use for wrapping TransactWriteItemsHandler requests
type TransactWriteItemsMiddleWare interface {
	TransactWriteItemsMiddleWare(next TransactWriteItemsHandler) TransactWriteItemsHandler
}

// TransactWriteItemsMiddleWareFunc is a functional TransactWriteItemsMiddleWare
type TransactWriteItemsMiddleWareFunc func(next TransactWriteItemsHandler) TransactWriteItemsHandler

// TransactWriteItemsMiddleWare implements the TransactWriteItemsMiddleWare interface
func (mw TransactWriteItemsMiddleWareFunc) TransactWriteItemsMiddleWare(next TransactWriteItemsHandler) TransactWriteItemsHandler {
	return mw(next)
}

// TransactWriteItems represents a TransactWriteItems operation
type TransactWriteItems struct {
	*BaseOperation
	input       *ddb.TransactWriteItemsInput
	middleWares []TransactWriteItemsMiddleWare
}

// NewTransactWriteItems creates a new TransactWriteItems operation
func NewTransactWriteItems(input *ddb.TransactWriteItemsInput, mws ...TransactWriteItemsMiddleWare) *TransactWriteItems {
	return &TransactWriteItems{
		BaseOperation: NewOperation(),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the TransactWriteItems operation in a goroutine and returns a TransactWriteItems operation
func (op *TransactWriteItems) Invoke(ctx context.Context, client *ddb.Client) *TransactWriteItems {
	op.SetRunning() // operation now waiting for a response

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the TransactWriteItems operation
func (op *TransactWriteItems) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	output := new(TransactWriteItemsOutput)

	defer func() { op.SetResponse(output.Get()) }()

	var h TransactWriteItemsHandler

	h = new(TransactWriteItemsFinalHandler)

	// loop in reverse to preserve middleware order
	for i := len(op.middleWares) - 1; i >= 0; i-- {
		h = op.middleWares[i].TransactWriteItemsMiddleWare(h)
	}

	requestCtx := &TransactWriteItemsContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	h.HandleTransactWriteItems(requestCtx, output)
}

// Await waits for the TransactWriteItems operation to be fulfilled and then returns a TransactWriteItemsOutput and error
func (op *TransactWriteItems) Await() (*ddb.TransactWriteItemsOutput, error) {
	out, err := op.BaseOperation.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.TransactWriteItemsOutput), err
}

// TransactWriteItemsBuilder is used to dynamically build a TransactWriteItemsInput request
type TransactWriteItemsBuilder struct {
	*ddb.TransactWriteItemsInput
	projection *expression.ProjectionBuilder
}

// NewTransactWriteItemsInput creates a new TransactWriteItemsInput with a table name and key
func NewTransactWriteItemsInput() *ddb.TransactWriteItemsInput {
	return &ddb.TransactWriteItemsInput{
		ReturnConsumedCapacity: ddbTypes.ReturnConsumedCapacityNone,
	}
}

// NewTransactWriteItemsBuilder returns a new TransactWriteItemsBuilder for given tableName if tableName is not nil
func NewTransactWriteItemsBuilder(input *ddb.TransactWriteItemsInput) *TransactWriteItemsBuilder {
	if input != nil {
		return &TransactWriteItemsBuilder{TransactWriteItemsInput: input}
	}

	return &TransactWriteItemsBuilder{TransactWriteItemsInput: NewTransactWriteItemsInput()}
}

// SetInput sets the TransactWriteItemsBuilder's dynamodb.TransactWriteItemsInput
func (bld *TransactWriteItemsBuilder) SetInput(input *ddb.TransactWriteItemsInput) *TransactWriteItemsBuilder {
	bld.TransactWriteItemsInput = input
	return bld
}


// SetReturnConsumedCapacity sets the ReturnConsumedCapacity field's value.
func (bld *TransactWriteItemsBuilder) SetReturnConsumedCapacity(v ddbTypes.ReturnConsumedCapacity) *TransactWriteItemsBuilder {
	bld.ReturnConsumedCapacity = v
	return bld
}

// AddTransactWriteItem adds a TransactWriteItem
func (bld *TransactWriteItemsBuilder) AddTransactWriteItem(write ddbTypes.TransactWriteItem) *TransactWriteItemsBuilder {
	bld.TransactItems = append(bld.TransactItems, write)
	return bld
}

// AddPut adds a put request
func (bld *TransactWriteItemsBuilder) AddPut(put *ddbTypes.Put, cnd *ddbTypes.ConditionCheck) *TransactWriteItemsBuilder {
	bld.TransactItems = append(bld.TransactItems,
		ddbTypes.TransactWriteItem{
			ConditionCheck: cnd,
			Put: put,
		})

	return bld
}

// AddDelete adds a delete request
func (bld *TransactWriteItemsBuilder) AddDelete(del *ddbTypes.Delete, cnd *ddbTypes.ConditionCheck) *TransactWriteItemsBuilder {
	bld.TransactItems = append(bld.TransactItems,
		ddbTypes.TransactWriteItem{
			ConditionCheck: cnd,
			Delete: del,
		})

	return bld
}

// AddUpdate adds an update request
func (bld *TransactWriteItemsBuilder) AddUpdate(update *ddbTypes.Update, cnd *ddbTypes.ConditionCheck) *TransactWriteItemsBuilder {
	bld.TransactItems = append(bld.TransactItems,
		ddbTypes.TransactWriteItem{
			ConditionCheck: cnd,
			Update: update,
		})

	return bld
}

// Build returns a dynamodb.TransactWriteItemsInput
func (bld *TransactWriteItemsBuilder) Build() *ddb.TransactWriteItemsInput {
	return bld.TransactWriteItemsInput
}
