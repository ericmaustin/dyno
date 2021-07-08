package dyno

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"sync"
)

// TransactGetItems executes TransactGetItems operation and returns a TransactGetItemsPromise
func (c *Client) TransactGetItems(ctx context.Context, input *ddb.TransactGetItemsInput, mw ...TransactGetItemsMiddleWare) *TransactGetItems {
	return NewTransactGetItems(input, mw...).Invoke(ctx, c.ddb)
}

// TransactGetItems executes a TransactGetItems operation with a TransactGetItemsInput in this pool and returns the TransactGetItemsPromise
func (p *Pool) TransactGetItems(input *ddb.TransactGetItemsInput, mw ...TransactGetItemsMiddleWare) *TransactGetItems {
	op := NewTransactGetItems(input, mw...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// TransactGetItemsContext represents an exhaustive TransactGetItems operation request context
type TransactGetItemsContext struct {
	context.Context
	Input  *ddb.TransactGetItemsInput
	Client *ddb.Client
}

// TransactGetItemsOutput represents the output for the TransactGetItems operation
type TransactGetItemsOutput struct {
	out *ddb.TransactGetItemsOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *TransactGetItemsOutput) Set(out *ddb.TransactGetItemsOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *TransactGetItemsOutput) Get() (out *ddb.TransactGetItemsOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()
	
	return
}

// TransactGetItemsHandler represents a handler for TransactGetItems requests
type TransactGetItemsHandler interface {
	HandleTransactGetItems(ctx *TransactGetItemsContext, output *TransactGetItemsOutput)
}

// TransactGetItemsHandlerFunc is a TransactGetItemsHandler function
type TransactGetItemsHandlerFunc func(ctx *TransactGetItemsContext, output *TransactGetItemsOutput)

// HandleTransactGetItems implements TransactGetItemsHandler
func (h TransactGetItemsHandlerFunc) HandleTransactGetItems(ctx *TransactGetItemsContext, output *TransactGetItemsOutput) {
	h(ctx, output)
}

// TransactGetItemsFinalHandler is the final TransactGetItemsHandler that executes a dynamodb TransactGetItems operation
type TransactGetItemsFinalHandler struct{}

// HandleTransactGetItems implements the TransactGetItemsHandler
func (h *TransactGetItemsFinalHandler) HandleTransactGetItems(ctx *TransactGetItemsContext, output *TransactGetItemsOutput) {
	output.Set(ctx.Client.TransactGetItems(ctx, ctx.Input))
}

// TransactGetItemsMiddleWare is a middleware function use for wrapping TransactGetItemsHandler requests
type TransactGetItemsMiddleWare interface {
	TransactGetItemsMiddleWare(next TransactGetItemsHandler) TransactGetItemsHandler
}

// TransactGetItemsMiddleWareFunc is a functional TransactGetItemsMiddleWare
type TransactGetItemsMiddleWareFunc func(next TransactGetItemsHandler) TransactGetItemsHandler

// TransactGetItemsMiddleWare implements the TransactGetItemsMiddleWare interface
func (mw TransactGetItemsMiddleWareFunc) TransactGetItemsMiddleWare(next TransactGetItemsHandler) TransactGetItemsHandler {
	return mw(next)
}

// TransactGetItems represents a TransactGetItems operation
type TransactGetItems struct {
	*Promise
	input       *ddb.TransactGetItemsInput
	middleWares []TransactGetItemsMiddleWare
}

// NewTransactGetItems creates a new TransactGetItems
func NewTransactGetItems(input *ddb.TransactGetItemsInput, mws ...TransactGetItemsMiddleWare) *TransactGetItems {
	return &TransactGetItems{
		Promise:     NewPromise(),
		input:       input,
		middleWares: mws,
	}
}

// Invoke invokes the TransactGetItems operation in a goroutine and returns a BatchGetItemAllPromise
func (op *TransactGetItems) Invoke(ctx context.Context, client *ddb.Client) *TransactGetItems {
	op.SetWaiting() // promise now waiting for a response
	go op.invoke(ctx, client)

	return op
}

// DynoInvoke implements the Operation interface
func (op *TransactGetItems) DynoInvoke(ctx context.Context, client *ddb.Client) {
	op.SetWaiting() // promise now waiting for a response
	op.invoke(ctx, client)
}

// invoke invokes the TransactGetItems operation
func (op *TransactGetItems) invoke(ctx context.Context, client *ddb.Client) {
	output := new(TransactGetItemsOutput)

	defer func() { op.SetResponse(output.Get()) }()

	requestCtx := &TransactGetItemsContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	var h TransactGetItemsHandler

	h = new(TransactGetItemsFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].TransactGetItemsMiddleWare(h)
		}
	}

	h.HandleTransactGetItems(requestCtx, output)
}

// Await waits for the TransactGetItemsPromise to be fulfilled and then returns a TransactGetItemsOutput and error
func (op *TransactGetItems) Await() (*ddb.TransactGetItemsOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.TransactGetItemsOutput), err
}

// TransactGetItemsBuilder is used to dynamically build a TransactGetItemsInput request
type TransactGetItemsBuilder struct {
	*ddb.TransactGetItemsInput
	projection *expression.ProjectionBuilder
}

// NewTransactGetItemsInput creates a new TransactGetItemsInput with a table name and key
func NewTransactGetItemsInput() *ddb.TransactGetItemsInput {
	return &ddb.TransactGetItemsInput{
		ReturnConsumedCapacity: ddbTypes.ReturnConsumedCapacityNone,
	}
}

// NewTransactGetItemsBuilder returns a new TransactGetItemsBuilder for given tableName if tableName is not nil
func NewTransactGetItemsBuilder(input *ddb.TransactGetItemsInput) *TransactGetItemsBuilder {
	if input != nil {
		return &TransactGetItemsBuilder{TransactGetItemsInput: input}
	}

	return &TransactGetItemsBuilder{TransactGetItemsInput: NewTransactGetItemsInput()}
}

// SetInput sets the TransactGetItemsBuilder's dynamodb.TransactGetItemsInput
func (bld *TransactGetItemsBuilder) SetInput(input *ddb.TransactGetItemsInput) *TransactGetItemsBuilder {
	bld.TransactGetItemsInput = input
	return bld
}


// SetReturnConsumedCapacity sets the ReturnConsumedCapacity field's value.
func (bld *TransactGetItemsBuilder) SetReturnConsumedCapacity(v ddbTypes.ReturnConsumedCapacity) *TransactGetItemsBuilder {
	bld.ReturnConsumedCapacity = v
	return bld
}

// AddGet adds a get request
func (bld *TransactGetItemsBuilder) AddGet(gets ...*ddbTypes.Get) *TransactGetItemsBuilder {
	for _, g := range gets {
		bld.TransactItems = append(bld.TransactItems,
			ddbTypes.TransactGetItem{Get: g})
	}
	return bld
}

// Build returns a dynamodb.TransactGetItemsInput
func (bld *TransactGetItemsBuilder) Build() *ddb.TransactGetItemsInput {
	return bld.TransactGetItemsInput
}
