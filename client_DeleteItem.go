package dyno

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ericmaustin/dyno/condition"
	"sync"
)


// DeleteItem executes DeleteItem operation and returns a DeleteItemPromise
func (c *Client) DeleteItem(ctx context.Context, input *ddb.DeleteItemInput, mw ...DeleteItemMiddleWare) *DeleteItem {
	return NewDeleteItem(input, mw...).Invoke(ctx, c.ddb)
}

// DeleteItem executes a DeleteItem operation with a DeleteItemInput in this pool and returns the DeleteItemPromise
func (p *Pool) DeleteItem(input *ddb.DeleteItemInput, mw ...DeleteItemMiddleWare) *DeleteItem {
	op := NewDeleteItem(input, mw...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// DeleteItemContext represents an exhaustive DeleteItem operation request context
type DeleteItemContext struct {
	context.Context
	Input  *ddb.DeleteItemInput
	Client *ddb.Client
}

// DeleteItemOutput represents the output for the DeleteItem opration
type DeleteItemOutput struct {
	out *ddb.DeleteItemOutput
	err error
	mu sync.RWMutex
}

// Set sets the output
func (o *DeleteItemOutput) Set(out *ddb.DeleteItemOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *DeleteItemOutput) Get() (out *ddb.DeleteItemOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()

	return
}

// DeleteItemHandler represents a handler for DeleteItem requests
type DeleteItemHandler interface {
	HandleDeleteItem(ctx *DeleteItemContext, output *DeleteItemOutput)
}

// DeleteItemHandlerFunc is a DeleteItemHandler function
type DeleteItemHandlerFunc func(ctx *DeleteItemContext, output *DeleteItemOutput)

// HandleDeleteItem implements DeleteItemHandler
func (h DeleteItemHandlerFunc) HandleDeleteItem(ctx *DeleteItemContext, output *DeleteItemOutput) {
	h(ctx, output)
}

// DeleteItemFinalHandler is the final DeleteItemHandler that executes a dynamodb DeleteItem operation
type DeleteItemFinalHandler struct {}

// HandleDeleteItem implements the DeleteItemHandler
func (h *DeleteItemFinalHandler) HandleDeleteItem(ctx *DeleteItemContext, output *DeleteItemOutput) {
	output.Set(ctx.Client.DeleteItem(ctx, ctx.Input))
}

// DeleteItemMiddleWare is a middleware function use for wrapping DeleteItemHandler requests
type DeleteItemMiddleWare interface {
	DeleteItemMiddleWare(next DeleteItemHandler) DeleteItemHandler
}

// DeleteItemMiddleWareFunc is a functional DeleteItemMiddleWare
type DeleteItemMiddleWareFunc func(handler DeleteItemHandler) DeleteItemHandler

// DeleteItemMiddleWare implements the DeleteItemMiddleWare interface
func (mw DeleteItemMiddleWareFunc) DeleteItemMiddleWare(h DeleteItemHandler) DeleteItemHandler {
	return mw(h)
}

// DeleteItem represents a DeleteItem operation
type DeleteItem struct {
	*Promise
	input       *ddb.DeleteItemInput
	middleWares []DeleteItemMiddleWare
}

// NewDeleteItem creates a new DeleteItem
func NewDeleteItem(input *ddb.DeleteItemInput, mws ...DeleteItemMiddleWare) *DeleteItem {
	return &DeleteItem{
		Promise: NewPromise(),
		input:       input,
		middleWares: mws,
	}
}

// Invoke invokes the DeleteItem operation and returns a DeleteItemPromise
func (op *DeleteItem) Invoke(ctx context.Context, client *ddb.Client) *DeleteItem {
	go op.DynoInvoke(ctx, client)

	return op
}

// DynoInvoke implements the Operation interface
func (op *DeleteItem) DynoInvoke(ctx context.Context, client *ddb.Client) {

	output := new(DeleteItemOutput)

	defer func() { op.SetResponse(output.Get()) }()

	requestCtx := &DeleteItemContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	var h DeleteItemHandler

	h = new(DeleteItemFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].DeleteItemMiddleWare(h)
		}
	}

	h.HandleDeleteItem(requestCtx, output)
}

// Await waits for the DeleteItemPromise to be fulfilled and then returns a DeleteItemOutput and error
func (op *DeleteItem) Await() (*ddb.DeleteItemOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.DeleteItemOutput), err
}

// NewDeleteItemInput creates a DeleteItemInput with a given table name and key
func NewDeleteItemInput(tableName *string, key map[string]ddbTypes.AttributeValue) *ddb.DeleteItemInput {
	return &ddb.DeleteItemInput{
		Key:                         key,
		TableName:                   tableName,
		ReturnConsumedCapacity:      ddbTypes.ReturnConsumedCapacityNone,
		ReturnItemCollectionMetrics: ddbTypes.ReturnItemCollectionMetricsNone,
		ReturnValues:                ddbTypes.ReturnValueNone,
	}
}

// DeleteItemBuilder is used for dynamically building a DeleteItemInput
type DeleteItemBuilder struct {
	*ddb.DeleteItemInput
	cnd *condition.Builder
}

// NewDeleteItemBuilder creates a new DeleteItemInput with DeleteItemOpt
func NewDeleteItemBuilder(input *ddb.DeleteItemInput) *DeleteItemBuilder {
	bld := &DeleteItemBuilder{
		cnd: new(condition.Builder),
	}

	if input != nil {
		bld.DeleteItemInput = input
	} else {
		bld.DeleteItemInput = NewDeleteItemInput(nil, nil)
	}

	return bld
}

// SetKey sets the target key for the item to tbe deleted
func (bld *DeleteItemBuilder) SetKey(key map[string]ddbTypes.AttributeValue) *DeleteItemBuilder {
	bld.Key = key
	return bld
}

// AddCondition adds a condition to this update
// adding multiple conditions by calling this multiple times will join the conditions with
// an AND
func (bld *DeleteItemBuilder) AddCondition(cnd expression.ConditionBuilder) *DeleteItemBuilder {
	bld.cnd.And(cnd)
	return bld
}

// SetConditionExpression sets the ConditionExpression field's value.
func (bld *DeleteItemBuilder) SetConditionExpression(v string) *DeleteItemBuilder {
	bld.ConditionExpression = &v
	return bld
}

// SetExpressionAttributeNames sets the ExpressionAttributeNames field's value.
func (bld *DeleteItemBuilder) SetExpressionAttributeNames(v map[string]string) *DeleteItemBuilder {
	bld.ExpressionAttributeNames = v
	return bld
}

// SetExpressionAttributeValues sets the ExpressionAttributeValues field's value.
func (bld *DeleteItemBuilder) SetExpressionAttributeValues(v map[string]ddbTypes.AttributeValue) *DeleteItemBuilder {
	bld.ExpressionAttributeValues = v
	return bld
}

// SetReturnConsumedCapacity sets the ReturnConsumedCapacity field's value.
func (bld *DeleteItemBuilder) SetReturnConsumedCapacity(v ddbTypes.ReturnConsumedCapacity) *DeleteItemBuilder {
	bld.ReturnConsumedCapacity = v
	return bld
}

// SetReturnItemCollectionMetrics sets the ReturnItemCollectionMetrics field's value.
func (bld *DeleteItemBuilder) SetReturnItemCollectionMetrics(v ddbTypes.ReturnItemCollectionMetrics) *DeleteItemBuilder {
	bld.ReturnItemCollectionMetrics = v
	return bld
}

// SetReturnValues sets the ReturnValues field's value.
func (bld *DeleteItemBuilder) SetReturnValues(v ddbTypes.ReturnValue) *DeleteItemBuilder {
	bld.ReturnValues = v
	return bld
}

// SetTableName sets the TableName field's value.
func (bld *DeleteItemBuilder) SetTableName(v string) *DeleteItemBuilder {
	bld.TableName = &v
	return bld
}

// Build builds the dynamodb.DeleteItemInput
// returns error if expression builder returns an error
func (bld *DeleteItemBuilder) Build() (*ddb.DeleteItemInput, error) {
	if !bld.cnd.Empty() {
		expr := expression.NewBuilder().WithCondition(bld.cnd.Builder())
		e, err := expr.Build()
		if err != nil {
			return nil, fmt.Errorf("DeleteItemInput.Build() encountered an error while attempting to build an expression: %v", err)
		}

		bld.ConditionExpression = e.Condition()
		bld.ExpressionAttributeNames = e.Names()
		bld.ExpressionAttributeValues = e.Values()
	}

	return bld.DeleteItemInput, nil
}
