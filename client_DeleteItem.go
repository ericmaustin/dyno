package dyno

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ericmaustin/dyno/condition"
)


// DeleteItem executes DeleteItem operation and returns a DeleteItemPromise
func (c *Client) DeleteItem(ctx context.Context, input *ddb.DeleteItemInput, mw ...DeleteItemMiddleWare) *DeleteItemPromise {
	return NewDeleteItem(input, mw...).Invoke(ctx, c.ddb)
}

// DeleteItem executes a DeleteItem operation with a DeleteItemInput in this pool and returns the DeleteItemPromise
func (p *Pool) DeleteItem(input *ddb.DeleteItemInput, mw ...DeleteItemMiddleWare) *DeleteItemPromise {
	op := NewDeleteItem(input, mw...)

	if err := p.Do(op); err != nil {
		op.promise.SetResponse(nil, err)
	}

	return op.promise
}

// DeleteItemContext represents an exhaustive DeleteItem operation request context
type DeleteItemContext struct {
	context.Context
	input  *ddb.DeleteItemInput
	client *ddb.Client
}

// DeleteItemPromise represents a promise for the DeleteItem
type DeleteItemPromise struct {
	*Promise
}

// Await waits for the DeleteItemPromise to be fulfilled and then returns a DeleteItemOutput and error
func (p *DeleteItemPromise) Await() (*ddb.DeleteItemOutput, error) {
	out, err := p.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.DeleteItemOutput), err
}

// newDeleteItemPromise returns a new DeleteItemPromise
func newDeleteItemPromise() *DeleteItemPromise {
	return &DeleteItemPromise{NewPromise()}
}

// DeleteItemHandler represents a handler for DeleteItem requests
type DeleteItemHandler interface {
	HandleDeleteItem(ctx *DeleteItemContext, promise *DeleteItemPromise)
}

// DeleteItemHandlerFunc is a DeleteItemHandler function
type DeleteItemHandlerFunc func(ctx *DeleteItemContext, promise *DeleteItemPromise)

// HandleDeleteItem implements DeleteItemHandler
func (h DeleteItemHandlerFunc) HandleDeleteItem(ctx *DeleteItemContext, promise *DeleteItemPromise) {
	h(ctx, promise)
}

// DeleteItemMiddleWare is a middleware function use for wrapping DeleteItemHandler requests
type DeleteItemMiddleWare func(handler DeleteItemHandler) DeleteItemHandler

// DeleteItemFinalHandler returns the final DeleteItemHandler that executes a dynamodb DeleteItem operation
func DeleteItemFinalHandler() DeleteItemHandler {
	return DeleteItemHandlerFunc(func(ctx *DeleteItemContext, promise *DeleteItemPromise) {
		promise.SetResponse(ctx.client.DeleteItem(ctx, ctx.input))
	})
}

// DeleteItem represents a DeleteItem operation
type DeleteItem struct {
	promise     *DeleteItemPromise
	input       *ddb.DeleteItemInput
	middleWares []DeleteItemMiddleWare
}

// NewDeleteItem creates a new DeleteItem
func NewDeleteItem(input *ddb.DeleteItemInput, mws ...DeleteItemMiddleWare) *DeleteItem {
	return &DeleteItem{
		input:       input,
		middleWares: mws,
		promise:     newDeleteItemPromise(),
	}
}

// Invoke invokes the DeleteItem operation and returns a DeleteItemPromise
func (op *DeleteItem) Invoke(ctx context.Context, client *ddb.Client) *DeleteItemPromise {
	go op.DynoInvoke(ctx, client)

	return op.promise
}

// DynoInvoke implements the Operation interface
func (op *DeleteItem) DynoInvoke(ctx context.Context, client *ddb.Client) {

	requestCtx := &DeleteItemContext{
		Context: ctx,
		client:  client,
		input:   op.input,
	}

	h := DeleteItemFinalHandler()

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i](h)
		}
	}

	h.HandleDeleteItem(requestCtx, op.promise)
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
	if input != nil {
		return &DeleteItemBuilder{DeleteItemInput: input}
	}
	return &DeleteItemBuilder{DeleteItemInput: NewDeleteItemInput(nil, nil)}
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
