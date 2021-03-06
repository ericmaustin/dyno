package dyno

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ericmaustin/dyno/condition"
	"github.com/ericmaustin/dyno/encoding"
	"sync"
)

// UpdateItem executes UpdateItem operation and returns a UpdateItem operation
func (s *Session) UpdateItem(input *ddb.UpdateItemInput, mw ...UpdateItemMiddleWare) *UpdateItem {
	return NewUpdateItem(input, mw...).Invoke(s.ctx, s.ddb)
}

// UpdateItem executes a UpdateItem operation with a UpdateItemInput in this pool and returns the UpdateItem operation
func (p *Pool) UpdateItem(input *ddb.UpdateItemInput, mw ...UpdateItemMiddleWare) *UpdateItem {
	op := NewUpdateItem(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// UpdateItemContext represents an exhaustive UpdateItem operation request context
type UpdateItemContext struct {
	context.Context
	Input  *ddb.UpdateItemInput
	Client *ddb.Client
}

// UpdateItemOutput represents the output for the UpdateItem operation
type UpdateItemOutput struct {
	out *ddb.UpdateItemOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *UpdateItemOutput) Set(out *ddb.UpdateItemOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *UpdateItemOutput) Get() (out *ddb.UpdateItemOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()

	return
}

// UpdateItemHandler represents a handler for UpdateItem requests
type UpdateItemHandler interface {
	HandleUpdateItem(ctx *UpdateItemContext, output *UpdateItemOutput)
}

// UpdateItemHandlerFunc is a UpdateItemHandler function
type UpdateItemHandlerFunc func(ctx *UpdateItemContext, output *UpdateItemOutput)

// HandleUpdateItem implements UpdateItemHandler
func (h UpdateItemHandlerFunc) HandleUpdateItem(ctx *UpdateItemContext, output *UpdateItemOutput) {
	h(ctx, output)
}

// UpdateItemFinalHandler is the final UpdateItemHandler that executes a dynamodb UpdateItem operation
type UpdateItemFinalHandler struct{}

// HandleUpdateItem implements the UpdateItemHandler
func (h *UpdateItemFinalHandler) HandleUpdateItem(ctx *UpdateItemContext, output *UpdateItemOutput) {
	output.Set(ctx.Client.UpdateItem(ctx, ctx.Input))
}

// UpdateItemMiddleWare is a middleware function use for wrapping UpdateItemHandler requests
type UpdateItemMiddleWare interface {
	UpdateItemMiddleWare(next UpdateItemHandler) UpdateItemHandler
}

// UpdateItemMiddleWareFunc is a functional UpdateItemMiddleWare
type UpdateItemMiddleWareFunc func(next UpdateItemHandler) UpdateItemHandler

// UpdateItemMiddleWare implements the UpdateItemMiddleWare interface
func (mw UpdateItemMiddleWareFunc) UpdateItemMiddleWare(next UpdateItemHandler) UpdateItemHandler {
	return mw(next)
}

// UpdateItem represents a UpdateItem operation
type UpdateItem struct {
	*BaseOperation
	input       *ddb.UpdateItemInput
	middleWares []UpdateItemMiddleWare
}

// NewUpdateItem creates a new UpdateItem operation
func NewUpdateItem(input *ddb.UpdateItemInput, mws ...UpdateItemMiddleWare) *UpdateItem {
	return &UpdateItem{
		BaseOperation: NewOperation(),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the UpdateItem operation in a goroutine and returns a UpdateItem operation
func (op *UpdateItem) Invoke(ctx context.Context, client *ddb.Client) *UpdateItem {
	op.SetRunning() // operation now waiting for a response

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the UpdateItem operation
func (op *UpdateItem) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	output := new(UpdateItemOutput)

	defer func() { op.SetResponse(output.Get()) }()

	var h UpdateItemHandler

	h = new(UpdateItemFinalHandler)

	// loop in reverse to preserve middleware order
	for i := len(op.middleWares) - 1; i >= 0; i-- {
		h = op.middleWares[i].UpdateItemMiddleWare(h)
	}

	requestCtx := &UpdateItemContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	h.HandleUpdateItem(requestCtx, output)
}

// Await waits for the UpdateItem operation to be fulfilled and then returns a UpdateItemOutput and error
func (op *UpdateItem) Await() (*ddb.UpdateItemOutput, error) {
	out, err := op.BaseOperation.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.UpdateItemOutput), err
}

func NewUpdateItemInput(tableName *string) *ddb.UpdateItemInput {
	return &ddb.UpdateItemInput{
		TableName:                   tableName,
		ReturnConsumedCapacity:      ddbTypes.ReturnConsumedCapacityNone,
		ReturnItemCollectionMetrics: ddbTypes.ReturnItemCollectionMetricsNone,
		ReturnValues:                ddbTypes.ReturnValueNone,
	}
}

// UpdateItemBuilder is used to build an UpdateItemInput
type UpdateItemBuilder struct {
	*ddb.UpdateItemInput
	updateBuilder expression.UpdateBuilder
	cnd           condition.Builder
}

// NewUpdateItemBuilder creates a new UpdateItemBuilder
func NewUpdateItemBuilder(input *ddb.UpdateItemInput) *UpdateItemBuilder {
	bld := &UpdateItemBuilder{}

	if input != nil {
		bld.UpdateItemInput = input
	} else {
		bld.UpdateItemInput = NewUpdateItemInput(nil)
	}

	return bld
}

// Add adds an Add operation on this update with the given field name and value
func (bld *UpdateItemBuilder) Add(field string, value interface{}) *UpdateItemBuilder {
	bld.updateBuilder = bld.updateBuilder.Add(expression.Name(field), expression.Value(value))
	return bld
}

// AddItem adds an add operation on this update with the given fields and values from an item
func (bld *UpdateItemBuilder) AddItem(item map[string]ddbTypes.AttributeValue) *UpdateItemBuilder {
	for key, value := range item {
		bld.updateBuilder = bld.updateBuilder.Add(expression.Name(key), expression.Value(value))
	}

	return bld
}

// Delete adds a Delete operation on this update with the given field name and value
func (bld *UpdateItemBuilder) Delete(field string, value interface{}) *UpdateItemBuilder {
	bld.updateBuilder = bld.updateBuilder.Delete(expression.Name(field), expression.Value(value))
	return bld
}

// DeleteItem adds a delete operation on this update with the given fields and values from an item
func (bld *UpdateItemBuilder) DeleteItem(item map[string]ddbTypes.AttributeValue) *UpdateItemBuilder {
	for key, value := range item {
		bld.updateBuilder = bld.updateBuilder.Delete(expression.Name(key), expression.Value(value))
	}

	return bld
}

// Remove adds one or more Remove operations on this update with the given field name
func (bld *UpdateItemBuilder) Remove(fields ...string) *UpdateItemBuilder {
	for _, field := range fields {
		bld.updateBuilder = bld.updateBuilder.Remove(expression.Name(field))
	}

	return bld
}

// Set adds a set operation on this update with the given field and value
func (bld *UpdateItemBuilder) Set(field string, value interface{}) *UpdateItemBuilder {
	bld.updateBuilder = bld.updateBuilder.Set(expression.Name(encoding.ToString(field)), expression.Value(value))
	return bld
}

// SetItem adds a set operation on this update with the given fields and values from an item
func (bld *UpdateItemBuilder) SetItem(item map[string]ddbTypes.AttributeValue) *UpdateItemBuilder {
	for key, value := range item {
		bld.updateBuilder = bld.updateBuilder.Set(expression.Name(key), expression.Value(value))
	}

	return bld
}

// AddCondition adds a condition to this update
// adding multiple conditions by calling this multiple times will join the conditions with
// an AND
func (bld *UpdateItemBuilder) AddCondition(cnd expression.ConditionBuilder) *UpdateItemBuilder {
	bld.cnd.And(cnd)
	return bld
}

// SetAttributeUpdates sets the AttributeUpdates field's value.
func (bld *UpdateItemBuilder) SetAttributeUpdates(v map[string]ddbTypes.AttributeValueUpdate) *UpdateItemBuilder {
	bld.AttributeUpdates = v
	return bld
}

// SetConditionExpression sets the ConditionExpression field's value.
func (bld *UpdateItemBuilder) SetConditionExpression(v string) *UpdateItemBuilder {
	bld.ConditionExpression = &v
	return bld
}

// SetConditionalOperator sets the ConditionalOperator field's value.
func (bld *UpdateItemBuilder) SetConditionalOperator(v ddbTypes.ConditionalOperator) *UpdateItemBuilder {
	bld.ConditionalOperator = v
	return bld
}

// SetExpected sets the Expected field's value.
func (bld *UpdateItemBuilder) SetExpected(v map[string]ddbTypes.ExpectedAttributeValue) *UpdateItemBuilder {
	bld.Expected = v
	return bld
}

// SetExpressionAttributeNames sets the ExpressionAttributeNames field's value.
func (bld *UpdateItemBuilder) SetExpressionAttributeNames(v map[string]string) *UpdateItemBuilder {
	bld.ExpressionAttributeNames = v
	return bld
}

// SetExpressionAttributeValues sets the ExpressionAttributeValues field's value.
func (bld *UpdateItemBuilder) SetExpressionAttributeValues(v map[string]ddbTypes.AttributeValue) *UpdateItemBuilder {
	bld.ExpressionAttributeValues = v
	return bld
}

// SetKey sets the Key field's value.
func (bld *UpdateItemBuilder) SetKey(v map[string]ddbTypes.AttributeValue) *UpdateItemBuilder {
	bld.Key = v
	return bld
}

// SetReturnConsumedCapacity sets the ReturnConsumedCapacity field's value.
func (bld *UpdateItemBuilder) SetReturnConsumedCapacity(v ddbTypes.ReturnConsumedCapacity) *UpdateItemBuilder {
	bld.ReturnConsumedCapacity = v
	return bld
}

// SetReturnItemCollectionMetrics sets the ReturnItemCollectionMetrics field's value.
func (bld *UpdateItemBuilder) SetReturnItemCollectionMetrics(v ddbTypes.ReturnItemCollectionMetrics) *UpdateItemBuilder {
	bld.ReturnItemCollectionMetrics = v
	return bld
}

// SetReturnValues sets the ReturnValues field's value.
func (bld *UpdateItemBuilder) SetReturnValues(v ddbTypes.ReturnValue) *UpdateItemBuilder {
	bld.ReturnValues = v
	return bld
}

// SetTableName sets the TableName field's value.
func (bld *UpdateItemBuilder) SetTableName(v string) *UpdateItemBuilder {
	bld.TableName = &v
	return bld
}

// SetUpdateExpression sets the UpdateExpression field's value.
func (bld *UpdateItemBuilder) SetUpdateExpression(v string) *UpdateItemBuilder {
	bld.UpdateExpression = &v
	return bld
}

// Build builds the dynamodb.UpdateItemInput
func (bld *UpdateItemBuilder) Build() (*ddb.UpdateItemInput, error) {
	expr := expression.NewBuilder().WithUpdate(bld.updateBuilder)
	if !bld.cnd.Empty() {
		expr.WithCondition(bld.cnd.Builder())
	}

	b, err := expr.Build()

	if err != nil {
		return nil, fmt.Errorf("UpdateItemBuilder.GetDynamoGlobalSecondaryIndex() failed while attempting to build expression: %v", err)
	}

	bld.ConditionExpression = b.Condition()
	bld.ExpressionAttributeNames = b.Names()
	bld.ExpressionAttributeValues = b.Values()
	bld.UpdateExpression = b.Update()

	return bld.UpdateItemInput, nil
}
