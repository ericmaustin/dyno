package dyno

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ericmaustin/dyno/condition"
	"github.com/ericmaustin/dyno/encoding"
)

// UpdateItem executes a scan api call with a UpdateItemInput
func (c *DefaultClient) UpdateItem(ctx context.Context, input *ddb.UpdateItemInput, optFns ...func(*UpdateItemOptions)) (*ddb.UpdateItemOutput, error) {
	opt := NewUpdateItem(input, optFns...)
	opt.DynoInvoke(ctx, c.ddb)

	return opt.Await()
}

// UpdateItemInputCallback is a callback that is called on a given UpdateItemInput before a UpdateItem operation api call executes
type UpdateItemInputCallback interface {
	UpdateItemInputCallback(context.Context, *ddb.UpdateItemInput) (*ddb.UpdateItemOutput, error)
}

// UpdateItemOutputCallback is a callback that is called on a given UpdateItemOutput after a UpdateItem operation api call executes
type UpdateItemOutputCallback interface {
	UpdateItemOutputCallback(context.Context, *ddb.UpdateItemOutput) error
}

// UpdateItemInputCallbackFunc is UpdateItemOutputCallback function
type UpdateItemInputCallbackFunc func(context.Context, *ddb.UpdateItemInput) (*ddb.UpdateItemOutput, error)

// UpdateItemInputCallback implements the UpdateItemOutputCallback interface
func (cb UpdateItemInputCallbackFunc) UpdateItemInputCallback(ctx context.Context, input *ddb.UpdateItemInput) (*ddb.UpdateItemOutput, error) {
	return cb(ctx, input)
}

// UpdateItemOutputCallbackFunc is UpdateItemOutputCallback function
type UpdateItemOutputCallbackFunc func(context.Context, *ddb.UpdateItemOutput) error

// UpdateItemOutputCallback implements the UpdateItemOutputCallback interface
func (cb UpdateItemOutputCallbackFunc) UpdateItemOutputCallback(ctx context.Context, input *ddb.UpdateItemOutput) error {
	return cb(ctx, input)
}

// UpdateItemOptions represents options passed to the UpdateItem operation
type UpdateItemOptions struct {
	InputCallbacks  []UpdateItemInputCallback
	OutputCallbacks []UpdateItemOutputCallback
}

// UpdateItemWithInputCallback adds a UpdateItemInputCallbackFunc to the InputCallbacks
func UpdateItemWithInputCallback(cb UpdateItemInputCallbackFunc) func(*UpdateItemOptions) {
	return func(opt *UpdateItemOptions) {
		opt.InputCallbacks = append(opt.InputCallbacks, cb)
	}
}

// UpdateItemWithOutputCallback adds a UpdateItemOutputCallback to the OutputCallbacks
func UpdateItemWithOutputCallback(cb UpdateItemOutputCallback) func(*UpdateItemOptions) {
	return func(opt *UpdateItemOptions) {
		opt.OutputCallbacks = append(opt.OutputCallbacks, cb)
	}
}

// UpdateItem represents a UpdateItem operation
type UpdateItem struct {
	*Promise
	input   *ddb.UpdateItemInput
	options UpdateItemOptions
}

// NewUpdateItem creates a new UpdateItem operation on the given client with a given UpdateItemInput and options
func NewUpdateItem(input *ddb.UpdateItemInput, optFns ...func(*UpdateItemOptions)) *UpdateItem {
	opts := UpdateItemOptions{}

	for _, opt := range optFns {
		opt(&opts)
	}

	return &UpdateItem{
		Promise: NewPromise(),
		input:   input,
		options: opts,
	}
}

// Await waits for the Operation to be complete and then returns a UpdateItemOutput and error
func (op *UpdateItem) Await() (*ddb.UpdateItemOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.UpdateItemOutput), err
}

// Invoke invokes the UpdateItem operation
func (op *UpdateItem) Invoke(ctx context.Context, client *ddb.Client) *UpdateItem {
	go op.DynoInvoke(ctx, client)
	return op
}

// DynoInvoke implements the Operation interface
func (op *UpdateItem) DynoInvoke(ctx context.Context, client *ddb.Client) {
	var (
		out *ddb.UpdateItemOutput
		err error
	)

	defer func() { op.SetResponse(out, err) }()

	for _, cb := range op.options.InputCallbacks {
		if out, err = cb.UpdateItemInputCallback(ctx, op.input); out != nil || err != nil {
			return
		}
	}

	if out, err = client.UpdateItem(ctx, op.input); err != nil {
		return
	}

	for _, cb := range op.options.OutputCallbacks {
		if err = cb.UpdateItemOutputCallback(ctx, out); err != nil {
			return
		}
	}

	return
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
	cnd           *condition.Builder
}

// NewUpdateItemBuilder creates a new UpdateItemBuilder
func NewUpdateItemBuilder(input *ddb.UpdateItemInput) *UpdateItemBuilder {
	if input != nil {
		return &UpdateItemBuilder{UpdateItemInput: input}
	}

	return &UpdateItemBuilder{UpdateItemInput: NewUpdateItemInput(nil)}
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
func (bld *UpdateItemBuilder) SetItem(item map[string]dynamodb.AttributeValue) *UpdateItemBuilder {
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
		return nil, fmt.Errorf("UpdateItemBuilder.Build() failed while attempting to build expression: %v", err)
	}
	bld.ConditionExpression = b.Condition()
	bld.ExpressionAttributeNames = b.Names()
	bld.ExpressionAttributeValues = b.Values()
	bld.UpdateExpression = b.Update()
	return bld.UpdateItemInput, nil
}
