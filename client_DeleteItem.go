package dyno

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ericmaustin/dyno/condition"
)


// DeleteItem executes a scan api call with a DeleteItemInput
func (c *Client) DeleteItem(ctx context.Context, input *ddb.DeleteItemInput, optFns ...func(*DeleteItemOptions)) (*ddb.DeleteItemOutput, error) {
	op := NewDeleteItem(input, optFns...)
	op.DynoInvoke(ctx, c.ddb)
	
	return op.Await()
}

// DeleteItem executes a DeleteItem operation with a DeleteItemInput in this pool and returns the DeleteItem for processing
func (p *Pool) DeleteItem(input *ddb.DeleteItemInput, optFns ...func(*DeleteItemOptions)) *DeleteItem {
	op := NewDeleteItem(input, optFns...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// DeleteItemInputCallback is a callback that is called on a given DeleteItemInput before a DeleteItem operation api call executes
type DeleteItemInputCallback interface {
	DeleteItemInputCallback(context.Context, *ddb.DeleteItemInput) (*ddb.DeleteItemOutput, error)
}

// DeleteItemOutputCallback is a callback that is called on a given DeleteItemOutput after a DeleteItem operation api call executes
type DeleteItemOutputCallback interface {
	DeleteItemOutputCallback(context.Context, *ddb.DeleteItemOutput) error
}

// DeleteItemInputCallbackFunc is DeleteItemOutputCallback function
type DeleteItemInputCallbackFunc func(context.Context, *ddb.DeleteItemInput) (*ddb.DeleteItemOutput, error)

// DeleteItemInputCallback implements the DeleteItemOutputCallback interface
func (cb DeleteItemInputCallbackFunc) DeleteItemInputCallback(ctx context.Context, input *ddb.DeleteItemInput) (*ddb.DeleteItemOutput, error) {
	return cb(ctx, input)
}

// DeleteItemOutputCallbackFunc is DeleteItemOutputCallback function
type DeleteItemOutputCallbackFunc func(context.Context, *ddb.DeleteItemOutput) error

// DeleteItemOutputCallback implements the DeleteItemOutputCallback interface
func (cb DeleteItemOutputCallbackFunc) DeleteItemOutputCallback(ctx context.Context, input *ddb.DeleteItemOutput) error {
	return cb(ctx, input)
}

// DeleteItemOptions represents options passed to the DeleteItem operation
type DeleteItemOptions struct {
	// InputCallbacks are called before the DeleteItem dynamodb api operation with the dynamodb.DeleteItemInput
	InputCallbacks []DeleteItemInputCallback
	// OutputCallbacks are called after the DeleteItem dynamodb api operation with the dynamodb.DeleteItemOutput
	OutputCallbacks []DeleteItemOutputCallback
}

// DeleteItemWithInputCallback adds a DeleteItemInputCallbackFunc to the InputCallbacks
func DeleteItemWithInputCallback(cb DeleteItemInputCallbackFunc) func(*DeleteItemOptions) {
	return func(opt *DeleteItemOptions) {
		opt.InputCallbacks = append(opt.InputCallbacks, cb)
	}
}

// DeleteItemWithOutputCallback adds a DeleteItemOutputCallback to the OutputCallbacks
func DeleteItemWithOutputCallback(cb DeleteItemOutputCallback) func(*DeleteItemOptions) {
	return func(opt *DeleteItemOptions) {
		opt.OutputCallbacks = append(opt.OutputCallbacks, cb)
	}
}

// DeleteItem represents a DeleteItem operation
type DeleteItem struct {
	*Promise
	input   *ddb.DeleteItemInput
	options DeleteItemOptions
}

// NewDeleteItem creates a new DeleteItem operation on the given client with a given DeleteItemInput and options
func NewDeleteItem(input *ddb.DeleteItemInput, optFns ...func(*DeleteItemOptions)) *DeleteItem {
	opts := DeleteItemOptions{}

	for _, opt := range optFns {
		opt(&opts)
	}

	return &DeleteItem{
		Promise: NewPromise(),
		input:   input,
		options: opts,
	}
}

// Await waits for the Operation to be complete and then returns a DeleteItemOutput and error
func (op *DeleteItem) Await() (*ddb.DeleteItemOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}
	
	return out.(*ddb.DeleteItemOutput), err
}

// Invoke invokes the DeleteItem operation
func (op *DeleteItem) Invoke(ctx context.Context, client *ddb.Client) *DeleteItem {
	go op.DynoInvoke(ctx, client)
	return op
}

// DynoInvoke implements the Operation interface
func (op *DeleteItem) DynoInvoke(ctx context.Context, client *ddb.Client) {
	var (
		out *ddb.DeleteItemOutput
		err error
	)

	defer func() { op.SetResponse(out, err) }()

	for _, cb := range op.options.InputCallbacks {
		if out, err = cb.DeleteItemInputCallback(ctx, op.input); out != nil || err != nil {
			return
		}
	}

	if out, err = client.DeleteItem(ctx, op.input); err != nil {
		return
	}

	for _, cb := range op.options.OutputCallbacks {
		if err = cb.DeleteItemOutputCallback(ctx, out); err != nil {
			return
		}
	}
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
