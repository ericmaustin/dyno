package dyno

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ericmaustin/dyno/condition"
)

// NewPutItem creates a new PutItem with this Client
func (c *Client) NewPutItem(input *ddb.PutItemInput, optFns ...func(*PutItemOptions)) *PutItem {
	return NewPutItem(c.ddb, input, optFns...)
}

// PutItem executes a scan api call with a PutItemInput
func (c *Client) PutItem(ctx context.Context, input *ddb.PutItemInput, optFns ...func(*PutItemOptions)) (*ddb.PutItemOutput, error) {
	scan := c.NewPutItem(input, optFns...)
	scan.DynoInvoke(ctx)
	return scan.Await()
}

// PutItemInputCallback is a callback that is called on a given PutItemInput before a PutItem operation api call executes
type PutItemInputCallback interface {
	PutItemInputCallback(context.Context, *ddb.PutItemInput) (*ddb.PutItemOutput, error)
}

// PutItemOutputCallback is a callback that is called on a given PutItemOutput after a PutItem operation api call executes
type PutItemOutputCallback interface {
	PutItemOutputCallback(context.Context, *ddb.PutItemOutput) error
}

// PutItemInputCallbackFunc is PutItemOutputCallback function
type PutItemInputCallbackFunc func(context.Context, *ddb.PutItemInput) (*ddb.PutItemOutput, error)

// PutItemInputCallback implements the PutItemOutputCallback interface
func (cb PutItemInputCallbackFunc) PutItemInputCallback(ctx context.Context, input *ddb.PutItemInput) (*ddb.PutItemOutput, error) {
	return cb(ctx, input)
}

// PutItemOutputCallbackFunc is PutItemOutputCallback function
type PutItemOutputCallbackFunc func(context.Context, *ddb.PutItemOutput) error

// PutItemOutputCallback implements the PutItemOutputCallback interface
func (cb PutItemOutputCallbackFunc) PutItemOutputCallback(ctx context.Context, input *ddb.PutItemOutput) error {
	return cb(ctx, input)
}

// PutItemOptions represents options passed to the PutItem operation
type PutItemOptions struct {
	//InputCallbacks are called before the PutItem dynamodb api operation with the dynamodb.PutItemInput
	InputCallbacks []PutItemInputCallback
	//OutputCallbacks are called after the PutItem dynamodb api operation with the dynamodb.PutItemOutput
	OutputCallbacks []PutItemOutputCallback
}

// PutItemWithInputCallback adds a PutItemInputCallbackFunc to the InputCallbacks
func PutItemWithInputCallback(cb PutItemInputCallbackFunc) func(*PutItemOptions) {
	return func(opt *PutItemOptions) {
		opt.InputCallbacks = append(opt.InputCallbacks, cb)
	}
}

// PutItemWithOutputCallback adds a PutItemOutputCallback to the OutputCallbacks
func PutItemWithOutputCallback(cb PutItemOutputCallback) func(*PutItemOptions) {
	return func(opt *PutItemOptions) {
		opt.OutputCallbacks = append(opt.OutputCallbacks, cb)
	}
}

// PutItem represents a PutItem operation
type PutItem struct {
	*Promise
	client  *ddb.Client
	input   *ddb.PutItemInput
	options PutItemOptions
}

// NewPutItem creates a new PutItem operation on the given client with a given PutItemInput and options
func NewPutItem(client *ddb.Client, input *ddb.PutItemInput, optFns ...func(*PutItemOptions)) *PutItem {
	opts := PutItemOptions{}
	for _, opt := range optFns {
		opt(&opts)
	}
	return &PutItem{
		Promise: NewPromise(),
		client:  client,
		input:   input,
		options: opts,
	}
}

// Await waits for the Operation to be complete and then returns a PutItemOutput and error
func (op *PutItem) Await() (*ddb.PutItemOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}
	return out.(*ddb.PutItemOutput), err
}

// Invoke invokes the PutItem operation
func (op *PutItem) Invoke(ctx context.Context) *PutItem {
	go op.DynoInvoke(ctx)
	return op
}

// DynoInvoke implements the Operation interface
func (op *PutItem) DynoInvoke(ctx context.Context) {
	var (
		out *ddb.PutItemOutput
		err error
	)
	defer op.SetResponse(out, err)
	for _, cb := range op.options.InputCallbacks {
		if out, err = cb.PutItemInputCallback(ctx, op.input); out != nil || err != nil {
			return
		}
	}
	if out, err = op.client.PutItem(ctx, op.input); err != nil {
		return
	}
	for _, cb := range op.options.OutputCallbacks {
		if err = cb.PutItemOutputCallback(ctx, out); err != nil {
			return
		}
	}
	return
}

// PutItemBuilder allows for dynamic building of a PutItem input
type PutItemBuilder struct {
	*ddb.PutItemInput
	cnd *condition.Builder
}

// NewPutItemBuilder creates a new PutItemBuilder with PutItemOpt
func NewPutItemBuilder(input *ddb.PutItemInput) *PutItemBuilder {
	if input != nil {
		return &PutItemBuilder{PutItemInput: input}
	}
	return &PutItemBuilder{PutItemInput: NewPutItemInput(nil, nil)}
}

// SetItem shadows dynamodb.PutItemInput and sets the item that will be used to build the put input
func (bld *PutItemBuilder) SetItem(item map[string]ddbTypes.AttributeValue) *PutItemBuilder {
	bld.Item = item
	return bld
}

// AddCondition adds a condition to this put
// adding multiple conditions by calling this multiple times will join the conditions with
// an AND
func (bld *PutItemBuilder) AddCondition(cnd expression.ConditionBuilder) *PutItemBuilder {
	bld.cnd.And(cnd)
	return bld
}

// SetConditionExpression sets the ConditionExpression field's value.
func (bld *PutItemBuilder) SetConditionExpression(v string) *PutItemBuilder {
	bld.ConditionExpression = &v
	return bld
}

// SetConditionalOperator sets the ConditionalOperator field's value.
func (bld *PutItemBuilder) SetConditionalOperator(v ddbTypes.ConditionalOperator) *PutItemBuilder {
	bld.ConditionalOperator = v
	return bld
}

// SetExpected sets the Expected field's value.
func (bld *PutItemBuilder) SetExpected(v map[string]ddbTypes.ExpectedAttributeValue) *PutItemBuilder {
	bld.Expected = v
	return bld
}

// SetExpressionAttributeNames sets the ExpressionAttributeNames field's value.
func (bld *PutItemBuilder) SetExpressionAttributeNames(v map[string]string) *PutItemBuilder {
	bld.ExpressionAttributeNames = v
	return bld
}

// SetExpressionAttributeValues sets the ExpressionAttributeValues field's value.
func (bld *PutItemBuilder) SetExpressionAttributeValues(v map[string]ddbTypes.AttributeValue) *PutItemBuilder {
	bld.ExpressionAttributeValues = v
	return bld
}

// SetReturnConsumedCapacity sets the ReturnConsumedCapacity field's value.
func (bld *PutItemBuilder) SetReturnConsumedCapacity(v ddbTypes.ReturnConsumedCapacity) *PutItemBuilder {
	bld.ReturnConsumedCapacity = v
	return bld
}

// SetReturnItemCollectionMetrics sets the ReturnItemCollectionMetrics field's value.
func (bld *PutItemBuilder) SetReturnItemCollectionMetrics(v ddbTypes.ReturnItemCollectionMetrics) *PutItemBuilder {
	bld.ReturnItemCollectionMetrics = v
	return bld
}

// SetReturnValues sets the ReturnValues field's value.
func (bld *PutItemBuilder) SetReturnValues(v ddbTypes.ReturnValue) *PutItemBuilder {
	bld.ReturnValues = v
	return bld
}

// SetTableName sets the TableName field's value.
func (bld *PutItemBuilder) SetTableName(v string) *PutItemBuilder {
	bld.TableName = &v
	return bld
}

// Build builds the PutItemInput
func (bld *PutItemBuilder) Build() (*ddb.PutItemInput, error) {
	if !bld.cnd.Empty() {
		// build the Expression
		b, err := bld.cnd.AddToExpression(expression.NewBuilder()).Build()
		if err != nil {
			return nil, err
		}
		bld.ConditionExpression = b.Condition()
		bld.ExpressionAttributeNames = b.Names()
		bld.ExpressionAttributeValues = b.Values()
	}
	return bld.PutItemInput, nil
}

// NewPutItemInput creates a new PutItemInput with a given table name and item
func NewPutItemInput(tableName *string, item map[string]ddbTypes.AttributeValue) *ddb.PutItemInput {
	return &ddb.PutItemInput{
		Item:                        item,
		TableName:                   tableName,
		ReturnConsumedCapacity:      ddbTypes.ReturnConsumedCapacityNone,
		ReturnItemCollectionMetrics: ddbTypes.ReturnItemCollectionMetricsNone,
		ReturnValues:                ddbTypes.ReturnValueNone,
	}
}
