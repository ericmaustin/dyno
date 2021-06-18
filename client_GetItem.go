package dyno

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// NewGetItem creates a new GetItem with this Client
func (c *Client) NewGetItem(input *ddb.GetItemInput, optFns ...func(*GetItemOptions)) *GetItem {
	return NewGetItem(c.ddb, input, optFns...)
}

// GetItem executes a scan api call with a GetItemInput
func (c *Client) GetItem(ctx context.Context, input *ddb.GetItemInput, optFns ...func(*GetItemOptions)) (*ddb.GetItemOutput, error) {
	scan := c.NewGetItem(input, optFns...)
	scan.DynoInvoke(ctx)
	return scan.Await()
}

// GetItemInputCallback is a callback that is called on a given GetItemInput before a GetItem operation api call executes
type GetItemInputCallback interface {
	GetItemInputCallback(context.Context, *ddb.GetItemInput) (*ddb.GetItemOutput, error)
}

// GetItemOutputCallback is a callback that is called on a given GetItemOutput after a GetItem operation api call executes
type GetItemOutputCallback interface {
	GetItemOutputCallback(context.Context, *ddb.GetItemOutput) error
}

// GetItemInputCallbackFunc is GetItemOutputCallback function
type GetItemInputCallbackFunc func(context.Context, *ddb.GetItemInput) (*ddb.GetItemOutput, error)

// GetItemInputCallback implements the GetItemOutputCallback interface
func (cb GetItemInputCallbackFunc) GetItemInputCallback(ctx context.Context, input *ddb.GetItemInput) (*ddb.GetItemOutput, error) {
	return cb(ctx, input)
}

// GetItemOutputCallbackFunc is GetItemOutputCallback function
type GetItemOutputCallbackFunc func(context.Context, *ddb.GetItemOutput) error

// GetItemOutputCallback implements the GetItemOutputCallback interface
func (cb GetItemOutputCallbackFunc) GetItemOutputCallback(ctx context.Context, input *ddb.GetItemOutput) error {
	return cb(ctx, input)
}

// GetItemOptions represents options passed to the GetItem operation
type GetItemOptions struct {
	//InputCallbacks are called before the GetItem dynamodb api operation with the dynamodb.GetItemInput
	InputCallbacks []GetItemInputCallback
	//OutputCallbacks are called after the GetItem dynamodb api operation with the dynamodb.GetItemOutput
	OutputCallbacks []GetItemOutputCallback
}

// GetItemWithInputCallback adds a GetItemInputCallbackFunc to the InputCallbacks
func GetItemWithInputCallback(cb GetItemInputCallbackFunc) func(*GetItemOptions) {
	return func(opt *GetItemOptions) {
		opt.InputCallbacks = append(opt.InputCallbacks, cb)
	}
}

// GetItemWithOutputCallback adds a GetItemOutputCallback to the OutputCallbacks
func GetItemWithOutputCallback(cb GetItemOutputCallback) func(*GetItemOptions) {
	return func(opt *GetItemOptions) {
		opt.OutputCallbacks = append(opt.OutputCallbacks, cb)
	}
}

// GetItem represents a GetItem operation
type GetItem struct {
	*Promise
	client  *ddb.Client
	input   *ddb.GetItemInput
	options GetItemOptions
}

// NewGetItem creates a new GetItem operation on the given client with a given GetItemInput and options
func NewGetItem(client *ddb.Client, input *ddb.GetItemInput, optFns ...func(*GetItemOptions)) *GetItem {
	opts := GetItemOptions{}
	for _, opt := range optFns {
		opt(&opts)
	}
	return &GetItem{
		Promise: NewPromise(),
		client:  client,
		input:   input,
		options: opts,
	}
}

// Await waits for the Operation to be complete and then returns a GetItemOutput and error
func (op *GetItem) Await() (*ddb.GetItemOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}
	return out.(*ddb.GetItemOutput), err
}

// Invoke invokes the GetItem operation
func (op *GetItem) Invoke(ctx context.Context) *GetItem {
	go op.DynoInvoke(ctx)
	return op
}

// DynoInvoke implements the Operation interface
func (op *GetItem) DynoInvoke(ctx context.Context) {
	var (
		out *ddb.GetItemOutput
		err error
	)
	defer op.SetResponse(out, err)
	for _, cb := range op.options.InputCallbacks {
		if out, err = cb.GetItemInputCallback(ctx, op.input); out != nil || err != nil {
			return
		}
	}
	if out, err = op.client.GetItem(ctx, op.input); err != nil {
		return
	}
	for _, cb := range op.options.OutputCallbacks {
		if err = cb.GetItemOutputCallback(ctx, out); err != nil {
			return
		}
	}
	return
}

// GetItemBuilder is used to dynamically build a GetItemInput request
type GetItemBuilder struct {
	*ddb.GetItemInput
	projection *expression.ProjectionBuilder
}

// NewGetItemInput creates a new GetItemInput with a table name and key
func NewGetItemInput(tableName *string, key map[string]ddbTypes.AttributeValue) *ddb.GetItemInput {
	return &ddb.GetItemInput{
		Key:                    key,
		TableName:              tableName,
		ReturnConsumedCapacity: ddbTypes.ReturnConsumedCapacityNone,
	}
}

// NewGetItemBuilder returns a new GetItemBuilder for given tableName if tableName is not nil
func NewGetItemBuilder(input *ddb.GetItemInput) *GetItemBuilder {
	if input != nil {
		return &GetItemBuilder{GetItemInput: input}
	}
	return &GetItemBuilder{GetItemInput: NewGetItemInput(nil, nil)}
}

// SetInput sets the GetItemBuilder's dynamodb.GetItemInput
func (bld *GetItemBuilder) SetInput(input *ddb.GetItemInput) *GetItemBuilder {
	bld.GetItemInput = input
	return bld
}

// AddProjectionNames adds additional field names to the projection with strings
func (bld *GetItemBuilder) AddProjectionNames(names ...string) *GetItemBuilder {
	addProjectionNames(bld.projection, names)
	return bld
}

// SetConsistentRead sets the ConsistentRead field's value.
func (bld *GetItemBuilder) SetConsistentRead(v bool) *GetItemBuilder {
	bld.ConsistentRead = &v
	return bld
}

// SetExpressionAttributeNames sets the ExpressionAttributeNames field's value.
func (bld *GetItemBuilder) SetExpressionAttributeNames(v map[string]string) *GetItemBuilder {
	bld.ExpressionAttributeNames = v
	return bld
}

// SetKey sets the Key field's value.
func (bld *GetItemBuilder) SetKey(v map[string]ddbTypes.AttributeValue) *GetItemBuilder {
	bld.Key = v
	return bld
}

// SetProjectionExpression sets the ProjectionExpression field's value.
func (bld *GetItemBuilder) SetProjectionExpression(v string) *GetItemBuilder {
	bld.ProjectionExpression = &v
	return bld
}

// SetReturnConsumedCapacity sets the ReturnConsumedCapacity field's value.
func (bld *GetItemBuilder) SetReturnConsumedCapacity(v ddbTypes.ReturnConsumedCapacity) *GetItemBuilder {
	bld.ReturnConsumedCapacity = v
	return bld
}

// SetTableName sets the TableName field's value.
func (bld *GetItemBuilder) SetTableName(v string) *GetItemBuilder {
	bld.TableName = &v
	return bld
}

// Build returns a dynamodb.GetItemInput
func (bld *GetItemBuilder) Build() (*ddb.GetItemInput, error) {
	if bld.projection != nil {
		// only use expression builder if we have a projection or a filter
		eb := expression.NewBuilder()
		eb = eb.WithProjection(*bld.projection)

		// build the Expression
		expr, err := eb.Build()
		if err != nil {
			return nil, fmt.Errorf("GetItemBuilder Build() failed while attempting to build expression: %v", err)
		}
		bld.ExpressionAttributeNames = expr.Names()
		bld.ProjectionExpression = expr.Projection()
	}
	return bld.GetItemInput, nil
}
