package dyno

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ericmaustin/dyno/condition"
	"github.com/ericmaustin/dyno/encoding"
)

// Query executes a scan api call with a QueryInput with this DefaultClient
func (c *DefaultClient) Query(ctx context.Context, input *ddb.QueryInput, optFns ...func(*QueryOptions)) (*ddb.QueryOutput, error) {
	op := NewQuery(input, optFns...)
	op.DynoInvoke(ctx, c.ddb)

	return op.Await()
}

// QueryAll executes an exhaustive api call with a QueryInput
// that will keep running Query sequentially until LastEvaluatedKey is empty
func (c *DefaultClient) QueryAll(ctx context.Context, input *ddb.QueryInput, optFns ...func(*QueryOptions)) ([]*ddb.QueryOutput, error) {
	scan := NewQueryAll(input, optFns...)
	scan.DynoInvoke(ctx, c.ddb)

	return scan.Await()
}

// QueryInputCallback is a callback that is called on a given QueryInput before a Query operation api call executes
type QueryInputCallback interface {
	QueryInputCallback(context.Context, *ddb.QueryInput) (*ddb.QueryOutput, error)
}

// QueryOutputCallback is a callback that is called on a given QueryOutput after a Query operation api call executes
type QueryOutputCallback interface {
	QueryOutputCallback(context.Context, *ddb.QueryOutput) error
}

// QueryInputCallbackFunc is QueryOutputCallback function
type QueryInputCallbackFunc func(context.Context, *ddb.QueryInput) (*ddb.QueryOutput, error)

// QueryInputCallback implements the QueryOutputCallback interface
func (cb QueryInputCallbackFunc) QueryInputCallback(ctx context.Context, input *ddb.QueryInput) (*ddb.QueryOutput, error) {
	return cb(ctx, input)
}

// QueryOutputCallbackFunc is QueryOutputCallback function
type QueryOutputCallbackFunc func(context.Context, *ddb.QueryOutput) error

// QueryOutputCallback implements the QueryOutputCallback interface
func (cb QueryOutputCallbackFunc) QueryOutputCallback(ctx context.Context, input *ddb.QueryOutput) error {
	return cb(ctx, input)
}

// QueryOptions represents options passed to the Query operation
type QueryOptions struct {
	// InputCallbacks are called before the Query dynamodb api operation with the dynamodb.QueryInput
	InputCallbacks []QueryInputCallback
	// OutputCallbacks are called after the Query dynamodb api operation with the dynamodb.QueryOutput
	OutputCallbacks []QueryOutputCallback
}

// QueryWithInputCallback adds a QueryInputCallbackFunc to the InputCallbacks
func QueryWithInputCallback(cb QueryInputCallbackFunc) func(*QueryOptions) {
	return func(opt *QueryOptions) {
		opt.InputCallbacks = append(opt.InputCallbacks, cb)
	}
}

// QueryWithOutputCallback adds a QueryOutputCallback to the OutputCallbacks
func QueryWithOutputCallback(cb QueryOutputCallback) func(*QueryOptions) {
	return func(opt *QueryOptions) {
		opt.OutputCallbacks = append(opt.OutputCallbacks, cb)
	}
}

// Query represents a Query operation
type Query struct {
	*Promise
	input   *ddb.QueryInput
	options QueryOptions
}

// NewQuery creates a new Query operation on the given client with a given QueryInput and options
func NewQuery(input *ddb.QueryInput, optFns ...func(*QueryOptions)) *Query {
	opts := QueryOptions{}

	for _, opt := range optFns {
		opt(&opts)
	}

	return &Query{
		//client:  nil,
		Promise: NewPromise(),
		input:   input,
		options: opts,
	}
}

// Await waits for the Operation to be complete and then returns a QueryOutput and error
func (op *Query) Await() (*ddb.QueryOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.QueryOutput), err
}

// Invoke invokes the Query operation
func (op *Query) Invoke(ctx context.Context, client *ddb.Client) *Query {
	go op.DynoInvoke(ctx, client)

	return op
}

// DynoInvoke implements the Operation interface
func (op *Query) DynoInvoke(ctx context.Context, client *ddb.Client) {
	var (
		out *ddb.QueryOutput
		err error
	)

	defer func() { op.SetResponse(out, err) }()

	for _, cb := range op.options.InputCallbacks {
		if out, err = cb.QueryInputCallback(ctx, op.input); out != nil || err != nil {
			return
		}
	}

	if out, err = client.Query(ctx, op.input); err != nil {
		return
	}

	for _, cb := range op.options.OutputCallbacks {
		if err = cb.QueryOutputCallback(ctx, out); err != nil {
			return
		}
	}
}

// QueryAll represents an exhaustive Query operation
type QueryAll struct {
	*Promise
	input   *ddb.QueryInput
	options QueryOptions
}

// NewQueryAll creates a new QueryAll operation on the given client with a given QueryInput and options
func NewQueryAll(input *ddb.QueryInput, optFns ...func(*QueryOptions)) *QueryAll {
	options := QueryOptions{}
	for _, opt := range optFns {
		opt(&options)
	}

	return &QueryAll{
		//client:  nil,
		Promise: NewPromise(),
		input:   input,
		options: options,
	}
}

// Await waits for the Operation to be complete and then returns a QueryOutput and error
func (op *QueryAll) Await() ([]*ddb.QueryOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}
	return out.([]*ddb.QueryOutput), err
}

// Invoke invokes the Query operation
func (op *QueryAll) Invoke(ctx context.Context, client *ddb.Client) *QueryAll {
	go op.DynoInvoke(ctx, client)
	return op
}

// DynoInvoke implements the Operation interface
func (op *QueryAll) DynoInvoke(ctx context.Context, client *ddb.Client) {
	var (
		outs []*ddb.QueryOutput
		out  *ddb.QueryOutput
		err  error
	)

	defer func() { op.SetResponse(outs, err) }()

	//copy the scan so we're not mutating the original
	input := CopyQuery(op.input)

	for {
		for _, cb := range op.options.InputCallbacks {
			if out, err = cb.QueryInputCallback(ctx, input); out != nil || err != nil {
				if out != nil {
					outs = append(outs, out)
				}
				return
			}
		}

		if out, err = client.Query(ctx, input); err != nil {
			return
		}

		for _, cb := range op.options.OutputCallbacks {
			if err = cb.QueryOutputCallback(ctx, out); err != nil {
				return
			}
		}

		outs = append(outs, out)

		if out.LastEvaluatedKey == nil {
			// no more work
			break
		}

		input.ExclusiveStartKey = out.LastEvaluatedKey
	}
}

// NewQueryInput creates a new QueryInput with a table name
func NewQueryInput(tableName *string) *ddb.QueryInput {
	return &ddb.QueryInput{
		TableName:              tableName,
		ReturnConsumedCapacity: ddbTypes.ReturnConsumedCapacityNone,
		Select:                 ddbTypes.SelectAllAttributes,
	}
}

// QueryBuilder dynamically constructs a QueryInput
type QueryBuilder struct {
	*ddb.QueryInput
	keyCnd     *expression.KeyConditionBuilder
	filter     *expression.ConditionBuilder
	projection *expression.ProjectionBuilder
}

// NewQueryBuilder creates a new QueryBuilder builder with QueryOpt
func NewQueryBuilder(input *ddb.QueryInput) *QueryBuilder {
	if input != nil {
		return &QueryBuilder{QueryInput: input}
	}
	return &QueryBuilder{QueryInput: NewQueryInput(nil)}
}

// SetAscOrder sets the query to return in ascending order
func (bld *QueryBuilder) SetAscOrder() *QueryBuilder {
	b := true
	bld.ScanIndexForward = &b

	return bld
}

// SetDescOrder sets the query to return in descending order
func (bld *QueryBuilder) SetDescOrder() *QueryBuilder {
	b := false
	bld.ScanIndexForward = &b

	return bld
}

// AddKeyCondition adds a key condition to this update
// adding multiple conditions by calling this multiple times will join the conditions with
// an AND
func (bld *QueryBuilder) AddKeyCondition(cnd expression.KeyConditionBuilder) *QueryBuilder {
	if bld.keyCnd == nil {
		bld.keyCnd = &cnd
	} else {
		cnd = condition.KeyAnd(*bld.keyCnd, cnd)
		bld.keyCnd = &cnd
	}

	return bld
}

// AddKeyEquals adds a equality key condition for the given fieldName and value
// this is a shortcut for adding an equality condition which is common for queries
func (bld *QueryBuilder) AddKeyEquals(fieldName string, value interface{}) *QueryBuilder {
	return bld.AddKeyCondition(condition.KeyEqual(fieldName, value))
}

// AddFilter adds a filter condition to this update
// adding multiple conditions by calling this multiple times will join the conditions with
// an AND
func (bld *QueryBuilder) AddFilter(cnd expression.ConditionBuilder) *QueryBuilder {
	if bld.filter == nil {
		bld.filter = &cnd
	} else {
		cnd = condition.And(*bld.filter, cnd)
		bld.filter = &cnd
	}
	return bld
}

// AddProjectionNames adds additional field names to the projection
func (bld *QueryBuilder) AddProjectionNames(names ...string) *QueryBuilder {
	nameBuilders := encoding.NameBuilders(names)

	if bld.projection == nil {
		proj := expression.ProjectionBuilder{}
		proj = proj.AddNames(nameBuilders...)
		bld.projection = &proj
	} else {
		*bld.projection = bld.projection.AddNames(nameBuilders...)
	}

	return bld
}

// SetAttributesToGet sets the AttributesToGet field's value.
func (bld *QueryBuilder) SetAttributesToGet(v []string) *QueryBuilder {
	bld.AttributesToGet = v
	return bld
}

// SetConditionalOperator sets the ConditionalOperator field's value.
func (bld *QueryBuilder) SetConditionalOperator(v ddbTypes.ConditionalOperator) *QueryBuilder {
	bld.ConditionalOperator = v
	return bld
}

// SetConsistentRead sets the ConsistentRead field's value.
func (bld *QueryBuilder) SetConsistentRead(v bool) *QueryBuilder {
	bld.ConsistentRead = &v
	return bld
}

// SetExclusiveStartKey sets the ExclusiveStartKey field's value.
func (bld *QueryBuilder) SetExclusiveStartKey(v map[string]ddbTypes.AttributeValue) *QueryBuilder {
	bld.ExclusiveStartKey = v
	return bld
}

// SetExpressionAttributeNames sets the ExpressionAttributeNames field's value.
func (bld *QueryBuilder) SetExpressionAttributeNames(v map[string]string) *QueryBuilder {
	bld.ExpressionAttributeNames = v
	return bld
}

// SetExpressionAttributeValues sets the ExpressionAttributeValues field's value.
func (bld *QueryBuilder) SetExpressionAttributeValues(v map[string]ddbTypes.AttributeValue) *QueryBuilder {
	bld.ExpressionAttributeValues = v
	return bld
}

// SetFilterExpression sets the FilterExpression field's value.
func (bld *QueryBuilder) SetFilterExpression(v string) *QueryBuilder {
	bld.FilterExpression = &v
	return bld
}

// SetIndexName sets the IndexName field's value.
func (bld *QueryBuilder) SetIndexName(v string) *QueryBuilder {
	bld.IndexName = &v
	return bld
}

// SetKeyConditionExpression sets the KeyConditionExpression field's value.
func (bld *QueryBuilder) SetKeyConditionExpression(v string) *QueryBuilder {
	bld.KeyConditionExpression = &v
	return bld
}

// SetKeyConditions sets the KeyConditions field's value.
func (bld *QueryBuilder) SetKeyConditions(v map[string]ddbTypes.Condition) *QueryBuilder {
	bld.KeyConditions = v
	return bld
}

// SetLimit sets the Limit field's value.
func (bld *QueryBuilder) SetLimit(v int32) *QueryBuilder {
	bld.Limit = &v
	return bld
}

// SetProjectionExpression sets the ProjectionExpression field's value.
func (bld *QueryBuilder) SetProjectionExpression(v string) *QueryBuilder {
	bld.ProjectionExpression = &v
	return bld
}

// SetQueryFilter sets the QueryFilter field's value.
func (bld *QueryBuilder) SetQueryFilter(v map[string]ddbTypes.Condition) *QueryBuilder {
	bld.QueryFilter = v
	return bld
}

// SetReturnConsumedCapacity sets the ReturnConsumedCapacity field's value.
func (bld *QueryBuilder) SetReturnConsumedCapacity(v ddbTypes.ReturnConsumedCapacity) *QueryBuilder {
	bld.ReturnConsumedCapacity = v
	return bld
}

// SetScanIndexForward sets the ScanIndexForward field's value.
func (bld *QueryBuilder) SetScanIndexForward(v bool) *QueryBuilder {
	bld.ScanIndexForward = &v
	return bld
}

// SetSelect sets the Select field's value.
func (bld *QueryBuilder) SetSelect(v ddbTypes.Select) *QueryBuilder {
	bld.Select = v
	return bld
}

// SetTableName sets the TableName field's value.
func (bld *QueryBuilder) SetTableName(v string) *QueryBuilder {
	bld.TableName = &v
	return bld
}

// Build builds the dynamodb.QueryInput
func (bld *QueryBuilder) Build() (*ddb.QueryInput, error) {
	if bld.projection == nil && bld.keyCnd == nil && bld.filter == nil {
		// no expression builder is needed
		return bld.QueryInput, nil
	}
	builder := expression.NewBuilder()

	// add projection
	if bld.projection != nil {
		builder = builder.WithProjection(*bld.projection)
	}

	// add key condition
	if bld.keyCnd != nil {
		builder = builder.WithKeyCondition(*bld.keyCnd)
	}

	// add filter
	if bld.filter != nil {
		builder = builder.WithFilter(*bld.filter)
	}

	// build the Expression
	expr, err := builder.Build()

	if err != nil {
		return nil, err
	}

	bld.ExpressionAttributeNames = expr.Names()
	bld.ExpressionAttributeValues = expr.Values()
	bld.FilterExpression = expr.Filter()
	bld.KeyConditionExpression = expr.KeyCondition()
	bld.ProjectionExpression = expr.Projection()

	return bld.QueryInput, nil
}

// CopyQuery creates a deep copy of a QueryInput
// note: CopyQuery does not copy legacy parameters
func CopyQuery(input *ddb.QueryInput) *ddb.QueryInput {
	clone := &ddb.QueryInput{
		ConditionalOperator:    input.ConditionalOperator,
		ReturnConsumedCapacity: input.ReturnConsumedCapacity,
		Select:                 input.Select,
	}

	if input.TableName != nil {
		clone.TableName = new(string)
		*clone.TableName = *input.TableName
	}

	if input.AttributesToGet != nil {
		copy(clone.AttributesToGet, input.AttributesToGet)
	}

	if input.ConsistentRead != nil {
		clone.ConsistentRead = new(bool)
		*clone.ConsistentRead = *input.ConsistentRead
	}

	if input.ExclusiveStartKey != nil {
		clone.ExclusiveStartKey = make(map[string]ddbTypes.AttributeValue, len(input.ExclusiveStartKey))
		for k, v := range input.ExclusiveStartKey {
			clone.ExclusiveStartKey[k] = CopyAttributeValue(v)
		}
	}

	if input.ExpressionAttributeNames != nil {
		clone.ExpressionAttributeNames = make(map[string]string, len(input.ExpressionAttributeNames))
		for k, v := range input.ExpressionAttributeNames {
			clone.ExpressionAttributeNames[k] = v
		}
	}

	if input.ExpressionAttributeValues != nil {
		clone.ExpressionAttributeValues = make(map[string]ddbTypes.AttributeValue, len(input.ExpressionAttributeValues))
		for k, v := range input.ExpressionAttributeValues {
			clone.ExpressionAttributeValues[k] = CopyAttributeValue(v)
		}
	}

	if input.KeyConditions != nil {
		clone.KeyConditions = make(map[string]ddbTypes.Condition)
		for k, v := range input.KeyConditions {
			clone.KeyConditions[k] = CopyCondition(v)
		}
	}

	if input.KeyConditionExpression != nil {
		clone.KeyConditionExpression = new(string)
		*clone.KeyConditionExpression = *input.KeyConditionExpression
	}

	if input.FilterExpression != nil {
		clone.FilterExpression = new(string)
		*clone.FilterExpression = *input.FilterExpression
	}

	if input.IndexName != nil {
		clone.IndexName = new(string)
		*clone.IndexName = *input.IndexName
	}

	if input.Limit != nil {
		clone.Limit = new(int32)
		*clone.Limit = *input.Limit
	}

	if input.ProjectionExpression != nil {
		clone.ProjectionExpression = new(string)
		*clone.ProjectionExpression = *input.ProjectionExpression
	}

	if input.QueryFilter != nil {
		clone.QueryFilter = make(map[string]ddbTypes.Condition, len(input.QueryFilter))
		for k, v := range input.QueryFilter {
			clone.QueryFilter[k] = CopyCondition(v)
		}
	}

	if input.TableName != nil {
		clone.TableName = new(string)
		*clone.TableName = *input.TableName
	}

	return clone
}
