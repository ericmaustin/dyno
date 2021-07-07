package dyno

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ericmaustin/dyno/condition"
	"sync"
)

// Query executes Query operation and returns a QueryPromise
func (c *Client) Query(ctx context.Context, input *ddb.QueryInput, mw ...QueryMiddleWare) *Query {
	return NewQuery(input, mw...).Invoke(ctx, c.ddb)
}

// Query executes a Query operation with a QueryInput in this pool and returns the QueryPromise
func (p *Pool) Query(input *ddb.QueryInput, mw ...QueryMiddleWare) *Query {
	op := NewQuery(input, mw...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// QueryAll executes QueryAll operation and returns a QueryAllPromise
func (c *Client) QueryAll(ctx context.Context, input *ddb.QueryInput, mw ...QueryAllMiddleWare) *QueryAll {
	return NewQueryAll(input, mw...).Invoke(ctx, c.ddb)
}

// QueryAll executes a QueryAll operation with a QueryInput in this pool and returns the QueryAllPromise
func (p *Pool) QueryAll(input *ddb.QueryInput, mw ...QueryAllMiddleWare) *QueryAll {
	op := NewQueryAll(input, mw...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// QueryContext represents an exhaustive Query operation request context
type QueryContext struct {
	context.Context
	Input  *ddb.QueryInput
	Client *ddb.Client
}

// QueryOutput represents the output for the Query opration
type QueryOutput struct {
	out *ddb.QueryOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *QueryOutput) Set(out *ddb.QueryOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *QueryOutput) Get() (out *ddb.QueryOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()
	return
}

// QueryHandler represents a handler for Query requests
type QueryHandler interface {
	HandleQuery(ctx *QueryContext, output *QueryOutput)
}

// QueryHandlerFunc is a QueryHandler function
type QueryHandlerFunc func(ctx *QueryContext, output *QueryOutput)

// HandleQuery implements QueryHandler
func (h QueryHandlerFunc) HandleQuery(ctx *QueryContext, output *QueryOutput) {
	h(ctx, output)
}

// QueryFinalHandler is the final QueryHandler that executes a dynamodb Query operation
type QueryFinalHandler struct{}

// HandleQuery implements the QueryHandler
func (h *QueryFinalHandler) HandleQuery(ctx *QueryContext, output *QueryOutput) {
	output.Set(ctx.Client.Query(ctx, ctx.Input))
}

// QueryMiddleWare is a middleware function use for wrapping QueryHandler requests
type QueryMiddleWare interface {
	QueryMiddleWare(next QueryHandler) QueryHandler
}

// QueryMiddleWareFunc is a functional QueryMiddleWare
type QueryMiddleWareFunc func(next QueryHandler) QueryHandler

// QueryMiddleWare implements the QueryMiddleWare interface
func (mw QueryMiddleWareFunc) QueryMiddleWare(next QueryHandler) QueryHandler {
	return mw(next)
}

// Query represents a Query operation
type Query struct {
	*Promise
	input       *ddb.QueryInput
	middleWares []QueryMiddleWare
}

// NewQuery creates a new Query
func NewQuery(input *ddb.QueryInput, mws ...QueryMiddleWare) *Query {
	return &Query{
		Promise:     NewPromise(),
		input:       input,
		middleWares: mws,
	}
}

// Invoke invokes the Query operation and returns a QueryPromise
func (op *Query) Invoke(ctx context.Context, client *ddb.Client) *Query {
	go op.DynoInvoke(ctx, client)

	return op
}

// DynoInvoke implements the Operation interface
func (op *Query) DynoInvoke(ctx context.Context, client *ddb.Client) {

	output := new(QueryOutput)

	defer func() { op.SetResponse(output.Get()) }()

	requestCtx := &QueryContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	var h QueryHandler

	h = new(QueryFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].QueryMiddleWare(h)
		}
	}

	h.HandleQuery(requestCtx, output)
}

// Await waits for the QueryPromise to be fulfilled and then returns a QueryOutput and error
func (op *Query) Await() (*ddb.QueryOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.QueryOutput), err
}

// QueryAllContext represents an exhaustive QueryAll operation request context
type QueryAllContext struct {
	context.Context
	Input  *ddb.QueryInput
	Client *ddb.Client
}

// QueryAllOutput represents the output for the QueryAll opration
type QueryAllOutput struct {
	out []*ddb.QueryOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *QueryAllOutput) Set(out []*ddb.QueryOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *QueryAllOutput) Get() (out []*ddb.QueryOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()
	return
}

// QueryAllHandler represents a handler for QueryAll requests
type QueryAllHandler interface {
	HandleQueryAll(ctx *QueryAllContext, output *QueryAllOutput)
}

// QueryAllHandlerFunc is a QueryAllHandler function
type QueryAllHandlerFunc func(ctx *QueryAllContext, output *QueryAllOutput)

// HandleQueryAll implements QueryAllHandler
func (h QueryAllHandlerFunc) HandleQueryAll(ctx *QueryAllContext, output *QueryAllOutput) {
	h(ctx, output)
}

// QueryAllMiddleWare is a middleware function use for wrapping QueryAllHandler requests
type QueryAllMiddleWare interface {
	QueryAllMiddleWare(next QueryAllHandler) QueryAllHandler
}

// QueryAllMiddleWareFunc is a functional QueryAllMiddleWare
type QueryAllMiddleWareFunc func(next QueryAllHandler) QueryAllHandler

// QueryAllMiddleWare implements the QueryAllMiddleWare interface
func (mw QueryAllMiddleWareFunc) QueryAllMiddleWare(next QueryAllHandler) QueryAllHandler {
	return mw(next)
}

// QueryAllFinalHandler is the final QueryAllHandler that executes a dynamodb QueryAll operation
type QueryAllFinalHandler struct{}

// HandleQueryAll implements the QueryAllHandler
func (h *QueryAllFinalHandler) HandleQueryAll(ctx *QueryAllContext, output *QueryAllOutput) {
	var (
		outs []*ddb.QueryOutput
		out  *ddb.QueryOutput
		err  error
	)

	defer func() { output.Set(outs, err) }()

	// copy the scan so we're not mutating the original
	input := CopyQuery(ctx.Input)

	for {

		if out, err = ctx.Client.Query(ctx, input); err != nil {
			return
		}

		outs = append(outs, out)

		if out.LastEvaluatedKey == nil || len(out.LastEvaluatedKey) == 0 {
			// no more work
			break
		}

		input.ExclusiveStartKey = out.LastEvaluatedKey
	}
}

// QueryAll represents a QueryAll operation
type QueryAll struct {
	*Promise
	input       *ddb.QueryInput
	middleWares []QueryAllMiddleWare
}

// NewQueryAll creates a new QueryAll
func NewQueryAll(input *ddb.QueryInput, mws ...QueryAllMiddleWare) *QueryAll {
	return &QueryAll{
		Promise:     NewPromise(),
		input:       input,
		middleWares: mws,
	}
}

// Invoke invokes the QueryAll operation and returns a QueryAllPromise
func (op *QueryAll) Invoke(ctx context.Context, client *ddb.Client) *QueryAll {
	go op.DynoInvoke(ctx, client)

	return op
}

// DynoInvoke the Operation interface
func (op *QueryAll) DynoInvoke(ctx context.Context, client *ddb.Client) {
	output := new(QueryAllOutput)

	defer func() { op.SetResponse(output.Get()) }()

	requestCtx := &QueryAllContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	var h QueryAllHandler

	h = new(QueryAllFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].QueryAllMiddleWare(h)
		}
	}

	h.HandleQueryAll(requestCtx, output)
}

// Await waits for the QueryAllPromise to be fulfilled and then returns a QueryAllOutput and error
func (op *QueryAll) Await() ([]*ddb.QueryOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.([]*ddb.QueryOutput), err
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
	keyCnd     condition.KeyConditionBuilder
	filter     condition.Builder
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
	bld.keyCnd.And(cnd)
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
	bld.filter.And(cnd)
	return bld
}

// AddProjection additional fields to the projection
func (bld *QueryBuilder) AddProjection(names interface{}) *QueryBuilder {
	addProjection(&bld.projection, names)
	return bld
}

// AddProjectionNames adds additional field names to the projection
func (bld *QueryBuilder) AddProjectionNames(names ...string) *QueryBuilder {
	addProjectionNames(&bld.projection, names)
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
	if bld.projection == nil && bld.keyCnd.Empty() && bld.filter.Empty() {
		// no expression builder is needed
		return bld.QueryInput, nil
	}
	builder := expression.NewBuilder()

	// add projection
	if bld.projection != nil {
		builder = builder.WithProjection(*bld.projection)
		bld.Select = ddbTypes.SelectSpecificAttributes
	}

	// add key condition
	if !bld.keyCnd.Empty() {
		builder = builder.WithKeyCondition(bld.keyCnd.Builder())
	}

	// add filter
	if !bld.filter.Empty() {
		builder = builder.WithFilter(bld.filter.Builder())
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
