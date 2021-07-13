package dyno

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"sync"
)

// GetItem executes GetItem operation and returns a GetItem operation
func (s *Session) GetItem(input *ddb.GetItemInput, mw ...GetItemMiddleWare) *GetItem {
	return NewGetItem(input, mw...).Invoke(s.ctx, s.ddb)
}

// GetItem executes a GetItem operation with a GetItemInput in this pool and returns the GetItem operation
func (p *Pool) GetItem(input *ddb.GetItemInput, mw ...GetItemMiddleWare) *GetItem {
	op := NewGetItem(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// GetItemContext represents an exhaustive GetItem operation request context
type GetItemContext struct {
	context.Context
	Input  *ddb.GetItemInput
	Client *ddb.Client
}

// GetItemOutput represents the output for the GetItem operation
type GetItemOutput struct {
	out *ddb.GetItemOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *GetItemOutput) Set(out *ddb.GetItemOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *GetItemOutput) Get() (out *ddb.GetItemOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()

	return
}

// GetItemHandler represents a handler for GetItem requests
type GetItemHandler interface {
	HandleGetItem(ctx *GetItemContext, output *GetItemOutput)
}

// GetItemHandlerFunc is a GetItemHandler function
type GetItemHandlerFunc func(ctx *GetItemContext, output *GetItemOutput)

// HandleGetItem implements GetItemHandler
func (h GetItemHandlerFunc) HandleGetItem(ctx *GetItemContext, output *GetItemOutput) {
	h(ctx, output)
}

// GetItemFinalHandler is the final GetItemHandler that executes a dynamodb GetItem operation
type GetItemFinalHandler struct{}

// HandleGetItem implements the GetItemHandler
func (h *GetItemFinalHandler) HandleGetItem(ctx *GetItemContext, output *GetItemOutput) {
	output.Set(ctx.Client.GetItem(ctx, ctx.Input))
}

// GetItemMiddleWare is a middleware function use for wrapping GetItemHandler requests
type GetItemMiddleWare interface {
	GetItemMiddleWare(next GetItemHandler) GetItemHandler
}

// GetItemMiddleWareFunc is a functional GetItemMiddleWare
type GetItemMiddleWareFunc func(next GetItemHandler) GetItemHandler

// GetItemMiddleWare implements the GetItemMiddleWare interface
func (mw GetItemMiddleWareFunc) GetItemMiddleWare(next GetItemHandler) GetItemHandler {
	return mw(next)
}

// GetItem represents a GetItem operation
type GetItem struct {
	*BaseOperation
	input       *ddb.GetItemInput
	middleWares []GetItemMiddleWare
}

// NewGetItem creates a new GetItem operation
func NewGetItem(input *ddb.GetItemInput, mws ...GetItemMiddleWare) *GetItem {
	return &GetItem{
		BaseOperation: NewOperation(),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the GetItem operation in a goroutine and returns a GetItem operation
func (op *GetItem) Invoke(ctx context.Context, client *ddb.Client) *GetItem {
	op.SetRunning() // operation now waiting for a response

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the GetItem operation
func (op *GetItem) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	output := new(GetItemOutput)

	defer func() { op.SetResponse(output.Get()) }()

	var h GetItemHandler

	h = new(GetItemFinalHandler)

	// loop in reverse to preserve middleware order
	for i := len(op.middleWares) - 1; i >= 0; i-- {
		h = op.middleWares[i].GetItemMiddleWare(h)
	}

	requestCtx := &GetItemContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	h.HandleGetItem(requestCtx, output)
}

// Await waits for the GetItem operation to be fulfilled and then returns a GetItemOutput and error
func (op *GetItem) Await() (*ddb.GetItemOutput, error) {
	out, err := op.BaseOperation.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.GetItemOutput), err
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

// AddProjection additional fields to the projection
func (bld *GetItemBuilder) AddProjection(names interface{}) *GetItemBuilder {
	addProjection(&bld.projection, names)
	return bld
}

// AddProjectionNames adds additional field names to the projection with strings
func (bld *GetItemBuilder) AddProjectionNames(names ...string) *GetItemBuilder {
	addProjectionNames(&bld.projection, names)
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
			return nil, fmt.Errorf("GetItemBuilder GetDynamoGlobalSecondaryIndex() failed while attempting to build expression: %v", err)
		}

		bld.ExpressionAttributeNames = expr.Names()
		bld.ProjectionExpression = expr.Projection()
	}

	return bld.GetItemInput, nil
}
