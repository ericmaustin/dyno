package dyno

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ericmaustin/dyno/condition"
	"sync"
)

// PutItem executes PutItem operation and returns it
func (c *Client) PutItem(ctx context.Context, input *ddb.PutItemInput, mw ...PutItemMiddleWare) *PutItem {
	return NewPutItem(input, mw...).Invoke(ctx, c.ddb)
}

// PutItem executes a PutItem operation with a PutItemInput in this pool and returns it
func (p *Pool) PutItem(input *ddb.PutItemInput, mw ...PutItemMiddleWare) *PutItem {
	op := NewPutItem(input, mw...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// PutItemContext represents an exhaustive PutItem operation request context
type PutItemContext struct {
	context.Context
	Input  *ddb.PutItemInput
	Client *ddb.Client
}

// PutItemOutput represents the output for the PutItem opration
type PutItemOutput struct {
	out *ddb.PutItemOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *PutItemOutput) Set(out *ddb.PutItemOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *PutItemOutput) Get() (out *ddb.PutItemOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()
	return
}


// PutItemHandler represents a handler for PutItem requests
type PutItemHandler interface {
	HandlePutItem(ctx *PutItemContext, output *PutItemOutput)
}

// PutItemHandlerFunc is a PutItemHandler function
type PutItemHandlerFunc func(ctx *PutItemContext, output *PutItemOutput)

// HandlePutItem implements PutItemHandler
func (h PutItemHandlerFunc) HandlePutItem(ctx *PutItemContext, output *PutItemOutput) {
	h(ctx, output)
}

// PutItemFinalHandler is the final PutItemHandler that executes a dynamodb PutItem operation
type PutItemFinalHandler struct{}

// HandlePutItem implements the PutItemHandler
func (h *PutItemFinalHandler) HandlePutItem(ctx *PutItemContext, output *PutItemOutput) {
	output.Set(ctx.Client.PutItem(ctx, ctx.Input))
}

// PutItemMiddleWare is a middleware function use for wrapping PutItemHandler requests
type PutItemMiddleWare interface {
	PutItemMiddleWare(next PutItemHandler) PutItemHandler
}

// PutItemMiddleWareFunc is a functional PutItemMiddleWare
type PutItemMiddleWareFunc func(next PutItemHandler) PutItemHandler

// PutItemMiddleWare implements the PutItemMiddleWare interface
func (mw PutItemMiddleWareFunc) PutItemMiddleWare(next PutItemHandler) PutItemHandler {
	return mw(next)
}

// PutItem represents a PutItem operation
type PutItem struct {
	*Promise
	input       *ddb.PutItemInput
	middleWares []PutItemMiddleWare
}

// NewPutItem creates a new PutItem
func NewPutItem(input *ddb.PutItemInput, mws ...PutItemMiddleWare) *PutItem {
	return &PutItem{
		Promise:    NewPromise(),
		input:       input,
		middleWares: mws,
	}
}

// Invoke invokes the PutItem operation and returns a PutItemPromise
func (op *PutItem) Invoke(ctx context.Context, client *ddb.Client) *PutItem {
	go op.DynoInvoke(ctx, client)

	return op
}

// DynoInvoke implements the Operation interface
func (op *PutItem) DynoInvoke(ctx context.Context, client *ddb.Client) {

	output := new(PutItemOutput)

	defer func() { op.SetResponse(output.Get()) }()

	requestCtx := &PutItemContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	var h PutItemHandler

	h = new(PutItemFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].PutItemMiddleWare(h)
		}
	}

	h.HandlePutItem(requestCtx, output)
}

// Await waits for the PutItemPromise to be fulfilled and then returns a PutItemOutput and error
func (op *PutItem) Await() (*ddb.PutItemOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.PutItemOutput), err
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
