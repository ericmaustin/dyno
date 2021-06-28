package dyno

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// BatchGetItem executes BatchGetItem operation and returns a BatchGetItemPromise
func (c *Client) BatchGetItem(ctx context.Context, input *ddb.BatchGetItemInput, mw ...BatchGetItemMiddleWare) *BatchGetItemPromise {
	return NewBatchGetItem(input, mw...).Invoke(ctx, c.ddb)
}

// BatchGetItem executes a BatchGetItem operation with a BatchGetItemInput in this pool and returns the BatchGetItemPromise
func (p *Pool) BatchGetItem(input *ddb.BatchGetItemInput, mw ...BatchGetItemMiddleWare) *BatchGetItemPromise {
	op := NewBatchGetItem(input, mw...)

	if err := p.Do(op); err != nil {
		op.promise.SetResponse(nil, err)
	}

	return op.promise
}

// BatchGetItemAll executes BatchGetItemAll operation and returns a BatchGetItemAllPromise
func (c *Client) BatchGetItemAll(ctx context.Context, input *ddb.BatchGetItemInput, mw ...BatchGetItemAllMiddleWare) *BatchGetItemAllPromise {
	return NewBatchGetItemAll(input, mw...).Invoke(ctx, c.ddb)
}

// BatchGetItemAll executes a BatchGetItemAll operation with a BatchGetItemInput in this pool and returns the BatchGetItemAllPromise
func (p *Pool) BatchGetItemAll(input *ddb.BatchGetItemInput, mw ...BatchGetItemAllMiddleWare) *BatchGetItemAllPromise {
	op := NewBatchGetItemAll(input, mw...)

	if err := p.Do(op); err != nil {
		op.promise.SetResponse(nil, err)
	}

	return op.promise
}

// BatchGetItemContext represents an exhaustive BatchGetItem operation request context
type BatchGetItemContext struct {
	context.Context
	input  *ddb.BatchGetItemInput
	client *ddb.Client
}

// BatchGetItemPromise represents a promise for the BatchGetItem
type BatchGetItemPromise struct {
	*Promise
}

// GetResponse returns the GetResponse output and error
// if Output has not been set yet nil is returned
func (p *BatchGetItemPromise) GetResponse() (*ddb.BatchGetItemOutput, error) {
	out, err := p.Promise.GetResponse()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.BatchGetItemOutput), err
}

// Await waits for the BatchGetItemPromise to be fulfilled and then returns a BatchGetItemOutput and error
func (p *BatchGetItemPromise) Await() (*ddb.BatchGetItemOutput, error) {
	out, err := p.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.BatchGetItemOutput), err
}

// newBatchGetItemPromise returns a new BatchGetItemPromise
func newBatchGetItemPromise() *BatchGetItemPromise {
	return &BatchGetItemPromise{NewPromise()}
}

// BatchGetItemHandler represents a handler for BatchGetItem requests
type BatchGetItemHandler interface {
	HandleBatchGetItem(ctx *BatchGetItemContext, promise *BatchGetItemPromise)
}

// BatchGetItemHandlerFunc is a BatchGetItemHandler function
type BatchGetItemHandlerFunc func(ctx *BatchGetItemContext, promise *BatchGetItemPromise)

// HandleBatchGetItem implements BatchGetItemHandler
func (h BatchGetItemHandlerFunc) HandleBatchGetItem(ctx *BatchGetItemContext, promise *BatchGetItemPromise) {
	h(ctx, promise)
}

// BatchGetItemMiddleWare is a middleware function use for wrapping BatchGetItemHandler requests
type BatchGetItemMiddleWare interface {
	BatchGetItemMiddleWare(h BatchGetItemHandler) BatchGetItemHandler
}

// BatchGetItemMiddleWareFunc is a functional BatchGetItemMiddleWare
type BatchGetItemMiddleWareFunc func(handler BatchGetItemHandler) BatchGetItemHandler

// BatchGetItemMiddleWare implements the BatchGetItemMiddleWare interface
func (mw BatchGetItemMiddleWareFunc) BatchGetItemMiddleWare(h BatchGetItemHandler) BatchGetItemHandler {
	return mw(h)
}

// BatchGetItemFinalHandler returns the final BatchGetItemHandler that executes a dynamodb BatchGetItem operation
func BatchGetItemFinalHandler() BatchGetItemHandler {
	return BatchGetItemHandlerFunc(func(ctx *BatchGetItemContext, promise *BatchGetItemPromise) {
		promise.SetResponse(ctx.client.BatchGetItem(ctx, ctx.input))
	})
}

// BatchGetItem represents a BatchGetItem operation
type BatchGetItem struct {
	promise     *BatchGetItemPromise
	input       *ddb.BatchGetItemInput
	middleWares []BatchGetItemMiddleWare
}

// NewBatchGetItem creates a new BatchGetItem
func NewBatchGetItem(input *ddb.BatchGetItemInput, mws ...BatchGetItemMiddleWare) *BatchGetItem {
	return &BatchGetItem{
		input:       input,
		middleWares: mws,
		promise:     newBatchGetItemPromise(),
	}
}

// Invoke invokes the BatchGetItem operation and returns a BatchGetItemPromise
func (op *BatchGetItem) Invoke(ctx context.Context, client *ddb.Client) *BatchGetItemPromise {
	go op.DynoInvoke(ctx, client)

	return op.promise
}

// DynoInvoke implements the Operation interface
func (op *BatchGetItem) DynoInvoke(ctx context.Context, client *ddb.Client) {

	requestCtx := &BatchGetItemContext{
		Context: ctx,
		client:  client,
		input:   op.input,
	}

	h := BatchGetItemFinalHandler()

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].BatchGetItemMiddleWare(h)
		}
	}

	h.HandleBatchGetItem(requestCtx, op.promise)
}

// BatchGetItemAllContext represents an exhaustive BatchGetItemAll operation request context
type BatchGetItemAllContext struct {
	context.Context
	input  *ddb.BatchGetItemInput
	client *ddb.Client
}

// BatchGetItemAllPromise represents a promise for the BatchGetItemAll
type BatchGetItemAllPromise struct {
	*Promise
}

// GetResponse returns the GetResponse output and error
// if Output has not been set yet nil is returned
func (p *BatchGetItemAllPromise) GetResponse() ([]*ddb.BatchGetItemOutput, error) {
	out, err := p.Promise.GetResponse()
	if out == nil {
		return nil, err
	}

	return out.([]*ddb.BatchGetItemOutput), err
}

// Await waits for the BatchGetItemAllPromise to be fulfilled and then returns a BatchGetItemAllOutput and error
func (p *BatchGetItemAllPromise) Await() ([]*ddb.BatchGetItemOutput, error) {
	out, err := p.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.([]*ddb.BatchGetItemOutput), err
}

// newBatchGetItemAllPromise returns a new BatchGetItemAllPromise
func newBatchGetItemAllPromise() *BatchGetItemAllPromise {
	return &BatchGetItemAllPromise{NewPromise()}
}

// BatchGetItemAllHandler represents a handler for BatchGetItemAll requests
type BatchGetItemAllHandler interface {
	HandleBatchGetItemAll(ctx *BatchGetItemAllContext, promise *BatchGetItemAllPromise)
}

// BatchGetItemAllHandlerFunc is a BatchGetItemAllHandler function
type BatchGetItemAllHandlerFunc func(ctx *BatchGetItemAllContext, promise *BatchGetItemAllPromise)

// HandleBatchGetItemAll implements BatchGetItemAllHandler
func (h BatchGetItemAllHandlerFunc) HandleBatchGetItemAll(ctx *BatchGetItemAllContext, promise *BatchGetItemAllPromise) {
	h(ctx, promise)
}

// BatchGetItemAllMiddleWare is a middleware function use for wrapping BatchGetItemAllHandler requests
type BatchGetItemAllMiddleWare interface {
	BatchGetItemAllMiddleWare(h BatchGetItemAllHandler) BatchGetItemAllHandler
}

// BatchGetItemAllMiddleWareFunc is a functional BatchGetItemAllMiddleWare
type BatchGetItemAllMiddleWareFunc func(handler BatchGetItemAllHandler) BatchGetItemAllHandler

// BatchGetItemAllMiddleWare implements the BatchGetItemAllMiddleWare interface
func (mw BatchGetItemAllMiddleWareFunc) BatchGetItemAllMiddleWare(h BatchGetItemAllHandler) BatchGetItemAllHandler {
	return mw(h)
}

// BatchGetItemAllFinalHandler returns the final BatchGetItemAllHandler that executes a dynamodb BatchGetItemAll operation
func BatchGetItemAllFinalHandler() BatchGetItemAllHandler {
	return BatchGetItemAllHandlerFunc(func(ctx *BatchGetItemAllContext, promise *BatchGetItemAllPromise) {
		var (
			outs []*ddb.BatchGetItemOutput
			out  *ddb.BatchGetItemOutput
			err  error
		)

		defer func() { promise.SetResponse(outs, err) }()

		// copy the scan so we're not mutating the original
		input := CopyBatchGetItem(ctx.input)

		for {
			if out, err = ctx.client.BatchGetItem(ctx, input); err != nil {
				return
			}

			outs = append(outs, out)

			if out.UnprocessedKeys == nil {
				// no more work
				break
			}

			input.RequestItems = out.UnprocessedKeys
		}
	})
}

// BatchGetItemAll represents a BatchGetItemAll operation
type BatchGetItemAll struct {
	promise     *BatchGetItemAllPromise
	input       *ddb.BatchGetItemInput
	middleWares []BatchGetItemAllMiddleWare
}

// NewBatchGetItemAll creates a new BatchGetItemAll
func NewBatchGetItemAll(input *ddb.BatchGetItemInput, mws ...BatchGetItemAllMiddleWare) *BatchGetItemAll {
	return &BatchGetItemAll{
		input:       input,
		middleWares: mws,
		promise:     newBatchGetItemAllPromise(),
	}
}

// Invoke invokes the BatchGetItemAll operation and returns a BatchGetItemAllPromise
func (op *BatchGetItemAll) Invoke(ctx context.Context, client *ddb.Client) *BatchGetItemAllPromise {
	go op.DynoInvoke(ctx, client)

	return op.promise
}

// DynoInvoke the Operation interface
func (op *BatchGetItemAll) DynoInvoke(ctx context.Context, client *ddb.Client) {
	requestCtx := &BatchGetItemAllContext{
		Context: ctx,
		client:  client,
		input:   op.input,
	}

	h := BatchGetItemAllFinalHandler()

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].BatchGetItemAllMiddleWare(h)
		}
	}

	h.HandleBatchGetItemAll(requestCtx, op.promise)
}

func NewBatchGetItemInput() *ddb.BatchGetItemInput {
	return &ddb.BatchGetItemInput{
		RequestItems:           make(map[string]ddbTypes.KeysAndAttributes),
		ReturnConsumedCapacity: ddbTypes.ReturnConsumedCapacityNone,
	}
}

// BatchGetItemBuilder used to dynamically build a BatchGetItemBuilder
type BatchGetItemBuilder struct {
	*ddb.BatchGetItemInput
	projection *expression.ProjectionBuilder
}

// NewBatchGetBuilder creates a new BatchGetItemBuilder
func NewBatchGetBuilder(input *ddb.BatchGetItemInput) *BatchGetItemBuilder {
	if input != nil {
		return &BatchGetItemBuilder{BatchGetItemInput: input}
	}

	return &BatchGetItemBuilder{BatchGetItemInput: NewBatchGetItemInput()}
}

// SetInput sets the BatchGetItemBuilder's dynamodb.BatchGetItemBuilder explicitly
func (bld *BatchGetItemBuilder) SetInput(input *ddb.BatchGetItemInput) {
	bld.BatchGetItemInput = input
}

func (bld *BatchGetItemBuilder) initTable(tableName string) {
	if _, ok := bld.RequestItems[tableName]; !ok {
		bld.BatchGetItemInput.RequestItems[tableName] = ddbTypes.KeysAndAttributes{}
	}
}

// AddProjection adds additional field names to the projection
func (bld *BatchGetItemBuilder) AddProjection(projection interface{}) *BatchGetItemBuilder {
	addProjection(bld.projection, projection)
	return bld
}

// AddProjectionNames adds additional field names to the projection with strings
func (bld *BatchGetItemBuilder) AddProjectionNames(names ...string) *BatchGetItemBuilder {
	addProjectionNames(bld.projection, names)
	return bld
}

// SetKeysAndAttributes sets the keys and attributes to get from the given table
func (bld *BatchGetItemBuilder) SetKeysAndAttributes(tableName string, keysAndAttributes ddbTypes.KeysAndAttributes) *BatchGetItemBuilder {
	bld.RequestItems[tableName] = keysAndAttributes
	return bld
}

// AddKey adds one or more keys to the request item map
func (bld *BatchGetItemBuilder) AddKey(tableName string, keys ...map[string]ddbTypes.AttributeValue) *BatchGetItemBuilder {
	bld.initTable(tableName)
	existingKeysAndAttributes := bld.RequestItems[tableName]
	existingKeysAndAttributes.Keys = append(existingKeysAndAttributes.Keys, keys...)
	bld.RequestItems[tableName] = existingKeysAndAttributes

	return bld
}

// SetRequestItems sets the RequestItems field's value.
func (bld *BatchGetItemBuilder) SetRequestItems(v map[string]ddbTypes.KeysAndAttributes) *BatchGetItemBuilder {
	bld.RequestItems = v
	return bld
}

// SetReturnConsumedCapacity sets the ReturnConsumedCapacity field's value.
func (bld *BatchGetItemBuilder) SetReturnConsumedCapacity(v ddbTypes.ReturnConsumedCapacity) *BatchGetItemBuilder {
	bld.ReturnConsumedCapacity = v
	return bld
}

// Build builds and returns the dynamodb.BatchGetItemOutput
func (bld *BatchGetItemBuilder) Build() (*ddb.BatchGetItemInput, error) {
	// remove all inputs that don't have any keys associated
	for tableName, keys := range bld.RequestItems {
		if keys.Keys == nil || len(keys.Keys) < 1 {
			delete(bld.RequestItems, tableName)
		}
	}

	if bld.projection != nil {
		// only use expression builder if we have a projection or a filter
		eb := expression.NewBuilder()
		eb = eb.WithProjection(*bld.projection)

		// build the Expression
		expr, err := eb.Build()
		if err != nil {
			return nil, fmt.Errorf("BatchGetItemBuilder.Build() encountered an error while building an expression: %v", err)
		}

		exprNames := expr.Names()
		exprProj := expr.Projection()

		for _, item := range bld.RequestItems {
			item.ExpressionAttributeNames = exprNames
			item.ProjectionExpression = exprProj
		}
	}

	return bld.BatchGetItemInput, nil
}

// CopyBatchGetItem creates a deep copy of a BatchGetItemInput
func CopyBatchGetItem(input *ddb.BatchGetItemInput) *ddb.BatchGetItemInput {
	clone := &ddb.BatchGetItemInput{
		ReturnConsumedCapacity: input.ReturnConsumedCapacity,
	}

	if input.RequestItems != nil {
		input.RequestItems = CopyKeysAndAttributesMap(input.RequestItems)
	}

	return clone
}

// ChunkBatchGetItemInputs chunks the input dynamodb.BatchGetItemBuilder into chunks of the given chunkSize
// note: chunks are not deep copies!!
func ChunkBatchGetItemInputs(input *ddb.BatchGetItemInput, chunkSize int) (out []*ddb.BatchGetItemInput) {
	for tableName, keysAndAttributes := range input.RequestItems {
		sliceOfKeys := make([][]map[string]ddbTypes.AttributeValue, 0)

		for i := 0; i < len(keysAndAttributes.Keys); i += chunkSize {
			end := i + chunkSize
			if end > len(keysAndAttributes.Keys) {
				end = len(keysAndAttributes.Keys)
			}

			sliceOfKeys = append(sliceOfKeys, keysAndAttributes.Keys[i:end])
		}

		for _, slice := range sliceOfKeys {
			newInput := CopyBatchGetItem(input)
			newInput.RequestItems = map[string]ddbTypes.KeysAndAttributes{
				tableName: {
					AttributesToGet:          keysAndAttributes.AttributesToGet,
					ConsistentRead:           keysAndAttributes.ConsistentRead,
					ExpressionAttributeNames: keysAndAttributes.ExpressionAttributeNames,
					Keys:                     slice,
					ProjectionExpression:     keysAndAttributes.ProjectionExpression,
				},
			}
			out = append(out, newInput)
		}
	}

	return
}
