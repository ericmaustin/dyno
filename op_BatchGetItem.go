package dyno

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"sync"
)

// BatchGetItem executes BatchGetItem operation and returns a BatchGetItem operation
func (s *Session) BatchGetItem(ctx context.Context, input *ddb.BatchGetItemInput, mw ...BatchGetItemMiddleWare) *BatchGetItem {
	return NewBatchGetItem(input, mw...).Invoke(ctx, s.ddb)
}

// BatchGetItem executes a BatchGetItem operation with a BatchGetItemInput in this pool and returns the BatchGetItem operation
func (p *Pool) BatchGetItem(input *ddb.BatchGetItemInput, mw ...BatchGetItemMiddleWare) *BatchGetItem {
	op := NewBatchGetItem(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// BatchGetItemAll executes BatchGetItem operation and returns a BatchGetItem operation
func (s *Session) BatchGetItemAll(ctx context.Context, input *ddb.BatchGetItemInput, mw ...BatchGetItemMiddleWare) *BatchGetItem {
	return NewBatchGetItemAll(input, mw...).Invoke(ctx, s.ddb)
}

// BatchGetItemAll executes a BatchGetItem operation with a BatchGetItemInput in this pool and returns the BatchGetItem operation
func (p *Pool) BatchGetItemAll(input *ddb.BatchGetItemInput, mw ...BatchGetItemMiddleWare) *BatchGetItem {
	op := NewBatchGetItemAll(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// BatchGetItemContext represents an exhaustive BatchGetItem operation request context
type BatchGetItemContext struct {
	context.Context
	Input  *ddb.BatchGetItemInput
	Client *ddb.Client
}

// BatchGetItemOutput represents the output for the BatchGetItem opration
type BatchGetItemOutput struct {
	out *ddb.BatchGetItemOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *BatchGetItemOutput) Set(out *ddb.BatchGetItemOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *BatchGetItemOutput) Get() (out *ddb.BatchGetItemOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()
	return
}

// BatchGetItemHandler represents a handler for BatchGetItem requests
type BatchGetItemHandler interface {
	HandleBatchGetItem(ctx *BatchGetItemContext, output *BatchGetItemOutput)
}

// BatchGetItemHandlerFunc is a BatchGetItemHandler function
type BatchGetItemHandlerFunc func(ctx *BatchGetItemContext, output *BatchGetItemOutput)

// HandleBatchGetItem implements BatchGetItemHandler
func (h BatchGetItemHandlerFunc) HandleBatchGetItem(ctx *BatchGetItemContext, output *BatchGetItemOutput) {
	h(ctx, output)
}

// BatchGetItemFinalHandler is the final BatchGetItemHandler that executes a dynamodb BatchGetItem operation
type BatchGetItemFinalHandler struct{}

// HandleBatchGetItem implements the BatchGetItemHandler
func (h *BatchGetItemFinalHandler) HandleBatchGetItem(ctx *BatchGetItemContext, output *BatchGetItemOutput) {
	output.Set(ctx.Client.BatchGetItem(ctx, ctx.Input))
}

// BatchGetItemAllFinalHandler is the final BatchGetItemAllHandler that executes a dynamodb BatchGetItemAll operation
type BatchGetItemAllFinalHandler struct{}

// HandleBatchGetItem implements the BatchGetItemHandler
func (h *BatchGetItemAllFinalHandler) HandleBatchGetItem(ctx *BatchGetItemContext, output *BatchGetItemOutput) {
	var (
		out, finalOut  *ddb.BatchGetItemOutput
		err  error
	)

	finalOut = &ddb.BatchGetItemOutput{
		Responses: make(map[string][]map[string]ddbTypes.AttributeValue),
		UnprocessedKeys: make(map[string]ddbTypes.KeysAndAttributes),
	}

	defer func() { output.Set(finalOut, err) }()

	// copy the scan so we're not mutating the original
	input := CopyBatchGetItem(ctx.Input)

	for {

		if out, err = ctx.Client.BatchGetItem(ctx, input); err != nil {
			return
		}

		finalOut.ConsumedCapacity = append(finalOut.ConsumedCapacity, out.ConsumedCapacity...)

		for k, avs := range out.Responses {
			if _, ok := finalOut.Responses[k]; !ok {
				finalOut.Responses[k] = []map[string]ddbTypes.AttributeValue{}
			}

			for _, av := range avs {
				finalOut.Responses[k] = append(finalOut.Responses[k], CopyAttributeValueMap(av))
			}
		}

		if out.UnprocessedKeys == nil || len(out.UnprocessedKeys) == 0 {
			// no more work
			return
		}

		input.RequestItems = CopyKeysAndAttributesMap(out.UnprocessedKeys)
	}
}

// BatchGetItemMiddleWare is a middleware function use for wrapping BatchGetItemHandler requests
type BatchGetItemMiddleWare interface {
	BatchGetItemMiddleWare(next BatchGetItemHandler) BatchGetItemHandler
}

// BatchGetItemMiddleWareFunc is a functional BatchGetItemMiddleWare
type BatchGetItemMiddleWareFunc func(next BatchGetItemHandler) BatchGetItemHandler

// BatchGetItemMiddleWare implements the BatchGetItemMiddleWare interface
func (mw BatchGetItemMiddleWareFunc) BatchGetItemMiddleWare(next BatchGetItemHandler) BatchGetItemHandler {
	return mw(next)
}

// BatchGetItem represents a BatchGetItem operation
type BatchGetItem struct {
	*BaseOperation
	Handler     BatchGetItemHandler
	input       *ddb.BatchGetItemInput
	middleWares []BatchGetItemMiddleWare
}

// NewBatchGetItem creates a new BatchGetItem
func NewBatchGetItem(input *ddb.BatchGetItemInput, mws ...BatchGetItemMiddleWare) *BatchGetItem {
	return &BatchGetItem{
		BaseOperation: NewOperation(),
		Handler:       new(BatchGetItemFinalHandler),
		input:         input,
		middleWares:   mws,
	}
}

// NewBatchGetItemAll creates a new BatchGetItemAll
func NewBatchGetItemAll(input *ddb.BatchGetItemInput, mws ...BatchGetItemMiddleWare) *BatchGetItem {
	return &BatchGetItem{
		BaseOperation: NewOperation(),
		Handler:       new(BatchGetItemAllFinalHandler),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the BatchGetItem operation in a goroutine and returns a BatchGetItemAllPromise
func (op *BatchGetItem) Invoke(ctx context.Context, client *ddb.Client) *BatchGetItem {
	op.SetRunning() // promise now waiting for a response

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the BatchGetItem operation
func (op *BatchGetItem) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	output := new(BatchGetItemOutput)

	defer func() { op.SetResponse(output.Get()) }()

	var h BatchGetItemHandler

	h = op.Handler

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].BatchGetItemMiddleWare(h)
		}
	}

	requestCtx := &BatchGetItemContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	h.HandleBatchGetItem(requestCtx, output)
}

// Await waits for the BatchGetItemPromise to be fulfilled and then returns a BatchGetItemOutput and error
func (op *BatchGetItem) Await() (*ddb.BatchGetItemOutput, error) {
	out, err := op.BaseOperation.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.BatchGetItemOutput), err
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
	addProjection(&bld.projection, projection)
	return bld
}

// AddProjectionNames adds additional field names to the projection with strings
func (bld *BatchGetItemBuilder) AddProjectionNames(names ...string) *BatchGetItemBuilder {
	addProjectionNames(&bld.projection, names)
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
