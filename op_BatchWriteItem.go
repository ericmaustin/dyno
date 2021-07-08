package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"sync"
)

// BatchWriteItem executes BatchWriteItem operation and returns a BatchWriteItem operation
func (s *Session) BatchWriteItem(input *ddb.BatchWriteItemInput, mw ...BatchWriteItemMiddleWare) *BatchWriteItem {
	return NewBatchWriteItem(input, mw...).Invoke(s.ctx, s.ddb)
}

// BatchWriteItem executes a BatchWriteItem operation with a BatchWriteItemInput in this pool and returns the BatchWriteItem operation
func (p *Pool) BatchWriteItem(input *ddb.BatchWriteItemInput, mw ...BatchWriteItemMiddleWare) *BatchWriteItem {
	op := NewBatchWriteItem(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// BatchWriteItemAll executes BatchWriteItem operation and returns a BatchWriteItem operation
func (s *Session) BatchWriteItemAll(input *ddb.BatchWriteItemInput, mw ...BatchWriteItemMiddleWare) *BatchWriteItem {
	return NewBatchWriteItemAll(input, mw...).Invoke(s.ctx, s.ddb)
}

// BatchWriteItemAll executes a BatchWriteItem operation with a BatchWriteItemInput in this pool and returns the BatchWriteItem operation
func (p *Pool) BatchWriteItemAll(input *ddb.BatchWriteItemInput, mw ...BatchWriteItemMiddleWare) *BatchWriteItem {
	op := NewBatchWriteItemAll(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// BatchWriteItemContext represents an exhaustive BatchWriteItem operation request context
type BatchWriteItemContext struct {
	context.Context
	Input  *ddb.BatchWriteItemInput
	Client *ddb.Client
}

// BatchWriteItemOutput represents the output for the BatchWriteItem opration
type BatchWriteItemOutput struct {
	out *ddb.BatchWriteItemOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *BatchWriteItemOutput) Set(out *ddb.BatchWriteItemOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *BatchWriteItemOutput) Get() (out *ddb.BatchWriteItemOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()
	return
}

// BatchWriteItemHandler represents a handler for BatchWriteItem requests
type BatchWriteItemHandler interface {
	HandleBatchWriteItem(ctx *BatchWriteItemContext, output *BatchWriteItemOutput)
}

// BatchWriteItemHandlerFunc is a BatchWriteItemHandler function
type BatchWriteItemHandlerFunc func(ctx *BatchWriteItemContext, output *BatchWriteItemOutput)

// HandleBatchWriteItem implements BatchWriteItemHandler
func (h BatchWriteItemHandlerFunc) HandleBatchWriteItem(ctx *BatchWriteItemContext, output *BatchWriteItemOutput) {
	h(ctx, output)
}

// BatchWriteItemFinalHandler is the final BatchWriteItemHandler that executes a dynamodb BatchWriteItem operation
type BatchWriteItemFinalHandler struct{}

// HandleBatchWriteItem implements the BatchWriteItemHandler
func (h *BatchWriteItemFinalHandler) HandleBatchWriteItem(ctx *BatchWriteItemContext, output *BatchWriteItemOutput) {
	output.Set(ctx.Client.BatchWriteItem(ctx, ctx.Input))
}

// BatchWriteItemAllFinalHandler is the final BatchWriteItemAllHandler that executes a dynamodb BatchWriteItemAll operation
type BatchWriteItemAllFinalHandler struct{}

// HandleBatchWriteItem implements the BatchWriteItemHandler
func (h *BatchWriteItemAllFinalHandler) HandleBatchWriteItem(ctx *BatchWriteItemContext, output *BatchWriteItemOutput) {
	var (
		out, finalOut *ddb.BatchWriteItemOutput
		err           error
	)

	finalOut = &ddb.BatchWriteItemOutput{
		ItemCollectionMetrics: map[string][]ddbTypes.ItemCollectionMetrics{},
	}

	defer func() { output.Set(finalOut, err) }()

	// copy the scan so we're not mutating the original
	input := CopyBatchWriteItemInput(ctx.Input)

	for {

		if out, err = ctx.Client.BatchWriteItem(ctx, input); err != nil {
			return
		}

		finalOut.ConsumedCapacity = append(finalOut.ConsumedCapacity, out.ConsumedCapacity...)

		for k, metrics := range out.ItemCollectionMetrics {
			if _, ok := finalOut.ItemCollectionMetrics[k]; !ok {
				finalOut.ItemCollectionMetrics[k] = []ddbTypes.ItemCollectionMetrics{}
			}

			for _, metric := range metrics {
				finalOut.ItemCollectionMetrics[k] = append(finalOut.ItemCollectionMetrics[k] , CopyItemCollectionMetrics(metric))
			}
		}

		if out.UnprocessedItems == nil || len(out.UnprocessedItems) == 0 {
			// no more work
			return
		}

		input.RequestItems = finalOut.UnprocessedItems
	}
}

// BatchWriteItemMiddleWare is a middleware function use for wrapping BatchWriteItemHandler requests
type BatchWriteItemMiddleWare interface {
	BatchWriteItemMiddleWare(next BatchWriteItemHandler) BatchWriteItemHandler
}

// BatchWriteItemMiddleWareFunc is a functional BatchWriteItemMiddleWare
type BatchWriteItemMiddleWareFunc func(next BatchWriteItemHandler) BatchWriteItemHandler

// BatchWriteItemMiddleWare implements the BatchWriteItemMiddleWare interface
func (mw BatchWriteItemMiddleWareFunc) BatchWriteItemMiddleWare(next BatchWriteItemHandler) BatchWriteItemHandler {
	return mw(next)
}

// BatchWriteItem represents a BatchWriteItem operation
type BatchWriteItem struct {
	*BaseOperation
	Handler     BatchWriteItemHandler
	input       *ddb.BatchWriteItemInput
	middleWares []BatchWriteItemMiddleWare
}

// NewBatchWriteItem creates a new BatchWriteItem
func NewBatchWriteItem(input *ddb.BatchWriteItemInput, mws ...BatchWriteItemMiddleWare) *BatchWriteItem {
	return &BatchWriteItem{
		BaseOperation: NewOperation(),
		Handler:       new(BatchWriteItemFinalHandler),
		input:         input,
		middleWares:   mws,
	}
}

// NewBatchWriteItemAll creates a new BatchWriteItemAll
func NewBatchWriteItemAll(input *ddb.BatchWriteItemInput, mws ...BatchWriteItemMiddleWare) *BatchWriteItem {
	return &BatchWriteItem{
		BaseOperation: NewOperation(),
		Handler:       new(BatchWriteItemAllFinalHandler),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the BatchWriteItem operation in a goroutine and returns a BatchWriteItemAllPromise
func (op *BatchWriteItem) Invoke(ctx context.Context, client *ddb.Client) *BatchWriteItem {
	op.SetRunning() // promise now waiting for a response

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the BatchWriteItem operation
func (op *BatchWriteItem) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	output := new(BatchWriteItemOutput)

	defer func() { op.SetResponse(output.Get()) }()

	var h BatchWriteItemHandler

	h = op.Handler

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].BatchWriteItemMiddleWare(h)
		}
	}

	requestCtx := &BatchWriteItemContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	h.HandleBatchWriteItem(requestCtx, output)
}

// Await waits for the BatchWriteItemPromise to be fulfilled and then returns a BatchWriteItemOutput and error
func (op *BatchWriteItem) Await() (*ddb.BatchWriteItemOutput, error) {
	out, err := op.BaseOperation.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.BatchWriteItemOutput), err
}

func NewBatchWriteItemInput() *ddb.BatchWriteItemInput {
	return &ddb.BatchWriteItemInput{
		RequestItems:           make(map[string][]ddbTypes.WriteRequest),
		ReturnConsumedCapacity: ddbTypes.ReturnConsumedCapacityNone,
	}
}

// CopyDeleteRequest creates a deep copy of a DeleteRequest
func CopyDeleteRequest(input *ddbTypes.DeleteRequest) *ddbTypes.DeleteRequest {
	if input == nil {
		return nil
	}

	return &ddbTypes.DeleteRequest{Key: CopyAttributeValueMap(input.Key)}
}

// CopyPutRequest creates a deep copy of a PutRequest
func CopyPutRequest(input *ddbTypes.PutRequest) *ddbTypes.PutRequest {
	if input == nil {
		return nil
	}

	return &ddbTypes.PutRequest{Item: CopyAttributeValueMap(input.Item)}
}

// CopyWriteRequest creates a deep copy of a WriteRequest
func CopyWriteRequest(input ddbTypes.WriteRequest) ddbTypes.WriteRequest {
	return ddbTypes.WriteRequest{
		DeleteRequest: CopyDeleteRequest(input.DeleteRequest),
		PutRequest:    CopyPutRequest(input.PutRequest),
	}
}

// CopyBatchWriteItemInput creates a deep copy of a v
func CopyBatchWriteItemInput(input *ddb.BatchWriteItemInput) *ddb.BatchWriteItemInput {
	clone := &ddb.BatchWriteItemInput{
		ReturnConsumedCapacity:      input.ReturnConsumedCapacity,
		ReturnItemCollectionMetrics: input.ReturnItemCollectionMetrics,
	}

	if input.RequestItems == nil {
		return clone
	}

	clone.RequestItems = make(map[string][]ddbTypes.WriteRequest)

	for k, v := range input.RequestItems {
		clone.RequestItems[k] = make([]ddbTypes.WriteRequest, len(v))
		for i, w := range input.RequestItems[k] {
			clone.RequestItems[k][i] = CopyWriteRequest(w)
		}
	}

	return clone
}

// CopyItemCollectionMetrics creates a deep copy of an item collection metrics
func CopyItemCollectionMetrics(input ddbTypes.ItemCollectionMetrics) ddbTypes.ItemCollectionMetrics {
	out := ddbTypes.ItemCollectionMetrics{}

	if input.ItemCollectionKey != nil {
		out.ItemCollectionKey = CopyAttributeValueMap(input.ItemCollectionKey)
	}

	if input.SizeEstimateRangeGB != nil {
		out.SizeEstimateRangeGB = make([]float64, len(input.SizeEstimateRangeGB))

		for i, f := range input.SizeEstimateRangeGB {
			out.SizeEstimateRangeGB[i] = f
		}
	}

	return out
}

type BatchWriteItemBuilder struct {
	*ddb.BatchWriteItemInput
}

// NewBatchWriteItemBuilder creates a new BatchWriteItemBuilder
func NewBatchWriteItemBuilder(input *ddb.BatchWriteItemInput) *BatchWriteItemBuilder {
	b := &BatchWriteItemBuilder{
		BatchWriteItemInput: &ddb.BatchWriteItemInput{
			RequestItems:                make(map[string][]ddbTypes.WriteRequest),
			ReturnConsumedCapacity:      ddbTypes.ReturnConsumedCapacityNone,
			ReturnItemCollectionMetrics: ddbTypes.ReturnItemCollectionMetricsNone,
		},
	}

	if input != nil {
		b.BatchWriteItemInput = input
	}

	return b
}

// SetRequestItems sets the RequestItems field's value.
func (bld *BatchWriteItemBuilder) SetRequestItems(v map[string][]ddbTypes.WriteRequest) *BatchWriteItemBuilder {
	bld.RequestItems = v
	return bld
}

// SetReturnConsumedCapacity sets the ReturnConsumedCapacity field's value.
func (bld *BatchWriteItemBuilder) SetReturnConsumedCapacity(v ddbTypes.ReturnConsumedCapacity) *BatchWriteItemBuilder {
	bld.ReturnConsumedCapacity = v
	return bld
}

// SetReturnItemCollectionMetrics sets the ReturnItemCollectionMetrics field's value.
func (bld *BatchWriteItemBuilder) SetReturnItemCollectionMetrics(v ddbTypes.ReturnItemCollectionMetrics) *BatchWriteItemBuilder {
	bld.ReturnItemCollectionMetrics = v
	return bld
}

// AddWriteRequests adds one or more WriteRequests for a given table to the input
func (bld *BatchWriteItemBuilder) AddWriteRequests(tableName string, requests ...ddbTypes.WriteRequest) *BatchWriteItemBuilder {
	if _, ok := bld.RequestItems[tableName]; !ok {
		bld.RequestItems[tableName] = make([]ddbTypes.WriteRequest, len(requests))
		for i, req := range requests {
			bld.RequestItems[tableName][i] = req
		}

		return bld
	}

	bld.RequestItems[tableName] = append(bld.RequestItems[tableName], requests...)

	return bld
}

// AddPuts adds multiple put requests from a given input that should be a slice of structs or maps
func (bld *BatchWriteItemBuilder) AddPuts(tableName string, items ...map[string]ddbTypes.AttributeValue) *BatchWriteItemBuilder {

	w := make([]ddbTypes.WriteRequest, len(items))
	for i, item := range items {
		w[i] = ddbTypes.WriteRequest{
			PutRequest: &ddbTypes.PutRequest{Item: item},
		}
	}

	return bld.AddWriteRequests(tableName, w...)
}

// AddDeletes adds a delete requests to the input
func (bld *BatchWriteItemBuilder) AddDeletes(table string, itemKeys ...map[string]ddbTypes.AttributeValue) *BatchWriteItemBuilder {

	w := make([]ddbTypes.WriteRequest, len(itemKeys))
	for i, item := range itemKeys {
		w[i] = ddbTypes.WriteRequest{
			DeleteRequest: &ddbTypes.DeleteRequest{Key: item},
		}
	}

	return bld.AddWriteRequests(table, w...)
}

// Build builds the dynamodb.UpdateItemInput
func (bld *BatchWriteItemBuilder) Build() *ddb.BatchWriteItemInput {
	return bld.BatchWriteItemInput
}

