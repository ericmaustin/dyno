package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// BatchWriteItem executes BatchWriteItem operation and returns a BatchWriteItemPromise
func (c *Client) BatchWriteItem(ctx context.Context, input *ddb.BatchWriteItemInput, mw ...BatchWriteItemMiddleWare) *BatchWriteItemPromise {
	return NewBatchWriteItem(input, mw...).Invoke(ctx, c.ddb)
}

// BatchWriteItem executes a BatchWriteItem operation with a BatchWriteItemInput in this pool and returns the BatchWriteItemPromise
func (p *Pool) BatchWriteItem(input *ddb.BatchWriteItemInput, mw ...BatchWriteItemMiddleWare) *BatchWriteItemPromise {
	op := NewBatchWriteItem(input, mw...)

	if err := p.Do(op); err != nil {
		op.promise.SetResponse(nil, err)
	}

	return op.promise
}

// BatchWriteItemAll executes BatchWriteItemAll operation and returns a BatchWriteItemAllPromise
func (c *Client) BatchWriteItemAll(ctx context.Context, input *ddb.BatchWriteItemInput, mw ...BatchWriteItemAllMiddleWare) *BatchWriteItemAllPromise {
	return NewBatchWriteItemAll(input, mw...).Invoke(ctx, c.ddb)
}

// BatchWriteItemAll executes a BatchWriteItemAll operation with a BatchWriteItemInput in this pool and returns the BatchWriteItemAllPromise
func (p *Pool) BatchWriteItemAll(input *ddb.BatchWriteItemInput, mw ...BatchWriteItemAllMiddleWare) *BatchWriteItemAllPromise {
	op := NewBatchWriteItemAll(input, mw...)

	if err := p.Do(op); err != nil {
		op.promise.SetResponse(nil, err)
	}

	return op.promise
}

// BatchWriteItemContext represents an exhaustive BatchWriteItem operation request context
type BatchWriteItemContext struct {
	context.Context
	input  *ddb.BatchWriteItemInput
	client *ddb.Client
}

// BatchWriteItemPromise represents a promise for the BatchWriteItem
type BatchWriteItemPromise struct {
	*Promise
}

// Await waits for the BatchWriteItemPromise to be fulfilled and then returns a BatchWriteItemOutput and error
func (p *BatchWriteItemPromise) Await() (*ddb.BatchWriteItemOutput, error) {
	out, err := p.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.BatchWriteItemOutput), err
}

// newBatchWriteItemPromise returns a new BatchWriteItemPromise
func newBatchWriteItemPromise() *BatchWriteItemPromise {
	return &BatchWriteItemPromise{NewPromise()}
}

// BatchWriteItemHandler represents a handler for BatchWriteItem requests
type BatchWriteItemHandler interface {
	HandleBatchWriteItem(ctx *BatchWriteItemContext, promise *BatchWriteItemPromise)
}

// BatchWriteItemHandlerFunc is a BatchWriteItemHandler function
type BatchWriteItemHandlerFunc func(ctx *BatchWriteItemContext, promise *BatchWriteItemPromise)

// HandleBatchWriteItem implements BatchWriteItemHandler
func (h BatchWriteItemHandlerFunc) HandleBatchWriteItem(ctx *BatchWriteItemContext, promise *BatchWriteItemPromise) {
	h(ctx, promise)
}

// BatchWriteItemMiddleWare is a middleware function use for wrapping BatchWriteItemHandler requests
type BatchWriteItemMiddleWare func(handler BatchWriteItemHandler) BatchWriteItemHandler

// BatchWriteItemFinalHandler returns the final BatchWriteItemHandler that executes a dynamodb BatchWriteItem operation
func BatchWriteItemFinalHandler() BatchWriteItemHandler {
	return BatchWriteItemHandlerFunc(func(ctx *BatchWriteItemContext, promise *BatchWriteItemPromise) {
		promise.SetResponse(ctx.client.BatchWriteItem(ctx, ctx.input))
	})
}

// BatchWriteItem represents a BatchWriteItem operation
type BatchWriteItem struct {
	promise     *BatchWriteItemPromise
	input       *ddb.BatchWriteItemInput
	middleWares []BatchWriteItemMiddleWare
}

// NewBatchWriteItem creates a new BatchWriteItem
func NewBatchWriteItem(input *ddb.BatchWriteItemInput, mws ...BatchWriteItemMiddleWare) *BatchWriteItem {
	return &BatchWriteItem{
		input:       input,
		middleWares: mws,
		promise:     newBatchWriteItemPromise(),
	}
}

// Invoke invokes the BatchWriteItem operation and returns a BatchWriteItemPromise
func (op *BatchWriteItem) Invoke(ctx context.Context, client *ddb.Client) *BatchWriteItemPromise {
	go op.DynoInvoke(ctx, client)

	return op.promise
}

// DynoInvoke implements the Operation interface
func (op *BatchWriteItem) DynoInvoke(ctx context.Context, client *ddb.Client) {

	requestCtx := &BatchWriteItemContext{
		Context: ctx,
		client:  client,
		input:   op.input,
	}

	h := BatchWriteItemFinalHandler()

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i](h)
		}
	}

	h.HandleBatchWriteItem(requestCtx, op.promise)
}

// BatchWriteItemAllContext represents an exhaustive BatchWriteItemAll operation request context
type BatchWriteItemAllContext struct {
	context.Context
	input  *ddb.BatchWriteItemInput
	client *ddb.Client
}

// BatchWriteItemAllPromise represents a promise for the BatchWriteItemAll
type BatchWriteItemAllPromise struct {
	*Promise
}

// Await waits for the BatchWriteItemAllPromise to be fulfilled and then returns a BatchWriteItemAllOutput and error
func (p *BatchWriteItemAllPromise) Await() ([]*ddb.BatchWriteItemOutput, error) {
	out, err := p.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.([]*ddb.BatchWriteItemOutput), err
}

// newBatchWriteItemAllPromise returns a new BatchWriteItemAllPromise
func newBatchWriteItemAllPromise() *BatchWriteItemAllPromise {
	return &BatchWriteItemAllPromise{NewPromise()}
}

// BatchWriteItemAllHandler represents a handler for BatchWriteItemAll requests
type BatchWriteItemAllHandler interface {
	HandleBatchWriteItemAll(ctx *BatchWriteItemAllContext, promise *BatchWriteItemAllPromise)
}

// BatchWriteItemAllHandlerFunc is a BatchWriteItemAllHandler function
type BatchWriteItemAllHandlerFunc func(ctx *BatchWriteItemAllContext, promise *BatchWriteItemAllPromise)

// HandleBatchWriteItemAll implements BatchWriteItemAllHandler
func (h BatchWriteItemAllHandlerFunc) HandleBatchWriteItemAll(ctx *BatchWriteItemAllContext, promise *BatchWriteItemAllPromise) {
	h(ctx, promise)
}

// BatchWriteItemAllMiddleWare is a middleware function use for wrapping BatchWriteItemAllHandler requests
type BatchWriteItemAllMiddleWare func(handler BatchWriteItemAllHandler) BatchWriteItemAllHandler

// BatchWriteItemAllFinalHandler returns the final BatchWriteItemAllHandler that executes a dynamodb BatchWriteItemAll operation
func BatchWriteItemAllFinalHandler() BatchWriteItemAllHandler {
	return BatchWriteItemAllHandlerFunc(func(ctx *BatchWriteItemAllContext, promise *BatchWriteItemAllPromise) {
		var (
			outs []*ddb.BatchWriteItemOutput
			out  *ddb.BatchWriteItemOutput
			err  error
		)

		defer func() { promise.SetResponse(outs, err) }()

		// copy the scan so we're not mutating the original
		input := CopyBatchWriteItemInput(ctx.input)

		for {
			if out, err = ctx.client.BatchWriteItem(ctx, input); err != nil {
				return
			}

			outs = append(outs, out)

			if out.UnprocessedItems == nil {
				// no more work
				return
			}

			input.RequestItems = out.UnprocessedItems
		}
	})
}

// BatchWriteItemAll represents a BatchWriteItemAll operation
type BatchWriteItemAll struct {
	promise     *BatchWriteItemAllPromise
	input       *ddb.BatchWriteItemInput
	middleWares []BatchWriteItemAllMiddleWare
}

// NewBatchWriteItemAll creates a new BatchWriteItemAll
func NewBatchWriteItemAll(input *ddb.BatchWriteItemInput, mws ...BatchWriteItemAllMiddleWare) *BatchWriteItemAll {
	return &BatchWriteItemAll{
		input:       input,
		middleWares: mws,
		promise:     newBatchWriteItemAllPromise(),
	}
}

// Invoke invokes the BatchWriteItemAll operation and returns a BatchWriteItemAllPromise
func (op *BatchWriteItemAll) Invoke(ctx context.Context, client *ddb.Client) *BatchWriteItemAllPromise {
	go op.DynoInvoke(ctx, client)

	return op.promise
}

// DynoInvoke the Operation interface
func (op *BatchWriteItemAll) DynoInvoke(ctx context.Context, client *ddb.Client) {
	requestCtx := &BatchWriteItemAllContext{
		Context: ctx,
		client:  client,
		input:   op.input,
	}

	h := BatchWriteItemAllFinalHandler()

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i](h)
		}
	}

	h.HandleBatchWriteItemAll(requestCtx, op.promise)
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

	if clone.RequestItems == nil {
		return clone
	}

	clone.RequestItems = make(map[string][]ddbTypes.WriteRequest, len(input.RequestItems))

	for k, v := range input.RequestItems {
		clone.RequestItems[k] = make([]ddbTypes.WriteRequest, len(v))
		for i, w := range input.RequestItems[k] {
			clone.RequestItems[k][i] = CopyWriteRequest(w)
		}
	}

	return clone
}

type BatchWriteItemBuilder struct {
	*ddb.BatchWriteItemInput
}

// NewBatchWriteItemBuilder creates a new BatchWriteItemBuilder
func NewBatchWriteItemBuilder() *BatchWriteItemBuilder {
	return &BatchWriteItemBuilder{
		BatchWriteItemInput: &ddb.BatchWriteItemInput{
			RequestItems:                make(map[string][]ddbTypes.WriteRequest),
			ReturnConsumedCapacity:      ddbTypes.ReturnConsumedCapacityNone,
			ReturnItemCollectionMetrics: ddbTypes.ReturnItemCollectionMetricsNone,
		},
	}
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
