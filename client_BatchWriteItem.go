package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"sync"
)

// BatchWriteItem executes BatchWriteItem operation and returns a BatchWriteItemPromise
func (c *Client) BatchWriteItem(ctx context.Context, input *ddb.BatchWriteItemInput, mw ...BatchWriteItemMiddleWare) *BatchWriteItem {
	return NewBatchWriteItem(input, mw...).Invoke(ctx, c.ddb)
}

// BatchWriteItem executes a BatchWriteItem operation with a BatchWriteItemInput in this pool and returns the BatchWriteItemPromise
func (p *Pool) BatchWriteItem(input *ddb.BatchWriteItemInput, mw ...BatchWriteItemMiddleWare) *BatchWriteItem {
	op := NewBatchWriteItem(input, mw...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// BatchWriteItemAll creates a new BatchWriteItemAll, invokes and returns it
func (c *Client) BatchWriteItemAll(ctx context.Context, input *ddb.BatchWriteItemInput, mw ...BatchWriteItemAllMiddleWare) *BatchWriteItemAll {
	return NewBatchWriteItemAll(input, mw...).Invoke(ctx, c.ddb)
}

// BatchWriteItemAll creates a new BatchWriteItemAll, passes it to the Pool and then returns the BatchWriteItemAll
func (p *Pool) BatchWriteItemAll(input *ddb.BatchWriteItemInput, mw ...BatchWriteItemAllMiddleWare) *BatchWriteItemAll {
	op := NewBatchWriteItemAll(input, mw...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// BatchWriteItemContext represents an exhaustive BatchWriteItem operation request context
type BatchWriteItemContext struct {
	context.Context
	Input  *ddb.BatchWriteItemInput
	Client *ddb.Client
}

// BatchWriteItemOutput represents the output for the BatchWriteItem operation
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

// BatchWriteItemFinalHandler is the final handler for all batchWriteItem operations
type BatchWriteItemFinalHandler struct{}

// HandleBatchWriteItem implements BatchWriteItemHandler
func (b *BatchWriteItemFinalHandler) HandleBatchWriteItem(ctx *BatchWriteItemContext, output *BatchWriteItemOutput) {
	output.Set(ctx.Client.BatchWriteItem(ctx, ctx.Input))
}

// BatchWriteItem represents a BatchWriteItem operation
type BatchWriteItem struct {
	*Promise
	input       *ddb.BatchWriteItemInput
	middleWares []BatchWriteItemMiddleWare
}

// NewBatchWriteItem creates a new BatchWriteItem
func NewBatchWriteItem(input *ddb.BatchWriteItemInput, mws ...BatchWriteItemMiddleWare) *BatchWriteItem {
	return &BatchWriteItem{
		Promise:     NewPromise(),
		input:       input,
		middleWares: mws,
	}
}

// Invoke invokes the BatchWriteItem operation and returns a BatchWriteItemPromise
func (op *BatchWriteItem) Invoke(ctx context.Context, client *ddb.Client) *BatchWriteItem {
	go op.DynoInvoke(ctx, client)

	return op
}

// DynoInvoke implements the Operation interface
func (op *BatchWriteItem) DynoInvoke(ctx context.Context, client *ddb.Client) {
	output := new(BatchWriteItemOutput)

	defer func() { op.SetResponse(output.Get()) }()

	requestCtx := &BatchWriteItemContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	var h BatchWriteItemHandler

	h = new(BatchWriteItemFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].BatchWriteItemMiddleWare(h)
		}
	}

	h.HandleBatchWriteItem(requestCtx, output)
}

// Await waits for the BatchWriteItemPromise to be fulfilled and then returns a BatchWriteItemOutput and error
func (op *BatchWriteItem) Await() (*ddb.BatchWriteItemOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.BatchWriteItemOutput), err
}

// BatchWriteItemAllContext represents an exhaustive BatchWriteItemAll operation request context
type BatchWriteItemAllContext struct {
	context.Context
	Input  *ddb.BatchWriteItemInput
	Client *ddb.Client
}

// BatchWriteItemAllOutput represents the output for the BatchWriteItemAll operation
type BatchWriteItemAllOutput struct {
	out []*ddb.BatchWriteItemOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *BatchWriteItemAllOutput) Set(out []*ddb.BatchWriteItemOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *BatchWriteItemAllOutput) Get() (out []*ddb.BatchWriteItemOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()
	return
}

// BatchWriteItemAllPromise represents a promise for the BatchWriteItemAll
type BatchWriteItemAllPromise struct {
	*Promise
}

// GetResponse returns the GetResponse output and error
// if Output has not been set yet nil is returned
func (p *BatchWriteItemAllPromise) GetResponse() ([]*ddb.BatchWriteItemOutput, error) {
	out, err := p.Promise.GetResponse()
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
	HandleBatchWriteItemAll(ctx *BatchWriteItemAllContext, output *BatchWriteItemAllOutput)
}

// BatchWriteItemAllHandlerFunc is a BatchWriteItemAllHandler function
type BatchWriteItemAllHandlerFunc func(ctx *BatchWriteItemAllContext, output *BatchWriteItemAllOutput)

// HandleBatchWriteItemAll implements BatchWriteItemAllHandler
func (h BatchWriteItemAllHandlerFunc) HandleBatchWriteItemAll(ctx *BatchWriteItemAllContext, output *BatchWriteItemAllOutput) {
	h(ctx, output)
}

// BatchWriteItemAllMiddleWare is a middleware function use for wrapping BatchWriteItemAllHandler requests
type BatchWriteItemAllMiddleWare interface {
	BatchWriteItemAllMiddleWare(next BatchWriteItemAllHandler) BatchWriteItemAllHandler
}

// BatchWriteItemAllMiddleWareFunc is a functional BatchWriteItemAllMiddleWare
type BatchWriteItemAllMiddleWareFunc func(next BatchWriteItemAllHandler) BatchWriteItemAllHandler

// BatchWriteItemAllMiddleWare implements the BatchWriteItemAllMiddleWare interface
func (mw BatchWriteItemAllMiddleWareFunc) BatchWriteItemAllMiddleWare(next BatchWriteItemAllHandler) BatchWriteItemAllHandler {
	return mw(next)
}

// BatchWriteItemAllFinalHandler is the final handler for all batchWriteItemAll operations
type BatchWriteItemAllFinalHandler struct{}

// HandleBatchWriteItemAll implements the HandleBatchWriteItem interface
func (b *BatchWriteItemAllFinalHandler) HandleBatchWriteItemAll(ctx *BatchWriteItemAllContext, output *BatchWriteItemAllOutput) {
	var (
		outs []*ddb.BatchWriteItemOutput
		out  *ddb.BatchWriteItemOutput
		err  error
	)

	defer func() { output.Set(outs, err) }()

	// copy the scan so we're not mutating the original
	input := CopyBatchWriteItemInput(ctx.Input)

	for {
		if out, err = ctx.Client.BatchWriteItem(ctx, input); err != nil {
			return
		}

		outs = append(outs, out)

		if out.UnprocessedItems == nil || len(out.UnprocessedItems) == 0 {
			// no more work
			return
		}

		input.RequestItems = out.UnprocessedItems
	}
}

// BatchWriteItemAll represents a BatchWriteItemAll operation
type BatchWriteItemAll struct {
	*Promise
	input       *ddb.BatchWriteItemInput
	middleWares []BatchWriteItemAllMiddleWare
}

// NewBatchWriteItemAll creates a new BatchWriteItemAll
func NewBatchWriteItemAll(input *ddb.BatchWriteItemInput, mws ...BatchWriteItemAllMiddleWare) *BatchWriteItemAll {
	return &BatchWriteItemAll{
		Promise:     NewPromise(),
		input:       input,
		middleWares: mws,
	}
}

// Invoke invokes the BatchWriteItemAll operation and returns a BatchWriteItemAllPromise
func (op *BatchWriteItemAll) Invoke(ctx context.Context, client *ddb.Client) *BatchWriteItemAll {
	go op.DynoInvoke(ctx, client)

	return op
}

// DynoInvoke the Operation interface
func (op *BatchWriteItemAll) DynoInvoke(ctx context.Context, client *ddb.Client) {
	output := new(BatchWriteItemAllOutput)

	defer func() { op.SetResponse(output.Get()) }()

	requestCtx := &BatchWriteItemAllContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	var h BatchWriteItemAllHandler

	h = new(BatchWriteItemAllFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].BatchWriteItemAllMiddleWare(h)
		}
	}

	h.HandleBatchWriteItemAll(requestCtx, output)
}

// Await waits for the BatchWriteItemAllPromise to be fulfilled and then returns a BatchWriteItemAllOutput and error
func (op *BatchWriteItemAll) Await() ([]*ddb.BatchWriteItemOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.([]*ddb.BatchWriteItemOutput), err
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
