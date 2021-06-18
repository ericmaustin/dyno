package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// NewBatchWriteItemAll creates a new BatchWriteItemAll with this Client
func (c *Client) NewBatchWriteItemAll(input *ddb.BatchWriteItemInput, optFns ...func(*BatchWriteItemOptions)) *BatchWriteItemAll {
	return NewBatchWriteItemAll(c.ddb, input, optFns...)
}

// BatchWriteItemAll executes a scan api call with a BatchWriteItemInput
func (c *Client) BatchWriteItemAll(ctx context.Context, input *ddb.BatchWriteItemInput, optFns ...func(*BatchWriteItemOptions)) ([]*ddb.BatchWriteItemOutput, error) {
	scan := c.NewBatchWriteItemAll(input, optFns...)
	scan.DynoInvoke(ctx)
	return scan.Await()
}

// BatchWriteItemInputCallback is a callback that is called on a given BatchWriteItemInput before a BatchWriteItem operation api call executes
type BatchWriteItemInputCallback interface {
	BatchWriteItemInputCallback(context.Context, *ddb.BatchWriteItemInput) (*ddb.BatchWriteItemOutput, error)
}

// BatchWriteItemOutputCallback is a callback that is called on a given BatchWriteItemOutput after a BatchWriteItem operation api call executes
type BatchWriteItemOutputCallback interface {
	BatchWriteItemOutputCallback(context.Context, *ddb.BatchWriteItemOutput) error
}

// BatchWriteItemInputCallbackFunc is BatchWriteItemOutputCallback function
type BatchWriteItemInputCallbackFunc func(context.Context, *ddb.BatchWriteItemInput) (*ddb.BatchWriteItemOutput, error)

// BatchWriteItemInputCallback implements the BatchWriteItemOutputCallback interface
func (cb BatchWriteItemInputCallbackFunc) BatchWriteItemInputCallback(ctx context.Context, input *ddb.BatchWriteItemInput) (*ddb.BatchWriteItemOutput, error) {
	return cb(ctx, input)
}

// BatchWriteItemOutputCallbackFunc is BatchWriteItemOutputCallback function
type BatchWriteItemOutputCallbackFunc func(context.Context, *ddb.BatchWriteItemOutput) error

// BatchWriteItemOutputCallback implements the BatchWriteItemOutputCallback interface
func (cb BatchWriteItemOutputCallbackFunc) BatchWriteItemOutputCallback(ctx context.Context, input *ddb.BatchWriteItemOutput) error {
	return cb(ctx, input)
}

// BatchWriteItemOptions represents options passed to the BatchWriteItem operation
type BatchWriteItemOptions struct {
	//InputCallbacks are called before the BatchWriteItem dynamodb api operation with the dynamodb.BatchWriteItemInput
	InputCallbacks []BatchWriteItemInputCallback
	//OutputCallbacks are called after the BatchWriteItem dynamodb api operation with the dynamodb.BatchWriteItemOutput
	OutputCallbacks []BatchWriteItemOutputCallback
}

// BatchWriteItemWithInputCallback adds a BatchWriteItemInputCallbackFunc to the InputCallbacks
func BatchWriteItemWithInputCallback(cb BatchWriteItemInputCallbackFunc) func(*BatchWriteItemOptions) {
	return func(opt *BatchWriteItemOptions) {
		opt.InputCallbacks = append(opt.InputCallbacks, cb)
	}
}

// BatchWriteItemWithOutputCallback adds a BatchWriteItemOutputCallback to the OutputCallbacks
func BatchWriteItemWithOutputCallback(cb BatchWriteItemOutputCallback) func(*BatchWriteItemOptions) {
	return func(opt *BatchWriteItemOptions) {
		opt.OutputCallbacks = append(opt.OutputCallbacks, cb)
	}
}

// BatchWriteItem represents a BatchWriteItem operation
type BatchWriteItem struct {
	*Promise
	client  *ddb.Client
	input   *ddb.BatchWriteItemInput
	options BatchWriteItemOptions
}

// NewBatchWriteItem creates a new BatchWriteItem operation on the given client with a given BatchWriteItemInput and options
func NewBatchWriteItem(client *ddb.Client, input *ddb.BatchWriteItemInput, optFns ...func(*BatchWriteItemOptions)) *BatchWriteItem {
	opts := BatchWriteItemOptions{}
	for _, opt := range optFns {
		opt(&opts)
	}
	return &BatchWriteItem{
		//client:  nil,
		Promise: NewPromise(),
		client:  client,
		input:   input,
		options: opts,
	}
}

// Await waits for the Operation to be complete and then returns a BatchWriteItemOutput and error
func (op *BatchWriteItem) Await() (*ddb.BatchWriteItemOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}
	return out.(*ddb.BatchWriteItemOutput), err
}

// Invoke invokes the BatchWriteItem operation
func (op *BatchWriteItem) Invoke(ctx context.Context) *BatchWriteItem {
	go op.DynoInvoke(ctx)
	return op
}

// DynoInvoke implements the Operation interface
func (op *BatchWriteItem) DynoInvoke(ctx context.Context) {
	var (
		out *ddb.BatchWriteItemOutput
		err error
	)
	defer op.SetResponse(out, err)
	for _, cb := range op.options.InputCallbacks {
		if out, err = cb.BatchWriteItemInputCallback(ctx, op.input); out != nil || err != nil {
			return
		}
	}
	if out, err = op.client.BatchWriteItem(ctx, op.input); err != nil {
		return
	}
	for _, cb := range op.options.OutputCallbacks {
		if err = cb.BatchWriteItemOutputCallback(ctx, out); err != nil {
			return
		}
	}
	return
}

// NewBatchWriteItem creates a new BatchWriteItem with this Client
func (c *Client) NewBatchWriteItem(input *ddb.BatchWriteItemInput, optFns ...func(*BatchWriteItemOptions)) *BatchWriteItem {
	return NewBatchWriteItem(c.ddb, input, optFns...)
}

// BatchWriteItem executes a scan api call with a BatchWriteItemInput
func (c *Client) BatchWriteItem(ctx context.Context, input *ddb.BatchWriteItemInput, optFns ...func(*BatchWriteItemOptions)) (*ddb.BatchWriteItemOutput, error) {
	scan := c.NewBatchWriteItem(input, optFns...)
	scan.DynoInvoke(ctx)
	return scan.Await()
}

// BatchWriteItemAll represents an exhaustive BatchWriteItem operation
type BatchWriteItemAll struct {
	*Promise
	client  *ddb.Client
	input   *ddb.BatchWriteItemInput
	options BatchWriteItemOptions
}

// NewBatchWriteItemAll creates a new BatchWriteItemAll operation on the given client with a given BatchWriteItemInput and options
func NewBatchWriteItemAll(client *ddb.Client, input *ddb.BatchWriteItemInput, optFns ...func(*BatchWriteItemOptions)) *BatchWriteItemAll {
	options := BatchWriteItemOptions{}
	for _, opt := range optFns {
		opt(&options)
	}
	return &BatchWriteItemAll{
		//client:  nil,
		Promise: NewPromise(),
		client:  client,
		input:   input,
		options: options,
	}
}

// Await waits for the Operation to be complete and then returns a BatchWriteItemOutput and error
func (op *BatchWriteItemAll) Await() ([]*ddb.BatchWriteItemOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}
	return out.([]*ddb.BatchWriteItemOutput), err
}

// Invoke invokes the BatchWriteItem operation
func (op *BatchWriteItemAll) Invoke(ctx context.Context) *BatchWriteItemAll {
	go op.DynoInvoke(ctx)
	return op
}

// DynoInvoke the Operation interface
func (op *BatchWriteItemAll) DynoInvoke(ctx context.Context) {
	var (
		outs []*ddb.BatchWriteItemOutput
		out  *ddb.BatchWriteItemOutput
		err  error
	)
	defer op.SetResponse(outs, err)
	//copy the scan so we're not mutating the original
	input := CopyBatchWriteItemInput(op.input)
	for {
		for _, cb := range op.options.InputCallbacks {
			if out, err = cb.BatchWriteItemInputCallback(ctx, input); out != nil || err != nil {
				if out != nil {
					outs = append(outs, out)
				}
				return
			}
		}
		if out, err = op.client.BatchWriteItem(ctx, input); err != nil {
			return
		}
		for _, cb := range op.options.OutputCallbacks {
			if err = cb.BatchWriteItemOutputCallback(ctx, out); err != nil {
				return
			}
		}
		outs = append(outs, out)
		if out.UnprocessedItems == nil {
			// no more work
			return
		}
		input.RequestItems = out.UnprocessedItems
	}
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

//NewBatchWriteItemBuilder creates a new BatchWriteItemBuilder
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
