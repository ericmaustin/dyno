package dyno

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtype "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// NewBatchGetItemAll creates a new BatchGetItemAll with this Client
func (c *Client) NewBatchGetItemAll(input *ddb.BatchGetItemInput, optFns ...func(*BatchGetItemOptions)) *BatchGetItemAll {
	return NewBatchGetItemAll(c.ddb, input, optFns...)
}

// BatchGetItemAll executes a scan api call with a BatchGetItemInput
func (c *Client) BatchGetItemAll(ctx context.Context, input *ddb.BatchGetItemInput, optFns ...func(*BatchGetItemOptions)) ([]*ddb.BatchGetItemOutput, error) {
	scan := c.NewBatchGetItemAll(input, optFns...)
	scan.DynoInvoke(ctx)
	return scan.Await()
}

// BatchGetItemInputCallback is a callback that is called on a given BatchGetItemInput before a BatchGetItem operation api call executes
type BatchGetItemInputCallback interface {
	BatchGetItemInputCallback(context.Context, *ddb.BatchGetItemInput) (*ddb.BatchGetItemOutput, error)
}

// BatchGetItemOutputCallback is a callback that is called on a given BatchGetItemOutput after a BatchGetItem operation api call executes
type BatchGetItemOutputCallback interface {
	BatchGetItemOutputCallback(context.Context, *ddb.BatchGetItemOutput) error
}

// BatchGetItemInputCallbackFunc is BatchGetItemOutputCallback function
type BatchGetItemInputCallbackFunc func(context.Context, *ddb.BatchGetItemInput) (*ddb.BatchGetItemOutput, error)

// BatchGetItemInputCallback implements the BatchGetItemOutputCallback interface
func (cb BatchGetItemInputCallbackFunc) BatchGetItemInputCallback(ctx context.Context, input *ddb.BatchGetItemInput) (*ddb.BatchGetItemOutput, error) {
	return cb(ctx, input)
}

// BatchGetItemOutputCallbackFunc is BatchGetItemOutputCallback function
type BatchGetItemOutputCallbackFunc func(context.Context, *ddb.BatchGetItemOutput) error

// BatchGetItemOutputCallback implements the BatchGetItemOutputCallback interface
func (cb BatchGetItemOutputCallbackFunc) BatchGetItemOutputCallback(ctx context.Context, input *ddb.BatchGetItemOutput) error {
	return cb(ctx, input)
}

// BatchGetItemOptions represents options passed to the BatchGetItem operation
type BatchGetItemOptions struct {
	//InputCallbacks are called before the BatchGetItem dynamodb api operation with the dynamodb.BatchGetItemInput
	InputCallbacks []BatchGetItemInputCallback
	//OutputCallbacks are called after the BatchGetItem dynamodb api operation with the dynamodb.BatchGetItemOutput
	OutputCallbacks []BatchGetItemOutputCallback
}

// BatchGetItemWithInputCallback adds a BatchGetItemInputCallbackFunc to the InputCallbacks
func BatchGetItemWithInputCallback(cb BatchGetItemInputCallbackFunc) func(*BatchGetItemOptions) {
	return func(opt *BatchGetItemOptions) {
		opt.InputCallbacks = append(opt.InputCallbacks, cb)
	}
}

// BatchGetItemWithOutputCallback adds a BatchGetItemOutputCallback to the OutputCallbacks
func BatchGetItemWithOutputCallback(cb BatchGetItemOutputCallback) func(*BatchGetItemOptions) {
	return func(opt *BatchGetItemOptions) {
		opt.OutputCallbacks = append(opt.OutputCallbacks, cb)
	}
}

// BatchGetItem represents a BatchGetItem operation
type BatchGetItem struct {
	*Promise
	client  *ddb.Client
	input   *ddb.BatchGetItemInput
	options BatchGetItemOptions
}

// NewBatchGetItem creates a new BatchGetItem operation on the given client with a given BatchGetItemInput and options
func NewBatchGetItem(client *ddb.Client, input *ddb.BatchGetItemInput, optFns ...func(*BatchGetItemOptions)) *BatchGetItem {
	opts := BatchGetItemOptions{}
	for _, opt := range optFns {
		opt(&opts)
	}
	return &BatchGetItem{
		//client:  nil,
		Promise: NewPromise(),
		client:  client,
		input:   input,
		options: opts,
	}
}

// Await waits for the Operation to be complete and then returns a BatchGetItemOutput and error
func (op *BatchGetItem) Await() (*ddb.BatchGetItemOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}
	return out.(*ddb.BatchGetItemOutput), err
}

// Invoke invokes the BatchGetItem operation
func (op *BatchGetItem) Invoke(ctx context.Context) *BatchGetItem {
	go op.DynoInvoke(ctx)
	return op
}

// DynoInvoke implements the Operation interface
func (op *BatchGetItem) DynoInvoke(ctx context.Context) {
	var (
		out *ddb.BatchGetItemOutput
		err error
	)
	defer op.SetResponse(out, err)
	for _, cb := range op.options.InputCallbacks {
		if out, err = cb.BatchGetItemInputCallback(ctx, op.input); out != nil || err != nil {
			return
		}
	}
	if out, err = op.client.BatchGetItem(ctx, op.input); err != nil {
		return
	}
	for _, cb := range op.options.OutputCallbacks {
		if err = cb.BatchGetItemOutputCallback(ctx, out); err != nil {
			return
		}
	}
	return
}

// NewBatchGetItem creates a new BatchGetItem with this Client
func (c *Client) NewBatchGetItem(input *ddb.BatchGetItemInput, optFns ...func(*BatchGetItemOptions)) *BatchGetItem {
	return NewBatchGetItem(c.ddb, input, optFns...)
}

// BatchGetItem executes a scan api call with a BatchGetItemInput
func (c *Client) BatchGetItem(ctx context.Context, input *ddb.BatchGetItemInput, optFns ...func(*BatchGetItemOptions)) (*ddb.BatchGetItemOutput, error) {
	scan := c.NewBatchGetItem(input, optFns...)
	scan.DynoInvoke(ctx)
	return scan.Await()
}

// BatchGetItemAll represents an exhaustive BatchGetItem operation
type BatchGetItemAll struct {
	*Promise
	client  *ddb.Client
	input   *ddb.BatchGetItemInput
	options BatchGetItemOptions
}

// NewBatchGetItemAll creates a new BatchGetItemAll operation on the given client with a given BatchGetItemInput and options
func NewBatchGetItemAll(client *ddb.Client, input *ddb.BatchGetItemInput, optFns ...func(*BatchGetItemOptions)) *BatchGetItemAll {
	options := BatchGetItemOptions{}
	for _, opt := range optFns {
		opt(&options)
	}
	return &BatchGetItemAll{
		//client:  nil,
		Promise: NewPromise(),
		client:  client,
		input:   input,
		options: options,
	}
}

// Await waits for the Operation to be complete and then returns a BatchGetItemOutput and error
func (op *BatchGetItemAll) Await() ([]*ddb.BatchGetItemOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}
	return out.([]*ddb.BatchGetItemOutput), err
}

// Invoke invokes the BatchGetItem operation
func (op *BatchGetItemAll) Invoke(ctx context.Context) *BatchGetItemAll {
	go op.DynoInvoke(ctx)
	return op
}

// DynoInvoke the Operation interface
func (op *BatchGetItemAll) DynoInvoke(ctx context.Context) {
	var (
		outs []*ddb.BatchGetItemOutput
		out  *ddb.BatchGetItemOutput
		err  error
	)
	defer op.SetResponse(outs, err)
	//copy the scan so we're not mutating the original
	input := CopyBatchGetItem(op.input)
	for {
		for _, cb := range op.options.InputCallbacks {
			if out, err = cb.BatchGetItemInputCallback(ctx, input); out != nil || err != nil {
				if out != nil {
					outs = append(outs, out)
				}
				return
			}
		}
		if out, err = op.client.BatchGetItem(ctx, input); err != nil {
			return
		}
		for _, cb := range op.options.OutputCallbacks {
			if err = cb.BatchGetItemOutputCallback(ctx, out); err != nil {
				return
			}
		}
		outs = append(outs, out)
		if out.UnprocessedKeys == nil {
			// no more work
			break
		}
		input.RequestItems = out.UnprocessedKeys
	}
	return
}

func NewBatchGetItemInput() *ddb.BatchGetItemInput {
	return &ddb.BatchGetItemInput{
		RequestItems:           make(map[string]ddbtype.KeysAndAttributes),
		ReturnConsumedCapacity: ddbtype.ReturnConsumedCapacityNone,
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
		bld.BatchGetItemInput.RequestItems[tableName] = ddbtype.KeysAndAttributes{}
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
func (bld *BatchGetItemBuilder) SetKeysAndAttributes(tableName string, keysAndAttributes ddbtype.KeysAndAttributes) *BatchGetItemBuilder {
	bld.RequestItems[tableName] = keysAndAttributes
	return bld
}

// AddKey adds one or more keys to the request item map
func (bld *BatchGetItemBuilder) AddKey(tableName string, keys ...map[string]ddbtype.AttributeValue) *BatchGetItemBuilder {
	bld.initTable(tableName)
	existingKeysAndAttributes := bld.RequestItems[tableName]
	existingKeysAndAttributes.Keys = append(existingKeysAndAttributes.Keys, keys...)
	bld.RequestItems[tableName] = existingKeysAndAttributes
	return bld
}

// SetRequestItems sets the RequestItems field's value.
func (bld *BatchGetItemBuilder) SetRequestItems(v map[string]ddbtype.KeysAndAttributes) *BatchGetItemBuilder {
	bld.RequestItems = v
	return bld
}

// SetReturnConsumedCapacity sets the ReturnConsumedCapacity field's value.
func (bld *BatchGetItemBuilder) SetReturnConsumedCapacity(v ddbtype.ReturnConsumedCapacity) *BatchGetItemBuilder {
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
		sliceOfKeys := make([][]map[string]ddbtype.AttributeValue, 0)
		for i := 0; i < len(keysAndAttributes.Keys); i += chunkSize {
			end := i + chunkSize
			if end > len(keysAndAttributes.Keys) {
				end = len(keysAndAttributes.Keys)
			}
			sliceOfKeys = append(sliceOfKeys, keysAndAttributes.Keys[i:end])
		}

		for _, slice := range sliceOfKeys {
			newInput := CopyBatchGetItem(input)
			newInput.RequestItems = map[string]ddbtype.KeysAndAttributes{
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
