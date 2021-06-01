package input

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/ericmaustin/dyno"
	"github.com/ericmaustin/dyno/encoding"
)

const BatchGetItemLimit = 100

// BatchGetItemBuilder used to dynamically build a BatchGetItemInput
type BatchGetItemBuilder struct {
	*dynamodb.BatchGetItemInput
	projection *expression.ProjectionBuilder
	tmpKeys map[string][]interface{}
}

// NewBatchGetBuilder creates a new BatchGetItemBuilder
func NewBatchGetBuilder() *BatchGetItemBuilder {
	return &BatchGetItemBuilder{
		BatchGetItemInput: &dynamodb.BatchGetItemInput{
			RequestItems: make(map[string]*dynamodb.KeysAndAttributes),
		},
		tmpKeys: make(map[string][]interface{}),
	}
}

// SetInput sets the BatchGetItemBuilder's dynamodb.BatchGetItemInput explicitly
func (builder *BatchGetItemBuilder) SetInput(input *dynamodb.BatchGetItemInput) {
	builder.BatchGetItemInput = input
}

func (builder *BatchGetItemBuilder) initTable(tableName string) {
	if _, ok := builder.BatchGetItemInput.RequestItems[tableName]; !ok {
		builder.BatchGetItemInput.RequestItems[tableName] = new(dynamodb.KeysAndAttributes)
	}
}

// AddProjection adds additional field names to the projection
func (builder *BatchGetItemBuilder) AddProjection(projection interface{})  *BatchGetItemBuilder {
	addProjection(builder.projection, projection)
	return builder
}

// AddProjectionNames adds additional field names to the projection with strings
func (builder *BatchGetItemBuilder) AddProjectionNames(names ...string)  *BatchGetItemBuilder {
	addProjectionNames(builder.projection, names)
	return builder
}

// SetConsistentRead sets the projection for the given table name
func (builder *BatchGetItemBuilder) SetConsistentRead(tableName string, consistentRead bool)  *BatchGetItemBuilder {
	builder.initTable(tableName)
	builder.RequestItems[tableName].ConsistentRead = &consistentRead
	return builder
}

// SetKeysAndAttributes sets the keys and attributes to get from the given table
func (builder *BatchGetItemBuilder) SetKeysAndAttributes(tableName string, keysAndAttributes *dynamodb.KeysAndAttributes)  *BatchGetItemBuilder {
	builder.RequestItems[tableName] = keysAndAttributes
	return builder
}

// AddKeys adds keys to the request item map
func (builder *BatchGetItemBuilder) AddKeys(tableName string, keys interface{}) *BatchGetItemBuilder {
	builder.tmpKeys[tableName] = append(builder.tmpKeys[tableName], keys)
	return builder
}

// Build builds and returns the BatchGetItemInput
func (builder *BatchGetItemBuilder) Build() (*dynamodb.BatchGetItemInput, error) {
	var err error
	for tblName, keys := range builder.tmpKeys {
		for _, key := range keys {
			builder.initTable(tblName)
			builder.RequestItems[tblName].Keys, err = encoding.MarshalItems(key)
			if err != nil {
				return nil, fmt.Errorf("BatchGetItemBuilder Build() failed while attempting to marshal item key: %v", err)
			}
		}
	}

	// remove all inputs that don't have any keys associated
	for tableName, keys := range builder.RequestItems {
		if keys.Keys == nil || len(keys.Keys) < 1 {
			delete(builder.RequestItems, tableName)
		}
	}
	if builder.ReturnConsumedCapacity == nil {
		builder.ReturnConsumedCapacity = dyno.StringPtr("NONE")
	}

	if builder.projection != nil {
		// only use expression builder if we have a projection or a filter
		eb := expression.NewBuilder()
		eb = eb.WithProjection(*builder.projection)

		// build the Expression
		expr, err := eb.Build()
		if err != nil {
			return nil, err
		}
		exprNames := expr.Names()
		exprProj := expr.Projection()
		for _, item := range builder.RequestItems {
			item.ExpressionAttributeNames = exprNames
			item.ProjectionExpression = exprProj
		}
	}
	return builder.BatchGetItemInput, nil
}

// BuildChunks build and returns the BatchGetItemInput and then splits the input into chunks of the given size
func (builder *BatchGetItemBuilder) BuildChunks(chunksize int) ([]*dynamodb.BatchGetItemInput, error) {
	out, err := builder.Build()
	if err != nil {
		return nil, err
	}

	return ChunkBatchGetItemInputs(out, chunksize), nil
}

// ChunkBatchGetItemInputs chunks the input dynamodb.BatchGetItemInput into chunks of the given chunkSize
func ChunkBatchGetItemInputs(input *dynamodb.BatchGetItemInput, chunkSize int) (out []*dynamodb.BatchGetItemInput) {

	var returnConsumedCapacity string
	if input.ReturnConsumedCapacity != nil {
		returnConsumedCapacity = *input.ReturnConsumedCapacity
	}

	for tableName, keysAndAttributes := range input.RequestItems {
		sliceOfKeys := make([][]map[string]*dynamodb.AttributeValue, 0)

		for i := 0; i < len(keysAndAttributes.Keys); i += chunkSize {
			end := i + chunkSize
			if end > len(keysAndAttributes.Keys) {
				end = len(keysAndAttributes.Keys)
			}
			sliceOfKeys = append(sliceOfKeys, keysAndAttributes.Keys[i:end])
		}

		for _, slice := range sliceOfKeys {
			out = append(out, &dynamodb.BatchGetItemInput{
				RequestItems: map[string]*dynamodb.KeysAndAttributes{
					tableName: {
						AttributesToGet:          keysAndAttributes.AttributesToGet,
						ConsistentRead:           keysAndAttributes.ConsistentRead,
						ExpressionAttributeNames: keysAndAttributes.ExpressionAttributeNames,
						Keys:                     slice,
						ProjectionExpression:     keysAndAttributes.ProjectionExpression,
					},
				},
				ReturnConsumedCapacity: &returnConsumedCapacity,
			})
		}
	}
	return
}

