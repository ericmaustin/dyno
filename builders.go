package dyno

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtype "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ericmaustin/dyno/encoding"
)

//BatchGetItemInput is a extension to BatchGetItemInput that adds handlers for dyno api calls
type BatchGetItemInput struct {
	ddbInput       *ddb.BatchGetItemInput
	InputCallback  BatchGetItemInputCallback
	OutputCallback BatchGetItemOutputCallback
}

// NewBatchGetItemInput creates a new BatchGetItemInput with a dynamodb.BatchGetItemInput
func NewBatchGetItemInput(input *ddb.BatchGetItemInput) *BatchGetItemInput {
	return new(BatchGetItemInput).SetInput(input)
}

//SetInput sets the BatchGetItemInput's *dynamodb.BatchGetItemInput
func (input *BatchGetItemInput) SetInput(in *ddb.BatchGetItemInput) *BatchGetItemInput {
	input.ddbInput = in
	return input
}

//SetInputCallback sets the BatchGetItemInputCallback
func (input *BatchGetItemInput) SetInputCallback(cb BatchGetItemInputCallback) *BatchGetItemInput {
	input.InputCallback = cb
	return input
}

//SetOutputCallback sets the BatchGetItemInputCallback
func (input *BatchGetItemInput) SetOutputCallback(cb BatchGetItemOutputCallback) *BatchGetItemInput {
	input.OutputCallback = cb
	return input
}

// ChunkBatchGetItemInputs chunks the input dynamodb.BatchGetItemBuilder into chunks of the given chunkSize
// note: chunks are not deep copies!!
func ChunkBatchGetItemInputs(input *BatchGetItemInput, chunkSize int) (out []*BatchGetItemInput) {
	for tableName, keysAndAttributes := range input.ddbInput.RequestItems {
		sliceOfKeys := make([][]map[string]ddbtype.AttributeValue, 0)

		for i := 0; i < len(keysAndAttributes.Keys); i += chunkSize {
			end := i + chunkSize
			if end > len(keysAndAttributes.Keys) {
				end = len(keysAndAttributes.Keys)
			}
			sliceOfKeys = append(sliceOfKeys, keysAndAttributes.Keys[i:end])
		}

		for _, slice := range sliceOfKeys {
			newInput := NewBatchGetItemInput(nil)
			newInput.ddbInput = &ddb.BatchGetItemInput{
				RequestItems: map[string]ddbtype.KeysAndAttributes{
					tableName: {
						AttributesToGet:          keysAndAttributes.AttributesToGet,
						ConsistentRead:           keysAndAttributes.ConsistentRead,
						ExpressionAttributeNames: keysAndAttributes.ExpressionAttributeNames,
						Keys:                     slice,
						ProjectionExpression:     keysAndAttributes.ProjectionExpression,
					},
				},
				ReturnConsumedCapacity: input.ddbInput.ReturnConsumedCapacity,
			}
			out = append(out, newInput)
		}
	}
	return
}

// BatchGetItemBuilder used to dynamically build a BatchGetItemBuilder
type BatchGetItemBuilder struct {
	*ddb.BatchGetItemInput
	projection *expression.ProjectionBuilder
}

// NewBatchGetBuilder creates a new BatchGetItemBuilder
func NewBatchGetBuilder() *BatchGetItemBuilder {
	return &BatchGetItemBuilder{
		BatchGetItemInput: &ddb.BatchGetItemInput{
			RequestItems:           make(map[string]ddbtype.KeysAndAttributes),
			ReturnConsumedCapacity: ddbtype.ReturnConsumedCapacityNone,
		},
	}
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

// Build builds and returns a BatchGetItemInput
func (bld *BatchGetItemBuilder) Build(inputCB BatchGetItemInputCallback, outputCB BatchGetItemOutputCallback) (*BatchGetItemInput, error) {
	ddbInput, err := bld.BuildDynamodbInput()
	if err != nil {
		return nil, err
	}
	return NewBatchGetItemInput(ddbInput).
		SetInputCallback(inputCB).
		SetOutputCallback(outputCB), nil
}

// BuildDynamodbInput builds and returns the dynamodb.BatchGetItemOutput
func (bld *BatchGetItemBuilder) BuildDynamodbInput() (*ddb.BatchGetItemInput, error) {
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

type CreateTableInput struct {
	ddbInput       *ddb.CreateTableInput
	InputCallback  CreateTableInputCallback
	OutputCallback CreateTableOutputCallback
}

// NewCreateTableInput creates a new CreateTableInput with a dynamodb.CreateTableInput
func NewCreateTableInput(input *ddb.CreateTableInput) *CreateTableInput {
	return new(CreateTableInput).SetInput(input)
}

//SetInput sets the CreateTableInput CreateTableInput
func (bld *CreateTableInput) SetInput(input *ddb.CreateTableInput) *CreateTableInput {
	bld.ddbInput = input
	return bld
}

//SetInputCallback sets the CreateTableHandler InputCallback
func (bld *CreateTableInput) SetInputCallback(inputCB CreateTableInputCallback) *CreateTableInput {
	bld.InputCallback = inputCB
	return bld
}

//SetOutputCallback sets the CreateTableHandler OutputCallback
func (bld *CreateTableInput) SetOutputCallback(outputCB CreateTableOutputCallback) *CreateTableInput {
	bld.OutputCallback = outputCB
	return bld
}

// CreateTableBuilder is used to construct a CreateTableBuilder dynamically
type CreateTableBuilder struct {
	*ddb.CreateTableInput
	pendingAttributeDefinitions []ddbtype.AttributeDefinition
}

// NewCreateTableBuilder creates a new CreateTableBuilder
func NewCreateTableBuilder() *CreateTableBuilder {
	return &CreateTableBuilder{
		CreateTableInput: &ddb.CreateTableInput{
			BillingMode: ddbtype.BillingModePayPerRequest,
		},
	}
}

// SetProvisionedThroughputCapacityUnits sets the provisioned write and read throughput for this table
func (bld *CreateTableBuilder) SetProvisionedThroughputCapacityUnits(rcu, wcu int64) *CreateTableBuilder {
	bld.SetProvisionedThroughput(&ddbtype.ProvisionedThroughput{
		ReadCapacityUnits:  &rcu,
		WriteCapacityUnits: &wcu,
	})
	bld.BillingMode = ddbtype.BillingModeProvisioned
	return bld
}

// AddAttributeDefinition adds an attribute definition to the builder
func (bld *CreateTableBuilder) AddAttributeDefinition(attribute ddbtype.AttributeDefinition) *CreateTableBuilder {
	bld.pendingAttributeDefinitions = append(bld.pendingAttributeDefinitions, attribute)
	return bld
}

// AddGlobalIndex adds one or more local global indexes to the builder
func (bld *CreateTableBuilder) AddGlobalIndex(gsi ...ddbtype.GlobalSecondaryIndex) *CreateTableBuilder {
	bld.GlobalSecondaryIndexes = append(bld.GlobalSecondaryIndexes, gsi...)
	return bld
}

// AddLocalIndex adds one or more local secondary indexes to the builder
func (bld *CreateTableBuilder) AddLocalIndex(lsi ...ddbtype.LocalSecondaryIndex) *CreateTableBuilder {
	bld.LocalSecondaryIndexes = append(bld.LocalSecondaryIndexes, lsi...)
	return bld
}

// AddTag adds a tag to the CreateTableBuilder using a key value pair of strings
func (bld *CreateTableBuilder) AddTag(key, value string) *CreateTableBuilder {
	bld.Tags = append(bld.Tags, ddbtype.Tag{
		Key:   &key,
		Value: &value,
	})
	return bld
}

// AddTags adds tags to the CreateTableBuilder
func (bld *CreateTableBuilder) AddTags(tags ...ddbtype.Tag) *CreateTableBuilder {
	bld.Tags = append(bld.Tags, tags...)
	return bld
}

// AddTagsFromMap adds tags to the CreateTableBuilder using a map of strings
func (bld *CreateTableBuilder) AddTagsFromMap(tags map[string]string) *CreateTableBuilder {
	for key, value := range tags {
		bld.AddTag(key, value)
	}
	return bld
}

// SetAttributeDefinitions sets the AttributeDefinitions field's value.
func (bld *CreateTableBuilder) SetAttributeDefinitions(v []ddbtype.AttributeDefinition) *CreateTableBuilder {
	bld.AttributeDefinitions = v
	return bld
}

// SetBillingMode sets the BillingMode field's value.
func (bld *CreateTableBuilder) SetBillingMode(v ddbtype.BillingMode) *CreateTableBuilder {
	bld.BillingMode = v
	return bld
}

// SetGlobalSecondaryIndexes sets the GlobalSecondaryIndexes field's value.
func (bld *CreateTableBuilder) SetGlobalSecondaryIndexes(v []ddbtype.GlobalSecondaryIndex) *CreateTableBuilder {
	bld.GlobalSecondaryIndexes = v
	return bld
}

// SetKeySchema sets the KeySchema field's value.
func (bld *CreateTableBuilder) SetKeySchema(v []ddbtype.KeySchemaElement) *CreateTableBuilder {
	bld.KeySchema = v
	return bld
}

// SetLocalSecondaryIndexes sets the LocalSecondaryIndexes field's value.
func (bld *CreateTableBuilder) SetLocalSecondaryIndexes(v []ddbtype.LocalSecondaryIndex) *CreateTableBuilder {
	bld.LocalSecondaryIndexes = v
	return bld
}

// SetProvisionedThroughput sets the ProvisionedThroughput field's value.
func (bld *CreateTableBuilder) SetProvisionedThroughput(v *ddbtype.ProvisionedThroughput) *CreateTableBuilder {
	bld.ProvisionedThroughput = v
	return bld
}

// SetSSESpecification sets the SSESpecification field's value.
func (bld *CreateTableBuilder) SetSSESpecification(v *ddbtype.SSESpecification) *CreateTableBuilder {
	bld.SSESpecification = v
	return bld
}

// SetStreamSpecification sets the StreamSpecification field's value.
func (bld *CreateTableBuilder) SetStreamSpecification(v *ddbtype.StreamSpecification) *CreateTableBuilder {
	bld.StreamSpecification = v
	return bld
}

// SetTableName sets the TableName field's value.
func (bld *CreateTableBuilder) SetTableName(v string) *CreateTableBuilder {
	bld.TableName = &v
	return bld
}

// SetTags sets the Tags field's value.
func (bld *CreateTableBuilder) SetTags(v []ddbtype.Tag) *CreateTableBuilder {
	bld.Tags = v
	return bld
}

// Build builds a CreateTableBuilder
func (bld *CreateTableBuilder) Build(inputCB CreateTableInputCallback, outputCB CreateTableOutputCallback) (out *CreateTableInput, err error) {
	var ddbInput *ddb.CreateTableInput
	if ddbInput, err = bld.BuildDynamodbInput(); err != nil {
		return nil, err
	}
	return NewCreateTableInput(ddbInput).
		SetInputCallback(inputCB).
		SetOutputCallback(outputCB), err
}

// BuildDynamodbInput builds the dynamodb.CreateTableBuilder
func (bld *CreateTableBuilder) BuildDynamodbInput() (*ddb.CreateTableInput, error) {

	if bld.pendingAttributeDefinitions != nil {
		for _, ad := range bld.pendingAttributeDefinitions {
			for _, attr := range bld.AttributeDefinitions {
				// don't add duplicate attribute names
				if *attr.AttributeName == *ad.AttributeName {
					if attr.AttributeType == ad.AttributeType {
						continue
					}
					return nil, fmt.Errorf("cannot add duplicate attribute with mismatched type."+
						"attrubuteName = %s, attributeTypes = %s, %s",
						*attr.AttributeName, attr.AttributeType, ad.AttributeType)
				}
			}
		}
	}
	if bld.GlobalSecondaryIndexes != nil {
		for _, gsi := range bld.GlobalSecondaryIndexes {
			if bld.BillingMode == ddbtype.BillingModePayPerRequest {
				gsi.ProvisionedThroughput = nil
			}
		}
	}
	return bld.CreateTableInput, nil
}

//DeleteTableInput extends dynamodb.DeleteTableInput and is used as the input for Request.DeleteTable
type DeleteTableInput struct {
	ddbInput       *ddb.DeleteTableInput
	InputCallback  DeleteTableInputCallback
	OutputCallback DeleteTableOutputCallback
}

//NewDeleteTableInput creates a new DeleteTableInput with a dynamodb.DeleteTableOpt
func NewDeleteTableInput(input *ddb.DeleteTableInput) *DeleteTableInput {
	return new(DeleteTableInput).SetInput(input)
}

//SetInput sets the CreateTableInput CreateTableInput
func (input *DeleteTableInput) SetInput(in *ddb.DeleteTableInput) *DeleteTableInput {
	input.ddbInput = in
	return input
}

//SetInputCallback sets the DeleteItemInputCallback callback
func (input *DeleteTableInput) SetInputCallback(cb DeleteTableInputCallback) *DeleteTableInput {
	input.InputCallback = cb
	return input
}

//SetOutputCallback sets the DeleteItemOutputCallback callback
func (input *DeleteTableInput) SetOutputCallback(cb DeleteTableOutputCallback) *DeleteTableInput {
	input.OutputCallback = cb
	return input
}

// no DeleteTableBuilder is needed

// DeleteItemInput extends dynamodb.DeleteItemInput to support condition building
type DeleteItemInput struct {
	ddbInput       *ddb.DeleteItemInput
	InputCallback  DeleteItemInputCallback
	OutputCallback DeleteItemOutputCallback
}

//NewDeleteItemInput creates a new DeleteItemInput with a dynamodb.DeleteItemInput
func NewDeleteItemInput(input *ddb.DeleteItemInput) *DeleteItemInput {
	return new(DeleteItemInput).SetInput(input)
}

//SetInput sets the DeleteItemInput
func (input *DeleteItemInput) SetInput(in *ddb.DeleteItemInput) *DeleteItemInput {
	input.ddbInput = in
	return input
}

//SetInputCallback sets the DeleteItemInputCallback callback
func (input *DeleteItemInput) SetInputCallback(cb DeleteItemInputCallback) *DeleteItemInput {
	input.InputCallback = cb
	return input
}

//SetOutputCallback sets the DeleteItemOutputCallback callback
func (input *DeleteItemInput) SetOutputCallback(cb DeleteItemOutputCallback) *DeleteItemInput {
	input.OutputCallback = cb
	return input
}


// GetItemInput extends dynamodb.GetItemInput to support condition building
type GetItemInput struct {
	ddbInput       *ddb.GetItemInput
	InputCallback  GetItemInputCallback
	OutputCallback GetItemOutputCallback
}

//NewGetItemInput creates a new GetItemInput with a dynamodb.GetItemInput
func NewGetItemInput(input *ddb.GetItemInput) *GetItemInput {
	return new(GetItemInput).SetInput(input)
}

//SetInput sets the GetItemInput
func (input *GetItemInput) SetInput(in *ddb.GetItemInput) *GetItemInput {
	input.ddbInput = in
	return input
}

//SetInputCallback sets the GetItemInputCallback callback
func (input *GetItemInput) SetInputCallback(cb GetItemInputCallback) *GetItemInput {
	input.InputCallback = cb
	return input
}

//SetOutputCallback sets the GetItemOutputCallback callback
func (input *GetItemInput) SetOutputCallback(cb GetItemOutputCallback) *GetItemInput {
	input.OutputCallback = cb
	return input
}




//RestoreTableFromBackupInput represents the input used in Request.RestoreTableFromBackup
type RestoreTableFromBackupInput struct {
	ddbInput       *ddb.RestoreTableFromBackupInput
	InputCallback  RestoreTableFromBackupInputCallback
	OutputCallback RestoreTableFromBackupOutputCallback
}

// NewRestoreTableFromBackupInput creates a new RestoreTableFromBackupInput with QueryOpt
func NewRestoreTableFromBackupInput(input *ddb.RestoreTableFromBackupInput) *RestoreTableFromBackupInput {
	return &RestoreTableFromBackupInput{
		ddbInput: input,
	}
}

// SetInput sets the RestoreTableFromBackupInput
func (input *RestoreTableFromBackupInput) SetInput(in *ddb.RestoreTableFromBackupInput) *RestoreTableFromBackupInput {
	input.ddbInput = in
	return input
}

// SetInputCallback sets the RestoreTableFromBackupInputCallback
func (input *RestoreTableFromBackupInput) SetInputCallback(cb RestoreTableFromBackupInputCallback) *RestoreTableFromBackupInput {
	input.InputCallback = cb
	return input
}

// SetOutputCallback sets the RestoreTableFromBackupOutputCallback
func (input *RestoreTableFromBackupInput) SetOutputCallback(cb RestoreTableFromBackupOutputCallback) *RestoreTableFromBackupInput {
	input.OutputCallback = cb
	return input
}

//BatchWriteItemInput represents the input used in Request.BatchWriteItem
type BatchWriteItemInput struct {
	ddbInput       *ddb.BatchWriteItemInput
	InputCallback  BatchWriteItemInputCallback
	OutputCallback BatchWriteItemOutputCallback
}

// NewBatchWriteItemInput creates a new BatchWriteItemInput with QueryOpt
func NewBatchWriteItemInput(input *ddb.BatchWriteItemInput) *BatchWriteItemInput {
	return &BatchWriteItemInput{
		ddbInput: input,
	}
}

// SetInput sets the BatchWriteItemInput
func (input *BatchWriteItemInput) SetInput(in *ddb.BatchWriteItemInput) *BatchWriteItemInput {
	input.ddbInput = in
	return input
}

// SetInputCallback sets the BatchWriteItemInputCallback
func (input *BatchWriteItemInput) SetInputCallback(cb BatchWriteItemInputCallback) *BatchWriteItemInput {
	input.InputCallback = cb
	return input
}

// SetOutputCallback sets the BatchWriteItemOutputCallback
func (input *BatchWriteItemInput) SetOutputCallback(cb BatchWriteItemOutputCallback) *BatchWriteItemInput {
	input.OutputCallback = cb
	return input
}

type BatchWriteItemBuilder struct {
	*ddb.BatchWriteItemInput
}

//NewBatchWriteItemBuilder creates a new BatchWriteItemBuilder
func NewBatchWriteItemBuilder() *BatchWriteItemBuilder {
	return &BatchWriteItemBuilder{
		BatchWriteItemInput: &ddb.BatchWriteItemInput{
			RequestItems:                make(map[string][]ddbtype.WriteRequest),
			ReturnConsumedCapacity:      ddbtype.ReturnConsumedCapacityNone,
			ReturnItemCollectionMetrics: ddbtype.ReturnItemCollectionMetricsNone,
		},
	}
}

// SetRequestItems sets the RequestItems field's value.
func (bld *BatchWriteItemBuilder) SetRequestItems(v map[string][]ddbtype.WriteRequest) *BatchWriteItemBuilder {
	bld.RequestItems = v
	return bld
}

// SetReturnConsumedCapacity sets the ReturnConsumedCapacity field's value.
func (bld *BatchWriteItemBuilder) SetReturnConsumedCapacity(v ddbtype.ReturnConsumedCapacity) *BatchWriteItemBuilder {
	bld.ReturnConsumedCapacity = v
	return bld
}

// SetReturnItemCollectionMetrics sets the ReturnItemCollectionMetrics field's value.
func (bld *BatchWriteItemBuilder) SetReturnItemCollectionMetrics(v ddbtype.ReturnItemCollectionMetrics) *BatchWriteItemBuilder {
	bld.ReturnItemCollectionMetrics = v
	return bld
}

// AddWriteRequests adds one or more WriteRequests for a given table to the input
func (bld *BatchWriteItemBuilder) AddWriteRequests(tableName string, requests ...ddbtype.WriteRequest) *BatchWriteItemBuilder {
	if _, ok := bld.RequestItems[tableName]; !ok {
		bld.RequestItems[tableName] = make([]ddbtype.WriteRequest, len(requests))
		for i, req := range requests {
			bld.RequestItems[tableName][i] = req
		}
		return bld
	}
	bld.RequestItems[tableName] = append(bld.RequestItems[tableName], requests...)
	return bld
}

// AddPuts adds multiple put requests from a given input that should be a slice of structs or maps
func (bld *BatchWriteItemBuilder) AddPuts(table string, items ...map[string]ddbtype.AttributeValue) *BatchWriteItemBuilder {

	w := make([]ddbtype.WriteRequest, len(items))
	for i, item := range items {
		w[i] = ddbtype.WriteRequest{
			PutRequest: &ddbtype.PutRequest{Item: item},
		}
	}
	return bld.AddWriteRequests(table, w...)
}

// AddDeletes adds a delete requests to the input
func (bld *BatchWriteItemBuilder) AddDeletes(table string, itemKeys ...map[string]ddbtype.AttributeValue) *BatchWriteItemBuilder {

	w := make([]ddbtype.WriteRequest, len(itemKeys))
	for i, item := range itemKeys {
		w[i] = ddbtype.WriteRequest{
			DeleteRequest: &ddbtype.DeleteRequest{Key: item},
		}
	}

	return bld.AddWriteRequests(table, w...)
}

// Build builds the UpdateItemInput
func (bld *BatchWriteItemBuilder) Build(inputCB BatchWriteItemInputCallback, outputCB BatchWriteItemOutputCallback) *BatchWriteItemInput {
	return NewBatchWriteItemInput(bld.BatchWriteItemInput).
		SetInputCallback(inputCB).
		SetOutputCallback(outputCB)
}

// BuildDynamodbInput builds the dynamodb.UpdateItemInput
func (bld *BatchWriteItemBuilder) BuildDynamodbInput() *ddb.BatchWriteItemInput {
	return bld.BatchWriteItemInput
}

// ListTablesInput is used as the input to Request.ListTables
type ListTablesInput struct {
	ddbInput       *ddb.ListTablesInput
	InputCallback  ListTablesInputCallback
	OutputCallback ListTablesOutputCallback
}

// NewListTablesInput creates a new ListTablesInput with QueryOpt
func NewListTablesInput(input *ddb.ListTablesInput) *ListTablesInput {
	return &ListTablesInput{
		ddbInput: input,
	}
}

// SetInput sets the ListTablesInput
func (input *ListTablesInput) SetInput(in *ddb.ListTablesInput) *ListTablesInput {
	input.ddbInput = in
	return input
}

// SetInputCallback sets the ListTablesInputCallback
func (input *ListTablesInput) SetInputCallback(cb ListTablesInputCallback) *ListTablesInput {
	input.InputCallback = cb
	return input
}

// SetOutputCallback sets the ListTablesOutputCallback
func (input *ListTablesInput) SetOutputCallback(cb ListTablesOutputCallback) *ListTablesInput {
	input.OutputCallback = cb
	return input
}

//DescribeTableInput represents input to Request.DescribeTable
type DescribeTableInput struct {
	ddbInput *ddb.DescribeTableInput
	InputCallback  DescribeTableInputCallback
	OutputCallback DescribeTableOutputCallback
}

//CreateBackupInput represents input to Request.CreateBackup
type CreateBackupInput struct {
	ddbInput *ddb.CreateBackupInput
	InputCallback  CreateBackupInputCallback
	OutputCallback CreateBackupOutputCallback
}

// NewCreateBackupInput creates a new CreateBackupInput with QueryOpt
func NewCreateBackupInput(input *ddb.CreateBackupInput) *CreateBackupInput {
	return &CreateBackupInput{
		ddbInput: input,
	}
}

// SetInput sets the CreateBackupInput
func (input *CreateBackupInput) SetInput(in *ddb.CreateBackupInput) *CreateBackupInput {
	input.ddbInput = in
	return input
}

// SetInputCallback sets the CreateBackupInputCallback
func (input *CreateBackupInput) SetInputCallback(cb CreateBackupInputCallback) *CreateBackupInput {
	input.InputCallback = cb
	return input
}

// SetOutputCallback sets the CreateBackupOutputCallback
func (input *CreateBackupInput) SetOutputCallback(cb CreateBackupOutputCallback) *CreateBackupInput {
	input.OutputCallback = cb
	return input
}

//DescribeBackupInput represents input to Request.DescribeBackup
type DescribeBackupInput struct {
	ddbInput *ddb.DescribeBackupInput
	InputCallback  DescribeBackupInputCallback
	OutputCallback DescribeBackupOutputCallback
}
