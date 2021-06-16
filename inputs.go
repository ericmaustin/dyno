package dyno

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtype "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ericmaustin/dyno/condition"
	"github.com/ericmaustin/dyno/encoding"
	"github.com/ericmaustin/dyno/util"
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
	ddbType        *ddb.CreateTableInput
	InputCallback  CreateTableInputCallback
	OutputCallback CreateTableOutputCallback
}

// NewCreateTableInput creates a new CreateTableInput with a dynamodb.CreateTableInput
func NewCreateTableInput(input *ddb.CreateTableInput) *CreateTableInput {
	return new(CreateTableInput).SetInput(input)
}

//SetInput sets the CreateTableInput CreateTableInput
func (bld *CreateTableInput) SetInput(input *ddb.CreateTableInput) *CreateTableInput {
	bld.ddbType = input
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

// DeleteItemBuilder is used for dynamically building a DeleteItemInput
type DeleteItemBuilder struct {
	*ddb.DeleteItemInput
	cnd *condition.Builder
}

// NewDeleteItemBuilder creates a new DeleteItemInput with DeleteItemOpt
func NewDeleteItemBuilder() *DeleteItemBuilder {
	return &DeleteItemBuilder{
		DeleteItemInput: &ddb.DeleteItemInput{
			ReturnConsumedCapacity:      ddbtype.ReturnConsumedCapacityNone,
			ReturnItemCollectionMetrics: ddbtype.ReturnItemCollectionMetricsNone,
			ReturnValues:                ddbtype.ReturnValueNone,
		},
	}
}

// SetKey sets the target key for the item to tbe deleted
func (bld *DeleteItemBuilder) SetKey(key map[string]ddbtype.AttributeValue) *DeleteItemBuilder {
	bld.Key = key
	return bld
}

// AddCondition adds a condition to this update
// adding multiple conditions by calling this multiple times will join the conditions with
// an AND
func (bld *DeleteItemBuilder) AddCondition(cnd expression.ConditionBuilder) *DeleteItemBuilder {
	bld.cnd.And(cnd)
	return bld
}

// SetConditionExpression sets the ConditionExpression field's value.
func (bld *DeleteItemBuilder) SetConditionExpression(v string) *DeleteItemBuilder {
	bld.ConditionExpression = &v
	return bld
}

// SetExpressionAttributeNames sets the ExpressionAttributeNames field's value.
func (bld *DeleteItemBuilder) SetExpressionAttributeNames(v map[string]string) *DeleteItemBuilder {
	bld.ExpressionAttributeNames = v
	return bld
}

// SetExpressionAttributeValues sets the ExpressionAttributeValues field's value.
func (bld *DeleteItemBuilder) SetExpressionAttributeValues(v map[string]ddbtype.AttributeValue) *DeleteItemBuilder {
	bld.ExpressionAttributeValues = v
	return bld
}

// SetReturnConsumedCapacity sets the ReturnConsumedCapacity field's value.
func (bld *DeleteItemBuilder) SetReturnConsumedCapacity(v ddbtype.ReturnConsumedCapacity) *DeleteItemBuilder {
	bld.ReturnConsumedCapacity = v
	return bld
}

// SetReturnItemCollectionMetrics sets the ReturnItemCollectionMetrics field's value.
func (bld *DeleteItemBuilder) SetReturnItemCollectionMetrics(v ddbtype.ReturnItemCollectionMetrics) *DeleteItemBuilder {
	bld.ReturnItemCollectionMetrics = v
	return bld
}

// SetReturnValues sets the ReturnValues field's value.
func (bld *DeleteItemBuilder) SetReturnValues(v ddbtype.ReturnValue) *DeleteItemBuilder {
	bld.ReturnValues = v
	return bld
}

// SetTableName sets the TableName field's value.
func (bld *DeleteItemBuilder) SetTableName(v string) *DeleteItemBuilder {
	bld.TableName = &v
	return bld
}

func (bld *DeleteItemBuilder) Build(inputCB DeleteItemInputCallback, outputCB DeleteItemOutputCallback) (out *DeleteItemInput, err error) {
	var ddbInput *ddb.DeleteItemInput
	if ddbInput, err = bld.BuildDynamodbInput(); err != nil {
		return nil, err
	}
	return NewDeleteItemInput(ddbInput).
		SetInputCallback(inputCB).
		SetOutputCallback(outputCB), nil
}

// BuildDynamodbInput builds the dynamodb.DeleteItemInput
// returns error if expression builder returns an error
func (bld *DeleteItemBuilder) BuildDynamodbInput() (*ddb.DeleteItemInput, error) {
	if !bld.cnd.Empty() {
		expr := expression.NewBuilder().WithCondition(bld.cnd.Builder())
		e, err := expr.Build()
		if err != nil {
			return nil, fmt.Errorf("DeleteItemInput.Build() encountered an error while attempting to build an expression: %v", err)
		}
		bld.ConditionExpression = e.Condition()
		bld.ExpressionAttributeNames = e.Names()
		bld.ExpressionAttributeValues = e.Values()
	}
	return bld.DeleteItemInput, nil
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

// GetItemBuilder is used to dynamically build a GetItemInput request
type GetItemBuilder struct {
	*ddb.GetItemInput
	projection *expression.ProjectionBuilder
}

// NewGetItemBuilder returns a new GetItemBuilder for given tableName if tableName is not nil
func NewGetItemBuilder() *GetItemBuilder {
	return &GetItemBuilder{
		GetItemInput: &ddb.GetItemInput{
			ReturnConsumedCapacity: ddbtype.ReturnConsumedCapacityNone,
		},
	}
}

// SetInput sets the GetItemBuilder's dynamodb.GetItemInput
func (bld *GetItemBuilder) SetInput(input *ddb.GetItemInput) *GetItemBuilder {
	bld.GetItemInput = input
	return bld
}

// AddProjectionNames adds additional field names to the projection with strings
func (bld *GetItemBuilder) AddProjectionNames(names ...string) *GetItemBuilder {
	addProjectionNames(bld.projection, names)
	return bld
}

// SetConsistentRead sets the ConsistentRead field's value.
func (bld *GetItemBuilder) SetConsistentRead(v bool) *GetItemBuilder {
	bld.ConsistentRead = &v
	return bld
}

// SetExpressionAttributeNames sets the ExpressionAttributeNames field's value.
func (bld *GetItemBuilder) SetExpressionAttributeNames(v map[string]string) *GetItemBuilder {
	bld.ExpressionAttributeNames = v
	return bld
}

// SetKey sets the Key field's value.
func (bld *GetItemBuilder) SetKey(v map[string]ddbtype.AttributeValue) *GetItemBuilder {
	bld.Key = v
	return bld
}

// SetProjectionExpression sets the ProjectionExpression field's value.
func (bld *GetItemBuilder) SetProjectionExpression(v string) *GetItemBuilder {
	bld.ProjectionExpression = &v
	return bld
}

// SetReturnConsumedCapacity sets the ReturnConsumedCapacity field's value.
func (bld *GetItemBuilder) SetReturnConsumedCapacity(v ddbtype.ReturnConsumedCapacity) *GetItemBuilder {
	bld.ReturnConsumedCapacity = v
	return bld
}

// SetTableName sets the TableName field's value.
func (bld *GetItemBuilder) SetTableName(v string) *GetItemBuilder {
	bld.TableName = &v
	return bld
}

// Build returns a GetItemInput
func (bld *GetItemBuilder) Build(inputCB GetItemInputCallback, outputCB GetItemOutputCallback) (out *GetItemInput, err error) {
	var ddbInput *ddb.GetItemInput
	if ddbInput, err = bld.BuildDynamodbInput(); err != nil {
		return nil, err
	}
	return NewGetItemInput(ddbInput).
		SetInputCallback(inputCB).
		SetOutputCallback(outputCB), nil
}

// BuildDynamodbInput returns a dynamodb.GetItemInput
func (bld *GetItemBuilder) BuildDynamodbInput() (*ddb.GetItemInput, error) {
	if bld.projection != nil {
		// only use expression builder if we have a projection or a filter
		eb := expression.NewBuilder()
		eb = eb.WithProjection(*bld.projection)

		// build the Expression
		expr, err := eb.Build()
		if err != nil {
			return nil, fmt.Errorf("GetItemBuilder Build() failed while attempting to build expression: %v", err)
		}
		bld.ExpressionAttributeNames = expr.Names()
		bld.ProjectionExpression = expr.Projection()
	}
	return bld.GetItemInput, nil
}

//ScanInput represents the input used in Request.Scan
type ScanInput struct {
	ddbInput       *ddb.ScanInput
	InputCallback  ScanInputCallback
	OutputCallback ScanOutputCallback
}

//NewScanInput creates a new ScanInput with ScanOpt
func NewScanInput(input *ddb.ScanInput) *ScanInput {
	return new(ScanInput).SetInput(input)
}

//SetInput sets the ScanInput
func (input *ScanInput) SetInput(in *ddb.ScanInput) *ScanInput {
	input.ddbInput = in
	return input
}

//SetInputCallback sets the ScanInputCallback
func (input *ScanInput) SetInputCallback(cb ScanInputCallback) *ScanInput {
	input.InputCallback = cb
	return input
}

//SetOutputCallback sets the ScanOutputCallback
func (input *ScanInput) SetOutputCallback(cb ScanOutputCallback) *ScanInput {
	input.OutputCallback = cb
	return input
}

//ScanBuilder extends dynamodb.ScanInput to allow dynamic input building
type ScanBuilder struct {
	*ddb.ScanInput
	filter     *expression.ConditionBuilder
	projection *expression.ProjectionBuilder
}

// NewScanBuilder creates a new scan builder with ScanOpt
func NewScanBuilder() *ScanBuilder {
	q := &ScanBuilder{
		ScanInput: &ddb.ScanInput{
			ReturnConsumedCapacity: ddbtype.ReturnConsumedCapacityNone,
			Select:                 ddbtype.SelectAllAttributes,
		},
	}
	return q
}

// SetTableName sets the TableName field's value.
func (bld *ScanBuilder) SetTableName(v string) *ScanBuilder {
	bld.TableName = &v
	return bld
}

// SetInput sets the ScanBuilder's dynamodb.ScanInput
func (bld *ScanBuilder) SetInput(input *ddb.ScanInput) *ScanBuilder {
	bld.ScanInput = input
	return bld
}

//AddProjection adds additional field names to the projection
func (bld *ScanBuilder) AddProjection(names interface{}) *ScanBuilder {
	nameBuilders := encoding.NameBuilders(names)
	if bld.projection == nil {
		proj := expression.ProjectionBuilder{}
		proj = proj.AddNames(nameBuilders...)
		bld.projection = &proj
	} else {
		*bld.projection = bld.projection.AddNames(nameBuilders...)
	}
	return bld
}

// AddProjectionNames adds additional field names to the projection with strings
func (bld *ScanBuilder) AddProjectionNames(names ...string) *ScanBuilder {
	//nameBuilders := encoding.NameBuilders(names)
	nameBuilders := make([]expression.NameBuilder, len(names))
	for i, name := range names {
		nameBuilders[i] = expression.Name(name)
	}
	if bld.projection == nil {
		proj := expression.ProjectionBuilder{}
		proj = proj.AddNames(nameBuilders...)
		bld.projection = &proj
	} else {
		*bld.projection = bld.projection.AddNames(nameBuilders...)
	}
	return bld
}

// AddFilter adds a filter condition to the scan
// adding multiple conditions by calling this multiple times will join the conditions with
// an AND
func (bld *ScanBuilder) AddFilter(cnd expression.ConditionBuilder) *ScanBuilder {
	if bld.filter == nil {
		bld.filter = &cnd
	} else {
		cnd = condition.And(*bld.filter, cnd)
		bld.filter = &cnd
	}
	return bld
}

// SetConsistentRead sets the ConsistentRead field's value.
func (bld *ScanBuilder) SetConsistentRead(v bool) *ScanBuilder {
	bld.ConsistentRead = &v
	return bld
}

// SetExclusiveStartKey sets the ExclusiveStartKey field's value.
func (bld *ScanBuilder) SetExclusiveStartKey(v map[string]ddbtype.AttributeValue) *ScanBuilder {
	bld.ExclusiveStartKey = v
	return bld
}

// SetExpressionAttributeNames sets the ExpressionAttributeNames field's value.
func (bld *ScanBuilder) SetExpressionAttributeNames(v map[string]string) *ScanBuilder {
	bld.ExpressionAttributeNames = v
	return bld
}

// SetExpressionAttributeValues sets the ExpressionAttributeValues field's value.
func (bld *ScanBuilder) SetExpressionAttributeValues(v map[string]ddbtype.AttributeValue) *ScanBuilder {
	bld.ExpressionAttributeValues = v
	return bld
}

// SetFilterExpression sets the FilterExpression field's value.
func (bld *ScanBuilder) SetFilterExpression(v string) *ScanBuilder {
	bld.FilterExpression = &v
	return bld
}

// SetIndexName sets the IndexName field's value.
func (bld *ScanBuilder) SetIndexName(v string) *ScanBuilder {
	bld.IndexName = &v
	return bld
}

// SetLimit sets the Limit field's value.
func (bld *ScanBuilder) SetLimit(v int32) *ScanBuilder {
	bld.Limit = &v
	return bld
}

// SetProjectionExpression sets the ProjectionExpression field's value.
func (bld *ScanBuilder) SetProjectionExpression(v string) *ScanBuilder {
	bld.ProjectionExpression = &v
	return bld
}

// SetReturnConsumedCapacity sets the ReturnConsumedCapacity field's value.
func (bld *ScanBuilder) SetReturnConsumedCapacity(v ddbtype.ReturnConsumedCapacity) *ScanBuilder {
	bld.ReturnConsumedCapacity = v
	return bld
}

// SetScanFilter sets the ScanFilter field's value.
func (bld *ScanBuilder) SetScanFilter(v map[string]ddbtype.Condition) *ScanBuilder {
	bld.ScanFilter = v
	return bld
}

// SetSegment sets the Segment field's value.
func (bld *ScanBuilder) SetSegment(v int32) *ScanBuilder {
	bld.Segment = &v
	return bld
}

// SetSelect sets the Select field's value.
func (bld *ScanBuilder) SetSelect(v ddbtype.Select) *ScanBuilder {
	bld.Select = v
	return bld
}

// SetTotalSegments sets the TotalSegments field's value.
func (bld *ScanBuilder) SetTotalSegments(v int32) *ScanBuilder {
	bld.TotalSegments = &v
	return bld
}

// Build builds the ScanInput
func (bld *ScanBuilder) Build(inputCB ScanInputCallback, outputCB ScanOutputCallback) (out *ScanInput, err error) {
	var ddbInput *ddb.ScanInput
	if ddbInput, err = bld.BuildDynamodbInput(); err != nil {
		return nil, err
	}
	return NewScanInput(ddbInput).
		SetInputCallback(inputCB).
		SetOutputCallback(outputCB), nil
}

// BuildDynamodbInput builds the dynamodb.ScanInput
func (bld *ScanBuilder) BuildDynamodbInput() (*ddb.ScanInput, error) {
	if bld.projection != nil || bld.filter != nil {
		// only use expression builder if we have a projection or a filter
		eb := expression.NewBuilder()
		// add projection
		if bld.projection != nil {
			eb = eb.WithProjection(*bld.projection)
		}
		// add filter
		if bld.filter != nil {
			eb = eb.WithFilter(*bld.filter)
		}
		// build the Expression
		expr, err := eb.Build()
		if err != nil {
			return nil, fmt.Errorf("ScanBuilder Build() failed while attempting to build expression: %v", err)
		}
		bld.ExpressionAttributeNames = expr.Names()
		bld.ExpressionAttributeValues = expr.Values()
		bld.FilterExpression = expr.Filter()
		bld.ProjectionExpression = expr.Projection()
	}
	return bld.ScanInput, nil
}

// BuildSegments builds the input input with included projection and creates seperate inputs for each segment
func (bld *ScanBuilder) BuildSegments(segments int32, inputCB ScanInputCallback, outputCB ScanOutputCallback) ([]*ScanInput, error) {
	input, err := bld.BuildDynamodbInput()

	if err != nil {
		return nil, err
	}

	if segments > 0 {
		bld.TotalSegments = &segments
	}

	ddbInputs := util.SplitScanIntoSegments(input, segments)
	inputs := make([]*ScanInput, len(ddbInputs))

	for i, in := range ddbInputs {
		inputs[i] = &ScanInput{
			ddbInput:       in,
			InputCallback:  inputCB,
			OutputCallback: outputCB,
		}
	}

	return inputs, nil
}

// PutItemInput represents the input used in Request.PutItem
type PutItemInput struct {
	ddbInput       *ddb.PutItemInput
	InputCallback  PutItemInputCallback
	OutputCallback PutItemOutputCallback
}

// NewPutItemInput creates a new PutItemInput with PutItemOpt
func NewPutItemInput(input *ddb.PutItemInput) *PutItemInput {
	return &PutItemInput{
		ddbInput: input,
	}
}

// SetInput sets the PutItemInput
func (input *PutItemInput) SetInput(in *ddb.PutItemInput) *PutItemInput {
	input.ddbInput = in
	return input
}

// SetInputCallback sets the PutItemInputCallback
func (input *PutItemInput) SetInputCallback(cb PutItemInputCallback) *PutItemInput {
	input.InputCallback = cb
	return input
}

// SetOutputCallback sets the PutItemOutputCallback
func (input *PutItemInput) SetOutputCallback(cb PutItemOutputCallback) *PutItemInput {
	input.OutputCallback = cb
	return input
}

// PutItemBuilder allows for dynamic building of a PutItem input
type PutItemBuilder struct {
	*ddb.PutItemInput
	cnd *condition.Builder
}

// NewPutItemBuilder creates a new PutItemBuilder with PutItemOpt
func NewPutItemBuilder() *PutItemBuilder {
	p := &PutItemBuilder{
		PutItemInput: &ddb.PutItemInput{
			ReturnConsumedCapacity:      ddbtype.ReturnConsumedCapacityNone,
			ReturnItemCollectionMetrics: ddbtype.ReturnItemCollectionMetricsNone,
			ReturnValues:                ddbtype.ReturnValueNone,
		},
		cnd: new(condition.Builder),
	}
	return p
}

// SetItem shadows dynamodb.PutItemInput and sets the item that will be used to build the put input
func (bld *PutItemBuilder) SetItem(item map[string]ddbtype.AttributeValue) *PutItemBuilder {
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
func (bld *PutItemBuilder) SetConditionalOperator(v ddbtype.ConditionalOperator) *PutItemBuilder {
	bld.ConditionalOperator = v
	return bld
}

// SetExpected sets the Expected field's value.
func (bld *PutItemBuilder) SetExpected(v map[string]ddbtype.ExpectedAttributeValue) *PutItemBuilder {
	bld.Expected = v
	return bld
}

// SetExpressionAttributeNames sets the ExpressionAttributeNames field's value.
func (bld *PutItemBuilder) SetExpressionAttributeNames(v map[string]string) *PutItemBuilder {
	bld.ExpressionAttributeNames = v
	return bld
}

// SetExpressionAttributeValues sets the ExpressionAttributeValues field's value.
func (bld *PutItemBuilder) SetExpressionAttributeValues(v map[string]ddbtype.AttributeValue) *PutItemBuilder {
	bld.ExpressionAttributeValues = v
	return bld
}

// SetReturnConsumedCapacity sets the ReturnConsumedCapacity field's value.
func (bld *PutItemBuilder) SetReturnConsumedCapacity(v ddbtype.ReturnConsumedCapacity) *PutItemBuilder {
	bld.ReturnConsumedCapacity = v
	return bld
}

// SetReturnItemCollectionMetrics sets the ReturnItemCollectionMetrics field's value.
func (bld *PutItemBuilder) SetReturnItemCollectionMetrics(v ddbtype.ReturnItemCollectionMetrics) *PutItemBuilder {
	bld.ReturnItemCollectionMetrics = v
	return bld
}

// SetReturnValues sets the ReturnValues field's value.
func (bld *PutItemBuilder) SetReturnValues(v ddbtype.ReturnValue) *PutItemBuilder {
	bld.ReturnValues = v
	return bld
}

// SetTableName sets the TableName field's value.
func (bld *PutItemBuilder) SetTableName(v string) *PutItemBuilder {
	bld.TableName = &v
	return bld
}

// BuildDynamodbInput builds the dynamodb.PutItemInput
func (bld *PutItemBuilder) BuildDynamodbInput() (*ddb.PutItemInput, error) {
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

// Build builds the PutItemInput
func (bld *PutItemBuilder) Build(inputCB PutItemInputCallback, outputCB PutItemOutputCallback) (out *PutItemInput, err error) {
	var ddbInput *ddb.PutItemInput
	if ddbInput, err = bld.BuildDynamodbInput(); err != nil {
		return
	}
	return NewPutItemInput(ddbInput).
		SetInputCallback(inputCB).
		SetOutputCallback(outputCB), nil
}

//QueryInput represents the input used in Request.Scan
type QueryInput struct {
	ddbInput       *ddb.QueryInput
	InputCallback  QueryInputCallback
	OutputCallback QueryOutputCallback
}

// NewQueryInput creates a new QueryInput with QueryOpt
func NewQueryInput(input *ddb.QueryInput) *QueryInput {
	return &QueryInput{
		ddbInput: input,
	}
}

// SetInput sets the QueryInput
func (input *QueryInput) SetInput(in *ddb.QueryInput) *QueryInput {
	input.ddbInput = in
	return input
}

// SetInputCallback sets the QueryInputCallback
func (input *QueryInput) SetInputCallback(cb QueryInputCallback) *QueryInput {
	input.InputCallback = cb
	return input
}

// SetOutputCallback sets the QueryOutputCallback
func (input *QueryInput) SetOutputCallback(cb QueryOutputCallback) *QueryInput {
	input.OutputCallback = cb
	return input
}

// QueryBuilder dynamically constructs a QueryInput
type QueryBuilder struct {
	*ddb.QueryInput
	keyCnd     *expression.KeyConditionBuilder
	filter     *expression.ConditionBuilder
	projection *expression.ProjectionBuilder
}

// NewQueryBuilder creates a new QueryBuilder builder with QueryOpt
func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{
		QueryInput: &ddb.QueryInput{
			ReturnConsumedCapacity: ddbtype.ReturnConsumedCapacityNone,
			Select:                 ddbtype.SelectAllAttributes,
		},
	}
}

// SetAscOrder sets the query to return in ascending order
func (bld *QueryBuilder) SetAscOrder() *QueryBuilder {
	b := true
	bld.ScanIndexForward = &b
	return bld
}

// SetDescOrder sets the query to return in descending order
func (bld *QueryBuilder) SetDescOrder() *QueryBuilder {
	b := false
	bld.ScanIndexForward = &b
	return bld
}

// AddKeyCondition adds a key condition to this update
// adding multiple conditions by calling this multiple times will join the conditions with
// an AND
func (bld *QueryBuilder) AddKeyCondition(cnd expression.KeyConditionBuilder) *QueryBuilder {
	if bld.keyCnd == nil {
		bld.keyCnd = &cnd
	} else {
		cnd = condition.KeyAnd(*bld.keyCnd, cnd)
		bld.keyCnd = &cnd
	}
	return bld
}

// AddKeyEquals adds a equality key condition for the given fieldName and value
// this is a shortcut for adding an equality condition which is common for queries
func (bld *QueryBuilder) AddKeyEquals(fieldName string, value interface{}) *QueryBuilder {
	return bld.AddKeyCondition(condition.KeyEqual(fieldName, value))
}

// AddFilter adds a filter condition to this update
// adding multiple conditions by calling this multiple times will join the conditions with
// an AND
func (bld *QueryBuilder) AddFilter(cnd expression.ConditionBuilder) *QueryBuilder {
	if bld.filter == nil {
		bld.filter = &cnd
	} else {
		cnd = condition.And(*bld.filter, cnd)
		bld.filter = &cnd
	}
	return bld
}

// AddProjectionNames adds additional field names to the projection
func (bld *QueryBuilder) AddProjectionNames(names ...string) *QueryBuilder {
	nameBuilders := encoding.NameBuilders(names)
	if bld.projection == nil {
		proj := expression.ProjectionBuilder{}
		proj = proj.AddNames(nameBuilders...)
		bld.projection = &proj
	} else {
		*bld.projection = bld.projection.AddNames(nameBuilders...)
	}
	return bld
}

// SetAttributesToGet sets the AttributesToGet field's value.
func (bld *QueryBuilder) SetAttributesToGet(v []string) *QueryBuilder {
	bld.AttributesToGet = v
	return bld
}

// SetConditionalOperator sets the ConditionalOperator field's value.
func (bld *QueryBuilder) SetConditionalOperator(v ddbtype.ConditionalOperator) *QueryBuilder {
	bld.ConditionalOperator = v
	return bld
}

// SetConsistentRead sets the ConsistentRead field's value.
func (bld *QueryBuilder) SetConsistentRead(v bool) *QueryBuilder {
	bld.ConsistentRead = &v
	return bld
}

// SetExclusiveStartKey sets the ExclusiveStartKey field's value.
func (bld *QueryBuilder) SetExclusiveStartKey(v map[string]ddbtype.AttributeValue) *QueryBuilder {
	bld.ExclusiveStartKey = v
	return bld
}

// SetExpressionAttributeNames sets the ExpressionAttributeNames field's value.
func (bld *QueryBuilder) SetExpressionAttributeNames(v map[string]string) *QueryBuilder {
	bld.ExpressionAttributeNames = v
	return bld
}

// SetExpressionAttributeValues sets the ExpressionAttributeValues field's value.
func (bld *QueryBuilder) SetExpressionAttributeValues(v map[string]ddbtype.AttributeValue) *QueryBuilder {
	bld.ExpressionAttributeValues = v
	return bld
}

// SetFilterExpression sets the FilterExpression field's value.
func (bld *QueryBuilder) SetFilterExpression(v string) *QueryBuilder {
	bld.FilterExpression = &v
	return bld
}

// SetIndexName sets the IndexName field's value.
func (bld *QueryBuilder) SetIndexName(v string) *QueryBuilder {
	bld.IndexName = &v
	return bld
}

// SetKeyConditionExpression sets the KeyConditionExpression field's value.
func (bld *QueryBuilder) SetKeyConditionExpression(v string) *QueryBuilder {
	bld.KeyConditionExpression = &v
	return bld
}

// SetKeyConditions sets the KeyConditions field's value.
func (bld *QueryBuilder) SetKeyConditions(v map[string]ddbtype.Condition) *QueryBuilder {
	bld.KeyConditions = v
	return bld
}

// SetLimit sets the Limit field's value.
func (bld *QueryBuilder) SetLimit(v int32) *QueryBuilder {
	bld.Limit = &v
	return bld
}

// SetProjectionExpression sets the ProjectionExpression field's value.
func (bld *QueryBuilder) SetProjectionExpression(v string) *QueryBuilder {
	bld.ProjectionExpression = &v
	return bld
}

// SetQueryFilter sets the QueryFilter field's value.
func (bld *QueryBuilder) SetQueryFilter(v map[string]ddbtype.Condition) *QueryBuilder {
	bld.QueryFilter = v
	return bld
}

// SetReturnConsumedCapacity sets the ReturnConsumedCapacity field's value.
func (bld *QueryBuilder) SetReturnConsumedCapacity(v ddbtype.ReturnConsumedCapacity) *QueryBuilder {
	bld.ReturnConsumedCapacity = v
	return bld
}

// SetScanIndexForward sets the ScanIndexForward field's value.
func (bld *QueryBuilder) SetScanIndexForward(v bool) *QueryBuilder {
	bld.ScanIndexForward = &v
	return bld
}

// SetSelect sets the Select field's value.
func (bld *QueryBuilder) SetSelect(v ddbtype.Select) *QueryBuilder {
	bld.Select = v
	return bld
}

// SetTableName sets the TableName field's value.
func (bld *QueryBuilder) SetTableName(v string) *QueryBuilder {
	bld.TableName = &v
	return bld
}

// Build builds the QueryInput
func (bld *QueryBuilder) Build(inputCB QueryInputCallback, outputCB QueryOutputCallback) (out *QueryInput, err error) {
	var ddbInput *ddb.QueryInput
	if ddbInput, err = bld.BuildDynamodbInput(); err != nil {
		return
	}
	return NewQueryInput(ddbInput).
		SetInputCallback(inputCB).
		SetOutputCallback(outputCB), nil
}

// BuildDynamodbInput builds the dynamodb.QueryInput
func (bld *QueryBuilder) BuildDynamodbInput() (*ddb.QueryInput, error) {
	if bld.projection == nil && bld.keyCnd == nil && bld.filter == nil {
		// no expression builder is needed
		return bld.QueryInput, nil
	}
	builder := expression.NewBuilder()
	// add projection
	if bld.projection != nil {
		builder = builder.WithProjection(*bld.projection)
	}
	// add key condition
	if bld.keyCnd != nil {
		builder = builder.WithKeyCondition(*bld.keyCnd)
	}
	// add filter
	if bld.filter != nil {
		builder = builder.WithFilter(*bld.filter)
	}
	// build the Expression
	expr, err := builder.Build()
	if err != nil {
		return nil, err
	}

	bld.ExpressionAttributeNames = expr.Names()
	bld.ExpressionAttributeValues = expr.Values()
	bld.FilterExpression = expr.Filter()
	bld.KeyConditionExpression = expr.KeyCondition()
	bld.ProjectionExpression = expr.Projection()
	return bld.QueryInput, nil
}

//UpdateItemInput represents the input used in Request.UpdateItem
type UpdateItemInput struct {
	ddbInput       *ddb.UpdateItemInput
	InputCallback  UpdateItemInputCallback
	OutputCallback UpdateItemOutputCallback
}

// NewUpdateItemInput creates a new UpdateItemInput with QueryOpt
func NewUpdateItemInput(input *ddb.UpdateItemInput) *UpdateItemInput {
	return &UpdateItemInput{
		ddbInput: input,
	}
}

// SetInput sets the UpdateItemInput
func (input *UpdateItemInput) SetInput(in *ddb.UpdateItemInput) *UpdateItemInput {
	input.ddbInput = in
	return input
}

// SetInputCallback sets the UpdateItemInputCallback
func (input *UpdateItemInput) SetInputCallback(cb UpdateItemInputCallback) *UpdateItemInput {
	input.InputCallback = cb
	return input
}

// SetOutputCallback sets the UpdateItemOutputCallback
func (input *UpdateItemInput) SetOutputCallback(cb UpdateItemOutputCallback) *UpdateItemInput {
	input.OutputCallback = cb
	return input
}

// UpdateItemBuilder is used to build an UpdateItemInput
type UpdateItemBuilder struct {
	*ddb.UpdateItemInput
	updateBuilder expression.UpdateBuilder
	cnd           *condition.Builder
}

// NewUpdateItemBuilder creates a new UpdateItemBuilder
func NewUpdateItemBuilder() *UpdateItemBuilder {
	return &UpdateItemBuilder{
		UpdateItemInput: &ddb.UpdateItemInput{
			ReturnConsumedCapacity:      ddbtype.ReturnConsumedCapacityNone,
			ReturnItemCollectionMetrics: ddbtype.ReturnItemCollectionMetricsNone,
			ReturnValues:                ddbtype.ReturnValueNone,
		},
	}
}

// Add adds an Add operation on this update with the given field name and value
func (bld *UpdateItemBuilder) Add(field string, value interface{}) *UpdateItemBuilder {
	bld.updateBuilder = bld.updateBuilder.Add(expression.Name(field), expression.Value(value))
	return bld
}

// AddItem adds an add operation on this update with the given fields and values from an item
func (bld *UpdateItemBuilder) AddItem(item map[string]ddbtype.AttributeValue) *UpdateItemBuilder {
	for key, value := range item {
		bld.updateBuilder = bld.updateBuilder.Add(expression.Name(key), expression.Value(value))
	}
	return bld
}

// Delete adds a Delete operation on this update with the given field name and value
func (bld *UpdateItemBuilder) Delete(field string, value interface{}) *UpdateItemBuilder {
	bld.updateBuilder = bld.updateBuilder.Delete(expression.Name(field), expression.Value(value))
	return bld
}

// DeleteItem adds a delete operation on this update with the given fields and values from an item
func (bld *UpdateItemBuilder) DeleteItem(item map[string]ddbtype.AttributeValue) *UpdateItemBuilder {
	for key, value := range item {
		bld.updateBuilder = bld.updateBuilder.Delete(expression.Name(key), expression.Value(value))
	}
	return bld
}

// Remove adds one or more Remove operations on this update with the given field name
func (bld *UpdateItemBuilder) Remove(fields ...string) *UpdateItemBuilder {
	for _, field := range fields {
		bld.updateBuilder = bld.updateBuilder.Remove(expression.Name(field))
	}
	return bld
}

// Set adds a set operation on this update with the given field and value
func (bld *UpdateItemBuilder) Set(field string, value interface{}) *UpdateItemBuilder {
	bld.updateBuilder = bld.updateBuilder.Set(expression.Name(encoding.ToString(field)), expression.Value(value))
	return bld
}

// SetItem adds a set operation on this update with the given fields and values from an item
func (bld *UpdateItemBuilder) SetItem(item map[string]dynamodb.AttributeValue) *UpdateItemBuilder {
	for key, value := range item {
		bld.updateBuilder = bld.updateBuilder.Set(expression.Name(key), expression.Value(value))
	}
	return bld
}

// AddCondition adds a condition to this update
// adding multiple conditions by calling this multiple times will join the conditions with
// an AND
func (bld *UpdateItemBuilder) AddCondition(cnd expression.ConditionBuilder) *UpdateItemBuilder {
	bld.cnd.And(cnd)
	return bld
}

// SetAttributeUpdates sets the AttributeUpdates field's value.
func (bld *UpdateItemBuilder) SetAttributeUpdates(v map[string]ddbtype.AttributeValueUpdate) *UpdateItemBuilder {
	bld.AttributeUpdates = v
	return bld
}

// SetConditionExpression sets the ConditionExpression field's value.
func (bld *UpdateItemBuilder) SetConditionExpression(v string) *UpdateItemBuilder {
	bld.ConditionExpression = &v
	return bld
}

// SetConditionalOperator sets the ConditionalOperator field's value.
func (bld *UpdateItemBuilder) SetConditionalOperator(v ddbtype.ConditionalOperator) *UpdateItemBuilder {
	bld.ConditionalOperator = v
	return bld
}

// SetExpected sets the Expected field's value.
func (bld *UpdateItemBuilder) SetExpected(v map[string]ddbtype.ExpectedAttributeValue) *UpdateItemBuilder {
	bld.Expected = v
	return bld
}

// SetExpressionAttributeNames sets the ExpressionAttributeNames field's value.
func (bld *UpdateItemBuilder) SetExpressionAttributeNames(v map[string]string) *UpdateItemBuilder {
	bld.ExpressionAttributeNames = v
	return bld
}

// SetExpressionAttributeValues sets the ExpressionAttributeValues field's value.
func (bld *UpdateItemBuilder) SetExpressionAttributeValues(v map[string]ddbtype.AttributeValue) *UpdateItemBuilder {
	bld.ExpressionAttributeValues = v
	return bld
}

// SetKey sets the Key field's value.
func (bld *UpdateItemBuilder) SetKey(v map[string]ddbtype.AttributeValue) *UpdateItemBuilder {
	bld.Key = v
	return bld
}

// SetReturnConsumedCapacity sets the ReturnConsumedCapacity field's value.
func (bld *UpdateItemBuilder) SetReturnConsumedCapacity(v ddbtype.ReturnConsumedCapacity) *UpdateItemBuilder {
	bld.ReturnConsumedCapacity = v
	return bld
}

// SetReturnItemCollectionMetrics sets the ReturnItemCollectionMetrics field's value.
func (bld *UpdateItemBuilder) SetReturnItemCollectionMetrics(v ddbtype.ReturnItemCollectionMetrics) *UpdateItemBuilder {
	bld.ReturnItemCollectionMetrics = v
	return bld
}

// SetReturnValues sets the ReturnValues field's value.
func (bld *UpdateItemBuilder) SetReturnValues(v ddbtype.ReturnValue) *UpdateItemBuilder {
	bld.ReturnValues = v
	return bld
}

// SetTableName sets the TableName field's value.
func (bld *UpdateItemBuilder) SetTableName(v string) *UpdateItemBuilder {
	bld.TableName = &v
	return bld
}

// SetUpdateExpression sets the UpdateExpression field's value.
func (bld *UpdateItemBuilder) SetUpdateExpression(v string) *UpdateItemBuilder {
	bld.UpdateExpression = &v
	return bld
}

// Build builds the UpdateItemInput
func (bld *UpdateItemBuilder) Build(inputCB UpdateItemInputCallback, outputCB UpdateItemOutputCallback) (out *UpdateItemInput, err error) {
	var ddbInput *ddb.UpdateItemInput
	if ddbInput, err = bld.BuildDynamodbInput(); err != nil {
		return
	}
	return NewUpdateItemInput(ddbInput).
		SetInputCallback(inputCB).
		SetOutputCallback(outputCB), err
}

// BuildDynamodbInput builds the dynamodb.UpdateItemInput
func (bld *UpdateItemBuilder) BuildDynamodbInput() (*ddb.UpdateItemInput, error) {
	expr := expression.NewBuilder().WithUpdate(bld.updateBuilder)
	if !bld.cnd.Empty() {
		expr.WithCondition(bld.cnd.Builder())
	}
	b, err := expr.Build()
	if err != nil {
		return nil, fmt.Errorf("UpdateItemBuilder.Build() failed while attempting to build expression: %v", err)
	}
	bld.ConditionExpression = b.Condition()
	bld.ExpressionAttributeNames = b.Names()
	bld.ExpressionAttributeValues = b.Values()
	bld.UpdateExpression = b.Update()
	return bld.UpdateItemInput, nil
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
	*ddb.DescribeTableInput
	InputCallback  DescribeTableInputCallback
	OutputCallback DescribeTableOutputCallback
}

//DescribeTableOpt represents an option passed to NewRestoreTableInput
type DescribeTableOpt func(input *DescribeTableInput)

//DescribeTableWithTableName adds a table name to the DescribeTableInput
func DescribeTableWithTableName(tableName string) DescribeTableOpt {
	return func(input *DescribeTableInput) {
		input.DescribeTableInput = &ddb.DescribeTableInput{TableName: &tableName}
	}
}

//DescribeTableWithInputCallback adds a DescribeTableInputCallback to the DescribeTableInput
func DescribeTableWithInputCallback(cb DescribeTableInputCallback) DescribeTableOpt {
	return func(input *DescribeTableInput) {
		input.InputCallback = cb
	}
}

//DescribeTableWithOutputCallback adds a DescribeTableOutputCallback to the DescribeTableInput
func DescribeTableWithOutputCallback(cb DescribeTableOutputCallback) DescribeTableOpt {
	return func(input *DescribeTableInput) {
		input.OutputCallback = cb
	}
}

//NewDescribeTableInput creates a new NewDescribeTableInput from a tableName and DescribeTableHandler
func NewDescribeTableInput(opts ...DescribeTableOpt) *DescribeTableInput {
	in := new(DescribeTableInput)
	for _, opt := range opts {
		opt(in)
	}
	if in.DescribeTableInput == nil {
		in.DescribeTableInput = new(ddb.DescribeTableInput)
	}
	return in
}

//SetTableName sets the table name
func (input *DescribeTableInput) SetTableName(tableName string) *DescribeTableInput {
	input.DescribeTableInput.SetTableName(tableName)
	return input
}

//CreateBackupInput represents input to Request.CreateBackup
type CreateBackupInput struct {
	*ddb.CreateBackupInput
	InputCallback  CreateBackupInputCallback
	OutputCallback CreateBackupOutputCallback
}

//CreateBackupOpt represents an option passed to NewRestoreTableInput
type CreateBackupOpt func(input *CreateBackupInput)

//CreateBackupWithTableName adds a table name to the CreateBackupInput
func CreateBackupWithTableName(tableName string) CreateBackupOpt {
	return func(input *CreateBackupInput) {
		if input.CreateBackupInput == nil {
			input.CreateBackupInput = new(ddb.CreateBackupInput)
		}
		input.SetTableName(tableName)
	}
}

//CreateBackupWithBackupName adds a backup name to the CreateBackupInput
func CreateBackupWithBackupName(backupName string) CreateBackupOpt {
	return func(input *CreateBackupInput) {
		if input.CreateBackupInput == nil {
			input.CreateBackupInput = new(ddb.CreateBackupInput)
		}
		input.SetBackupName(backupName)
	}
}

//CreateBackupWithInputCallback adds a CreateBackupInputCallback to the CreateBackupInput
func CreateBackupWithInputCallback(cb CreateBackupInputCallback) CreateBackupOpt {
	return func(input *CreateBackupInput) {
		input.InputCallback = cb
	}
}

//CreateBackupWithOutputCallback adds a CreateBackupOutputCallback to the CreateBackupInput
func CreateBackupWithOutputCallback(cb CreateBackupOutputCallback) CreateBackupOpt {
	return func(input *CreateBackupInput) {
		input.OutputCallback = cb
	}
}

//NewCreateBackupInput creates a new CreateBackupInput with CreateBackupOpt
func NewCreateBackupInput(opts ...CreateBackupOpt) *CreateBackupInput {
	in := new(CreateBackupInput)
	for _, opt := range opts {
		opt(in)
	}
	if in.CreateBackupInput == nil {
		in.CreateBackupInput = new(ddb.CreateBackupInput)
	}
	return in
}

// SetBackupName sets the BackupName field's value.
func (input *CreateBackupInput) SetBackupName(v string) *CreateBackupInput {
	input.BackupName = &v
	return input
}

// SetTableName sets the TableName field's value.
func (input *CreateBackupInput) SetTableName(v string) *CreateBackupInput {
	input.TableName = &v
	return input
}

//DescribeBackupInput represents input to Request.DescribeBackup
type DescribeBackupInput struct {
	*ddb.DescribeBackupInput
	InputCallback  DescribeBackupInputCallback
	OutputCallback DescribeBackupOutputCallback
}

//NewDescribeBackupInput creates a new DescribeBackupInput with DescribeBackupOpt
func NewDescribeBackupInput() *DescribeBackupInput {
	return &DescribeBackupInput{
		DescribeBackupInput: new(ddb.DescribeBackupInput),
	}
}

// SetBackupArn sets the BackupArn field's value.
func (s *DescribeBackupInput) SetBackupArn(v string) *DescribeBackupInput {
	s.BackupArn = &v
	return s
}

//addProjectionNames adds a string slice of projection values to a expression.ProjectionBuilder
func addProjectionNames(projectionBuilder *expression.ProjectionBuilder, names []string) {
	//nameBuilders := encoding.NameBuilders(names)
	nameBuilders := make([]expression.NameBuilder, len(names))
	for i, name := range names {
		nameBuilders[i] = expression.Name(name)
	}
	if projectionBuilder != nil {
		*projectionBuilder = projectionBuilder.AddNames(nameBuilders...)
		return
	}

	proj := expression.ProjectionBuilder{}
	proj = proj.AddNames(nameBuilders...)
	projectionBuilder = &proj
}

//addProjection adds a projection value interface to a expression.ProjectionBuilder
func addProjection(projectionBuilder *expression.ProjectionBuilder, projection interface{}) {
	nameBuilders := encoding.NameBuilders(projection)
	if projectionBuilder != nil {
		*projectionBuilder = projectionBuilder.AddNames(nameBuilders...)
		return
	}
	proj := expression.ProjectionBuilder{}
	proj = proj.AddNames(nameBuilders...)
	projectionBuilder = &proj
}
