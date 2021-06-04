package dyno

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/ericmaustin/dyno/condition"
	"github.com/ericmaustin/dyno/encoding"
)

// BatchGetItemBuilder used to dynamically build a BatchGetItemInput
type BatchGetItemBuilder struct {
	*dynamodb.BatchGetItemInput
	projection *expression.ProjectionBuilder
}

// NewBatchGetBuilder creates a new BatchGetItemBuilder
func NewBatchGetBuilder() *BatchGetItemBuilder {
	return &BatchGetItemBuilder{
		BatchGetItemInput: &dynamodb.BatchGetItemInput{
			RequestItems: make(map[string]*dynamodb.KeysAndAttributes),
		},
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
func (builder *BatchGetItemBuilder) AddProjection(projection interface{}) *BatchGetItemBuilder {
	addProjection(builder.projection, projection)
	return builder
}

// AddProjectionNames adds additional field names to the projection with strings
func (builder *BatchGetItemBuilder) AddProjectionNames(names ...string) *BatchGetItemBuilder {
	addProjectionNames(builder.projection, names)
	return builder
}

// SetConsistentRead sets the projection for the given table name
func (builder *BatchGetItemBuilder) SetConsistentRead(tableName string, consistentRead bool) *BatchGetItemBuilder {
	builder.initTable(tableName)
	builder.RequestItems[tableName].ConsistentRead = &consistentRead
	return builder
}

// SetKeysAndAttributes sets the keys and attributes to get from the given table
func (builder *BatchGetItemBuilder) SetKeysAndAttributes(tableName string, keysAndAttributes *dynamodb.KeysAndAttributes) *BatchGetItemBuilder {
	builder.RequestItems[tableName] = keysAndAttributes
	return builder
}

// AddKey adds one or more keys to the request item map
func (builder *BatchGetItemBuilder) AddKey(tableName string, keys ...map[string]*dynamodb.AttributeValue) *BatchGetItemBuilder {
	builder.initTable(tableName)
	builder.RequestItems[tableName].Keys = append(builder.RequestItems[tableName].Keys, keys...)
	return builder
}

// Build builds and returns the BatchGetItemInput
func (builder *BatchGetItemBuilder) Build() (*dynamodb.BatchGetItemInput, error) {

	// remove all inputs that don't have any keys associated
	for tableName, keys := range builder.RequestItems {
		if keys.Keys == nil || len(keys.Keys) < 1 {
			delete(builder.RequestItems, tableName)
		}
	}
	if builder.ReturnConsumedCapacity == nil {
		builder.ReturnConsumedCapacity = StringPtr("NONE")
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

// CreateTableBuilder is used to construct a CreateTableInput dynamically
type CreateTableBuilder struct {
	*dynamodb.CreateTableInput
	pendingAttributeDefinitions []*dynamodb.AttributeDefinition
}

// NewCreateTableBuilder creates a new CreateTableBuilder
func NewCreateTableBuilder() *CreateTableBuilder {
	b := &CreateTableBuilder{
		CreateTableInput: &dynamodb.CreateTableInput{
			BillingMode: StringPtr(dynamodb.BillingModePayPerRequest),
		},
	}
	return b
}

// SetInput sets the CreateTableBuilder's dynamodb.CreateTableInput
func (b *CreateTableBuilder) SetInput(input *dynamodb.CreateTableInput) *CreateTableBuilder {
	b.CreateTableInput = input
	return b
}

// SetProvisionedThroughputCapacityUnits sets the provisioned write and read throughput for this table
func (b *CreateTableBuilder) SetProvisionedThroughputCapacityUnits(rcu, wcu int64) *CreateTableBuilder {
	b.SetProvisionedThroughput(&dynamodb.ProvisionedThroughput{
		ReadCapacityUnits:  &rcu,
		WriteCapacityUnits: &wcu,
	})
	b.SetBillingMode(dynamodb.BillingModeProvisioned)
	return b
}

// AddAttributeDefinition adds an attribute definition to the builder
func (b *CreateTableBuilder) AddAttributeDefinition(attribute *dynamodb.AttributeDefinition) *CreateTableBuilder {
	b.pendingAttributeDefinitions = append(b.pendingAttributeDefinitions, attribute)
	return b
}

// AddGlobalIndex adds one or more local global indexes to the builder
func (b *CreateTableBuilder) AddGlobalIndex(gsi ...*dynamodb.GlobalSecondaryIndex) *CreateTableBuilder {
	b.GlobalSecondaryIndexes = append(b.GlobalSecondaryIndexes, gsi...)
	return b
}

// AddLocalIndex adds one or more local secondary indexes to the builder
func (b *CreateTableBuilder) AddLocalIndex(lsi ...*dynamodb.LocalSecondaryIndex) *CreateTableBuilder {
	b.LocalSecondaryIndexes = append(b.LocalSecondaryIndexes, lsi...)
	return b
}

// AddTag adds a tag to the CreateTableInput using a key value pair of strings
func (b *CreateTableBuilder) AddTag(key, value string) *CreateTableBuilder {
	tag := &dynamodb.Tag{
		Key:   &key,
		Value: &value,
	}
	b.Tags = append(b.Tags, tag)
	return b
}

// AddTags adds tags to the CreateTableInput
func (b *CreateTableBuilder) AddTags(tags ...*dynamodb.Tag) *CreateTableBuilder {
	b.Tags = append(b.Tags, tags...)
	return b
}

// AddTagsFromMap adds tags to the CreateTableInput using a map of strings
func (b *CreateTableBuilder) AddTagsFromMap(tags map[string]string) *CreateTableBuilder {
	for key, value := range tags {
		b.AddTag(key, value)
	}
	return b
}

// Build builds the dynamodb.CreateTableInput
func (b *CreateTableBuilder) Build() (*dynamodb.CreateTableInput, error) {

	if b.pendingAttributeDefinitions != nil {
		for _, ad := range b.pendingAttributeDefinitions {
			for _, attr := range b.AttributeDefinitions {
				// don't add duplicate attribute names
				if *attr.AttributeName == *ad.AttributeName {
					if *attr.AttributeType == *ad.AttributeType {
						continue
					}
					return nil, fmt.Errorf("cannot add duplicate attribute with mismatched type."+
						"attrubuteName = %s, attributeTypes = %s, %s",
						*attr.AttributeName, *attr.AttributeType, *ad.AttributeType)
				}
			}
		}
	}

	if b.GlobalSecondaryIndexes != nil {
		for _, gsi := range b.GlobalSecondaryIndexes {
			if *b.BillingMode == dynamodb.BillingModePayPerRequest {
				gsi.ProvisionedThroughput = nil
			}
		}
	}

	return b.CreateTableInput, nil
}

// DeleteItemBuilder extends dynamodb.DeleteItemInput to support condition building
type DeleteItemBuilder struct {
	*dynamodb.DeleteItemInput
	cnd *expression.ConditionBuilder
}

// NewDeleteItemBuilder creates a new DeleteItemBuilder for a given table if tableName is not nil
func NewDeleteItemBuilder(tableName *string) *DeleteItemBuilder {
	return &DeleteItemBuilder{
		DeleteItemInput: &dynamodb.DeleteItemInput{
			TableName: tableName,
		},
	}
}

// SetKey sets the target key for the item to tbe deleted
func (builder *DeleteItemBuilder) SetKey(key map[string]*dynamodb.AttributeValue) *DeleteItemBuilder {
	builder.Key = key
	return builder
}

// AddCondition adds a condition to this update
// adding multiple conditions by calling this multiple times will join the conditions with
// an AND
func (builder *DeleteItemBuilder) AddCondition(cnd expression.ConditionBuilder) *DeleteItemBuilder {
	if builder.cnd == nil {
		builder.cnd = &cnd
	} else {
		cnd = condition.And(*builder.cnd, cnd)
		builder.cnd = &cnd
	}
	return builder
}

// Build builds the dynamodb.DeleteItemInput
// returns error if expression builder returns an error
func (builder *DeleteItemBuilder) Build() (*dynamodb.DeleteItemInput, error) {
	if builder.cnd != nil {
		expr := expression.NewBuilder().WithCondition(*builder.cnd)
		e, err := expr.Build()
		if err != nil {
			return nil, err
		}
		builder.ConditionExpression = e.Condition()
		builder.ExpressionAttributeNames = e.Names()
		builder.ExpressionAttributeValues = e.Values()
	}
	return builder.DeleteItemInput, nil
}

//DeleteTableInput creates a new delete table input for the given table name
func DeleteTableInput(tableName string) *dynamodb.DeleteTableInput {
	return &dynamodb.DeleteTableInput{TableName: &tableName}
}

// GetItemBuilder is used to dynamically build a get request
type GetItemBuilder struct {
	*dynamodb.GetItemInput
	projection *expression.ProjectionBuilder
}

// NewGetBuilder returns a new GetItemBuilder for given tableName if tableName is not nil
func NewGetBuilder(tbl *string) *GetItemBuilder {
	return &GetItemBuilder{
		GetItemInput: &dynamodb.GetItemInput{
			TableName: tbl,
		},
	}
}

// SetInput sets the GetItemBuilder's dynamodb.GetItemInput
func (builder *GetItemBuilder) SetInput(input *dynamodb.GetItemInput) *GetItemBuilder {
	builder.GetItemInput = input
	return builder
}

// SetKey sets the key for the get input
func (builder *GetItemBuilder) SetKey(key interface{}) *GetItemBuilder {
	builder.Key = encoding.MustMarshalItem(key)
	return builder
}

// AddProjection adds additional field names to the projection
func (builder *GetItemBuilder) AddProjection(projection interface{}) *GetItemBuilder {
	addProjection(builder.projection, projection)
	return builder
}

// AddProjectionNames adds additional field names to the projection with strings
func (builder *GetItemBuilder) AddProjectionNames(names ...string) *GetItemBuilder {
	addProjectionNames(builder.projection, names)
	return builder
}

// Build returns a GetOperation using the GetItemInput and ProjectionBuilder
// returns error if expression builder returns an error
func (builder *GetItemBuilder) Build() (*dynamodb.GetItemInput, error) {
	if builder.projection != nil {
		// only use expression builder if we have a projection or a filter
		eb := expression.NewBuilder()
		eb = eb.WithProjection(*builder.projection)

		// build the Expression
		expr, err := eb.Build()
		if err != nil {
			return nil, fmt.Errorf("GetItemBuilder Build() failed while attempting to build expression: %v", err)
		}
		builder.ExpressionAttributeNames = expr.Names()
		builder.ProjectionExpression = expr.Projection()
	}
	return builder.GetItemInput, nil
}

var validScanSelect = map[string]struct{}{
	dynamodb.SelectCount:                  {},
	dynamodb.SelectAllAttributes:          {},
	dynamodb.SelectSpecificAttributes:     {},
	dynamodb.SelectAllProjectedAttributes: {},
}

//ScanBuilder extends dynamodb.ScanInput too allow dynamic input building
type ScanBuilder struct {
	*dynamodb.ScanInput
	filter     *expression.ConditionBuilder
	projection *expression.ProjectionBuilder
}

// NewScanBuilder creates a new scan builder for provided table if tableName is not nil
func NewScanBuilder(tableName *string) *ScanBuilder {
	q := &ScanBuilder{
		ScanInput: &dynamodb.ScanInput{
			TableName: tableName,
		},
	}
	return q
}

// SetInput sets the ScanBuilder's dynamodb.ScanInput
func (s *ScanBuilder) SetInput(input *dynamodb.ScanInput) *ScanBuilder {
	s.ScanInput = input
	return s
}

// SetSelect sets the ScanBuilder's Select value
func (s *ScanBuilder) SetSelect(sel string) *ScanBuilder {
	if _, ok := validScanSelect[sel]; !ok {
		panic(fmt.Errorf("select %s is not valid", sel))
	}
	s.ScanInput.SetSelect(sel)
	return s
}

//AddProjection adds additional field names to the projection
func (s *ScanBuilder) AddProjection(names interface{}) *ScanBuilder {
	nameBuilders := encoding.NameBuilders(names)
	if s.projection == nil {
		proj := expression.ProjectionBuilder{}
		proj = proj.AddNames(nameBuilders...)
		s.projection = &proj
	} else {
		*s.projection = s.projection.AddNames(nameBuilders...)
	}
	return s
}

// AddProjectionNames adds additional field names to the projection with strings
func (s *ScanBuilder) AddProjectionNames(names ...string) *ScanBuilder {
	//nameBuilders := encoding.NameBuilders(names)
	nameBuilders := make([]expression.NameBuilder, len(names))
	for i, name := range names {
		nameBuilders[i] = expression.Name(name)
	}
	if s.projection == nil {
		proj := expression.ProjectionBuilder{}
		proj = proj.AddNames(nameBuilders...)
		s.projection = &proj
	} else {
		*s.projection = s.projection.AddNames(nameBuilders...)
	}
	return s
}

// AddFilter adds a filter condition to the scan
// adding multiple conditions by calling this multiple times will join the conditions with
// an AND
func (s *ScanBuilder) AddFilter(cnd expression.ConditionBuilder) *ScanBuilder {
	if s.filter == nil {
		s.filter = &cnd
	} else {
		cnd = condition.And(*s.filter, cnd)
		s.filter = &cnd
	}
	return s
}

// Build builds the input input with included projection, key conditions, and filters
func (s *ScanBuilder) Build() (*dynamodb.ScanInput, error) {
	if s.projection != nil || s.filter != nil {
		// only use expression builder if we have a projection or a filter
		builder := expression.NewBuilder()
		// add projection
		if s.projection != nil {
			builder = builder.WithProjection(*s.projection)
		}
		// add filter
		if s.filter != nil {
			builder = builder.WithFilter(*s.filter)
		}
		// build the Expression
		expr, err := builder.Build()
		if err != nil {
			return nil, fmt.Errorf("ScanBuilder Build() failed while attempting to build expression: %v", err)
		}
		s.ExpressionAttributeNames = expr.Names()
		s.ExpressionAttributeValues = expr.Values()
		s.FilterExpression = expr.Filter()
		s.ProjectionExpression = expr.Projection()
	}
	return s.ScanInput, nil
}

// BuildSegments builds the input input with included projection and creates seperate inputs for each segment
func (s *ScanBuilder) BuildSegments(segments int64) ([]*dynamodb.ScanInput, error) {
	if _, err := s.Build(); err != nil {
		return nil, err
	}

	if segments > 0 {
		s.TotalSegments = &segments
	}

	return splitScanInputIntoSegments(s.ScanInput), nil
}

// CopyInput creates a copy of the ScanInput
func CopyInput(input *dynamodb.ScanInput) *dynamodb.ScanInput {
	n := &dynamodb.ScanInput{}
	if input.AttributesToGet != nil {
		attributesToGet := make([]*string, len(input.AttributesToGet))
		copy(attributesToGet, input.AttributesToGet)
		n.SetAttributesToGet(attributesToGet)
	}
	if input.ConditionalOperator != nil {
		n.SetConditionalOperator(*input.ConditionalOperator)
	}
	if input.ConsistentRead != nil {
		n.SetConsistentRead(*input.ConsistentRead)
	}
	if input.ExclusiveStartKey != nil {
		exclusiveStartKey := make(map[string]*dynamodb.AttributeValue)
		for k, v := range input.ExclusiveStartKey {
			newV := *v
			exclusiveStartKey[k] = &newV
		}
		n.SetExclusiveStartKey(exclusiveStartKey)
	}
	if input.ExpressionAttributeNames != nil {
		expressionAttributeNames := make(map[string]*string)
		for k, v := range input.ExpressionAttributeNames {
			newV := *v
			expressionAttributeNames[k] = &newV
		}
		n.SetExpressionAttributeNames(expressionAttributeNames)
	}
	if input.ExpressionAttributeValues != nil {
		expressionAttributeValues := make(map[string]*dynamodb.AttributeValue)
		for k, v := range input.ExpressionAttributeValues {
			newV := *v
			expressionAttributeValues[k] = &newV
		}
		n.SetExpressionAttributeValues(expressionAttributeValues)
	}
	if input.FilterExpression != nil {
		n.SetFilterExpression(*input.FilterExpression)
	}
	if input.IndexName != nil {
		n.SetFilterExpression(*input.IndexName)
	}
	if input.Limit != nil {
		n.SetLimit(*input.Limit)
	}
	if input.ProjectionExpression != nil {
		n.SetProjectionExpression(*input.ProjectionExpression)
	}
	if input.ReturnConsumedCapacity != nil {
		n.SetReturnConsumedCapacity(*input.ReturnConsumedCapacity)
	}
	if input.ScanFilter != nil {
		scanFilter := make(map[string]*dynamodb.Condition)
		for k, v := range input.ScanFilter {
			newV := *v
			scanFilter[k] = &newV
		}
		n.SetScanFilter(scanFilter)
	}
	if input.Segment != nil {
		n.SetSegment(*input.Segment)
	}
	if input.Select != nil {
		n.SetSelect(*input.Select)
	}
	if input.TableName != nil {
		n.SetTableName(*input.TableName)
	}
	if input.TotalSegments != nil {
		n.SetTotalSegments(*input.TotalSegments)
	}
	return n
}

func splitScanInputIntoSegments(input *dynamodb.ScanInput) (inputs []*dynamodb.ScanInput) {
	if input.TotalSegments == nil || *input.TotalSegments < 2 {
		// only one segment
		return []*dynamodb.ScanInput{input}
	}
	// split into multiple
	inputs = make([]*dynamodb.ScanInput, *input.TotalSegments)

	for i := int64(0); i < *input.TotalSegments; i++ {
		// copy the input
		scanCopy := CopyInput(input)
		// set the segment to i
		scanCopy.SetSegment(i)
		inputs[i] = scanCopy
	}
	return
}

// PutItemBuilder allows for dynamic building of a PutItem input
type PutItemBuilder struct {
	*dynamodb.PutItemInput
	cnd *condition.Builder
}

// NewPutBuilder creates a new PutItemBuilder
func NewPutBuilder(item map[string]*dynamodb.AttributeValue) *PutItemBuilder {
	p := &PutItemBuilder{
		PutItemInput: &dynamodb.PutItemInput{
			// default to returning no values
			ReturnValues: StringPtr(dynamodb.ReturnValueNone),
		},
		cnd: new(condition.Builder),
	}
	p.SetItem(item)
	return p
}

// SetInput sets the PutItemBuilder's dynamodb.PutItemInput explicitly
func (p *PutItemBuilder) SetInput(input *dynamodb.PutItemInput) *PutItemBuilder {
	p.PutItemInput = input
	return p
}

// SetItem shadows dynamodb.PutItemInput and sets the item that will be used to build the put input
func (p *PutItemBuilder) SetItem(item map[string]*dynamodb.AttributeValue) *PutItemBuilder {
	p.PutItemInput.SetItem(item)
	return p
}

// AddCondition adds a condition to this put
// adding multiple conditions by calling this multiple times will join the conditions with
// an AND
func (p *PutItemBuilder) AddCondition(cnd expression.ConditionBuilder) *PutItemBuilder {
	p.cnd.And(cnd)
	return p
}

// Build builds the put input
// returns an error if condition expression building had an error
func (p *PutItemBuilder) Build() (*dynamodb.PutItemInput, error) {
	if !p.cnd.Empty() {
		// build the Expression
		b, err := p.cnd.AddToExpression(expression.NewBuilder()).Build()
		if err != nil {
			return nil, err
		}
		p.ConditionExpression = b.Condition()
		p.ExpressionAttributeNames = b.Names()
		p.ExpressionAttributeValues = b.Values()
	}
	return p.PutItemInput, nil
}

func addProjectionNames(projectionBuilder *expression.ProjectionBuilder, names []string) {
	//nameBuilders := encoding.NameBuilders(names)
	nameBuilders := make([]expression.NameBuilder, len(names))
	for i, name := range names {
		nameBuilders[i] = expression.Name(name)
	}
	if projectionBuilder == nil {
		proj := expression.ProjectionBuilder{}
		proj = proj.AddNames(nameBuilders...)
		projectionBuilder = &proj
	} else {
		*projectionBuilder = projectionBuilder.AddNames(nameBuilders...)
	}
}

func addProjection(projectionBuilder *expression.ProjectionBuilder, projection interface{}) {
	nameBuilders := encoding.NameBuilders(projection)
	if projectionBuilder == nil {
		proj := expression.ProjectionBuilder{}
		proj = proj.AddNames(nameBuilders...)
		projectionBuilder = &proj
	} else {
		*projectionBuilder = projectionBuilder.AddNames(nameBuilders...)
	}
}
