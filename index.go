package dyno

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var validAttributeTypes = map[string]struct{}{
	dynamodb.ScalarAttributeTypeS: {},
	dynamodb.ScalarAttributeTypeN: {},
	dynamodb.ScalarAttributeTypeB: {},
}

// GSI represents a Global Secondary Index
type GSI struct {
	*dynamodb.GlobalSecondaryIndex
	PartitionKeyAttributeDefinition *dynamodb.AttributeDefinition
	SortKeyAttributeDefinition      *dynamodb.AttributeDefinition
}

// NewGSI creae a new Global Secondary Index with a given name, key, cost units
func NewGSI(name string) *GSI {
	gsi := &GSI{
		GlobalSecondaryIndex: &dynamodb.GlobalSecondaryIndex{
			IndexName: &name,
			Projection: &dynamodb.Projection{
				ProjectionType: StringPtr(dynamodb.ProjectionTypeAll),
			},
		},
	}
	return gsi
}

func (g *GSI) IsOnDemand() bool {
	return g.ProvisionedThroughput == nil
}

// SetOnDemand removes provisioned throughput, setting the GSI to ON DEMAND pricing
func (g *GSI) SetOnDemand() *GSI {
	g.ProvisionedThroughput = nil
	return g
}

//PartitionKeyName returns the projection key name for this LSI or nil if not set
func (g *GSI) PartitionKeyName() *string {
	return getPartitionKeyNameFromKeySchema(g.KeySchema)
}

//SortKeyName returns the sort key name for this LSI or nil if not set
func (g *GSI) SortKeyName() *string {
	return getSortKeyNameFromKeySchema(g.KeySchema)
}

// SetCostUnits sets the cost units for this  global secondary index and turns off on demand pricing if set
// if wcus and rcus are < 0 e.g. (-1, -1) then on demand pricing is set
func (g *GSI) SetCostUnits(wcus, rcus int64) *GSI {
	if wcus == 0 && rcus == 0 {
		g.ProvisionedThroughput = nil
		return g
	}
	if g.ProvisionedThroughput == nil {
		g.ProvisionedThroughput = new(dynamodb.ProvisionedThroughput)
	}
	g.ProvisionedThroughput.SetWriteCapacityUnits(wcus)
	g.ProvisionedThroughput.SetReadCapacityUnits(rcus)
	return g
}

// SetPartitionKey sets the partition key for this GSI
func (g *GSI) SetPartitionKey(pkName string, attributeType string) *GSI {
	if _, ok := validAttributeTypes[attributeType]; !ok {
		panic(fmt.Errorf("attribute type %s is not valid", attributeType))
	}

	g.KeySchema = addPartitionKeyToKeySchema(g.KeySchema, pkName)
	g.PartitionKeyAttributeDefinition = &dynamodb.AttributeDefinition{
		AttributeName: &pkName,
		AttributeType: &attributeType,
	}
	return g
}

// SetSortKey sets the sortKey key for this GSI
func (g *GSI) SetSortKey(skName string, attributeType string) *GSI {
	if _, ok := validAttributeTypes[attributeType]; !ok {
		panic(fmt.Errorf("attribute type %s is not valid", attributeType))
	}

	g.KeySchema = addSortKeyToKeySchema(g.KeySchema, skName)
	g.SortKeyAttributeDefinition = &dynamodb.AttributeDefinition{
		AttributeName: &skName,
		AttributeType: &attributeType,
	}
	return g
}

// AddProjectionNames adds projection names to this SortKey
func (g *GSI) AddProjectionNames(names ...string) *GSI {
	if g.Projection == nil {
		g.Projection = new(dynamodb.Projection)
	}

	skn := g.SortKeyName()
	pkn := g.PartitionKeyName()

	cnt := 0

	for _, n := range names {
		if (skn != nil && n != *skn) && (pkn != nil && n != *pkn) { // execute not add the key names
			cnt++
			g.Projection.NonKeyAttributes = append(g.Projection.NonKeyAttributes, &n)
		}
	}

	if cnt > 0 {
		g.Projection.SetProjectionType(dynamodb.ProjectionTypeInclude)
	}

	return g
}

// SetProjectionTypeKeysOnly sets the Keys Only projection type
func (g *GSI) SetProjectionTypeKeysOnly(skName string) *GSI {
	g.Projection = &dynamodb.Projection{
		NonKeyAttributes: nil,
		ProjectionType:   StringPtr(dynamodb.ProjectionTypeKeysOnly),
	}
	return g
}

// ExtractKeys extracts key values from a dynamodb.AttributeValue map
func (g *GSI) ExtractKeys(avMap map[string]*dynamodb.AttributeValue) map[string]*dynamodb.AttributeValue {
	return extractKeyAttributeValuesFromKeySchema(g.KeySchema, avMap)
}

// ExtractAllKeys extracts all key values from a slice of dynamodb.AttributeValue maps
func (g *GSI) ExtractAllKeys(avMaps []map[string]*dynamodb.AttributeValue) []map[string]*dynamodb.AttributeValue {
	return extractAllKeyAttributeValuesFromKeySchema(g.KeySchema, avMaps)
}

// DynamoGlobalSecondaryIndex gets the global secondary index dynamo object
func (g *GSI) DynamoGlobalSecondaryIndex() *dynamodb.GlobalSecondaryIndex {
	return g.GlobalSecondaryIndex
}

//LSI represents a Local Secondary Index
type LSI struct {
	*dynamodb.LocalSecondaryIndex
	PartitionKeyAttributeDefinition *dynamodb.AttributeDefinition
	SortKeyAttributeDefinition      *dynamodb.AttributeDefinition
}

// NewLSI creates a new Local Secondary Index with a given name and key
func NewLSI(name string) *LSI {
	if len(name) < 1 {
		panic(fmt.Errorf("index name must not be empty"))
	}
	return &LSI{
		//Index: newIndex(name, key),
		LocalSecondaryIndex: &dynamodb.LocalSecondaryIndex{
			IndexName: &name,
			Projection: &dynamodb.Projection{
				ProjectionType: StringPtr(dynamodb.ProjectionTypeAll),
			},
		},
	}
}

//GetIndexName returns the index name for this LSI
// will panic on nil dereference error if index name is nil
func (l *LSI) GetIndexName() string {
	return *l.IndexName
}

//PartitionKeyName returns the projection key name for this LSI or nil if not set
func (l *LSI) PartitionKeyName() *string {
	return getPartitionKeyNameFromKeySchema(l.KeySchema)
}

//SortKeyName returns the sort key name for this LSI or nil if not set
func (l *LSI) SortKeyName() *string {
	return getSortKeyNameFromKeySchema(l.KeySchema)
}

// SetPartitionKey sets the partition key for this local index
func (l *LSI) SetPartitionKey(pkName string, attributeType string) *LSI {
	if _, ok := validAttributeTypes[attributeType]; !ok {
		panic(fmt.Errorf("attribute type %s is not valid", attributeType))
	}

	l.KeySchema = addPartitionKeyToKeySchema(l.KeySchema, pkName)
	l.PartitionKeyAttributeDefinition = &dynamodb.AttributeDefinition{
		AttributeName: &pkName,
		AttributeType: &attributeType,
	}
	return l
}

// SetSortKey sets the sortKey key for this local index
func (l *LSI) SetSortKey(skName string, attributeType string) *LSI {
	if _, ok := validAttributeTypes[attributeType]; !ok {
		panic(fmt.Errorf("attribute type %s is not valid", attributeType))
	}

	l.KeySchema = addSortKeyToKeySchema(l.KeySchema, skName)
	l.SortKeyAttributeDefinition = &dynamodb.AttributeDefinition{
		AttributeName: &skName,
		AttributeType: &attributeType,
	}
	return l
}

// SetProjectionTypeKeysOnly sets the Keys Only projection type
func (l *LSI) SetProjectionTypeKeysOnly(skName string) *LSI {
	l.Projection = &dynamodb.Projection{
		NonKeyAttributes: nil,
		ProjectionType:   StringPtr(dynamodb.ProjectionTypeKeysOnly),
	}
	return l
}

// AddProjectionNames adds projection names to this SortKey
func (l *LSI) AddProjectionNames(names ...string) *LSI {
	if l.Projection == nil {
		l.Projection = new(dynamodb.Projection)
	}

	skn := l.SortKeyName()
	pkn := l.PartitionKeyName()

	cnt := 0

	for _, n := range names {
		if (skn != nil && n != *skn) && (pkn != nil && n != *pkn) { // execute not add the key names
			cnt++
			l.Projection.NonKeyAttributes = append(l.Projection.NonKeyAttributes, &n)
		}
	}

	if cnt > 0 {
		l.Projection.SetProjectionType(dynamodb.ProjectionTypeInclude)
	}

	return l
}

// ExtractKeys extracts key values from a dynamodb.AttributeValue map
func (l *LSI) ExtractKeys(avMap map[string]*dynamodb.AttributeValue) map[string]*dynamodb.AttributeValue {
	return extractKeyAttributeValuesFromKeySchema(l.KeySchema, avMap)
}

// ExtractAllKeys extracts all key values from a slice of dynamodb.AttributeValue maps
func (l *LSI) ExtractAllKeys(avMaps []map[string]*dynamodb.AttributeValue) []map[string]*dynamodb.AttributeValue {
	return extractAllKeyAttributeValuesFromKeySchema(l.KeySchema, avMaps)
}

// DynamoLocalSecondaryIndex gets a local secondary index dynamo object representation
func (l *LSI) DynamoLocalSecondaryIndex() *dynamodb.LocalSecondaryIndex {
	return l.LocalSecondaryIndex
}

//addSortKeyToKeySchema adds a sort key to they keySchema
func addSortKeyToKeySchema(keySchema []*dynamodb.KeySchemaElement, skName string) []*dynamodb.KeySchemaElement {
	var iExisting *int

	keySchemaElement := &dynamodb.KeySchemaElement{
		AttributeName: &skName,
		KeyType:       StringPtr(dynamodb.KeyTypeRange),
	}

	for i, ks := range keySchema {
		if ks.KeyType != nil && *ks.KeyType == dynamodb.KeyTypeRange {
			iExisting = &i
		}
	}

	if iExisting != nil {
		keySchema[*iExisting] = keySchemaElement
		return keySchema
	}

	keySchema = append(keySchema, keySchemaElement)
	return keySchema
}

//addPartitionKeyToKeySchema adds a partition key to they keySchema
func addPartitionKeyToKeySchema(keySchema []*dynamodb.KeySchemaElement, pkName string) []*dynamodb.KeySchemaElement {
	var iExisting *int

	keySchemaElement := &dynamodb.KeySchemaElement{
		AttributeName: &pkName,
		KeyType:       StringPtr(dynamodb.KeyTypeHash),
	}

	for i, ks := range keySchema {
		if ks.KeyType != nil && *ks.KeyType == dynamodb.KeyTypeHash {
			iExisting = &i
		}
	}

	if iExisting != nil {
		keySchema[*iExisting] = keySchemaElement
		return keySchema
	}

	keySchema = append(keySchema, keySchemaElement)
	return keySchema
}

func getPartitionKeyNameFromKeySchema(keySchema []*dynamodb.KeySchemaElement) *string {
	if len(keySchema) == 0 {
		return nil
	}
	for _, ks := range keySchema {
		if ks.KeyType != nil && *ks.KeyType == dynamodb.KeyTypeHash {
			return ks.AttributeName
		}
	}
	return nil
}

func getSortKeyNameFromKeySchema(keySchema []*dynamodb.KeySchemaElement) *string {
	if len(keySchema) == 0 {
		return nil
	}
	for _, ks := range keySchema {
		if ks.KeyType != nil && *ks.KeyType == dynamodb.KeyTypeRange {
			return ks.AttributeName
		}
	}
	return nil
}

// extractKeyAttributeValuesFromKeySchema converts a list of records to a list of dynamodb attribute items
func extractKeyAttributeValuesFromKeySchema(keySchema []*dynamodb.KeySchemaElement, avMap map[string]*dynamodb.AttributeValue) map[string]*dynamodb.AttributeValue {

	names := make([]string, len(keySchema))

	for i, ks := range keySchema {
		names[i] = *ks.AttributeName
	}

	out := make(map[string]*dynamodb.AttributeValue)

	for _, n := range names {
		if _, ok := avMap[n]; ok {
			out[n] = avMap[n]
		}
	}

	return out
}

// extractAllKeyAttributeValuesFromKeySchema converts a list of records to a list of dynamodb attribute items
func extractAllKeyAttributeValuesFromKeySchema(keySchema []*dynamodb.KeySchemaElement, avMaps []map[string]*dynamodb.AttributeValue) []map[string]*dynamodb.AttributeValue {
	out := make([]map[string]*dynamodb.AttributeValue, len(avMaps))

	for i, avMap := range avMaps {
		out[i] = extractKeyAttributeValuesFromKeySchema(keySchema, avMap)
	}

	return out
}

// extractPartitionKeyAttributeValueFromKeySchema extracts the partition key value from a dynamodb.AttributeValue with given keySchema
func extractPartitionKeyAttributeValueFromKeySchema(keySchema []*dynamodb.KeySchemaElement, avMap map[string]*dynamodb.AttributeValue) *dynamodb.AttributeValue {

	pkName := getPartitionKeyNameFromKeySchema(keySchema)

	if pkName == nil {
		return nil
	}

	if av, ok := avMap[*pkName]; ok {
		return av
	}

	return nil
}

// extractSortKeyAttributeValueFromKeySchema extracts the sort key value from a dynamodb.AttributeValue with given keySchema
func extractSortKeyAttributeValueFromKeySchema(keySchema []*dynamodb.KeySchemaElement, avMap map[string]*dynamodb.AttributeValue) *dynamodb.AttributeValue {

	skName := getSortKeyNameFromKeySchema(keySchema)

	if skName == nil {
		return nil
	}

	if av, ok := avMap[*skName]; ok {
		return av
	}

	return nil
}

//appendUniqueAttributeDefinitions appends only uniques
// this func will panic if any of the dynamodb.AttributeDefinition have a nil AttributeName
func appendUniqueAttributeDefinitions(defs []*dynamodb.AttributeDefinition, new ...*dynamodb.AttributeDefinition) []*dynamodb.AttributeDefinition {
	adKeys := make(map[string]struct{}, len(defs))
	for _, ad := range defs {
		if ad.AttributeName == nil {
			panic(fmt.Errorf("AttributeDefinition.AttributeName must not be nil"))
		}
		adKeys[*ad.AttributeName] = struct{}{}
	}
	for _, ad := range new {
		if ad.AttributeName == nil {
			panic(fmt.Errorf("AttributeDefinition.AttributeName must not be nil"))
		}
		if _, ok := adKeys[*ad.AttributeName]; ok {
			//unique only
			continue
		}
		defs = append(defs, ad)
	}
	return defs
}
