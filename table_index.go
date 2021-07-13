package dyno

import (
	"fmt"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// GSI represents a Global Secondary IndexDefinition
type GSI struct {
	IndexName             string
	SortKey               *ddb.AttributeDefinition
	PartitionKey          *ddb.AttributeDefinition
	ProvisionedThroughput *ddb.ProvisionedThroughput
	Projection            []string
}

// PartitionKeyName returns the projection key name for this LSI or nil if not set
func (g *GSI) PartitionKeyName() *string {
	if g.PartitionKey == nil {
		return nil
	}

	return g.PartitionKey.AttributeName
}

// NewGSI create a new Global Secondary IndexDefinition with a given name, key, cost units
func NewGSI(name string) *GSI {
	return &GSI{IndexName: name}
}

//SetName sets this LSIs sort key key
func (g *GSI) SetName(name string) *GSI {
	g.IndexName = name
	return g
}

// SortKeyName returns the sort key name for this LSI or nil if not set
func (g *GSI) SortKeyName() *string {
	if g.SortKey == nil {
		return nil
	}

	return g.SortKey.AttributeName
}

func (g *GSI) IsOnDemand() bool {
	return g.ProvisionedThroughput == nil
}

// SetOnDemand removes provisioned throughput, setting the GSI to ON DEMAND pricing
func (g *GSI) SetOnDemand() *GSI {
	g.ProvisionedThroughput = nil
	return g
}

// SetCostUnits sets the cost units for this  global secondary IndexDefinition and turns off on demand pricing if set
// if wcus and rcus are < 0 e.g. (-1, -1) then on demand pricing is set
func (g *GSI) SetCostUnits(wcus, rcus int64) *GSI {
	if wcus == 0 && rcus == 0 {
		g.ProvisionedThroughput = nil
		return g
	}
	if g.ProvisionedThroughput == nil {
		g.ProvisionedThroughput = new(ddb.ProvisionedThroughput)
	}
	g.ProvisionedThroughput.WriteCapacityUnits = &wcus
	g.ProvisionedThroughput.ReadCapacityUnits = &rcus
	return g
}

// SetPartitionKey sets the partition key for this GSI
func (g *GSI) SetPartitionKey(pkName string, attributeType ddb.ScalarAttributeType) *GSI {
	g.PartitionKey = &ddb.AttributeDefinition{
		AttributeName: &pkName,
		AttributeType: attributeType,
	}
	return g
}

// SetPartitionKeyAttributeDefinition sets the partition key for this GSI with an AttributeDefinition
func (g *GSI) SetPartitionKeyAttributeDefinition(def *ddb.AttributeDefinition) *GSI {
	g.PartitionKey = def
	return g
}

// SetSortKey sets the sortKey key for this GSI
func (g *GSI) SetSortKey(skName string, attributeType ddb.ScalarAttributeType) *GSI {
	g.SortKey = &ddb.AttributeDefinition{
		AttributeName: &skName,
		AttributeType: attributeType,
	}
	return g
}

// SetSortKeyAttributeDefinition sets the partition key for this GSI with an AttributeDefinition
func (g *GSI) SetSortKeyAttributeDefinition(def *ddb.AttributeDefinition) *GSI {
	g.SortKey = def
	return g
}

// AddProjectionNames adds projection names to this SortKey
func (g *GSI) AddProjectionNames(names ...string) *GSI {
	g.Projection = append(g.Projection, names...)
	return g
}

// GetDynamoKeySchema gets the dynamodb KeySchema
func (g *GSI) GetDynamoKeySchema() []ddb.KeySchemaElement {
	return getKeySchema(g.PartitionKey, g.SortKey)
}

// GetDynamoProjection gets the dynamodb Projection
func (g *GSI) GetDynamoProjection() *ddb.Projection {
	return getProjection(getKeyAttributeNames(g.PartitionKey, g.SortKey), g.Projection)
}

// GetDynamoGlobalSecondaryIndex gets the global secondary IndexDefinition dynamo object
func (g *GSI) GetDynamoGlobalSecondaryIndex() ddb.GlobalSecondaryIndex {
	return ddb.GlobalSecondaryIndex{
		IndexName:             &g.IndexName,
		KeySchema:             g.GetDynamoKeySchema(),
		Projection:            g.GetDynamoProjection(),
		ProvisionedThroughput: g.ProvisionedThroughput,
	}
}

//LSI represents a Local Secondary IndexDefinition
type LSI struct {
	IndexName                       string
	PartitionKey                    *ddb.AttributeDefinition
	SortKey                         *ddb.AttributeDefinition
	Projection                      []string
}

// NewLSI creates a new Local Secondary IndexDefinition with a given name and key
func NewLSI(name string) *LSI {
	return &LSI{IndexName: name}
}

//SetName sets this LSIs sort key key
func (l *LSI) SetName(name string) *LSI {
	l.IndexName = name
	return l
}

// PartitionKeyName returns the projection key name for this LSI or nil if not set
func (l *LSI) PartitionKeyName() *string {
	if l.PartitionKey == nil {
		return nil
	}

	return l.PartitionKey.AttributeName
}

// SortKeyName returns the sort key name for this LSI or nil if not set
func (l *LSI) SortKeyName() *string {
	if l.SortKey == nil {
		return nil
	}

	return l.SortKey.AttributeName
}

// SetPartitionKey sets the partition key for this local IndexDefinition
func (l *LSI) SetPartitionKey(pkName string, attributeType ddb.ScalarAttributeType) *LSI {
	l.PartitionKey = &ddb.AttributeDefinition{
		AttributeName: &pkName,
		AttributeType: attributeType,
	}
	return l
}

// SetPartitionKeyAttributeDefinition sets the partition key for this LSI with an AttributeDefinition
func (l *LSI) SetPartitionKeyAttributeDefinition(def *ddb.AttributeDefinition) *LSI {
	l.PartitionKey = def
	return l
}

// SetSortKey sets the sortKey key for this local IndexDefinition
func (l *LSI) SetSortKey(skName string, attributeType ddb.ScalarAttributeType) *LSI {
	l.SortKey = &ddb.AttributeDefinition{
		AttributeName: &skName,
		AttributeType: attributeType,
	}
	return l
}

// SetSortKeyAttributeDefinition sets the partition key for this LSI with an AttributeDefinition
func (l *LSI) SetSortKeyAttributeDefinition(def *ddb.AttributeDefinition) *LSI {
	l.SortKey = def
	return l
}

// AddProjectionNames adds projection names to this SortKey
func (l *LSI) AddProjectionNames(names ...string) *LSI {
	l.Projection = append(l.Projection, names...)
	return l
}


// GetDynamoKeySchema gets the dynamodb KeySchema
func (l *LSI) GetDynamoKeySchema() []ddb.KeySchemaElement {
	return getKeySchema(l.PartitionKey, l.SortKey)
}

// GetDynamoProjection gets the dynamodb Projection
func (l *LSI) GetDynamoProjection() *ddb.Projection {
	return getProjection(getKeyAttributeNames(l.PartitionKey, l.SortKey), l.Projection)
}

// GetDynamoLocalSecondaryIndex gets a local secondary IndexDefinition dynamo object representation
func (l *LSI) GetDynamoLocalSecondaryIndex() ddb.LocalSecondaryIndex {
	return ddb.LocalSecondaryIndex{
		IndexName:  &l.IndexName,
		KeySchema:  l.GetDynamoKeySchema(),
		Projection: l.GetDynamoProjection(),
	}
}

//addSortKeyToKeySchema adds a sort key to they keySchema
func addSortKeyToKeySchema(keySchema []ddb.KeySchemaElement, skName string) []ddb.KeySchemaElement {
	var iExisting *int

	keySchemaElement := ddb.KeySchemaElement{
		AttributeName: &skName,
		KeyType:       ddb.KeyTypeRange,
	}

	for i, ks := range keySchema {
		if ks.KeyType == ddb.KeyTypeRange {
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
func addPartitionKeyToKeySchema(keySchema []ddb.KeySchemaElement, pkName string) []ddb.KeySchemaElement {
	var iExisting *int

	keySchemaElement := ddb.KeySchemaElement{
		AttributeName: &pkName,
		KeyType:       ddb.KeyTypeHash,
	}

	for i, ks := range keySchema {
		if ks.KeyType == ddb.KeyTypeHash {
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

func getPartitionKeyNameFromKeySchema(keySchema []ddb.KeySchemaElement) *string {
	if len(keySchema) == 0 {
		return nil
	}
	for _, ks := range keySchema {
		if ks.KeyType == ddb.KeyTypeHash {
			return ks.AttributeName
		}
	}
	return nil
}

func getSortKeyNameFromKeySchema(keySchema []ddb.KeySchemaElement) *string {
	if len(keySchema) == 0 {
		return nil
	}
	for _, ks := range keySchema {
		if ks.KeyType == ddb.KeyTypeRange {
			return ks.AttributeName
		}
	}
	return nil
}

// extractKeyAttributeValuesFromKeySchema converts a list of records to a list of ddb attribute items
func extractKeyAttributeValuesFromKeySchema(keySchema []ddb.KeySchemaElement, avMap map[string]ddb.AttributeValue) map[string]ddb.AttributeValue {

	names := make([]string, len(keySchema))

	for i, ks := range keySchema {
		names[i] = *ks.AttributeName
	}

	out := make(map[string]ddb.AttributeValue)

	for _, n := range names {
		if _, ok := avMap[n]; ok {
			out[n] = avMap[n]
		}
	}

	return out
}

// extractAllKeyAttributeValuesFromKeySchema converts a list of records to a list of ddb attribute items
func extractAllKeyAttributeValuesFromKeySchema(keySchema []ddb.KeySchemaElement, avMaps []map[string]ddb.AttributeValue) []map[string]ddb.AttributeValue {
	out := make([]map[string]ddb.AttributeValue, len(avMaps))

	for i, avMap := range avMaps {
		out[i] = extractKeyAttributeValuesFromKeySchema(keySchema, avMap)
	}

	return out
}

// extractPartitionKeyAttributeValueFromKeySchema extracts the partition key value from a dynamodb.AttributeValue with given keySchema
func extractPartitionKeyAttributeValueFromKeySchema(keySchema []ddb.KeySchemaElement, avMap map[string]ddb.AttributeValue) ddb.AttributeValue {

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
func extractSortKeyAttributeValueFromKeySchema(keySchema []ddb.KeySchemaElement, avMap map[string]ddb.AttributeValue) ddb.AttributeValue {

	skName := getSortKeyNameFromKeySchema(keySchema)

	if skName == nil {
		return nil
	}

	if av, ok := avMap[*skName]; ok {
		return av
	}

	return nil
}

// appendUniqueAttributeDefinitions appends only uniques
// this func will panic if any of the dynamodb.AttributeDefinition have a nil AttributeName
func appendUniqueAttributeDefinitions(defs []ddb.AttributeDefinition, new ...ddb.AttributeDefinition) []ddb.AttributeDefinition {
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

func getKeyAttributeNames(pk, sk *ddb.AttributeDefinition) []string {
	var names []string

	if pk != nil && pk.AttributeName != nil {
		names = append(names, *pk.AttributeName)
	}

	if sk != nil && sk.AttributeName != nil {
		names = append(names, *sk.AttributeName)
	}

	return names
}

func getProjection(attributeNames, projectionNames []string) *ddb.Projection {
	var (
		keysLen, keysFound int
		proj               []string
		projection         *ddb.Projection
	)

	keyNameMap := make(map[string]struct{})

	for _, name := range attributeNames {
		keyNameMap[name] = struct{}{}
	}

	keysLen = len(keyNameMap)

	// this will remove duplicates in the Projection
	projMap := make(map[string]struct{})

	for _, name := range projectionNames {
		projMap[name] = struct{}{}
	}

	for name := range projMap {
		if _, ok := keyNameMap[name]; ok {
			keysFound++
			continue
		}
		proj = append(proj, name)
	}

	projection = new(ddb.Projection)

	for _, n := range proj {
		projection.NonKeyAttributes = append(projection.NonKeyAttributes, n)
	}

	switch {
	case len(proj) > 0 && keysLen == keysFound:
		projection.ProjectionType = ddb.ProjectionTypeKeysOnly
	case len(proj) > 0:
		projection.ProjectionType = ddb.ProjectionTypeInclude
	default:
		projection.ProjectionType = ddb.ProjectionTypeAll
	}

	return projection
}

func getKeySchema(pk, sk *ddb.AttributeDefinition) []ddb.KeySchemaElement {

	var keySchema []ddb.KeySchemaElement

	if pk != nil && pk.AttributeName != nil {
		keySchema = append(keySchema, ddb.KeySchemaElement{
			AttributeName: pk.AttributeName,
			KeyType:       ddb.KeyTypeHash,
		})
	}

	if sk != nil && sk.AttributeName != nil {
		keySchema = append(keySchema, ddb.KeySchemaElement{
			AttributeName: sk.AttributeName,
			KeyType:       ddb.KeyTypeRange,
		})
	}

	return keySchema
}

// extractKeys extracts key values from a dynamodb.AttributeValue map
func extractKeys(attributeNames []string, avMap map[string]ddb.AttributeValue) map[string]ddb.AttributeValue {
	out := make(map[string]ddb.AttributeValue)

	for _, n := range attributeNames {
		if _, ok := avMap[n]; ok {
			out[n] = avMap[n]
		}
	}

	return out
}

// extractAllKeys extracts all key values from a slice of dynamodb.AttributeValue maps
func extractAllKeys(attributeNames []string, avMaps []map[string]ddb.AttributeValue) []map[string]ddb.AttributeValue {
	out := make([]map[string]ddb.AttributeValue, len(avMaps))

	for i, avMap := range avMaps {
		out[i] = extractKeys(attributeNames, avMap)
	}

	return out
}
