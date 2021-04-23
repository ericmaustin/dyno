package table

import (
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/ericmaustin/dyno"
	"github.com/ericmaustin/dyno/condition"
)

// KeyBase is the base struct used for all key types
type KeyBase struct {
	name                string
	attributeType       string
	attributeDefinition *dynamodb.AttributeDefinition
}

func newKey(name, attributeType string) *KeyBase {
	if attributeType != AttributeNumber && attributeType != AttributeString && attributeType != AttributeBinary {
		panic(fmt.Errorf("attribute type '%s' is not valid", attributeType))
	}
	return &KeyBase{
		name:          name,
		attributeType: attributeType,
		attributeDefinition: &dynamodb.AttributeDefinition{
			AttributeName: dyno.StringPtr(name),
			AttributeType: dyno.StringPtr(attributeType),
		},
	}
}

// PartitionKey represents a partition key in a dynamodb table
type PartitionKey struct {
	*KeyBase
}

// Name returns the name of this key
func (k *KeyBase) Name() string {
	return k.name
}

// Equals returns a new ``KeyConditionBuilder`` with this key equal to input value
func (k *KeyBase) Equals(value interface{}) *expression.KeyConditionBuilder {
	exp := expression.Key(k.name).Equal(expression.Value(value))
	return &exp
}

func (k *KeyBase) extractValue(avMap map[string]*dynamodb.AttributeValue) *dynamodb.AttributeValue {
	if _, ok := avMap[k.name]; !ok {
		return nil
	}
	return avMap[k.name]
}

// NewPartitionKey creates a new partitionKey key with given name and attribute type
func NewPartitionKey(name string, attributeType string) *PartitionKey {
	return &PartitionKey{newKey(name, attributeType)}
}

// NewPartitionStringKey creates a new string partitionKey key with given name
func NewPartitionStringKey(name string) *PartitionKey {
	return NewPartitionKey(name, AttributeString)
}

// NewPartitionNumberKey creates a new numeric partitionKey key with given name
func NewPartitionNumberKey(name string) *PartitionKey {
	return NewPartitionKey(name, AttributeNumber)
}

// NewPartitionBinaryKey creates a new binary partitionKey key with given name
func NewPartitionBinaryKey(name string) *PartitionKey {
	return NewPartitionKey(name, AttributeBinary)
}

// SortKey represents a sortKey (hash) key for a table
type SortKey struct {
	*KeyBase
}

// NewSortKey creates a new sortKey Key with the given name
func NewSortKey(name string, attributeType string) *SortKey {
	return &SortKey{newKey(name, attributeType)}
}

// NewSortStringKey returns a new sort key with a String attribute
func NewSortStringKey(name string) *SortKey {
	return NewSortKey(name, AttributeString)
}

// NewSortNumberKey returns a new sort key with a Number attribute
func NewSortNumberKey(name string) *SortKey {
	return NewSortKey(name, AttributeNumber)
}

// NewSortBinaryKey returns a new sort key with a Binary attribute
func NewSortBinaryKey(name string) *SortKey {
	return NewSortKey(name, AttributeBinary)
}

// Condition returns a dynamodbKey Builder object for this key definition
func (sk *SortKey) Condition(operator interface{}, values ...interface{}) (expression.KeyConditionBuilder, error) {
	return GetKeyCondition(sk.KeyBase, operator, values...)
}

// NotEquals returns a KeyConditionBuilder with a NotEquals condition
func (sk *SortKey) NotEquals(value interface{}) *expression.KeyConditionBuilder {
	cnd, err := GetKeyCondition(sk.KeyBase, condition.OperatorNotEquals, value)
	if err != nil {
		panic(err)
	}
	return &cnd
}

// GreaterThan returns a KeyConditionBuilder with a GreaterThan condition
func (sk *SortKey) GreaterThan(value interface{}) *expression.KeyConditionBuilder {
	cnd, err := GetKeyCondition(sk.KeyBase, condition.OperatorGreaterThan, value)
	if err != nil {
		panic(err)
	}
	return &cnd
}

// GreaterThanEquals returns a KeyConditionBuilder with a GreaterThanEquals condition
func (sk *SortKey) GreaterThanEquals(value interface{}) *expression.KeyConditionBuilder {
	cnd, err := GetKeyCondition(sk.KeyBase, condition.OperatorGreaterThanEqual, value)
	if err != nil {
		panic(err)
	}
	return &cnd
}

// LessThan returns a KeyConditionBuilder with a LessThan condition
func (sk *SortKey) LessThan(value interface{}) *expression.KeyConditionBuilder {
	cnd, err := GetKeyCondition(sk.KeyBase, condition.OperatorLessThan, value)
	if err != nil {
		panic(err)
	}
	return &cnd
}

// LessThanEquals returns a KeyConditionBuilder with a LessThanEquals condition
func (sk *SortKey) LessThanEquals(value interface{}) *expression.KeyConditionBuilder {
	cnd, err := GetKeyCondition(sk.KeyBase, condition.OperatorLessThanEqual, value)
	if err != nil {
		panic(err)
	}
	return &cnd
}

// Between returns a KeyConditionBuilder with a between condition
func (sk *SortKey) Between(lower, upper interface{}) *expression.KeyConditionBuilder {
	cnd, err := GetKeyCondition(sk.KeyBase, condition.OperatorBetween, lower, upper)
	if err != nil {
		panic(err)
	}
	return &cnd
}

// Key represents the key for a table with a partitionKey ProjectionColumns name and a sortKey ProjectionColumns name
type Key struct {
	partitionKey *PartitionKey
	sortKey      *SortKey
	schema       []*dynamodb.KeySchemaElement
	attributes   []*dynamodb.AttributeDefinition
}

// NewKey returns a new table key with the provided Partition and Sort keys
func NewKey(pk *PartitionKey, sk *SortKey) *Key {
	k := &Key{
		partitionKey: pk,
		sortKey:      sk,
	}
	k.buildSchemaAndAttributeDefinitions()
	return k
}

// PartitionKey returns the PartitionKey from this key
func (k *Key) PartitionKey() *PartitionKey {
	return k.partitionKey
}

// SortKey returns the SortKey from this key
func (k *Key) SortKey() *SortKey {
	return k.sortKey
}

// SetSortKey sets the sortKey key and rebuilds the key schema
func (k *Key) SetSortKey(key *SortKey) *Key {
	k.sortKey = key
	k.buildSchemaAndAttributeDefinitions()
	return k
}

// SetPartitionKey sets the partitionKey key and rebuilds the key schema
func (k *Key) SetPartitionKey(key *PartitionKey) *Key {
	k.partitionKey = key
	k.buildSchemaAndAttributeDefinitions()
	return k
}

func (k *Key) buildSchemaAndAttributeDefinitions() {

	var (
		attributes []*dynamodb.AttributeDefinition
		keySchema  []*dynamodb.KeySchemaElement
	)

	if k.partitionKey != nil {
		attributes = append(attributes, k.partitionKey.attributeDefinition)
		keySchema = append(keySchema, &dynamodb.KeySchemaElement{
			AttributeName: dyno.StringPtr(k.partitionKey.name),
			KeyType:       dyno.StringPtr("HASH"),
		})
	}

	if k.sortKey != nil {
		attributes = append(attributes, k.sortKey.attributeDefinition)
		keySchema = append(keySchema, &dynamodb.KeySchemaElement{
			AttributeName: dyno.StringPtr(k.sortKey.name),
			KeyType:       dyno.StringPtr("RANGE"),
		})
	}

	k.schema = keySchema
	k.attributes = attributes
}

// String implements the string interface for the table key
func (k *Key) String() string {

	str := ""

	if k.partitionKey != nil {
		str += fmt.Sprintf("PK:'%s'", k.partitionKey.name)
	}

	if k.sortKey != nil {
		str += fmt.Sprintf("SK:'%s'", k.sortKey.name)
	}

	return str
}

// PartitionName returns the PartitionKey's name string
func (k *Key) PartitionName() string {
	return k.partitionKey.Name()
}

// SortName returns the SortKey's name string
func (k *Key) SortName() string {
	if k.sortKey == nil {
		return ""
	}
	return k.sortKey.Name()
}

//PartitionAttributeDefinition returns the partition key dynamodb.AttributeDefinition
func (k *Key) PartitionAttributeDefinition() *dynamodb.AttributeDefinition {
	return k.partitionKey.attributeDefinition
}

//SortAttributeDefinition returns the sort key dynamodb.AttributeDefinition
func (k *Key) SortAttributeDefinition() *dynamodb.AttributeDefinition {
	return k.sortKey.attributeDefinition
}

// Copy makes a deep copy of this table key
func (k *Key) Copy() *Key {
	return &Key{
		partitionKey: k.partitionKey,
		sortKey:      k.sortKey,
	}
}

// Equals tests whether 2 table keys are equal
func (k *Key) Equals(t2 *Key) (match bool) {
	return k.partitionKey == t2.partitionKey && k.sortKey == t2.sortKey
}

// GetKeyCondition for provided key string, operator and ProjectionColumns
func GetKeyCondition(key *KeyBase, operator interface{}, values ...interface{}) (expression.KeyConditionBuilder, error) {

	var op condition.Operator

	switch o := operator.(type) {
	case string:
		// if we have ae string, figure batchGetOutput which operator const to use
		switch o {
		case "=":
			op = condition.OperatorEqual
		case ">":
			op = condition.OperatorGreaterThan
		case "<":
			op = condition.OperatorLessThan
		case ">=":
			op = condition.OperatorGreaterThanEqual
		case "<=":
			op = condition.OperatorLessThanEqual
		case "b":
			op = condition.OperatorBeginsWith
		}
	case condition.Operator:
		op = o
	default:
		return expression.KeyConditionBuilder{}, &dyno.Error{
			Code:    dyno.ErrOperatorNotFound,
			Message: fmt.Sprintf("No operator exists for '%v'", o)}
	}

	encoded := make([]expression.ValueBuilder, len(values))

	for i, val := range values {
		encoded[i] = expression.Value(val)
	}

	switch op {
	case condition.OperatorEqual:
		return expression.Key(key.Name()).Equal(encoded[0]), nil
	case condition.OperatorLessThan:
		return expression.Key(key.Name()).LessThan(encoded[0]), nil
	case condition.OperatorLessThanEqual:
		return expression.Key(key.Name()).LessThanEqual(encoded[0]), nil
	case condition.OperatorGreaterThan:
		return expression.Key(key.Name()).GreaterThan(encoded[0]), nil
	case condition.OperatorGreaterThanEqual:
		return expression.Key(key.Name()).GreaterThanEqual(encoded[0]), nil
	case condition.OperatorBetween:
		return expression.Key(key.Name()).Between(encoded[0], encoded[1]), nil
	// string only operators
	case condition.OperatorBeginsWith:
		if str, ok := values[0].(string); ok {
			// operator beings with a string
			return expression.Key(key.Name()).BeginsWith(str), nil
		}

		return expression.KeyConditionBuilder{},
			fmt.Errorf("KeyConditoin Set Begins with operator requires string ProjectionColumns")
	}

	return expression.KeyConditionBuilder{}, &dyno.Error{
		Code:    dyno.ErrOperatorNotFound,
		Message: fmt.Sprintf("No operator exists for '%v'", op)}
}

func (k *Key) extract(item map[string]*dynamodb.AttributeValue) map[string]*dynamodb.AttributeValue {

	keyMap := map[string]*dynamodb.AttributeValue{}

	if k.partitionKey != nil {
		av := k.partitionKey.extractValue(item)
		if av == nil {
			panic(fmt.Sprintf("item %v does not have required partitionKey key '%s' value",
				item, k.partitionKey.name))
		}

		keyMap[k.partitionKey.name] = av
	}

	if k.sortKey != nil {
		av := k.sortKey.extractValue(item)
		if av == nil {
			panic(fmt.Sprintf("item %v does not have required sortKey key '%s' value",
				item, k.sortKey.name))
		}

		keyMap[k.sortKey.name] = av
	}

	return keyMap
}

func (k *Key) extractAll(items []map[string]*dynamodb.AttributeValue) []map[string]*dynamodb.AttributeValue {
	out := make([]map[string]*dynamodb.AttributeValue, len(items))
	for i, item := range items {
		out[i] = k.extract(item)
	}
	return out
}
