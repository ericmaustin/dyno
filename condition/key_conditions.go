package condition

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/ericmaustin/dyno/encoding"
)

// KeyGreaterThan returns a KeyConditionBuilder with a GreaterThan condition for the given field name and value
func KeyGreaterThan(name string, value interface{}) expression.KeyConditionBuilder {
	return expression.Key(name).GreaterThan(expression.Value(value))
}

// KeyGreaterThanEqual returns a KeyConditionBuilder with a GreaterThanEqual condition for the given field name and value
func KeyGreaterThanEqual(name string, value interface{}) expression.KeyConditionBuilder {
	return expression.Key(name).GreaterThanEqual(expression.Value(value))
}

// KeyLessThan returns a KeyConditionBuilder with a LessThan condition for the given field name and value
func KeyLessThan(name string, value interface{}) expression.KeyConditionBuilder {
	return expression.Key(name).LessThan(expression.Value(value))
}

// KeyLessThanEqual returns a KeyConditionBuilder with a LessThanEqual condition for the given field name and value
func KeyLessThanEqual(name string, value interface{}) expression.KeyConditionBuilder {
	return expression.Key(name).LessThanEqual(expression.Value(value))
}

// KeyEqual returns a KeyConditionBuilder with a Equal condition for the given field name and value
func KeyEqual(name string, value interface{}) expression.KeyConditionBuilder {
	return expression.Key(name).Equal(expression.Value(value))
}

// KeyBetween returns a KeyConditionBuilder with a Between condition for the given field name and values
func KeyBetween(name string, a, b interface{}) expression.KeyConditionBuilder {
	return expression.Key(name).Between(expression.Value(a), expression.Value(b))
}

// KeyBeginsWith returns a KeyConditionBuilder with a BeginsWith condition for the given field name and value
func KeyBeginsWith(name string, value interface{}) expression.KeyConditionBuilder {
	return expression.Key(name).BeginsWith(encoding.ToString(value))
}

// KeyAnd combines multiple KeyConditionBuilder conditions into And conditions
func KeyAnd(left expression.KeyConditionBuilder, right ...expression.KeyConditionBuilder) expression.KeyConditionBuilder {

	if len(right) < 1 {
		// if we only have 1 condition just return it
		return left
	}

	cnd := left

	for _, c := range right {
		cnd = cnd.And(c)
	}

	return cnd
}

// KeyConditionBuilder represents a key condition builder that can have conditions set dynmaically
type KeyConditionBuilder struct {
	*expression.KeyConditionBuilder
}

// IsEmpty checks is the condition set is empty
func (ks *KeyConditionBuilder) IsEmpty() bool {
	return ks.KeyConditionBuilder == nil
}

// And adds an And condition to the set
func (ks *KeyConditionBuilder) And(conditions ...expression.KeyConditionBuilder) *KeyConditionBuilder {

	var builder expression.KeyConditionBuilder

	switch len(conditions) {
	case 0:
		return ks
	case 1:
		builder = conditions[0]
	default:
		builder = KeyAnd(conditions[0], conditions[1:]...)
	}

	if ks.IsEmpty() {
		ks.KeyConditionBuilder = &builder
		return ks
	}

	ks.KeyConditionBuilder.And(builder)

	return ks
}

// Builder returns the condition set's complete Builder
func (ks *KeyConditionBuilder) Builder() expression.KeyConditionBuilder {
	if ks == nil || ks.KeyConditionBuilder == nil {
		// empty expression.KeyConditionBuilder
		return expression.KeyConditionBuilder{}
	}

	return *ks.KeyConditionBuilder
}
