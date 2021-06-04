package condition

import (
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/ericmaustin/dyno/encoding"
)

// GreaterThan returns a Builder with a GreaterThan condition for the input name and value
func GreaterThan(name string, value interface{}) expression.ConditionBuilder {
	return expression.Name(name).GreaterThan(expression.Value(value))
}

// GreaterThanEqual returns a Builder with a GreaterThanEqual condition for the input name and value
func GreaterThanEqual(name string, value interface{}) expression.ConditionBuilder {
	return expression.Name(name).GreaterThanEqual(expression.Value(value))
}

// LessThan returns a Builder with a LessThan condition for the input name and value
func LessThan(name string, value interface{}) expression.ConditionBuilder {
	return expression.Name(name).LessThan(expression.Value(value))
}

// LessThanEqual returns a Builder with a LessThanEqual condition for the input name and value
func LessThanEqual(name string, value interface{}) expression.ConditionBuilder {
	return expression.Name(name).LessThanEqual(expression.Value(value))
}

// Equal returns a Builder with a In condition for the input name and value
func Equal(name string, value interface{}) expression.ConditionBuilder {
	return expression.Name(name).Equal(expression.Value(value))
}

// In returns a Builder with a In condition for the input name and values
func In(name string, values ...interface{}) expression.ConditionBuilder {
	encoded := make([]expression.ValueBuilder, len(values))
	for i, val := range values {
		encoded[i] = expression.Value(val)
	}
	var right expression.OperandBuilder
	other := make([]expression.OperandBuilder, 0)
	for _, v := range encoded {
		if right == nil {
			right = v
		} else {
			other = append(other, v)
		}
	}
	return expression.Name(name).In(right, other...)
}

// Between returns a Builder with a Between condition for the input name and values
func Between(name string, lower, upper interface{}) expression.ConditionBuilder {
	return expression.Name(name).Between(expression.Value(lower), expression.Value(upper))
}

// NotEqual returns a Builder with a NotEqual condition for the input name and value
func NotEqual(name string, value interface{}) expression.ConditionBuilder {
	return expression.Name(name).NotEqual(expression.Value(value))
}

// BeginsWith returns a Builder with a BeginsWith condition for the input name and value
func BeginsWith(name string, value interface{}) expression.ConditionBuilder {
	return expression.Name(name).BeginsWith(encoding.ToString(value))
}

// Contains returns a Builder with a Contains condition for the input name and value
func Contains(name string, value interface{}) expression.ConditionBuilder {
	return expression.Name(name).Contains(encoding.ToString(value))
}

// Exists returns a Builder with a AttributeExists condition for the input name
func Exists(name string) expression.ConditionBuilder {
	return expression.Name(name).AttributeExists()
}

// NotExists returns a Builder with a AttributeNotExists condition for the input name
func NotExists(name string) expression.ConditionBuilder {
	return expression.Name(name).AttributeNotExists()
}

//And (Condition And) returns an Expression.Build that joins one or more provided conditions with an AND condition
//	returns
//		expression.ConditionBuilder: the Expression builder value
func And(left expression.ConditionBuilder, right ...expression.ConditionBuilder) expression.ConditionBuilder {

	if len(right) < 1 {
		return left
	}

	cnd := left

	// join all with chained Ands
	for _, c := range right {
		cnd = cnd.And(c)
	}

	return cnd
}

// Or (Condition Or) returns an Expression.Build that joins one or more provided conditions with an OR condition
//	returns
//		expression.Builder: the Expression builder value
func Or(left expression.ConditionBuilder, right ...expression.ConditionBuilder) expression.ConditionBuilder {

	if len(right) < 1 {
		return left
	}

	cnd := left

	// join all with chained Ors
	for _, c := range right {
		cnd = cnd.Or(c)
	}

	return cnd
}

// Builder extends expression.ConditionBuilder to facilitate more dynamic condition building
type Builder struct {
	*expression.ConditionBuilder
}

// Empty returns true if the condition set is empty
func (b *Builder) Empty() bool {
	return b.ConditionBuilder == nil
}

// And adds an And condition to the set
func (b *Builder) And(conditions ...expression.ConditionBuilder) *Builder {

	var builder expression.ConditionBuilder

	switch len(conditions) {
	case 0:
		return b
	case 1:
		builder = conditions[0]
	default:
		builder = And(conditions[0], conditions[1:]...)
	}

	if b.Empty() {
		b.ConditionBuilder = &builder
		return b
	}
	builder = b.ConditionBuilder.And(builder)
	return b
}

// Or adds an Or condition to the set
func (b *Builder) Or(conditions ...expression.ConditionBuilder) *Builder {

	var builder expression.ConditionBuilder

	switch len(conditions) {
	case 0:
		return b
	case 1:
		builder = conditions[0]
	default:
		builder = And(conditions[0], conditions[1:]...)
	}

	if b.Empty() {
		b.ConditionBuilder = &builder
		return b
	}
	builder = b.ConditionBuilder.Or(builder)
	return b
}

// Builder returns the expression.ConditionBuilder
func (b *Builder) Builder() expression.ConditionBuilder {
	if b.ConditionBuilder == nil {
		return expression.ConditionBuilder{}
	}
	return *b.ConditionBuilder
}

// AddToExpression adds the Builder expression.ConditionBuilder to an expression.Builder if
// the Builder is not empty
func (b *Builder) AddToExpression(exp expression.Builder) expression.Builder {
	if b.Empty() {
		return exp
	}
	return exp.WithCondition(b.Builder())
}
