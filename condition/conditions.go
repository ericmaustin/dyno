package condition

import (
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/ericmaustin/dyno"
	"github.com/ericmaustin/dyno/encoding"
)

// Operator is a string that contains the operator type
type Operator string

const (
	// OperatorEqual "="
	OperatorEqual = Operator("=")
	// OperatorNotEquals "<>"
	OperatorNotEquals = Operator("<>")
	// OperatorLessThan "<"
	OperatorLessThan = Operator("<")
	// OperatorLessThanEqual "<="
	OperatorLessThanEqual = Operator("<=")
	// OperatorGreaterThan ">"
	OperatorGreaterThan = Operator(">")
	// OperatorGreaterThanEqual ">="
	OperatorGreaterThanEqual = Operator(">=")
	// OperatorBetween "><"
	OperatorBetween = Operator("><")
	// OperatorBeginsWith "begins"
	OperatorBeginsWith = Operator("begins")
	// OperatorContains "contains"
	OperatorContains = Operator("contains")
	// OperatorIn "in"
	OperatorIn = Operator("in")
	// OperatorExists "HasValue"
	OperatorExists = Operator("ex")
	// OperatorNotExists operator "nex"
	OperatorNotExists = Operator("nex")
)

// StringToOperator converts a string input to an Operator
func StringToOperator(in string) Operator {
	// if we have ae string, figure out which operator const to use
	switch in {
	case "=":
		return OperatorEqual
	case "<>", "!=":
		return OperatorNotEquals
	case ">":
		return OperatorGreaterThan
	case "<":
		return OperatorLessThan
	case ">=":
		return OperatorGreaterThanEqual
	case "<=":
		return OperatorLessThanEqual
	case "><":
		return OperatorBetween
	case "between":
		return OperatorBetween
	case "b", "begins":
		return OperatorBeginsWith
	case "c", "contains":
		return OperatorContains
	case "in":
		return OperatorIn
	case "exists", "ex":
		return OperatorExists
	case "not exists", "nex", "missing":
		return OperatorExists
	}
	panic(fmt.Errorf("no operator found for %s", in))
}

/*
Condition creates an dynamodb.Expression.Builder with a provided field name ``name``, ``operator`` as a ``string``
or as an ``Operator`` type, and values
This func will panic on error
*/
func Condition(name string, operator interface{}, values ...interface{}) expression.ConditionBuilder {
	var op Operator

	switch o := operator.(type) {
	case string:
		op = StringToOperator(o)
	case fmt.Stringer:
		op = StringToOperator(o.String())
	case Operator:
		// use operators as is
		op = o
	default:
		panic(&dyno.Error{
			Code:    dyno.ErrOperatorNotFound,
			Message: fmt.Sprintf("No operator exists for '%v'", o)})
	}

	switch op {
	case OperatorEqual:
		return Equal(name, values[0])
	case OperatorNotEquals:
		return NotEqual(name, values[0])
	case OperatorLessThan:
		return LessThan(name, values[0])
	case OperatorLessThanEqual:
		return LessThanEqual(name, values[0])
	case OperatorGreaterThan:
		return GreaterThan(name, values[0])
	case OperatorGreaterThanEqual:
		return GreaterThanEqual(name, values[0])
	case OperatorBetween:
		return Between(name, values[0], values[1])
	case OperatorIn:
		return In(name, values...)
	case OperatorBeginsWith:
		return BeginsWith(name, values[0])
	case OperatorContains:
		return Contains(name, values[0])
	case OperatorExists:
		return Exists(name)
	case OperatorNotExists:
		return NotExists(name)
	default:
		// no matching condition found
		panic(&dyno.Error{
			Code:    dyno.ErrOperatorNotFound,
			Message: fmt.Sprintf("No operator exists for '%v'", operator),
		})
	}
}

// GreaterThan returns a ConditionBuilder with a GreaterThan condition for the input name and value
func GreaterThan(name string, value interface{}) expression.ConditionBuilder {
	return expression.Name(name).GreaterThan(expression.Value(value))
}

// GreaterThanEqual returns a ConditionBuilder with a GreaterThanEqual condition for the input name and value
func GreaterThanEqual(name string, value interface{}) expression.ConditionBuilder {
	return expression.Name(name).GreaterThanEqual(expression.Value(value))
}

// LessThan returns a ConditionBuilder with a LessThan condition for the input name and value
func LessThan(name string, value interface{}) expression.ConditionBuilder {
	return expression.Name(name).LessThan(expression.Value(value))
}

// LessThanEqual returns a ConditionBuilder with a LessThanEqual condition for the input name and value
func LessThanEqual(name string, value interface{}) expression.ConditionBuilder {
	return expression.Name(name).LessThanEqual(expression.Value(value))
}

// Equal returns a ConditionBuilder with a In condition for the input name and value
func Equal(name string, value interface{}) expression.ConditionBuilder {
	return expression.Name(name).Equal(expression.Value(value))
}

// In returns a ConditionBuilder with a In condition for the input name and values
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

// Between returns a ConditionBuilder with a Between condition for the input name and values
func Between(name string, lower, upper interface{}) expression.ConditionBuilder {
	return expression.Name(name).Between(expression.Value(lower), expression.Value(upper))
}

// NotEqual returns a ConditionBuilder with a NotEqual condition for the input name and value
func NotEqual(name string, value interface{}) expression.ConditionBuilder {
	return expression.Name(name).NotEqual(expression.Value(value))
}

// BeginsWith returns a ConditionBuilder with a BeginsWith condition for the input name and value
func BeginsWith(name string, value interface{}) expression.ConditionBuilder {
	return expression.Name(name).BeginsWith(encoding.ToString(value))
}

// Contains returns a ConditionBuilder with a Contains condition for the input name and value
func Contains(name string, value interface{}) expression.ConditionBuilder {
	return expression.Name(name).Contains(encoding.ToString(value))
}

// Exists returns a ConditionBuilder with a AttributeExists condition for the input name
func Exists(name string) expression.ConditionBuilder {
	return expression.Name(name).AttributeExists()
}

// NotExists returns a ConditionBuilder with a AttributeNotExists condition for the input name
func NotExists(name string) expression.ConditionBuilder {
	return expression.Name(name).AttributeNotExists()
}

/*
And (Condition And) returns an Expression.Builder that joins one or more provided conditions with an AND condition
	returns
		Expression.Builder: the Expression builder value
		error: an error thrown by the fields.Builder method if any
*/
func And(conditions ...expression.ConditionBuilder) expression.ConditionBuilder {

	if len(conditions) == 1 {
		// if we only have 1 condition just return it
		return conditions[0]
	}

	// else we have multiple conditions
	cnd, conditions := conditions[0], conditions[1:]

	for _, c := range conditions {
		cnd = cnd.And(c)
	}

	return cnd
}

// Or (Condition Or) returns an Expression.Builder that joins one or more provided conditions with an OR condition
//	returns
//		Expression.Builder: the Expression builder value
//		error: an error thrown by the fields.Builder method if any
func Or(conditions ...expression.ConditionBuilder) expression.ConditionBuilder {

	if len(conditions) == 1 {
		// if we only have 1 condition just return it
		return conditions[0]
	}

	cnd, conditions := conditions[0], conditions[1:]

	for _, c := range conditions {
		cnd = cnd.Or(c)
	}

	return cnd
}

// Set represents a condition builder that can have conditions set dynmaically
type Set struct {
	dynamoConditionBuilder *expression.ConditionBuilder
}

// NewSet creates a new condition builder set
func NewSet() *Set {
	return &Set{}
}

// IsEmpty checks is the condition set is empty
func (b *Set) IsEmpty() bool {
	return b.dynamoConditionBuilder == nil
}

// AddAnd adds an And condition to the set
func (b *Set) AddAnd(conditions ...expression.ConditionBuilder) {
	builder := And(conditions...)
	if b.dynamoConditionBuilder != nil {
		builder = b.dynamoConditionBuilder.And(builder)
	}
	b.dynamoConditionBuilder = &builder
}

// AddOr adds an Or condition to the set
func (b *Set) AddOr(conditions ...expression.ConditionBuilder) {
	builder := Or(conditions...)
	if b.dynamoConditionBuilder != nil {
		builder = b.dynamoConditionBuilder.Or(builder)
	}
	b.dynamoConditionBuilder = &builder

}

// Builder returns the condition set's complete ConditionBuilder
func (b *Set) Builder() expression.ConditionBuilder {
	if b.dynamoConditionBuilder == nil {
		return expression.ConditionBuilder{}
	}
	return *b.dynamoConditionBuilder
}
