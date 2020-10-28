package condition

import (
	"fmt"
	"git-codecommit.us-east-1.amazonaws.com/v1/repos/dyno.git"
	"git-codecommit.us-east-1.amazonaws.com/v1/repos/dyno.git/encoding"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
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
	// OperatorNotExists
	OperatorNotExists = Operator("nex")
)

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

	var (
		builder expression.ConditionBuilder
	)

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
	return builder
}

func GreaterThan(name string, value interface{}) expression.ConditionBuilder {
	return expression.Name(name).GreaterThan(expression.Value(value))
}

func GreaterThanEqual(name string, value interface{}) expression.ConditionBuilder {
	return expression.Name(name).GreaterThanEqual(expression.Value(value))
}

func LessThan(name string, value interface{}) expression.ConditionBuilder {
	return expression.Name(name).LessThan(expression.Value(value))
}

func LessThanEqual(name string, value interface{}) expression.ConditionBuilder {
	return expression.Name(name).LessThanEqual(expression.Value(value))
}

func Equal(name string, value interface{}) expression.ConditionBuilder {
	return expression.Name(name).Equal(expression.Value(value))
}

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

func Between(name string, lower, upper interface{}) expression.ConditionBuilder {
	return expression.Name(name).Between(expression.Value(lower), expression.Value(upper))
}

func NotEqual(name string, value interface{}) expression.ConditionBuilder {
	return expression.Name(name).NotEqual(expression.Value(value))
}

func BeginsWith(name string, value interface{}) expression.ConditionBuilder {
	return expression.Name(name).BeginsWith(encoding.ToString(value))
}

func Contains(name string, value interface{}) expression.ConditionBuilder {
	return expression.Name(name).Contains(encoding.ToString(value))
}

func Exists(name string) expression.ConditionBuilder {
	return expression.Name(name).AttributeExists()
}

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

type Set struct {
	dynamoConditionBuilder *expression.ConditionBuilder
}

func NewSet() *Set {
	return &Set{}
}

func (b *Set) Empty() bool {
	return b.dynamoConditionBuilder == nil
}

func (b *Set) AddAnd(conditions ...expression.ConditionBuilder) {
	builder := And(conditions...)
	if b.dynamoConditionBuilder != nil {
		builder = b.dynamoConditionBuilder.And(builder)
	}
	b.dynamoConditionBuilder = &builder
}

func (b *Set) AddOr(conditions ...expression.ConditionBuilder) {
	builder := Or(conditions...)
	if b.dynamoConditionBuilder != nil {
		builder = b.dynamoConditionBuilder.Or(builder)
	}
	b.dynamoConditionBuilder = &builder

}

func (b *Set) Builder() expression.ConditionBuilder {
	if b.dynamoConditionBuilder == nil {
		return expression.ConditionBuilder{}
	}
	return *b.dynamoConditionBuilder
}
