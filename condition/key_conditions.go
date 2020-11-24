package condition

import (
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/ericmaustin/dyno"
	"github.com/ericmaustin/dyno/encoding"
)

// StringToKeyOperator converts the given string to an Operator
func StringToKeyOperator(in string) Operator {
	// if we have ae string, figure out which operator const to use
	switch in {
	case "=":
		return OperatorEqual
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
	default:
		panic(fmt.Errorf("no key operator found for %s", in))
	}
}

// GetKeyCondition for provided key string, operator and ProjectionColumns
func GetKeyCondition(key string, operator interface{}, values ...interface{}) expression.KeyConditionBuilder {

	var op Operator

	switch o := operator.(type) {
	case string:
		op = StringToKeyOperator(o)
	case fmt.Stringer:
		op = StringToKeyOperator(o.String())
	case Operator:
		// use operators as is
		op = o
	default:
		panic(&dyno.Error{
			Code:    dyno.ErrOperatorNotFound,
			Message: fmt.Sprintf("No key operator exists for '%v'", o)})
	}

	switch op {
	case OperatorEqual:
		return KeyEqual(key, values[0])
	case OperatorLessThan:
		return KeyLessThan(key, values[0])
	case OperatorLessThanEqual:
		return KeyLessThanEqual(key, values[0])
	case OperatorGreaterThan:
		return KeyGreaterThan(key, values[0])
	case OperatorGreaterThanEqual:
		return KeyGreaterThanEqual(key, values[0])
	case OperatorBetween:
		return KeyBetween(key, values[0], values[1])
	// string only operators
	case OperatorBeginsWith:
		return KeyBeginsWith(key, values[0])
	}

	panic(&dyno.Error{
		Code:    dyno.ErrOperatorNotFound,
		Message: fmt.Sprintf("No operator exists for '%v'", op)})
}

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
func KeyAnd(conditions ...expression.KeyConditionBuilder) expression.KeyConditionBuilder {

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
