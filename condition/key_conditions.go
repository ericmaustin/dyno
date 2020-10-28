package condition

import (
	"fmt"
	"git-codecommit.us-east-1.amazonaws.com/v1/repos/dyno.git"
	"git-codecommit.us-east-1.amazonaws.com/v1/repos/dyno.git/encoding"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

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

func KeyGreaterThan(name string, value interface{}) expression.KeyConditionBuilder {
	return expression.Key(name).GreaterThan(expression.Value(value))
}

func KeyGreaterThanEqual(name string, value interface{}) expression.KeyConditionBuilder {
	return expression.Key(name).GreaterThanEqual(expression.Value(value))
}

func KeyLessThan(name string, value interface{}) expression.KeyConditionBuilder {
	return expression.Key(name).LessThan(expression.Value(value))
}

func KeyLessThanEqual(name string, value interface{}) expression.KeyConditionBuilder {
	return expression.Key(name).LessThanEqual(expression.Value(value))
}

func KeyEqual(name string, value interface{}) expression.KeyConditionBuilder {
	return expression.Key(name).Equal(expression.Value(value))
}

func KeyBetween(name string, a, b interface{}) expression.KeyConditionBuilder {
	return expression.Key(name).Between(expression.Value(a), expression.Value(b))
}

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
