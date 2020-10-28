package encoding

import (
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

// ProjectionBuilder returns a dynamo Expression name builder object with a set of Projection
// or if Projection param is nil then all fields Projection
func ProjectionBuilder(input interface{}) *expression.ProjectionBuilder {
	if input == nil {
		return nil
	}
	strNames, err := FieldNames(input)
	if err != nil {
		panic(err)
	}

	if len(strNames) < 1 {
		return nil
	}
	nameExpressions := make([]expression.NameBuilder, len(strNames))
	// getActive each fields by name
	for i, n := range strNames {
		nameExpressions[i] = expression.Name(n)
	}

	var exp expression.ProjectionBuilder
	if len(nameExpressions) > 1 {
		exp = expression.NamesList(nameExpressions[0], nameExpressions[1:]...)
	} else {
		exp = expression.NamesList(nameExpressions[0])
	}

	return &exp
}

// NameBuilders returns a dynamo Expression name builder object for the given input
func NameBuilders(input interface{}) []expression.NameBuilder {
	if input == nil {
		return nil
	}
	if _, ok := input.([]expression.NameBuilder); ok {
		return input.([]expression.NameBuilder)
	}
	strNames, err := FieldNames(input)
	if err != nil {
		panic(err)
	}

	if len(strNames) < 1 {
		return nil
	}
	nameBuilders := make([]expression.NameBuilder, len(strNames))
	// getActive each fields by name
	for i, n := range strNames {
		nameBuilders[i] = expression.Name(n)
	}

	return nameBuilders
}
