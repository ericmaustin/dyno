package encoding

import (
	"fmt"
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
	var (
		strNames []string
		err      error
	)

	switch inputTyped := input.(type) {
	case string:
		strNames = []string{inputTyped}
	case *string:
		strNames = []string{*inputTyped}
	case fmt.Stringer:
		strNames = []string{inputTyped.String()}
	case []string:
		strNames = append(strNames, inputTyped...)
	case []*string:
		for _, s := range inputTyped {
			strNames = append(strNames, *s)
		}
	case []fmt.Stringer:
		for _, s := range inputTyped {
			strNames = append(strNames, s.String())
		}
	case []expression.NameBuilder:
		return inputTyped
	default:
		strNames, err = FieldNames(input)
		if err != nil {
			panic(err)
		}
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
