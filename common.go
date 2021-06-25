package dyno

import (
	"errors"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ericmaustin/dyno/encoding"
	"gopkg.in/yaml.v2"
)

// CopyAttributeValue creates a deep copy of an attribute value
func CopyAttributeValue(av types.AttributeValue) types.AttributeValue {
	switch v := av.(type) {
	case *types.AttributeValueMemberS:
		return &types.AttributeValueMemberS{Value: v.Value}
	case *types.AttributeValueMemberN:
		return &types.AttributeValueMemberN{Value: v.Value}
	case *types.AttributeValueMemberBOOL:
		return &types.AttributeValueMemberBOOL{Value: v.Value}
	case *types.AttributeValueMemberB:
		newB := make([]byte, len(v.Value))
		copy(newB, v.Value)

		return &types.AttributeValueMemberB{Value: newB}

	case *types.AttributeValueMemberSS:
		newSS := make([]string, len(v.Value))
		copy(newSS, v.Value)

		return &types.AttributeValueMemberSS{Value: newSS}

	case *types.AttributeValueMemberNS:
		newNS := make([]string, len(v.Value))
		copy(newNS, v.Value)

		return &types.AttributeValueMemberNS{Value: newNS}

	case *types.AttributeValueMemberBS:
		newBS := make([][]byte, len(v.Value))
		for i, b := range v.Value {
			newBS[i] = make([]byte, len(b))
			copy(newBS[i], b)
		}

		return &types.AttributeValueMemberBS{Value: newBS}

	case *types.AttributeValueMemberM:
		return &types.AttributeValueMemberM{Value: CopyAttributeValueMap(v.Value)}
	case *types.AttributeValueMemberL:
		l := make([]types.AttributeValue, len(v.Value))
		for i, _v := range v.Value {
			l[i] = CopyAttributeValue(_v)
		}

		return &types.AttributeValueMemberL{Value: l}

	case *types.AttributeValueMemberNULL:
		return &types.AttributeValueMemberNULL{Value: v.Value}
	}

	panic(errors.New("AttributeValue has an unknown type"))
}

// CopyAttributeValueMap creates a deep copy of a `map[string]ddb.AttributeValue`
func CopyAttributeValueMap(input map[string]types.AttributeValue) map[string]types.AttributeValue {
	if input == nil {
		return nil
	}
	out := make(map[string]types.AttributeValue, len(input))
	for k, v := range input {
		out[k] = CopyAttributeValue(v)
	}
	return out
}

// CopyCondition copies a Condition
func CopyCondition(cnd types.Condition) types.Condition {
	newCnd := types.Condition{
		ComparisonOperator: cnd.ComparisonOperator,
		AttributeValueList: make([]types.AttributeValue, len(cnd.AttributeValueList)),
	}
	for i, av := range cnd.AttributeValueList {
		newCnd.AttributeValueList[i] = CopyAttributeValue(av)
	}
	return newCnd
}

//addProjectionNames adds a string slice of projection values to a expression.ProjectionBuilder
func addProjectionNames(projectionBuilder *expression.ProjectionBuilder, names []string) {
	//nameBuilders := encoding.NameBuilders(names)
	nameBuilders := make([]expression.NameBuilder, len(names))
	for i, name := range names {
		nameBuilders[i] = expression.Name(name)
	}
	if projectionBuilder != nil {
		*projectionBuilder = projectionBuilder.AddNames(nameBuilders...)
		return
	}

	proj := expression.ProjectionBuilder{}
	proj = proj.AddNames(nameBuilders...)
	projectionBuilder = &proj
}

//addProjection adds a projection value interface to a expression.ProjectionBuilder
func addProjection(projectionBuilder *expression.ProjectionBuilder, projection interface{}) {
	nameBuilders := encoding.NameBuilders(projection)
	if projectionBuilder != nil {
		*projectionBuilder = projectionBuilder.AddNames(nameBuilders...)
		return
	}
	proj := expression.ProjectionBuilder{}
	proj = proj.AddNames(nameBuilders...)
	projectionBuilder = &proj
}

//
//// IsAwsErrorCode checks to see if the provided err is an aws error, and if so if it matches any of the provided codes
//func IsAwsErrorCode(err error, codes ...string) bool {
//	if err == nil {
//		return false
//	}
//	if awsErr, ok := err.(awserr.Error); ok {
//		for _, code := range codes {
//			if awsErr.Code() == code {
//				return true
//			}
//		}
//	}
//	return false
//}

// CopyKeysAndAttributes creates a deep copy of a KeysAndAttributes
func CopyKeysAndAttributes(input types.KeysAndAttributes) types.KeysAndAttributes {
	clone := types.KeysAndAttributes{}

	if len(input.Keys) > 0 {
		clone.Keys = make([]map[string]types.AttributeValue, len(input.Keys))
		for i, m := range clone.Keys {
			clone.Keys[i] = CopyAttributeValueMap(m)
		}
	}

	if len(input.AttributesToGet) > 0 {
		copy(clone.AttributesToGet, input.AttributesToGet)
	}

	if input.ConsistentRead != nil {
		clone.ConsistentRead = new(bool)
		*clone.ConsistentRead = *input.ConsistentRead
	}

	if input.ExpressionAttributeNames != nil {
		clone.ExpressionAttributeNames = make(map[string]string, len(input.ExpressionAttributeNames))
		for k, v := range input.ExpressionAttributeNames {
			clone.ExpressionAttributeNames[k] = v
		}
	}

	if input.ProjectionExpression != nil {
		clone.ProjectionExpression = new(string)
		*clone.ProjectionExpression = *input.ProjectionExpression
	}

	return clone
}

// CopyKeysAndAttributesMap creates a deep copy of a map of KeysAndAttributes
func CopyKeysAndAttributesMap(input map[string]types.KeysAndAttributes) map[string]types.KeysAndAttributes {
	if input == nil {
		return nil
	}
	out := make(map[string]types.KeysAndAttributes, len(input))
	for k, v := range input {
		out[k] = CopyKeysAndAttributes(v)
	}
	return out
}

// MustYamlString is a convenience function that generates a yaml string or panics
func MustYamlString(in interface{}) string {
	out, err := yaml.Marshal(in)

	if err != nil {
		panic(err)
	}

	return string(out)
}
