package util

import (
	"errors"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// CopyAttributeValue copies an attribute value
func CopyAttributeValue(av ddb.AttributeValue) ddb.AttributeValue {
	switch v := av.(type) {
	case *ddb.AttributeValueMemberS:
		return &ddb.AttributeValueMemberS{Value: v.Value}
	case *ddb.AttributeValueMemberN:
		return &ddb.AttributeValueMemberN{Value: v.Value}
	case *ddb.AttributeValueMemberBOOL:
		return &ddb.AttributeValueMemberBOOL{Value: v.Value}
	case *ddb.AttributeValueMemberB:
		newB := make([]byte, len(v.Value))
		copy(newB, v.Value)
		return &ddb.AttributeValueMemberB{Value: newB}
	case *ddb.AttributeValueMemberSS:
		newSS := make([]string, len(v.Value))
		copy(newSS, v.Value)
		return &ddb.AttributeValueMemberSS{Value: newSS}
	case *ddb.AttributeValueMemberNS:
		newNS := make([]string, len(v.Value))
		copy(newNS, v.Value)
		return &ddb.AttributeValueMemberNS{Value: newNS}
	case *ddb.AttributeValueMemberBS:
		newBS := make([][]byte, len(v.Value))
		for i, b := range v.Value {
			newBS[i] = make([]byte, len(b))
			copy(newBS[i], b)
		}
		return &ddb.AttributeValueMemberBS{Value: newBS}
	case *ddb.AttributeValueMemberM:
		m := make(map[string]ddb.AttributeValue, len(v.Value))
		for k, _v := range v.Value {
			m[k] = CopyAttributeValue(_v)
		}
		return &ddb.AttributeValueMemberM{Value: m}
	case *ddb.AttributeValueMemberL:
		l := make([]ddb.AttributeValue, len(v.Value))
		for i, _v := range v.Value {
			l[i] = CopyAttributeValue(_v)
		}
		return &ddb.AttributeValueMemberL{Value: l}
	case *ddb.AttributeValueMemberNULL:
		return &ddb.AttributeValueMemberNULL{Value: v.Value}
	}

	panic(errors.New("AttributeValue has an unknown type"))
}

//CopyCondition copies a Condition
func CopyCondition(cnd ddb.Condition) ddb.Condition {
	newCnd := ddb.Condition{
		ComparisonOperator: cnd.ComparisonOperator,
		AttributeValueList: make([]ddb.AttributeValue, len(cnd.AttributeValueList)),
	}
	for i, av := range cnd.AttributeValueList {
		newCnd.AttributeValueList[i] = CopyAttributeValue(av)
	}
	return newCnd
}

// CopyScan creates a deep copy of a ScanInput
// note: CopyScan does not copy legacy parameters
func CopyScan(input *dynamodb.ScanInput) *dynamodb.ScanInput {
	n := &dynamodb.ScanInput{
		TableName:              input.TableName,
		ConditionalOperator:    input.ConditionalOperator,
		ReturnConsumedCapacity: input.ReturnConsumedCapacity,
		Select:                 input.Select,
	}
	if input.AttributesToGet != nil {
		n.AttributesToGet = make([]string, len(input.AttributesToGet))
		copy(n.AttributesToGet, input.AttributesToGet)
	}
	if input.ConsistentRead != nil {
		n.ConsistentRead = new(bool)
		*n.ConsistentRead = *input.ConsistentRead
	}
	if input.ExclusiveStartKey != nil {
		n.ExclusiveStartKey = make(map[string]ddb.AttributeValue, len(input.ExclusiveStartKey))
		for k, v := range input.ExclusiveStartKey {
			n.ExclusiveStartKey[k] = CopyAttributeValue(v)
		}
	}
	if input.ExpressionAttributeNames != nil {
		n.ExpressionAttributeNames = make(map[string]string, len(input.ExpressionAttributeNames))
		for k, v := range input.ExpressionAttributeNames {
			n.ExpressionAttributeNames[k] = v
		}
	}
	if input.ExpressionAttributeValues != nil {
		n.ExpressionAttributeValues = make(map[string]ddb.AttributeValue, len(input.ExpressionAttributeValues))
		for k, v := range input.ExpressionAttributeValues {
			n.ExpressionAttributeValues[k] = CopyAttributeValue(v)
		}
	}
	if input.FilterExpression != nil {
		n.FilterExpression = new(string)
		*n.FilterExpression = *input.FilterExpression
	}
	if input.IndexName != nil {
		n.IndexName = new(string)
		*n.IndexName = *input.IndexName
	}
	if input.Limit != nil {
		n.Limit = new(int32)
		*n.Limit = *input.Limit
	}
	if input.ProjectionExpression != nil {
		n.ProjectionExpression = new(string)
		*n.ProjectionExpression = *input.ProjectionExpression
	}
	if input.ScanFilter != nil {
		n.ScanFilter = make(map[string]ddb.Condition, len(input.ScanFilter))
		for k, v := range input.ScanFilter {
			n.ScanFilter[k] = CopyCondition(v)
		}
	}
	if input.Segment != nil {
		n.Segment = new(int32)
		*n.Segment = *input.Segment
	}
	if input.TableName != nil {
		n.TableName = new(string)
		*n.TableName = *input.TableName
	}
	if input.TotalSegments != nil {
		n.TotalSegments = new(int32)
		*n.TotalSegments = *input.TotalSegments
	}
	return n
}

// CopyQuery creates a deep copy of a QueryInput
// note: CopyQuery does not copy legacy parameters
func CopyQuery(input *dynamodb.QueryInput) *dynamodb.QueryInput {
	n := &dynamodb.QueryInput{
		TableName:              input.TableName,
		ConditionalOperator:    input.ConditionalOperator,
		ReturnConsumedCapacity: input.ReturnConsumedCapacity,
		Select:                 input.Select,
	}
	if input.AttributesToGet != nil {
		n.AttributesToGet = make([]string, len(input.AttributesToGet))
		copy(n.AttributesToGet, input.AttributesToGet)
	}
	if input.ConsistentRead != nil {
		n.ConsistentRead = new(bool)
		*n.ConsistentRead = *input.ConsistentRead
	}
	if input.ExclusiveStartKey != nil {
		n.ExclusiveStartKey = make(map[string]ddb.AttributeValue, len(input.ExclusiveStartKey))
		for k, v := range input.ExclusiveStartKey {
			n.ExclusiveStartKey[k] = CopyAttributeValue(v)
		}
	}
	if input.ExpressionAttributeNames != nil {
		n.ExpressionAttributeNames = make(map[string]string, len(input.ExpressionAttributeNames))
		for k, v := range input.ExpressionAttributeNames {
			n.ExpressionAttributeNames[k] = v
		}
	}
	if input.ExpressionAttributeValues != nil {
		n.ExpressionAttributeValues = make(map[string]ddb.AttributeValue, len(input.ExpressionAttributeValues))
		for k, v := range input.ExpressionAttributeValues {
			n.ExpressionAttributeValues[k] = CopyAttributeValue(v)
		}
	}
	if input.KeyConditions != nil {
		n.KeyConditions = make(map[string]ddb.Condition)
		for k, v := range input.KeyConditions {
			n.KeyConditions[k] = CopyCondition(v)
		}
	}
	if input.KeyConditionExpression != nil {
		n.KeyConditionExpression = new(string)
		*n.KeyConditionExpression = *input.KeyConditionExpression
	}
	if input.FilterExpression != nil {
		n.FilterExpression = new(string)
		*n.FilterExpression = *input.FilterExpression
	}
	if input.IndexName != nil {
		n.IndexName = new(string)
		*n.IndexName = *input.IndexName
	}
	if input.Limit != nil {
		n.Limit = new(int32)
		*n.Limit = *input.Limit
	}
	if input.ProjectionExpression != nil {
		n.ProjectionExpression = new(string)
		*n.ProjectionExpression = *input.ProjectionExpression
	}
	if input.QueryFilter != nil {
		n.QueryFilter = make(map[string]ddb.Condition, len(input.QueryFilter))
		for k, v := range input.QueryFilter {
			n.QueryFilter[k] = CopyCondition(v)
		}
	}
	if input.TableName != nil {
		n.TableName = new(string)
		*n.TableName = *input.TableName
	}
	return n
}

func SplitScanIntoSegments(input *dynamodb.ScanInput, segments int32) (inputs []*dynamodb.ScanInput) {
	if input.TotalSegments == nil || *input.TotalSegments < 2 {
		// only one segment
		return []*dynamodb.ScanInput{input}
	}
	// split into multiple
	inputs = make([]*dynamodb.ScanInput, segments)
	for i := int32(0); i < segments; i++ {
		// copy the input
		scanCopy := CopyScan(input)
		// set the segment to i
		scanCopy.Segment = &i
		scanCopy.TotalSegments = &segments
		inputs[i] = scanCopy
	}
	return
}
