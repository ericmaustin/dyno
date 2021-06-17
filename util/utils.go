package util

import (
	"errors"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)



//CopyKeysAndAttributes creates a deep copy of a KeysAndAttributes
func CopyKeysAndAttributes(input ddbTypes.KeysAndAttributes) ddbTypes.KeysAndAttributes {
	clone := ddbTypes.KeysAndAttributes{}

	if len(input.Keys) > 0 {
		clone.Keys = make([]map[string]ddbTypes.AttributeValue, len(input.Keys))
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

// CopyBatchGetItemInput creates a deep copy of a BatchGetItemInput
func CopyBatchGetItemInput(input *ddb.BatchGetItemInput) *ddb.BatchGetItemInput {
	clone := &ddb.BatchGetItemInput{
		ReturnConsumedCapacity: input.ReturnConsumedCapacity,
	}

	if clone.RequestItems == nil {
		return clone
	}

	clone.RequestItems = make(map[string]ddbTypes.KeysAndAttributes, len(input.RequestItems))

	for k, v := range input.RequestItems {
		clone.RequestItems[k] = CopyKeysAndAttributes(v)
	}

	return clone
}

// CopyDeleteRequest creates a deep copy of a DeleteRequest
func CopyDeleteRequest(input *ddbTypes.DeleteRequest) *ddbTypes.DeleteRequest {
	if input == nil {
		return nil
	}
	return &ddbTypes.DeleteRequest{Key: CopyAttributeValueMap(input.Key)}
}

// CopyPutRequest creates a deep copy of a PutRequest
func CopyPutRequest(input *ddbTypes.PutRequest) *ddbTypes.PutRequest {
	if input == nil {
		return nil
	}
	return &ddbTypes.PutRequest{Item: CopyAttributeValueMap(input.Item)}
}

// CopyWriteRequest creates a deep copy of a WriteRequest
func CopyWriteRequest(input ddbTypes.WriteRequest) ddbTypes.WriteRequest {
	return ddbTypes.WriteRequest{
		DeleteRequest: CopyDeleteRequest(input.DeleteRequest),
		PutRequest:    CopyPutRequest(input.PutRequest),
	}
}

// CopyBatchWriteItemInput creates a deep copy of a v
func CopyBatchWriteItemInput(input *ddb.BatchWriteItemInput) *ddb.BatchWriteItemInput {
	clone := &ddb.BatchWriteItemInput{
		ReturnConsumedCapacity: input.ReturnConsumedCapacity,
		ReturnItemCollectionMetrics: input.ReturnItemCollectionMetrics,
	}

	if clone.RequestItems == nil {
		return clone
	}

	clone.RequestItems = make(map[string][]ddbTypes.WriteRequest, len(input.RequestItems))

	for k, v := range input.RequestItems {
		clone.RequestItems[k] = make([]ddbTypes.WriteRequest, len(v))
		for i, w := range input.RequestItems[k] {
			clone.RequestItems[k][i] = CopyWriteRequest(w)
		}
	}

	return clone
}
