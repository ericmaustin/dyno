package input

import "github.com/aws/aws-sdk-go/service/dynamodb"

//DeleteTableInput creates a new delete table input for the given table name
func DeleteTableInput(tableName *string) *dynamodb.DeleteTableInput {
	return &dynamodb.DeleteTableInput{TableName: tableName}
}
