package operation

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/ericmaustin/dyno/condition"
	"github.com/ericmaustin/dyno/encoding"
)

// DeleteBuilder builds DeleteItemInputs for use with the DeleteItemOperation
type DeleteBuilder struct {
	*dynamodb.DeleteItemInput
	cnd   *expression.ConditionBuilder
}

// NewBuilder creates a new DeleteBuilder with optional existing UpdateItemInput as the BaseOperation
func NewBuilder() *DeleteBuilder {
	return &DeleteBuilder{
		DeleteItemInput: new(dynamodb.DeleteItemInput),
	}
}

// SetKey sets the target key for the item to tbe deleted
func (b *DeleteBuilder) SetKey(key interface{}) (err error) {
	b.Key, err = encoding.MarshalItem(key)
	return
}

// AddCondition adds a condition to this update
// adding multiple conditions by calling this multiple times will join the conditions with
// an AND
func (b *DeleteBuilder) AddCondition(cnd expression.ConditionBuilder) {
	if b.cnd == nil {
		b.cnd = &cnd
	} else {
		cnd = condition.And(*b.cnd, cnd)
		b.cnd = &cnd
	}
}

// SetTable sets the table name
func (b *DeleteBuilder) SetTable(table string) {
	b.SetTableName(table)
}

// Build builds the dynamodb.DeleteItemInput
func (b *DeleteBuilder) Build() *dynamodb.DeleteItemInput {
	if b.cnd != nil {
		expr := expression.NewBuilder().WithCondition(*b.cnd)
		e, buildErr := expr.Build()
		if buildErr != nil {
			panic(buildErr)
		}
		b.ConditionExpression = e.Condition()
		b.ExpressionAttributeNames = e.Names()
		b.ExpressionAttributeValues = e.Values()
	}
	return b.DeleteItemInput
}