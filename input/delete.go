package input

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/ericmaustin/dyno/condition"
	"github.com/ericmaustin/dyno/encoding"
)

// DeleteItemBuilder extends dynamodb.DeleteItemInput to support condition building
type DeleteItemBuilder struct {
	*dynamodb.DeleteItemInput
	cnd   *expression.ConditionBuilder
	tmpKey interface{}
}

// NewDeleteItemBuilder creates a new DeleteItemBuilder for a given table if tableName is not nil
func NewDeleteItemBuilder(tableName *string) *DeleteItemBuilder {
	return &DeleteItemBuilder{
		DeleteItemInput: &dynamodb.DeleteItemInput{
			TableName: tableName,
		},
	}
}

// SetKey sets the target key for the item to tbe deleted
func (builder *DeleteItemBuilder) SetKey(key interface{}) *DeleteItemBuilder  {
	builder.tmpKey = key
	return builder
}

// AddCondition adds a condition to this update
// adding multiple conditions by calling this multiple times will join the conditions with
// an AND
func (builder *DeleteItemBuilder) AddCondition(cnd expression.ConditionBuilder) *DeleteItemBuilder {
	if builder.cnd == nil {
		builder.cnd = &cnd
	} else {
		cnd = condition.And(*builder.cnd, cnd)
		builder.cnd = &cnd
	}
	return builder
}

// Build builds the dynamodb.DeleteItemInput
// returns error if expression builder returns an error
func (builder *DeleteItemBuilder) Build() (*dynamodb.DeleteItemInput, error) {
	var err error
	if builder.tmpKey != nil {
		builder.Key, err = encoding.MarshalItem(builder.tmpKey)
		if err != nil {
			return nil, fmt.Errorf("DeleteItemBuilder Build() failed while attempting to marshal item key: %v", err)
		}
	}
	if builder.cnd != nil {
		expr := expression.NewBuilder().WithCondition(*builder.cnd)
		e, err := expr.Build()
		if err != nil {
			return nil, err
		}
		builder.ConditionExpression = e.Condition()
		builder.ExpressionAttributeNames = e.Names()
		builder.ExpressionAttributeValues = e.Values()
	}
	return builder.DeleteItemInput, nil
}
