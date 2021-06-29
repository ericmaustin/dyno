package condition

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// CheckBuilder builds a ConditionCheck
type CheckBuilder struct {
	*types.ConditionCheck
	cnd Builder
}

func (c *CheckBuilder) SetReturnValuesOnConditionCheckFailure(v types.ReturnValuesOnConditionCheckFailure) *CheckBuilder {
	c.ReturnValuesOnConditionCheckFailure = v
	return c
}

// SetTableName sets the table name
func (c *CheckBuilder) SetTableName(tableName string) *CheckBuilder {
	c.TableName = &tableName
	return c
}

// SetKey sets the condition check key
func (c *CheckBuilder) SetKey(item map[string]types.AttributeValue) *CheckBuilder {
	c.Key = item
	return c
}

// AddCondition adds a condition
func (c *CheckBuilder) AddCondition(cnd expression.ConditionBuilder) *CheckBuilder {
	c.cnd.And(cnd)

	return c
}

// Build builds the dynamodb.ScanInput
func (c *CheckBuilder) Build() (*types.ConditionCheck, error) {
	if !c.cnd.Empty() {
		// only use expression builder if we have a projection or a filter
		eb := expression.NewBuilder()

		eb = eb.WithCondition(c.cnd.Builder())

		// build the Expression
		expr, err := eb.Build()

		if err != nil {
			return nil, fmt.Errorf("CheckBuilder Build() failed while attempting to build expression: %v", err)
		}

		c.ExpressionAttributeNames = expr.Names()
		c.ExpressionAttributeValues = expr.Values()
	}

	return c.ConditionCheck, nil
}