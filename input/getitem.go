package input

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/ericmaustin/dyno/encoding"
)

// GetItemBuilder is used to dynamically build a get request
type GetItemBuilder struct {
	*dynamodb.GetItemInput
	projection *expression.ProjectionBuilder
}

// NewGetBuilder returns a new GetItemBuilder for given tableName if tableName is not nil
func NewGetBuilder(tbl *string) *GetItemBuilder {
	return &GetItemBuilder{
		GetItemInput: &dynamodb.GetItemInput{
			TableName: tbl,
		},
	}
}

// SetInput sets the GetItemBuilder's dynamodb.GetItemInput
func (builder *GetItemBuilder) SetInput(input *dynamodb.GetItemInput) *GetItemBuilder {
	builder.GetItemInput = input
	return builder
}

// SetKey sets the key for the get input
func (builder *GetItemBuilder) SetKey(key interface{}) *GetItemBuilder {
	builder.Key = encoding.MustMarshalItem(key)
	return builder
}

// AddProjection adds additional field names to the projection
func (builder *GetItemBuilder) AddProjection(projection interface{}) *GetItemBuilder {
	addProjection(builder.projection, projection)
	return builder
}

// AddProjectionNames adds additional field names to the projection with strings
func (builder *GetItemBuilder) AddProjectionNames(names ...string) *GetItemBuilder {
	addProjectionNames(builder.projection, names)
	return builder
}

// Build returns a GetOperation using the GetItemInput and ProjectionBuilder
// returns error if expression builder returns an error
func (builder *GetItemBuilder) Build() (*dynamodb.GetItemInput, error) {
	if builder.projection != nil {
		// only use expression builder if we have a projection or a filter
		eb := expression.NewBuilder()
		eb = eb.WithProjection(*builder.projection)

		// build the Expression
		expr, err := eb.Build()
		if err != nil {
			return nil, fmt.Errorf("GetItemBuilder Build() failed while attempting to build expression: %v", err)
		}
		builder.ExpressionAttributeNames = expr.Names()
		builder.ProjectionExpression = expr.Projection()
	}
	return builder.GetItemInput, nil
}

