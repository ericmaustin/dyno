package input

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/ericmaustin/dyno/encoding"
)

func addProjectionNames(projectionBuilder *expression.ProjectionBuilder, names []string) {
	//nameBuilders := encoding.NameBuilders(names)
	nameBuilders := make([]expression.NameBuilder, len(names))
	for i, name := range names {
		nameBuilders[i] = expression.Name(name)
	}
	if projectionBuilder == nil {
		proj := expression.ProjectionBuilder{}
		proj = proj.AddNames(nameBuilders...)
		projectionBuilder = &proj
	} else {
		*projectionBuilder = projectionBuilder.AddNames(nameBuilders...)
	}
}

func addProjection(projectionBuilder *expression.ProjectionBuilder, projection interface{}) {
	nameBuilders := encoding.NameBuilders(projection)
	if projectionBuilder== nil {
		proj := expression.ProjectionBuilder{}
		proj = proj.AddNames(nameBuilders...)
		projectionBuilder = &proj
	} else {
		*projectionBuilder = projectionBuilder.AddNames(nameBuilders...)
	}
}

// KeysAndAttributes used to create a dynamodb KeysAndAttributes struct easily
func KeysAndAttributes(itemKeys interface{}, projection interface{}, consistentRead bool) (*dynamodb.KeysAndAttributes, error) {
	item, err := encoding.MarshalItems(itemKeys)
	if err != nil {
		return nil, err
	}

	k := &dynamodb.KeysAndAttributes{
		Keys:           item,
		ConsistentRead: &consistentRead,
	}

	if projection != nil {
		builder := expression.NewBuilder().
			WithProjection(*encoding.ProjectionBuilder(projection))
		expr, err := builder.Build()
		if err != nil {
			return nil, err
		}
		k.ExpressionAttributeNames = expr.Names()
		k.ProjectionExpression = expr.Projection()
	}
	return k, nil
}