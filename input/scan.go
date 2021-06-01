package input

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/ericmaustin/dyno/condition"
	"github.com/ericmaustin/dyno/encoding"
)

//scanSelect used to tell the ScanBuilder operation what values to select
type scanSelect string

const (
	//ScanSelectAllAttributes returns all attributes
	ScanSelectAllAttributes = scanSelect("ALL_ATTRIBUTES")
	//ScanSelectAllProjectedAttributes returns all projected attributes
	ScanSelectAllProjectedAttributes = scanSelect("ALL_PROJECTED_ATTRIBUTES")
	//ScanSelectCount returns just the COUNT
	ScanSelectCount = scanSelect("COUNT")
	//ScanSelectSpecificAttributes returns only the selected attributes
	ScanSelectSpecificAttributes = scanSelect("SPECIFIC_ATTRIBUTES")
)

//ScanBuilder extends dynamodb.ScanInput too allow dynamic input building
type ScanBuilder struct {
	*dynamodb.ScanInput
	filter     *expression.ConditionBuilder
	projection *expression.ProjectionBuilder
}

// NewScanBuilder creates a new scan builder for provided table if tableName is not nil
func NewScanBuilder(tableName *string) *ScanBuilder {
	q := &ScanBuilder{
		ScanInput: &dynamodb.ScanInput{
			TableName: tableName,
		},
	}
	return q
}

// SetInput sets the ScanBuilder's dynamodb.ScanInput
func (s *ScanBuilder) SetInput(input *dynamodb.ScanInput) *ScanBuilder {
	s.ScanInput = input
	return s
}

// SetSelect sets the ScanBuilder's Select value
func (s *ScanBuilder) SetSelect(sel scanSelect) *ScanBuilder {
	s.ScanInput.SetSelect(string(sel))
	return s
}

//AddProjection adds additional field names to the projection
func (s *ScanBuilder) AddProjection(names interface{})  *ScanBuilder {
	nameBuilders := encoding.NameBuilders(names)
	if s.projection == nil {
		proj := expression.ProjectionBuilder{}
		proj = proj.AddNames(nameBuilders...)
		s.projection = &proj
	} else {
		*s.projection = s.projection.AddNames(nameBuilders...)
	}
	return s
}

// AddProjectionNames adds additional field names to the projection with strings
func (s *ScanBuilder) AddProjectionNames(names ...string) *ScanBuilder {
	//nameBuilders := encoding.NameBuilders(names)
	nameBuilders := make([]expression.NameBuilder, len(names))
	for i, name := range names {
		nameBuilders[i] = expression.Name(name)
	}
	if s.projection == nil {
		proj := expression.ProjectionBuilder{}
		proj = proj.AddNames(nameBuilders...)
		s.projection = &proj
	} else {
		*s.projection = s.projection.AddNames(nameBuilders...)
	}
	return s
}


// AddFilter adds a filter condition to the scan
// adding multiple conditions by calling this multiple times will join the conditions with
// an AND
func (s *ScanBuilder) AddFilter(cnd expression.ConditionBuilder) *ScanBuilder {
	if s.filter == nil {
		s.filter = &cnd
	} else {
		cnd = condition.And(*s.filter, cnd)
		s.filter = &cnd
	}
	return s
}

// Build builds the input input with included projection, key conditions, and filters
func (s *ScanBuilder) Build() (*dynamodb.ScanInput, error) {
	if s.projection != nil || s.filter != nil {
		// only use expression builder if we have a projection or a filter
		builder := expression.NewBuilder()
		// add projection
		if s.projection != nil {
			builder = builder.WithProjection(*s.projection)
		}
		// add filter
		if s.filter != nil {
			builder = builder.WithFilter(*s.filter)
		}
		// build the Expression
		expr, err := builder.Build()
		if err != nil {
			return nil, fmt.Errorf("ScanBuilder Build() failed while attempting to build expression: %v", err)
		}
		s.ExpressionAttributeNames = expr.Names()
		s.ExpressionAttributeValues = expr.Values()
		s.FilterExpression = expr.Filter()
		s.ProjectionExpression = expr.Projection()
	}
	return s.ScanInput, nil
}

// BuildSegments builds the input input with included projection and creates seperate inputs for each segment
func (s *ScanBuilder) BuildSegments(segments int64) ([]*dynamodb.ScanInput, error) {
	if _, err := s.Build(); err != nil {
		return nil, err
	}

	if segments > 0 {
		s.TotalSegments = &segments
	}

	return splitScanInputIntoSegments(s.ScanInput), nil
}

// CopyInput creates a copy of the ScanInput
func CopyInput(input *dynamodb.ScanInput) *dynamodb.ScanInput {
	n := &dynamodb.ScanInput{}
	if input.AttributesToGet != nil {
		attributesToGet := make([]*string, len(input.AttributesToGet))
		copy(attributesToGet, input.AttributesToGet)
		n.SetAttributesToGet(attributesToGet)
	}
	if input.ConditionalOperator != nil {
		n.SetConditionalOperator(*input.ConditionalOperator)
	}
	if input.ConsistentRead != nil {
		n.SetConsistentRead(*input.ConsistentRead)
	}
	if input.ExclusiveStartKey != nil {
		exclusiveStartKey := make(map[string]*dynamodb.AttributeValue)
		for k, v := range input.ExclusiveStartKey {
			newV := *v
			exclusiveStartKey[k] = &newV
		}
		n.SetExclusiveStartKey(exclusiveStartKey)
	}
	if input.ExpressionAttributeNames != nil {
		expressionAttributeNames := make(map[string]*string)
		for k, v := range input.ExpressionAttributeNames {
			newV := *v
			expressionAttributeNames[k] = &newV
		}
		n.SetExpressionAttributeNames(expressionAttributeNames)
	}
	if input.ExpressionAttributeValues != nil {
		expressionAttributeValues := make(map[string]*dynamodb.AttributeValue)
		for k, v := range input.ExpressionAttributeValues {
			newV := *v
			expressionAttributeValues[k] = &newV
		}
		n.SetExpressionAttributeValues(expressionAttributeValues)
	}
	if input.FilterExpression != nil {
		n.SetFilterExpression(*input.FilterExpression)
	}
	if input.IndexName != nil {
		n.SetFilterExpression(*input.IndexName)
	}
	if input.Limit != nil {
		n.SetLimit(*input.Limit)
	}
	if input.ProjectionExpression != nil {
		n.SetProjectionExpression(*input.ProjectionExpression)
	}
	if input.ReturnConsumedCapacity != nil {
		n.SetReturnConsumedCapacity(*input.ReturnConsumedCapacity)
	}
	if input.ScanFilter != nil {
		scanFilter := make(map[string]*dynamodb.Condition)
		for k, v := range input.ScanFilter {
			newV := *v
			scanFilter[k] = &newV
		}
		n.SetScanFilter(scanFilter)
	}
	if input.Segment != nil {
		n.SetSegment(*input.Segment)
	}
	if input.Select != nil {
		n.SetSelect(*input.Select)
	}
	if input.TableName != nil {
		n.SetTableName(*input.TableName)
	}
	if input.TotalSegments != nil {
		n.SetTotalSegments(*input.TotalSegments)
	}
	return n
}

func splitScanInputIntoSegments(input *dynamodb.ScanInput) (inputs []*dynamodb.ScanInput) {
	if input.TotalSegments == nil || *input.TotalSegments < 2 {
		// only one segment
		return []*dynamodb.ScanInput{input}
	}
	// split into multiple
	inputs = make([]*dynamodb.ScanInput, *input.TotalSegments)

	for i := int64(0); i < *input.TotalSegments; i++ {
		// copy the input
		scanCopy := CopyInput(input)
		// set the segment to i
		scanCopy.SetSegment(i)
		inputs[i] = scanCopy
	}
	return
}