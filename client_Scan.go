package dyno

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ericmaustin/dyno/condition"
	"github.com/ericmaustin/dyno/encoding"
)

// Scan executes a Scan operation with a ScanInput
func (c *Client) Scan(ctx context.Context, input *ddb.ScanInput, optFns ...func(*ScanOptions)) (*ddb.ScanOutput, error) {
	opt := NewScan(input, optFns...)
	opt.DynoInvoke(ctx, c.ddb)

	return opt.Await()
}

// Scan executes a Scan operation with a ScanInput in this pool and returns the Scan for processing
func (p *Pool) Scan(input *ddb.ScanInput, optFns ...func(*ScanOptions)) *Scan {
	op := NewScan(input, optFns...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// ScanAll executes a Scan operation with a ScanInput
func (c *Client) ScanAll(ctx context.Context, input *ddb.ScanInput, optFns ...func(*ScanOptions)) ([]*ddb.ScanOutput, error) {
	opt := NewScanAll(input, optFns...)
	opt.DynoInvoke(ctx, c.ddb)

	return opt.Await()
}

// ScanAll executes a ScanAll operation with a ScanInput in this pool and returns the ScanAll for processing
func (p *Pool) ScanAll(input *ddb.ScanInput, optFns ...func(*ScanOptions)) *ScanAll {
	op := NewScanAll(input, optFns...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// ScanInputCallback is a callback that is called on a given ScanInput before a Scan operation api call executes
type ScanInputCallback interface {
	ScanInputCallback(context.Context, *ddb.ScanInput) (*ddb.ScanOutput, error)
}

// ScanOutputCallback is a callback that is called on a given ScanOutput after a Scan operation api call executes
type ScanOutputCallback interface {
	ScanOutputCallback(context.Context, *ddb.ScanOutput) error
}

// ScanInputCallbackFunc is ScanOutputCallback function
type ScanInputCallbackFunc func(context.Context, *ddb.ScanInput) (*ddb.ScanOutput, error)

// ScanInputCallback implements the ScanOutputCallback interface
func (cb ScanInputCallbackFunc) ScanInputCallback(ctx context.Context, input *ddb.ScanInput) (*ddb.ScanOutput, error) {
	return cb(ctx, input)
}

// ScanOutputCallbackF is ScanOutputCallback function
type ScanOutputCallbackF func(context.Context, *ddb.ScanOutput) error

// ScanOutputCallback implements the ScanOutputCallback interface
func (cb ScanOutputCallbackF) ScanOutputCallback(ctx context.Context, input *ddb.ScanOutput) error {
	return cb(ctx, input)
}

// ScanOptions represents options passed to the Scan operation
type ScanOptions struct {
	// InputCallbacks are called before the Scan dynamodb api operation with the dynamodb.ScanInput
	InputCallbacks []ScanInputCallback
	// OutputCallbacks are called after the Scan dynamodb api operation with the dynamodb.ScanOutput
	OutputCallbacks []ScanOutputCallback
}

// ScanWithInputCallback adds a ScanInputCallbackFunc to the InputCallbacks
func ScanWithInputCallback(cb ScanInputCallbackFunc) func(*ScanOptions) {
	return func(opt *ScanOptions) {
		opt.InputCallbacks = append(opt.InputCallbacks, cb)
	}
}

// ScanWithOutputCallback adds a ScanOutputCallback to the OutputCallbacks
func ScanWithOutputCallback(cb ScanOutputCallback) func(*ScanOptions) {
	return func(opt *ScanOptions) {
		opt.OutputCallbacks = append(opt.OutputCallbacks, cb)
	}
}

// Scan represents a Scan operation
type Scan struct {
	*Promise
	input   *ddb.ScanInput
	options ScanOptions
}

// NewScan creates a new Scan operation on the given client with a given ScanInput and options
func NewScan(input *ddb.ScanInput, optFns ...func(*ScanOptions)) *Scan {
	opts := ScanOptions{}
	for _, opt := range optFns {
		opt(&opts)
	}

	return &Scan{
		Promise: NewPromise(),
		input:   input,
		options: opts,
	}
}

// Await waits for the Operation to be complete and then returns a ScanOutput and error
func (op *Scan) Await() (*ddb.ScanOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}
	fmt.Println("Scan.Await()\n", MustYamlString(out))
	return out.(*ddb.ScanOutput), err
}

// Invoke invokes the Scan operation
func (op *Scan) Invoke(ctx context.Context, client *ddb.Client) *Scan {
	go op.DynoInvoke(ctx, client)
	return op
}

// DynoInvoke implements the Operation interface
func (op *Scan) DynoInvoke(ctx context.Context, client *ddb.Client) {
	var (
		out *ddb.ScanOutput
		err error
	)

	defer func() {
		op.SetResponse(out, err)
	}()

	for _, cb := range op.options.InputCallbacks {
		if out, err = cb.ScanInputCallback(ctx, op.input); out != nil || err != nil {
			return
		}
	}

	if out, err = client.Scan(ctx, op.input); err != nil {
		return
	}

	for _, cb := range op.options.OutputCallbacks {
		if err = cb.ScanOutputCallback(ctx, out); err != nil {
			return
		}
	}
}

// ScanAll represents an exhaustive Scan operation
type ScanAll struct {
	*Promise
	input   *ddb.ScanInput
	options ScanOptions
}

// NewScanAll creates a new ScanAll operation on the given client with a given ScanInput and options
func NewScanAll(input *ddb.ScanInput, optFns ...func(*ScanOptions)) *ScanAll {
	options := ScanOptions{}
	for _, opt := range optFns {
		opt(&options)
	}
	
	return &ScanAll{
		//client:  nil,
		Promise: NewPromise(),
		input:   input,
		options: options,
	}
}

// Await waits for the Operation to be complete and then returns a ScanOutput and error
func (op *ScanAll) Await() ([]*ddb.ScanOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}
	
	return out.([]*ddb.ScanOutput), err
}

// Invoke invokes the Scan operation
func (op *ScanAll) Invoke(ctx context.Context, client *ddb.Client) *ScanAll {
	go op.DynoInvoke(ctx, client)
	return op
}

// DynoInvoke the Operation interface
func (op *ScanAll) DynoInvoke(ctx context.Context, client *ddb.Client) {
	var (
		outs []*ddb.ScanOutput
		out  *ddb.ScanOutput
		err  error
	)
	
	defer func() {
		op.SetResponse(out, err)
	}()
	
	//copy the scan so we're not mutating the original
	input := CopyScan(op.input)
	
	for {
		for _, cb := range op.options.InputCallbacks {
			if out, err = cb.ScanInputCallback(ctx, input); out != nil || err != nil {
				if out != nil {
					outs = append(outs, out)
				}
				
				return
			}
		}

		if out, err = client.Scan(ctx, input); err != nil {
			return
		}

		for _, cb := range op.options.OutputCallbacks {
			if err = cb.ScanOutputCallback(ctx, out); err != nil {
				return
			}
		}

		outs = append(outs, out)

		if out.LastEvaluatedKey == nil {
			// no more work
			break
		}

		input.ExclusiveStartKey = out.LastEvaluatedKey
	}

	return
}

// NewScanInput creates a new ScanInput with a table name
func NewScanInput(tableName *string) *ddb.ScanInput {
	return &ddb.ScanInput{
		TableName:              tableName,
		ReturnConsumedCapacity: ddbTypes.ReturnConsumedCapacityNone,
		Select:                 ddbTypes.SelectAllAttributes,
	}
}

//ScanBuilder extends dynamodb.ScanInput to allow dynamic input building
type ScanBuilder struct {
	*ddb.ScanInput
	filter     *expression.ConditionBuilder
	projection *expression.ProjectionBuilder
}

// NewScanBuilder creates a new scan builder with ScanOption
func NewScanBuilder(input *ddb.ScanInput) *ScanBuilder {
	if input != nil {
		return &ScanBuilder{ScanInput: input}
	}
	return &ScanBuilder{ScanInput: NewScanInput(nil)}
}

// SetTableName sets the TableName field's value.
func (bld *ScanBuilder) SetTableName(v string) *ScanBuilder {
	bld.TableName = &v
	return bld
}

// SetInput sets the ScanBuilder's dynamodb.ScanInput
func (bld *ScanBuilder) SetInput(input *ddb.ScanInput) *ScanBuilder {
	bld.ScanInput = input
	return bld
}

//AddProjection adds additional field names to the projection
func (bld *ScanBuilder) AddProjection(names interface{}) *ScanBuilder {
	nameBuilders := encoding.NameBuilders(names)

	if bld.projection == nil {
		proj := expression.ProjectionBuilder{}
		proj = proj.AddNames(nameBuilders...)
		bld.projection = &proj
	} else {
		*bld.projection = bld.projection.AddNames(nameBuilders...)
	}

	return bld
}

// AddProjectionNames adds additional field names to the projection with strings
func (bld *ScanBuilder) AddProjectionNames(names ...string) *ScanBuilder {
	//nameBuilders := encoding.NameBuilders(names)
	nameBuilders := make([]expression.NameBuilder, len(names))

	for i, name := range names {
		nameBuilders[i] = expression.Name(name)
	}

	if bld.projection == nil {
		proj := expression.ProjectionBuilder{}
		proj = proj.AddNames(nameBuilders...)
		bld.projection = &proj
	} else {
		*bld.projection = bld.projection.AddNames(nameBuilders...)
	}

	return bld
}

// AddFilter adds a filter condition to the scan
// adding multiple conditions by calling this multiple times will join the conditions with
// an AND
func (bld *ScanBuilder) AddFilter(cnd expression.ConditionBuilder) *ScanBuilder {
	if bld.filter == nil {
		bld.filter = &cnd
	} else {
		cnd = condition.And(*bld.filter, cnd)
		bld.filter = &cnd
	}

	return bld
}

// SetConsistentRead sets the ConsistentRead field's value.
func (bld *ScanBuilder) SetConsistentRead(v bool) *ScanBuilder {
	bld.ConsistentRead = &v
	return bld
}

// SetExclusiveStartKey sets the ExclusiveStartKey field's value.
func (bld *ScanBuilder) SetExclusiveStartKey(v map[string]ddbTypes.AttributeValue) *ScanBuilder {
	bld.ExclusiveStartKey = v
	return bld
}

// SetExpressionAttributeNames sets the ExpressionAttributeNames field's value.
func (bld *ScanBuilder) SetExpressionAttributeNames(v map[string]string) *ScanBuilder {
	bld.ExpressionAttributeNames = v
	return bld
}

// SetExpressionAttributeValues sets the ExpressionAttributeValues field's value.
func (bld *ScanBuilder) SetExpressionAttributeValues(v map[string]ddbTypes.AttributeValue) *ScanBuilder {
	bld.ExpressionAttributeValues = v
	return bld
}

// SetFilterExpression sets the FilterExpression field's value.
func (bld *ScanBuilder) SetFilterExpression(v string) *ScanBuilder {
	bld.FilterExpression = &v
	return bld
}

// SetIndexName sets the IndexName field's value.
func (bld *ScanBuilder) SetIndexName(v string) *ScanBuilder {
	bld.IndexName = &v
	return bld
}

// SetLimit sets the Limit field's value.
func (bld *ScanBuilder) SetLimit(v int32) *ScanBuilder {
	bld.Limit = &v
	return bld
}

// SetProjectionExpression sets the ProjectionExpression field's value.
func (bld *ScanBuilder) SetProjectionExpression(v string) *ScanBuilder {
	bld.ProjectionExpression = &v
	return bld
}

// SetReturnConsumedCapacity sets the ReturnConsumedCapacity field's value.
func (bld *ScanBuilder) SetReturnConsumedCapacity(v ddbTypes.ReturnConsumedCapacity) *ScanBuilder {
	bld.ReturnConsumedCapacity = v
	return bld
}

// SetScanFilter sets the ScanFilter field's value.
func (bld *ScanBuilder) SetScanFilter(v map[string]ddbTypes.Condition) *ScanBuilder {
	bld.ScanFilter = v
	return bld
}

// SetSegment sets the Segment field's value.
func (bld *ScanBuilder) SetSegment(v int32) *ScanBuilder {
	bld.Segment = &v
	return bld
}

// SetSelect sets the Select field's value.
func (bld *ScanBuilder) SetSelect(v ddbTypes.Select) *ScanBuilder {
	bld.Select = v
	return bld
}

// SetTotalSegments sets the TotalSegments field's value.
func (bld *ScanBuilder) SetTotalSegments(v int32) *ScanBuilder {
	bld.TotalSegments = &v
	return bld
}

// Build builds the dynamodb.ScanInput
func (bld *ScanBuilder) Build() (*ddb.ScanInput, error) {
	if bld.projection != nil || bld.filter != nil {
		// only use expression builder if we have a projection or a filter
		eb := expression.NewBuilder()

		// add projection
		if bld.projection != nil {
			eb = eb.WithProjection(*bld.projection)
		}

		// add filter
		if bld.filter != nil {
			eb = eb.WithFilter(*bld.filter)
		}

		// build the Expression
		expr, err := eb.Build()

		if err != nil {
			return nil, fmt.Errorf("ScanBuilder Build() failed while attempting to build expression: %v", err)
		}
		bld.ExpressionAttributeNames = expr.Names()
		bld.ExpressionAttributeValues = expr.Values()
		bld.FilterExpression = expr.Filter()
		bld.ProjectionExpression = expr.Projection()
	}

	return bld.ScanInput, nil
}

// BuildSegments builds the input input with included projection and creates separate inputs for each segment
func (bld *ScanBuilder) BuildSegments(segments int32, inputCB ScanInputCallback, outputCB ScanOutputCallback) ([]*ddb.ScanInput, error) {
	input, err := bld.Build()

	if err != nil {
		return nil, err
	}

	if segments > 0 {
		bld.TotalSegments = &segments
	}

	return SplitScanIntoSegments(input, segments), nil
}

// SplitScanIntoSegments splits an input into segments, each segment is a deep copy of the original
// with a unique segment number
func SplitScanIntoSegments(input *ddb.ScanInput, segments int32) (inputs []*ddb.ScanInput) {
	if input.TotalSegments == nil || *input.TotalSegments < 2 {
		// only one segment
		return []*ddb.ScanInput{input}
	}
	// split into multiple
	inputs = make([]*ddb.ScanInput, segments)

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

// CopyScan creates a deep copy of a ScanInput
// note: CopyScan does not copy legacy parameters
func CopyScan(input *ddb.ScanInput) *ddb.ScanInput {
	clone := &ddb.ScanInput{
		ConditionalOperator:    input.ConditionalOperator,
		ReturnConsumedCapacity: input.ReturnConsumedCapacity,
		Select:                 input.Select,
	}

	if input.TableName != nil {
		clone.TableName = new(string)
		*clone.TableName = *input.TableName
	}

	if input.AttributesToGet != nil {
		copy(clone.AttributesToGet, input.AttributesToGet)
	}

	if input.ConsistentRead != nil {
		clone.ConsistentRead = new(bool)
		*clone.ConsistentRead = *input.ConsistentRead
	}

	if input.ExclusiveStartKey != nil {
		clone.ExclusiveStartKey = make(map[string]ddbTypes.AttributeValue, len(input.ExclusiveStartKey))
		for k, v := range input.ExclusiveStartKey {
			clone.ExclusiveStartKey[k] = CopyAttributeValue(v)
		}
	}

	if input.ExpressionAttributeNames != nil {
		clone.ExpressionAttributeNames = make(map[string]string, len(input.ExpressionAttributeNames))
		for k, v := range input.ExpressionAttributeNames {
			clone.ExpressionAttributeNames[k] = v
		}
	}

	if input.ExpressionAttributeValues != nil {
		clone.ExpressionAttributeValues = make(map[string]ddbTypes.AttributeValue, len(input.ExpressionAttributeValues))
		for k, v := range input.ExpressionAttributeValues {
			clone.ExpressionAttributeValues[k] = CopyAttributeValue(v)
		}
	}

	if input.FilterExpression != nil {
		clone.FilterExpression = new(string)
		*clone.FilterExpression = *input.FilterExpression
	}

	if input.IndexName != nil {
		clone.IndexName = new(string)
		*clone.IndexName = *input.IndexName
	}

	if input.Limit != nil {
		clone.Limit = new(int32)
		*clone.Limit = *input.Limit
	}

	if input.ProjectionExpression != nil {
		clone.ProjectionExpression = new(string)
		*clone.ProjectionExpression = *input.ProjectionExpression
	}

	if input.ScanFilter != nil {
		clone.ScanFilter = make(map[string]ddbTypes.Condition, len(input.ScanFilter))
		for k, v := range input.ScanFilter {
			clone.ScanFilter[k] = CopyCondition(v)
		}
	}

	if input.Segment != nil {
		clone.Segment = new(int32)
		*clone.Segment = *input.Segment
	}

	if input.TableName != nil {
		clone.TableName = new(string)
		*clone.TableName = *input.TableName
	}

	if input.TotalSegments != nil {
		clone.TotalSegments = new(int32)
		*clone.TotalSegments = *input.TotalSegments
	}

	return clone
}
