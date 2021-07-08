package dyno

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ericmaustin/dyno/condition"
	"sync"
)

// Scan executes Scan operation and returns a Scan operation
func (s *Session) Scan(ctx context.Context, input *ddb.ScanInput, mw ...ScanMiddleWare) *Scan {
	return NewScan(input, mw...).Invoke(ctx, s.ddb)
}

// Scan executes a Scan operation with a ScanInput in this pool and returns the Scan operation
func (p *Pool) Scan(input *ddb.ScanInput, mw ...ScanMiddleWare) *Scan {
	op := NewScan(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// ScanAll executes Scan operation and returns a Scan operation that runs as many queries as it takes
// to get all of the values
func (s *Session) ScanAll(input *ddb.ScanInput, mw ...ScanMiddleWare) *Scan {
	return NewScanAll(input, mw...).Invoke(s.ctx, s.ddb)
}

// ScanAll executes a Scan operation with a ScanInput in this pool and returns a Scan operation
// that runs as many queries as it takes to get all of the values
func (p *Pool) ScanAll(input *ddb.ScanInput, mw ...ScanMiddleWare) *Scan {
	op := NewScanAll(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// ScanContext represents an exhaustive Scan operation request context
type ScanContext struct {
	context.Context
	Input  *ddb.ScanInput
	Client *ddb.Client
}

// ScanOutput represents the output for the Scan opration
type ScanOutput struct {
	out *ddb.ScanOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *ScanOutput) Set(out *ddb.ScanOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *ScanOutput) Get() (out *ddb.ScanOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()
	return
}

// ScanHandler represents a handler for Scan requests
type ScanHandler interface {
	HandleScan(ctx *ScanContext, output *ScanOutput)
}

// ScanHandlerFunc is a ScanHandler function
type ScanHandlerFunc func(ctx *ScanContext, output *ScanOutput)

// HandleScan implements ScanHandler
func (h ScanHandlerFunc) HandleScan(ctx *ScanContext, output *ScanOutput) {
	h(ctx, output)
}

// ScanFinalHandler is the final ScanHandler that executes a dynamodb Scan operation
type ScanFinalHandler struct{}

// HandleScan implements the ScanHandler
func (h *ScanFinalHandler) HandleScan(ctx *ScanContext, output *ScanOutput) {
	output.Set(ctx.Client.Scan(ctx, ctx.Input))
}

// ScanAllFinalHandler is the final ScanAllHandler that executes a dynamodb ScanAll operation
type ScanAllFinalHandler struct{}

// HandleScan implements the ScanHandler
func (h *ScanAllFinalHandler) HandleScan(ctx *ScanContext, output *ScanOutput) {
	var (
		out, finalOut *ddb.ScanOutput
		err           error
	)

	finalOut = new(ddb.ScanOutput)

	defer func() { output.Set(finalOut, err) }()

	// copy the scan so we're not mutating the original
	input := CopyScan(ctx.Input)

	for {
		if out, err = ctx.Client.Scan(ctx, input); err != nil {
			return
		}

		finalOut.Items = append(finalOut.Items, out.Items...)
		finalOut.Count += out.Count
		finalOut.ScannedCount += out.ScannedCount

		if out.LastEvaluatedKey == nil || len(out.LastEvaluatedKey) == 0 {
			// no more keys left
			return
		}

		input.ExclusiveStartKey = out.LastEvaluatedKey
	}
}

// ScanMiddleWare is a middleware function use for wrapping ScanHandler requests
type ScanMiddleWare interface {
	ScanMiddleWare(next ScanHandler) ScanHandler
}

// ScanMiddleWareFunc is a functional ScanMiddleWare
type ScanMiddleWareFunc func(next ScanHandler) ScanHandler

// ScanMiddleWare implements the ScanMiddleWare interface
func (mw ScanMiddleWareFunc) ScanMiddleWare(next ScanHandler) ScanHandler {
	return mw(next)
}

// Scan represents a Scan operation
type Scan struct {
	*BaseOperation
	Handler     ScanHandler
	input       *ddb.ScanInput
	middleWares []ScanMiddleWare
}

// NewScan creates a new Scan
func NewScan(input *ddb.ScanInput, mws ...ScanMiddleWare) *Scan {
	return &Scan{
		BaseOperation: NewOperation(),
		Handler:       new(ScanFinalHandler),
		input:         input,
		middleWares:   mws,
	}
}

// NewScanAll creates a new ScanAll
func NewScanAll(input *ddb.ScanInput, mws ...ScanMiddleWare) *Scan {
	return &Scan{
		BaseOperation: NewOperation(),
		Handler:       new(ScanAllFinalHandler),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the Scan operation in a goroutine and returns a BatchGetItemAllPromise
func (op *Scan) Invoke(ctx context.Context, client *ddb.Client) *Scan {
	op.SetRunning() // promise now waiting for a response

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the Scan operation
func (op *Scan) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	output := new(ScanOutput)

	defer func() { op.SetResponse(output.Get()) }()

	var h ScanHandler

	h = op.Handler

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].ScanMiddleWare(h)
		}
	}

	requestCtx := &ScanContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	h.HandleScan(requestCtx, output)
}

// Await waits for the ScanPromise to be fulfilled and then returns a ScanOutput and error
func (op *Scan) Await() (*ddb.ScanOutput, error) {
	out, err := op.BaseOperation.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.ScanOutput), err
}

// NewScanInput creates a new ScanInput with a table name
func NewScanInput(tableName *string) *ddb.ScanInput {
	return &ddb.ScanInput{
		TableName:              tableName,
		ReturnConsumedCapacity: ddbTypes.ReturnConsumedCapacityNone,
		Select:                 ddbTypes.SelectAllAttributes,
	}
}

// ScanBuilder extends dynamodb.ScanInput to allow dynamic input building
type ScanBuilder struct {
	*ddb.ScanInput
	filter     condition.Builder
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

// AddProjection adds additional field names to the projection
func (bld *ScanBuilder) AddProjection(names interface{}) *ScanBuilder {
	addProjection(&bld.projection, names)
	return bld
}

// AddProjectionNames adds additional field names to the projection with strings
func (bld *ScanBuilder) AddProjectionNames(names ...string) *ScanBuilder {
	addProjectionNames(&bld.projection, names)
	return bld
}

// AddFilter adds a filter condition to the scan
// adding multiple conditions by calling this multiple times will join the conditions with
// an AND
func (bld *ScanBuilder) AddFilter(cnd expression.ConditionBuilder) *ScanBuilder {
	bld.filter.And(cnd)
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
	if bld.projection == nil && bld.filter.Empty() {
		return bld.ScanInput, nil
	}

	// only use expression builder if we have a projection or a filter
	eb := expression.NewBuilder()

	// add projection
	if bld.projection != nil {
		eb = eb.WithProjection(*bld.projection)
		bld.Select = ddbTypes.SelectSpecificAttributes
	}

	// add filter
	if !bld.filter.Empty() {
		eb = eb.WithFilter(bld.filter.Builder())
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

	return bld.ScanInput, nil
}

// BuildSegments builds the input input with included projection and creates separate inputs for each segment
func (bld *ScanBuilder) BuildSegments(segments int32) ([]*ddb.ScanInput, error) {
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
