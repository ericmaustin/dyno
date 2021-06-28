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

// Scan executes Scan operation and returns a ScanPromise
func (c *Client) Scan(ctx context.Context, input *ddb.ScanInput, mw ...ScanMiddleWare) *ScanPromise {
	return NewScan(input, mw...).Invoke(ctx, c.ddb)
}

// Scan executes a Scan operation with a ScanInput in this pool and returns the ScanPromise
func (p *Pool) Scan(input *ddb.ScanInput, mw ...ScanMiddleWare) *ScanPromise {
	op := NewScan(input, mw...)

	if err := p.Do(op); err != nil {
		op.promise.SetResponse(nil, err)
	}

	return op.promise
}

// ScanAll executes ScanAll operation and returns a ScanAllPromise
func (c *Client) ScanAll(ctx context.Context, input *ddb.ScanInput, mw ...ScanAllMiddleWare) *ScanAllPromise {
	return NewScanAll(input, mw...).Invoke(ctx, c.ddb)
}

// ScanAll executes a ScanAll operation with a ScanInput in this pool and returns the ScanAllPromise
func (p *Pool) ScanAll(input *ddb.ScanInput, mw ...ScanAllMiddleWare) *ScanAllPromise {
	op := NewScanAll(input, mw...)

	if err := p.Do(op); err != nil {
		op.promise.SetResponse(nil, err)
	}

	return op.promise
}

// ScanContext represents an exhaustive Scan operation request context
type ScanContext struct {
	context.Context
	input  *ddb.ScanInput
	client *ddb.Client
}

// ScanPromise represents a promise for the Scan
type ScanPromise struct {
	*Promise
}

// GetResponse returns the GetResponse output and error
// if Output has not been set yet nil is returned
func (p *ScanPromise) GetResponse() (*ddb.ScanOutput, error) {
	out, err := p.Promise.GetResponse()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.ScanOutput), err
}

// Await waits for the ScanPromise to be fulfilled and then returns a ScanOutput and error
func (p *ScanPromise) Await() (*ddb.ScanOutput, error) {
	out, err := p.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.ScanOutput), err
}

// newScanPromise returns a new ScanPromise
func newScanPromise() *ScanPromise {
	return &ScanPromise{NewPromise()}
}

// ScanHandler represents a handler for Scan requests
type ScanHandler interface {
	HandleScan(ctx *ScanContext, promise *ScanPromise)
}

// ScanHandlerFunc is a ScanHandler function
type ScanHandlerFunc func(ctx *ScanContext, promise *ScanPromise)

// HandleScan implements ScanHandler
func (h ScanHandlerFunc) HandleScan(ctx *ScanContext, promise *ScanPromise) {
	h(ctx, promise)
}

// ScanFinalHandler is the final ScanHandler that executes a dynamodb Scan operation
type ScanFinalHandler struct {}

// HandleScan implements the ScanHandler
func (h *ScanFinalHandler) HandleScan(ctx *ScanContext, promise *ScanPromise) {
	promise.SetResponse(ctx.client.Scan(ctx, ctx.input))
}

// ScanMiddleWare is a middleware function use for wrapping ScanHandler requests
type ScanMiddleWare interface {
	ScanMiddleWare(h ScanHandler) ScanHandler
}

// ScanMiddleWareFunc is a functional ScanMiddleWare
type ScanMiddleWareFunc func(handler ScanHandler) ScanHandler

// ScanMiddleWare implements the ScanMiddleWare interface
func (mw ScanMiddleWareFunc) ScanMiddleWare(h ScanHandler) ScanHandler {
	return mw(h)
}

// Scan represents a Scan operation
type Scan struct {
	promise     *ScanPromise
	input       *ddb.ScanInput
	middleWares []ScanMiddleWare
}

// NewScan creates a new Scan
func NewScan(input *ddb.ScanInput, mws ...ScanMiddleWare) *Scan {
	return &Scan{
		input:       input,
		middleWares: mws,
		promise:     newScanPromise(),
	}
}

// Invoke invokes the Scan operation and returns a ScanPromise
func (op *Scan) Invoke(ctx context.Context, client *ddb.Client) *ScanPromise {
	go op.DynoInvoke(ctx, client)

	return op.promise
}

// DynoInvoke implements the Operation interface
func (op *Scan) DynoInvoke(ctx context.Context, client *ddb.Client) {

	requestCtx := &ScanContext{
		Context: ctx,
		client:  client,
		input:   op.input,
	}
	
	var h ScanHandler
	
	h = new(ScanFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].ScanMiddleWare(h)
		}
	}

	h.HandleScan(requestCtx, op.promise)
}

// ScanAllContext represents an exhaustive ScanAll operation request context
type ScanAllContext struct {
	context.Context
	input  *ddb.ScanInput
	client *ddb.Client
}

// ScanAllPromise represents a promise for the ScanAll
type ScanAllPromise struct {
	*Promise
}

// GetResponse returns the GetResponse output and error
// if Output has not been set yet nil is returned
func (p *ScanAllPromise) GetResponse() ([]*ddb.ScanOutput, error) {
	out, err := p.Promise.GetResponse()
	if out == nil {
		return nil, err
	}

	return out.([]*ddb.ScanOutput), err
}

// Await waits for the ScanAllPromise to be fulfilled and then returns a ScanAllOutput and error
func (p *ScanAllPromise) Await() ([]*ddb.ScanOutput, error) {
	out, err := p.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.([]*ddb.ScanOutput), err
}

// newScanAllPromise returns a new ScanAllPromise
func newScanAllPromise() *ScanAllPromise {
	return &ScanAllPromise{NewPromise()}
}

// ScanAllHandler represents a handler for ScanAll requests
type ScanAllHandler interface {
	HandleScanAll(ctx *ScanAllContext, promise *ScanAllPromise)
}

// ScanAllHandlerFunc is a ScanAllHandler function
type ScanAllHandlerFunc func(ctx *ScanAllContext, promise *ScanAllPromise)

// HandleScanAll implements ScanAllHandler
func (h ScanAllHandlerFunc) HandleScanAll(ctx *ScanAllContext, promise *ScanAllPromise) {
	h(ctx, promise)
}

// ScanAllMiddleWare is a middleware function use for wrapping ScanAllHandler requests
type ScanAllMiddleWare interface {
	ScanAllMiddleWare(h ScanAllHandler) ScanAllHandler
}

// ScanAllMiddleWareFunc is a functional ScanAllMiddleWare
type ScanAllMiddleWareFunc func(handler ScanAllHandler) ScanAllHandler

// ScanAllMiddleWare implements the ScanAllMiddleWare interface
func (mw ScanAllMiddleWareFunc) ScanAllMiddleWare(h ScanAllHandler) ScanAllHandler {
	return mw(h)
}

// ScanAllFinalHandler is the final ScanAllHandler that executes a dynamodb ScanAll operation
type ScanAllFinalHandler struct {}

// HandleScanAll implements the ScanAllHandler
func (h *ScanAllFinalHandler) HandleScanAll(ctx *ScanAllContext, promise *ScanAllPromise) {
	var (
		outs []*ddb.ScanOutput
		out  *ddb.ScanOutput
		err  error
	)

	defer func() { promise.SetResponse(outs, err) }()

	// copy the scan so we're not mutating the original
	input := CopyScan(ctx.input)

	for {

		if out, err = ctx.client.Scan(ctx, input); err != nil {
			return
		}

		outs = append(outs, out)

		if out.LastEvaluatedKey == nil || len(out.LastEvaluatedKey) == 0 {
			// no more work
			break
		}

		input.ExclusiveStartKey = out.LastEvaluatedKey
	}
}

// ScanAll represents a ScanAll operation
type ScanAll struct {
	promise     *ScanAllPromise
	input       *ddb.ScanInput
	middleWares []ScanAllMiddleWare
}

// NewScanAll creates a new ScanAll
func NewScanAll(input *ddb.ScanInput, mws ...ScanAllMiddleWare) *ScanAll {
	return &ScanAll{
		input:       input,
		middleWares: mws,
		promise:     newScanAllPromise(),
	}
}

// Invoke invokes the ScanAll operation and returns a ScanAllPromise
func (op *ScanAll) Invoke(ctx context.Context, client *ddb.Client) *ScanAllPromise {
	go op.DynoInvoke(ctx, client)

	return op.promise
}

// DynoInvoke the Operation interface
func (op *ScanAll) DynoInvoke(ctx context.Context, client *ddb.Client) {
	requestCtx := &ScanAllContext{
		Context: ctx,
		client:  client,
		input:   op.input,
	}

	var h ScanAllHandler

	h = new(ScanAllFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].ScanAllMiddleWare(h)
		}
	}

	h.HandleScanAll(requestCtx, op.promise)
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
