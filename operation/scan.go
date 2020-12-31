package operation

import (
	"sync"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/ericmaustin/dyno"
	"github.com/ericmaustin/dyno/condition"
	"github.com/ericmaustin/dyno/encoding"
)

type ScanSelect string

const (
	ScanSelectAllAttributes          = ScanSelect("ALL_ATTRIBUTES")
	ScanSelectAllProjectedAttributes = ScanSelect("ALL_PROJECTED_ATTRIBUTES")
	ScanSelectCount                  = ScanSelect("COUNT")
	ScanSelectSpecificAttributes     = ScanSelect("SPECIFIC_ATTRIBUTES")
)

type ScanBuilder struct {
	input      *dynamodb.ScanInput
	filter     *expression.ConditionBuilder
	projection *expression.ProjectionBuilder
}

// NewScanBuilder creates a new scan builder with optional Scan input as the baseOperation
func NewScanBuilder() *ScanBuilder {
	q := &ScanBuilder{
		input: &dynamodb.ScanInput{},
	}
	return q
}

// SetInput sets the ScanBuilder's dynamodb.ScanInput
func (s *ScanBuilder) SetInput(input *dynamodb.ScanInput) *ScanBuilder {
	s.input = input
	return s
}

// SetTable sets the table for this input
func (s *ScanBuilder) SetTable(tableName string) *ScanBuilder {
	s.input.TableName = &tableName
	return s
}

// SetIndex sets the index for this input
func (s *ScanBuilder) SetIndex(index string) *ScanBuilder {
	s.input.SetIndexName(index)
	return s
}

// SetSelect sets the index for this input
func (s *ScanBuilder) SetSelect(scanSelect ScanSelect) *ScanBuilder {
	s.input.SetSelect(string(scanSelect))
	return s
}

// SetConsistentRead sets the consistent read boolean value for this input
func (s *ScanBuilder) SetConsistentRead(consistentRead bool) *ScanBuilder {
	s.input.SetConsistentRead(consistentRead)
	return s
}

// SetLimit sets the limit for this input
func (s *ScanBuilder) SetLimit(limit int64) *ScanBuilder {
	s.input.SetLimit(limit)
	return s
}

// AddProjectionNames adds additional field names to the projection
func (s *ScanBuilder) AddProjectionNames(names interface{}) *ScanBuilder {
	nameBuilders := encoding.NameBuilders(names)
	if s.projection == nil {
		proj := expression.ProjectionBuilder{}
		proj.AddNames(nameBuilders...)
		s.projection = &proj
	} else {
		s.projection.AddNames(nameBuilders...)
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
func (s *ScanBuilder) Build() *dynamodb.ScanInput {
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
			panic(err)
		}
		s.input.ExpressionAttributeNames = expr.Names()
		s.input.ExpressionAttributeValues = expr.Values()
		s.input.FilterExpression = expr.Filter()
		s.input.ProjectionExpression = expr.Projection()
	}
	return s.input
}

// Operation returns a new ScanOperation with this builder's input
func (s *ScanBuilder) Operation() *ScanOperation {
	return Scan(s.Build())
}

// CountOperation returns a new ScanCountOperation with this builder's input
func (s *ScanBuilder) CountOperation() *ScanCountOperation {
	return ScanCount(s.Build())
}

// CopyScanInput creates a copy of the ScanInput
func CopyScanInput(input *dynamodb.ScanInput) *dynamodb.ScanInput {
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

// GetResult is returned by the ScanOperation Execution in a channel when operation completes
type ScanResult struct {
	resultBase
	output []*dynamodb.ScanOutput
}

// OutputInterface returns the ScanOutput slice from the ScanResult as an interface
func (s *ScanResult) OutputInterface() interface{} {
	return s.output
}

// Output returns the ScanOutput slice from the ScanResult
func (s *ScanResult) Output() []*dynamodb.ScanOutput {
	return s.output
}

// OutputError returns the ScanOutput slice and the error from the ScanResult for convenience
func (s *ScanResult) OutputError() ([]*dynamodb.ScanOutput, error) {
	return s.output, s.err
}

// ScanOperation handles scan Input operations
type ScanOperation struct {
	*baseOperation
	input     *dynamodb.ScanInput
	handler   ItemSliceHandler
	handlerMu *sync.Mutex
}

// Scan creates a new scan operation with the given scan input and handler
func Scan(input *dynamodb.ScanInput) *ScanOperation {
	return &ScanOperation{
		baseOperation: newBase(),
		input:         input,
	}
}

// Input returns a ptr to the scan Input
func (s *ScanOperation) Input() *dynamodb.ScanInput {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.input
}

// SetInput sets the ScanInput
// panics with an InvalidState error if operation isn't pending
func (s *ScanOperation) SetInput(input *dynamodb.ScanInput) *ScanOperation {
	if !s.IsPending() {
		panic(&InvalidState{})
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.input = input
	return s
}

// SetHandler sets the handler to be used for this scan
// panics with an InvalidState error if operation isn't pending
func (s *ScanOperation) SetHandler(handler ItemSliceHandler) *ScanOperation {
	if !s.IsPending() {
		panic(&InvalidState{})
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handler = handler
	return s
}

// SetHandlerMutex sets the optional handler mutex that will be locked before handler is called
func (s *ScanOperation) SetHandlerMutex(mu *sync.Mutex) *ScanOperation {
	if !s.IsPending() {
		panic(&InvalidState{})
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlerMu = mu
	return s
}

// SetLimit sets the scan limit
func (s *ScanOperation) SetLimit(limit int64) *ScanOperation {
	if !s.IsPending() {
		panic(&InvalidState{})
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.input.SetLimit(limit)
	return s
}

// SetSegments sets the total number of segments for the scan operation
// this will change the number of scan workers that will run to complete the scan
func (s *ScanOperation) SetSegments(totalSegments int64) *ScanOperation {
	if !s.IsPending() {
		panic(&InvalidState{})
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.input.SetTotalSegments(totalSegments)
	return s
}

// SetStartKey sets the start key on the ScanInput
func (s *ScanOperation) SetStartKey(startKey map[string]*dynamodb.AttributeValue) *ScanOperation {
	if !s.IsPending() {
		panic(&InvalidState{})
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.input.ExclusiveStartKey = startKey
	return s
}

// ExecuteInBatch executes the ScanOperation using given Request
// and returns a Result used for executing this operation in a batch
func (s *ScanOperation) ExecuteInBatch(req *dyno.Request) Result {
	return s.Execute(req)
}

// GoExecute Execute the ScanOperation request in a go routine and returns a channel
// that will pass a ScanResult when execution completes
func (s *ScanOperation) GoExecute(req *dyno.Request) <-chan *ScanResult {
	outCh := make(chan *ScanResult)
	go func() {
		defer close(outCh)
		outCh <- s.Execute(req)
	}()
	return outCh
}

type scanState struct {
	out []*dynamodb.ScanOutput
	cnt int64
	err error
	mu  sync.RWMutex
}

func (s *scanState) addOutput(output *dynamodb.ScanOutput) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.out == nil {
		s.out = []*dynamodb.ScanOutput{output}
	} else {
		s.out = append(s.out, output)
	}
	if output.Count != nil {
		s.cnt += *output.Count
	}
}

func (s *scanState) setError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

func (s *scanState) error() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.err
}

// doScan executes this ScanOperation
func (s *ScanOperation) Execute(req *dyno.Request) (out *ScanResult) {
	out = &ScanResult{}
	s.setRunning()
	// stop execution timing when done
	defer s.setDone(out)

	if s.handlerMu == nil {
		s.handlerMu = &sync.Mutex{}
	}
	// always clear the segment value as this will be set when we split the input based on total segments
	s.input.Segment = nil

	// get all of the segments
	segments := splitScanInputIntoSegments(s.input)

	wg := sync.WaitGroup{}
	state := &scanState{
		mu: sync.RWMutex{},
	}

	for _, segment := range segments {
		if state.error() != nil {
			break
		}
		wg.Add(1)
		// run the segment
		go scanSegment(req, segment, state, &wg, s.handler, s.handlerMu)
	}

	wg.Wait()

	out.output = state.out
	out.err = state.err
	return
}

// GetResult is returned by the ScanOperation Execution in a channel when operation completes
type ScanCountResult struct {
	resultBase
	output int64
}

// OutputInterface returns the count as an interface from the ScanCountResult
func (s *ScanCountResult) OutputInterface() interface{} {
	return s.output
}

// Output returns the count as an int64 from the ScanCountResult
func (s *ScanCountResult) Output() int64 {
	return s.output
}

// OutputError returns the res and the error from the ScanCountResult for convenience
func (s *ScanCountResult) OutputError() (int64, error) {
	return s.output, s.err
}

// ScanOperation is used to run a scan
type ScanCountOperation struct {
	*baseOperation
	input     *dynamodb.ScanInput
	handlerMu *sync.Mutex
}

// Input returns a ptr to the scan Input
func (s *ScanCountOperation) Input() *dynamodb.ScanInput {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.input
}

// SetInput sets the ScanInput
// panics with an InvalidState error if operation isn't pending
func (s *ScanCountOperation) SetInput(input *dynamodb.ScanInput) *ScanCountOperation {
	if !s.IsPending() {
		panic(&InvalidState{})
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.input = input
	return s
}

// ScanCount creates a new ScanCountOperation with optional ScanInput
func ScanCount(input *dynamodb.ScanInput) *ScanCountOperation {
	return &ScanCountOperation{
		baseOperation: newBase(),
		input:         input,
	}
}

// SetSegments sets the total number of segments for the scan operation
// this will change the number of scan workers that will run to complete the scan
func (s *ScanCountOperation) SetSegments(totalSegments int64) *ScanCountOperation {
	if !s.IsPending() {
		panic(&InvalidState{})
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.input.SetTotalSegments(totalSegments)
	return s
}

// SetStartKey sets the start key on the ScanInput
func (s *ScanCountOperation) SetStartKey(startKey map[string]*dynamodb.AttributeValue) *ScanCountOperation {
	if !s.IsPending() {
		panic(&InvalidState{})
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.input.ExclusiveStartKey = startKey
	return s
}

// ExecuteInBatch executes the ScanCountOperation using given Request
// and returns a Result used for executing this operation in a batch
func (s *ScanCountOperation) ExecuteInBatch(req *dyno.Request) Result {
	return s.Execute(req)
}

// GoExecute executes the ScanCountOperation request in a go routine
func (s *ScanCountOperation) GoExecute(req *dyno.Request) <-chan *ScanCountResult {
	outCh := make(chan *ScanCountResult)
	go func() {
		outCh <- s.Execute(req)
	}()
	return outCh
}

// doScan executes this ScanOperation
func (s *ScanCountOperation) Execute(req *dyno.Request) (out *ScanCountResult) {
	out = &ScanCountResult{}
	s.setRunning()
	defer s.setDone(out)

	if s.handlerMu == nil {
		s.handlerMu = &sync.Mutex{}
	}

	// always clear the segment value as this will be set when we split the input based on total segments
	s.input.Segment = nil
	// always set the operation to count
	s.input.SetSelect(string(ScanSelectCount))

	// get all of the segments
	segments := splitScanInputIntoSegments(s.input)

	wg := sync.WaitGroup{}
	state := &scanState{
		mu: sync.RWMutex{},
	}

	for _, segment := range segments {
		if state.error() != nil {
			break
		}
		wg.Add(1)
		// run the segment
		go scanSegment(req, segment, state, &wg, nil, s.handlerMu)
	}

	wg.Wait()

	out.output = state.cnt
	out.err = state.err
	return
}

func scanSegment(req *dyno.Request,
	segment *dynamodb.ScanInput,
	state *scanState,
	wg *sync.WaitGroup,
	handler ItemSliceHandler,
	handlerMu *sync.Mutex) {
	defer wg.Done()
	// start a for loop that keeps scanning as we page through returned ProjectionColumns
	for {
		// run the input
		output, err := req.Scan(segment)
		if err != nil {
			state.setError(err)
			return
		}

		state.addOutput(output)

		// if we have items and a handler, run the handler
		if len(output.Items) > 0 && handler != nil {
			handlerMu.Lock()
			err = handler(output.Items)
			handlerMu.Unlock()
			if err != nil {
				state.setError(err)
				return
			}
		}

		if *output.Count < 1 || output.LastEvaluatedKey == nil {
			// nothing left to do
			break
		}

		// set the start to key to the last evaluated key to keep looping
		segment.ExclusiveStartKey = output.LastEvaluatedKey
	}
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
		scanCopy := CopyScanInput(input)
		// set the segment to i
		scanCopy.SetSegment(i)
		inputs[i] = scanCopy
	}
	return
}
