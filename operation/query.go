package operation

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/ericmaustin/dyno"
	"github.com/ericmaustin/dyno/condition"
	"github.com/ericmaustin/dyno/encoding"
)

// QueryResult is returned by the QueryOperation Execution in a channel when operation completes
type QueryResult struct {
	resultBase
	output []*dynamodb.QueryOutput
}

// OutputInterface returns the QueryOutput from the QueryResult as an interface
func (q *QueryResult) OutputInterface() interface{} {
	return q.output
}

// Output returns the QueryOutput slice from the QueryResult
func (q *QueryResult) Output() []*dynamodb.QueryOutput {
	return q.output
}

// OutputError returns the QueryOutput slice and the error from the QueryResult for convenience
func (q *QueryResult) OutputError() ([]*dynamodb.QueryOutput, error) {
	return q.Output(), q.err
}

// QueryBuilder dynamically constructs a QueryInput
type QueryBuilder struct {
	input      *dynamodb.QueryInput
	keyCnd     *expression.KeyConditionBuilder
	filter     *expression.ConditionBuilder
	projection *expression.ProjectionBuilder
}

// NewQueryBuilder creates a new input condition builder
func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{
		input: &dynamodb.QueryInput{},
	}
}

// SetTable sets the table for this input
func (q *QueryBuilder) SetTable(tableName string) *QueryBuilder {
	q.input.TableName = &tableName
	return q
}

// SetIndex sets the index for this input
func (q *QueryBuilder) SetIndex(index string) *QueryBuilder {
	q.input.SetIndexName(index)
	return q
}

// SetConsistentRead sets the consistent read boolean value for this input
func (q *QueryBuilder) SetConsistentRead(consistentRead bool) *QueryBuilder {
	q.input.SetConsistentRead(consistentRead)
	return q
}

// SetLimit sets the limit for this input
func (q *QueryBuilder) SetLimit(limit int64) *QueryBuilder {
	q.input.SetLimit(limit)
	return q
}

// SetAscOrder sets the query to return in ascending order
func (q *QueryBuilder) SetAscOrder() *QueryBuilder {
	q.input.ScanIndexForward = dyno.BoolPtr(true)
	return q
}

// SetDescOrder sets the query to return in descending order
func (q *QueryBuilder) SetDescOrder() *QueryBuilder {
	q.input.ScanIndexForward = dyno.BoolPtr(false)
	return q
}

// AddKeyCondition adds a key condition to this update
// adding multiple conditions by calling this multiple times will join the conditions with
// an AND
func (q *QueryBuilder) AddKeyCondition(cnd expression.KeyConditionBuilder) *QueryBuilder {
	if q.keyCnd == nil {
		q.keyCnd = &cnd
	} else {
		cnd = condition.KeyAnd(*q.keyCnd, cnd)
		q.keyCnd = &cnd
	}
	return q
}

// AddKeyEquals adds a equality key condition for the given fieldName and value
// this is a shortcut for adding an equality condition which is common for queries
func (q *QueryBuilder) AddKeyEquals(fieldName string, value interface{}) *QueryBuilder {
	return q.AddKeyCondition(condition.KeyEqual(fieldName, value))
}

// AddFilter adds a filter condition to this update
// adding multiple conditions by calling this multiple times will join the conditions with
// an AND
func (q *QueryBuilder) AddFilter(cnd expression.ConditionBuilder) *QueryBuilder {
	if q.filter == nil {
		q.filter = &cnd
	} else {
		cnd = condition.And(*q.filter, cnd)
		q.filter = &cnd
	}
	return q
}

// AddProjectionNames adds additional field names to the projection
func (q *QueryBuilder) AddProjectionNames(names interface{}) *QueryBuilder {
	nameBuilders := encoding.NameBuilders(names)
	if q.projection == nil {
		proj := expression.ProjectionBuilder{}
		proj = proj.AddNames(nameBuilders...)
		q.projection = &proj
	} else {
		*q.projection = q.projection.AddNames(nameBuilders...)
	}
	return q
}

// Build builds the input input with included projection, key conditions, and filters
func (q *QueryBuilder) Build() *dynamodb.QueryInput {
	if q.projection == nil && q.keyCnd == nil && q.filter == nil {
		// no expression builder is needed
		return q.input
	}
	builder := expression.NewBuilder()
	// add projection
	if q.projection != nil {
		builder = builder.WithProjection(*q.projection)
	}
	// add key condition
	if q.keyCnd != nil {
		builder = builder.WithKeyCondition(*q.keyCnd)
	}
	// add filter
	if q.filter != nil {
		builder = builder.WithFilter(*q.filter)
	}
	// build the Expression
	expr, err := builder.Build()
	if err != nil {
		panic(err)
	}

	q.input.ExpressionAttributeNames = expr.Names()
	q.input.ExpressionAttributeValues = expr.Values()
	q.input.FilterExpression = expr.Filter()
	q.input.KeyConditionExpression = expr.KeyCondition()
	q.input.ProjectionExpression = expr.Projection()
	return q.input
}

// BuildCount builds the input input with included projection, key conditions, and filters
// and removes the projection values to only return counts
func (q *QueryBuilder) BuildCount() *dynamodb.QueryInput {
	if q.projection == nil && q.keyCnd == nil && q.filter == nil {
		// no expression builder is needed
		return q.input
	}
	builder := expression.NewBuilder()

	// set the selection to be a count
	q.input.SetSelect("COUNT")
	// add key condition
	if q.keyCnd != nil {
		builder = builder.WithKeyCondition(*q.keyCnd)
	}
	// add filter
	if q.filter != nil {
		builder = builder.WithFilter(*q.filter)
	}
	// build the Expression
	expr, err := builder.Build()
	if err != nil {
		panic(err)
	}

	q.input.ExpressionAttributeNames = expr.Names()
	q.input.ExpressionAttributeValues = expr.Values()
	q.input.FilterExpression = expr.Filter()
	q.input.KeyConditionExpression = expr.KeyCondition()
	return q.input
}

// BuildOperation builds the input and returns a QueryOperation
func (q *QueryBuilder) BuildOperation() *QueryOperation {
	return Query(q.Build())
}

// BuildCountOperation builds the input and returns a QueryCountOperation
func (q *QueryBuilder) BuildCountOperation() *QueryCountOperation {
	return QueryCount(q.BuildCount())
}

// QueryOperation runs query operations and handles their res
type QueryOperation struct {
	*baseOperation
	input   *dynamodb.QueryInput
	handler ItemSliceHandler
}

// Input returns a ptr to the Input Input
func (q *QueryOperation) Input() *dynamodb.QueryInput {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.input
}

// SetInput sets the QueryInput
// panics with an ErrInvalidState error if operation isn't pending
func (q *QueryOperation) SetInput(input *dynamodb.QueryInput) *QueryOperation {
	if !q.IsPending() {
		panic(&ErrInvalidState{})
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.input = input
	return q
}

// Query creates a new input object with the required session request and primary key
func Query(input *dynamodb.QueryInput) *QueryOperation {
	return &QueryOperation{
		baseOperation: newBase(),
		input:         input,
	}
}

// SetHandler sets the handler to be used for this Input
// panics with an ErrInvalidState error if operation isn't pending
func (q *QueryOperation) SetHandler(handler ItemSliceHandler) *QueryOperation {
	if !q.IsPending() {
		panic(&ErrInvalidState{})
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.handler = handler
	return q
}

// SetLimit sets the page size
func (q *QueryOperation) SetLimit(limit int64) *QueryOperation {
	if !q.IsPending() {
		panic(&ErrInvalidState{})
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.input.SetLimit(limit)
	return q
}

// SetStartKey sets the start key on the QueryInput
func (q *QueryOperation) SetStartKey(startKey map[string]*dynamodb.AttributeValue) *QueryOperation {
	if !q.IsPending() {
		panic(&ErrInvalidState{})
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.input.ExclusiveStartKey = startKey
	return q
}

// SetScanIndexForward tells the Input to return results records desc order by the scan index
func (q *QueryOperation) SetScanIndexForward(indexForward bool) *QueryOperation {
	if !q.IsPending() {
		panic(&ErrInvalidState{})
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.input.SetScanIndexForward(indexForward)
	return q
}

// ExecuteInBatch executes the QueryOperation using given Request
// and returns a Result used for executing this operation in a batch
func (q *QueryOperation) ExecuteInBatch(req *dyno.Request) Result {
	return q.Execute(req)
}

// Execute executes the Input
func (q *QueryOperation) Execute(req *dyno.Request) (out *QueryResult) {
	out = &QueryResult{}
	q.setRunning()
	defer q.setDone(out)

	out.output = make([]*dynamodb.QueryOutput, 0)

	var output *dynamodb.QueryOutput

	// start a for loop that keeps scanning as we page through returned ProjectionColumns
	for {
		// Execute the input
		output, out.err = req.Query(q.input)

		if out.err != nil {
			return
		}

		// append the res
		out.output = append(out.output, output)

		// if we have items and a handler, Execute the handler
		if len(output.Items) > 0 && q.handler != nil {
			out.err = q.handler(output.Items)
			if out.err != nil {
				return
			}
		}

		if *output.Count < 1 || output.LastEvaluatedKey == nil {
			// nothing left to do
			break
		}

		// set the start to key to the last evaluated key to keep looping
		q.input.ExclusiveStartKey = output.LastEvaluatedKey
	}

	return
}

// GoExecute executes the QueryOperation and returns a channel that will contain a QueryResult
// when operation completes
func (q *QueryOperation) GoExecute(req *dyno.Request) <-chan *QueryResult {
	outCh := make(chan *QueryResult)
	go func() {
		defer close(outCh)
		outCh <- q.Execute(req)
	}()
	return outCh
}

// QueryCountResult is returned by the QueryCountOperation Execution in a channel when operation completes
type QueryCountResult struct {
	resultBase
	output int64
}

// OutputInterface returns the QueryOutput from the QueryCountResult as an interface
func (qc *QueryCountResult) OutputInterface() interface{} {
	return qc.output
}

// Output returns the QueryOutput slice from the QueryResult
func (qc *QueryCountResult) Output() int64 {
	return qc.output
}

// OutputError returns the QueryOutput slice and the error from the QueryResult for convenience
func (qc *QueryCountResult) OutputError() (int64, error) {
	return qc.Output(), qc.err
}

// QueryCountOperation runs query operations and handles their res
type QueryCountOperation struct {
	*baseOperation
	input *dynamodb.QueryInput
}

// Input returns a ptr to the Input Input
func (qc *QueryCountOperation) Input() *dynamodb.QueryInput {
	qc.mu.RLock()
	defer qc.mu.RUnlock()
	return qc.input
}

// SetInput sets the QueryInput
// panics with an ErrInvalidState error if operation isn't pending
func (qc *QueryCountOperation) SetInput(input *dynamodb.QueryInput) *QueryCountOperation {
	if !qc.IsPending() {
		panic(&ErrInvalidState{})
	}
	qc.mu.Lock()
	defer qc.mu.Unlock()
	qc.input = input
	return qc
}

// QueryCount creates a new QueryCountOperation
func QueryCount(input *dynamodb.QueryInput) *QueryCountOperation {
	return &QueryCountOperation{
		baseOperation: newBase(),
		input:         input,
	}
}

// SetStartKey sets the start key on the QueryCountInput
func (qc *QueryCountOperation) SetStartKey(startKey map[string]*dynamodb.AttributeValue) *QueryCountOperation {
	if !qc.IsPending() {
		panic(&ErrInvalidState{})
	}
	qc.mu.Lock()
	defer qc.mu.Unlock()
	qc.input.ExclusiveStartKey = startKey
	return qc
}

// ExecuteInBatch executes the QueryCountOperation using given Request
// and returns a Result used for executing this operation in a batch
func (qc *QueryCountOperation) ExecuteInBatch(req *dyno.Request) Result {
	return qc.Execute(req)
}

// Execute executes the QueryCountOperation
func (qc *QueryCountOperation) Execute(req *dyno.Request) (out *QueryCountResult) {
	out = &QueryCountResult{}
	qc.setRunning()
	defer qc.setDone(out)

	var output *dynamodb.QueryOutput

	// start a for loop that keeps scanning as we page through returned ProjectionColumns
	for {
		// Execute the input
		output, out.err = req.Query(qc.input)

		if out.err != nil {
			return
		}

		// add the count
		out.output += *output.Count

		if *output.Count < 1 || output.LastEvaluatedKey == nil {
			// nothing left to do
			break
		}

		// set the start to key to the last evaluated key to keep looping
		qc.input.ExclusiveStartKey = output.LastEvaluatedKey
	}

	return
}

// GoExecute executes the QueryOperation and returns a channel that will contain a QueryResult
// when operation completes
func (qc *QueryCountOperation) GoExecute(req *dyno.Request) <-chan *QueryCountResult {
	outCh := make(chan *QueryCountResult)
	go func() {
		defer close(outCh)
		outCh <- qc.Execute(req)
	}()
	return outCh
}
