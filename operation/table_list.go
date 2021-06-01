package operation

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ericmaustin/dyno"
)

// ListTableResult is returned as the result of a ListTableOperation
type ListTableResult struct {
	ResultBase
	names []string
}

// OutputInterface returns the DeleteItemOutput from the DeleteResult as an interface
func (d *ListTableResult) OutputInterface() interface{} {
	return d.names
}

// Output returns the ListTablesOutput from the ListTableResult
func (d *ListTableResult) Output() []string {
	return d.names
}

// OutputError returns the ListTablesOutput the error from the ListTableResult for convenience
func (d *ListTableResult) OutputError() ([]string, error) {
	return d.names, d.Err
}

// ListTableBuilder used for dynamically creating a ListTablesInput
type ListTableBuilder struct {
	input *dynamodb.ListTablesInput
}

// NewListTableBuilder creates a new ListTableBuilder
func NewListTableBuilder() *ListTableBuilder {
	return &ListTableBuilder{
		input: &dynamodb.ListTablesInput{},
	}
}

// SetInput sets the ListTableBuilder's dynamodb.ListTablesInput
func (b *ListTableBuilder) SetInput(input *dynamodb.ListTablesInput) *ListTableBuilder {
	b.input = input
	return b
}

// SetStartTable sets the inputs ExclusiveStartTableName
func (b *ListTableBuilder) SetStartTable(tableName string) *ListTableBuilder {
	b.input.SetExclusiveStartTableName(tableName)
	return b
}

// SetLimit sets the inputs Limit
func (b *ListTableBuilder) SetLimit(limit int64) *ListTableBuilder {
	b.input.SetLimit(limit)
	return b
}

// Build returns the input
func (b *ListTableBuilder) Build() *dynamodb.ListTablesInput {
	return b.input
}

// Operation returns a new ListTableOperation using this builder's input
func (b *ListTableBuilder) Operation() *ListTableOperation {
	return ListTables(b.Build())
}

// ListTableOperation represents a single ListTables operation
type ListTableOperation struct {
	*BaseOperation
	input *dynamodb.ListTablesInput
}

// ListTables creates a new ListTableOperation with optional ListTablesInput
func ListTables(input *dynamodb.ListTablesInput) *ListTableOperation {
	l := &ListTableOperation{
		BaseOperation: NewBase(),
		input:         input,
	}
	if l.input == nil {
		l.input = &dynamodb.ListTablesInput{}
	}
	return l
}

// Input returns current ListTablesInput
func (l *ListTableOperation) Input() *dynamodb.ListTablesInput {
	l.Mu.RLock()
	defer l.Mu.RUnlock()
	return l.input
}

// SetInput sets the ListTablesInput
// panics with ErrInvalidState error if operation is not pending
func (l *ListTableOperation) SetInput(input *dynamodb.ListTablesInput) *ListTableOperation {
	if !l.IsPending() {
		panic(&ErrInvalidState{})
	}
	l.Mu.Lock()
	defer l.Mu.Unlock()
	l.input = input
	return l
}

// SetStartTable sets the ExclusiveStartTableName on the Input
// panics with ErrInvalidState error if operation is not pending
func (l *ListTableOperation) SetStartTable(startTableName string) *ListTableOperation {
	if !l.IsPending() {
		panic(&ErrInvalidState{})
	}
	l.Mu.Lock()
	defer l.Mu.Unlock()
	l.input.SetExclusiveStartTableName(startTableName)
	return l
}

// SetLimit sets the Limit on the Input
// panics with ErrInvalidState error if operation is not pending
func (l *ListTableOperation) SetLimit(limit int64) *ListTableOperation {
	if !l.IsPending() {
		panic(&ErrInvalidState{})
	}
	l.Mu.Lock()
	defer l.Mu.Unlock()
	l.input.SetLimit(limit)
	return l
}

// ExecuteInBatch executes the ListTableOperation using given Request
// and returns a Result used for executing this operation in a batch
func (l *ListTableOperation) ExecuteInBatch(req *dyno.Request) Result {
	return l.Execute(req)
}

// GoExecute executes the ListTableOperation in a go routine and returns a channel
// that will pass a ListTableResult when execution completes
func (l *ListTableOperation) GoExecute(req *dyno.Request) <-chan *ListTableResult {
	outCh := make(chan *ListTableResult)
	go func() {
		defer close(outCh)
		outCh <- l.Execute(req)
	}()
	return outCh
}

// Execute runs the ListTableOperation and returns a ListTableResult
func (l *ListTableOperation) Execute(req *dyno.Request) (out *ListTableResult) {
	out = &ListTableResult{
		names: make([]string, 0),
	}
	l.SetRunning()
	defer l.SetDone(out)

	var output *dynamodb.ListTablesOutput

	for {
		if output != nil && output.LastEvaluatedTableName != nil {
			l.input.ExclusiveStartTableName = output.LastEvaluatedTableName
		}

		output, out.Err = req.ListTables(l.input)

		if out.Err != nil {
			break
		}

		for _, name := range output.TableNames {
			out.names = append(out.names, *name)
		}

		if len(out.names) < 1 || output.LastEvaluatedTableName == nil {
			break
		}
	}
	return
}
