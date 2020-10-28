package operation

import (
	"git-codecommit.us-east-1.amazonaws.com/v1/repos/dyno.git"
	"git-codecommit.us-east-1.amazonaws.com/v1/repos/dyno.git/encoding"
	"github.com/aws/aws-sdk-go/service/dynamodb"
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
	return d.names, d.err
}

// ListTableBuilder used for dynamically creating a ListTablesInput
type ListTableBuilder struct {
	input *dynamodb.ListTablesInput
}

// NewListTableBuilder creates a new ListTableBuilder
func NewListTableBuilder(input *dynamodb.ListTablesInput) *ListTableBuilder {
	b := &ListTableBuilder{
		input: input,
	}
	if input == nil {
		b.input = &dynamodb.ListTablesInput{}
	}
	return b
}

// SetStartTable sets the inputs ExclusiveStartTableName
func (b *ListTableBuilder) SetStartTable(tableName interface{}) *ListTableBuilder {
	b.input.SetExclusiveStartTableName(encoding.ToString(tableName))
	return b
}

// SetLimit sets the inputs Limit
func (b *ListTableBuilder) SetLimit(limit int64) *ListTableBuilder {
	b.input.SetLimit(limit)
	return b
}

// Input returns the input
func (b *ListTableBuilder) Input() *dynamodb.ListTablesInput {
	return b.input
}

// Operation returns a new ListTableOperation using this builder's input
func (b *ListTableBuilder) Operation() *ListTableOperation {
	return ListTables(b.Input())
}

// ListTableOperation represents a single ListTables operation
type ListTableOperation struct {
	*Base
	input *dynamodb.ListTablesInput
}

// ListTables creates a new ListTableOperation with optional ListTablesInput
func ListTables(input *dynamodb.ListTablesInput) *ListTableOperation {
	l := &ListTableOperation{
		Base:  newBase(),
		input: input,
	}
	if l.input == nil {
		l.input = &dynamodb.ListTablesInput{}
	}
	return l
}

// Input returns current ListTablesInput
func (l *ListTableOperation) Input() *dynamodb.ListTablesInput {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.input
}

// SetInput sets the ListTablesInput
// panics with InvalidState error if operation is not pending
func (l *ListTableOperation) SetInput(input *dynamodb.ListTablesInput) *ListTableOperation {
	if !l.IsPending() {
		panic(&InvalidState{})
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.input = input
	return l
}

// SetStartTable sets the ExclusiveStartTableName on the Input
// panics with InvalidState error if operation is not pending
func (l *ListTableOperation) SetStartTable(startTableName string) *ListTableOperation {
	if !l.IsPending() {
		panic(&InvalidState{})
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.input.SetExclusiveStartTableName(startTableName)
	return l
}

// SetLimit sets the Limit on the Input
// panics with InvalidState error if operation is not pending
func (l *ListTableOperation) SetLimit(limit int64) *ListTableOperation {
	if !l.IsPending() {
		panic(&InvalidState{})
	}
	l.mu.Lock()
	defer l.mu.Unlock()
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
	l.setRunning()
	defer l.setDone(out)

	var output *dynamodb.ListTablesOutput

	for {
		if output != nil && output.LastEvaluatedTableName != nil {
			l.input.ExclusiveStartTableName = output.LastEvaluatedTableName
		}

		output, out.err = req.ListTables(l.input)

		if out.err != nil {
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
