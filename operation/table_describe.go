package operation

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ericmaustin/dyno"
)

// DescribeTableResult is returned as the result of a DescribeTableOperation
type DescribeTableResult struct {
	resultBase
	output *dynamodb.DescribeTableOutput
}

// OutputInterface returns the DeleteItemOutput from the DescribeTableResult as an interface
func (d *DescribeTableResult) OutputInterface() interface{} {
	return d.output
}

// Output returns the DescribeTableOutput from the DescribeTableResult
func (d *DescribeTableResult) Output() *dynamodb.DescribeTableOutput {
	return d.output
}

// OutputError returns the DescribeTableOutput the error from the DescribeTableResult for convenience
func (d *DescribeTableResult) OutputError() (*dynamodb.DescribeTableOutput, error) {
	return d.output, d.err
}

// DescribeTableOperation represents a describe table operation
type DescribeTableOperation struct {
	*baseOperation
	input *dynamodb.DescribeTableInput
}

// DescribeTable creates a new DescribeTableInput with a given table name
func DescribeTable(tableName string) *DescribeTableOperation {
	d := &DescribeTableOperation{
		baseOperation: newBase(),
		input: &dynamodb.DescribeTableInput{TableName: &tableName},
	}

	return d
}

// Input returns current DescribeTableInput
func (d *DescribeTableOperation) Input() *dynamodb.DescribeTableInput {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.input
}

// SetInput sets the current DescribeTableInput
func (d *DescribeTableOperation) SetInput(input *dynamodb.DescribeTableInput) *DescribeTableOperation {
	if !d.IsPending() {
		panic(&InvalidState{})
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.input = input
	return d
}

// ExecuteInBatch executes the DescribeTableOperation using given Request
// and returns a Result used for executing this operation in a batch
func (d *DescribeTableOperation) ExecuteInBatch(req *dyno.Request) Result {
	return d.Execute(req)
}

// GoExecute executes the DescribeTableOperation in a go routine and returns a channel
// that will pass a DescribeTableResult when execution completes
func (d *DescribeTableOperation) GoExecute(req *dyno.Request) <-chan *DescribeTableResult {
	outCh := make(chan *DescribeTableResult)
	go func() {
		defer close(outCh)
		outCh <- d.Execute(req)
	}()
	return outCh
}

// Execute runs the DescribeTableOperation and returns a DescribeTableResult
func (d *DescribeTableOperation) Execute(req *dyno.Request) (out *DescribeTableResult) {
	out = &DescribeTableResult{}
	d.setRunning()
	defer d.setDone(out)

	out.output, out.err = req.DescribeTable(d.input)
	return
}
