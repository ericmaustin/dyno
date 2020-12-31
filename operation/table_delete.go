package operation

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ericmaustin/dyno"
)

// DeleteTableResult is returned as the result of a DeleteTableOperation
type DeleteTableResult struct {
	resultBase
	output *dynamodb.DeleteTableOutput
}

// OutputInterface returns the DeleteTableOutput from the DeleteResult as an interface
func (d *DeleteTableResult) OutputInterface() interface{} {
	return d.output
}

// Output returns the DeleteTableOutput from the DeleteResult
func (d *DeleteTableResult) Output() *dynamodb.DeleteTableOutput {
	return d.output
}

// OutputError returns the DeleteTableOutput the error from the DeleteResult for convenience
func (d *DeleteTableResult) OutputError() (*dynamodb.DeleteTableOutput, error) {
	return d.output, d.err
}

// DeleteTableOperation represents a delete table operation
type DeleteTableOperation struct {
	*baseOperation
	input *dynamodb.DeleteTableInput
}

// DeleteTable creates a new DeleteTableOperation for the given table name
func DeleteTable(tableName string) *DeleteTableOperation {
	input := &dynamodb.DeleteTableInput{
		TableName: &tableName,
	}
	d := &DeleteTableOperation{
		baseOperation: newBase(),
		input:         input,
	}
	return d
}

// Input returns current DeleteTableInput
func (d *DeleteTableOperation) Input() *dynamodb.DeleteTableInput {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.input
}

// SetInput sets the current DeleteTableInput
func (d *DeleteTableOperation) SetInput(input *dynamodb.DeleteTableInput) *DeleteTableOperation {
	if !d.IsPending() {
		panic(&InvalidState{})
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.input = input
	return d
}

// ExecuteInBatch executes the DeleteTableOperation using given Request
// and returns a Result used for executing this operation in a batch
func (d *DeleteTableOperation) ExecuteInBatch(req *dyno.Request) Result {
	return d.Execute(req)
}

// GoExecute executes the DeleteTableOperation in a go routine
func (d *DeleteTableOperation) GoExecute(req *dyno.Request) <-chan *DeleteTableResult {
	outCh := make(chan *DeleteTableResult)
	go func() {
		defer close(outCh)
		outCh <- d.Execute(req)
	}()
	return outCh
}

// Execute executes the DeleteTableOperation
func (d *DeleteTableOperation) Execute(req *dyno.Request) (out *DeleteTableResult) {
	out = &DeleteTableResult{}
	d.setRunning()
	defer d.setDone(out)

	out.output, out.err = req.DeleteTable(d.input)
	return
}
