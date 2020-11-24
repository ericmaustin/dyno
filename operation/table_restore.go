package operation

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ericmaustin/dyno"
	"github.com/ericmaustin/dyno/encoding"
)

// RestoreTableResult is returned as the result of a RestoreTableOperation
type RestoreTableResult struct {
	ResultBase
	output *dynamodb.RestoreTableFromBackupOutput
}

// OutputInterface returns the RestoreTableFromBackupOutput from the RestoreTableResult as an interface
func (r *RestoreTableResult) OutputInterface() interface{} {
	return r.output
}

// Output returns the RestoreTableFromBackupOutput from the RestoreTableResult
func (r *RestoreTableResult) Output() *dynamodb.RestoreTableFromBackupOutput {
	return r.output
}

// OutputError returns the RestoreTableFromBackupOutput the error from the RestoreTableResult for convenience
func (r *RestoreTableResult) OutputError() (*dynamodb.RestoreTableFromBackupOutput, error) {
	return r.output, r.err
}

// RestoreTableOperation represents a restore table operation
type RestoreTableOperation struct {
	*Base
	input *dynamodb.RestoreTableFromBackupInput
}

// RestoreTable creates a new RestoreTableOperation with optional RestoreTableFromBackupInput
func RestoreTable(backupArn, tableName interface{}) *RestoreTableOperation {
	input := &dynamodb.RestoreTableFromBackupInput{}
	if backupArn != nil {
		input.SetBackupArn(encoding.ToString(backupArn))
	}
	if tableName != nil {
		input.SetTargetTableName(encoding.ToString(tableName))
	}
	b := &RestoreTableOperation{
		Base:  newBase(),
		input: input,
	}
	return b
}

// Input returns current RestoreTableFromBackupInput
func (r *RestoreTableOperation) Input() *dynamodb.RestoreTableFromBackupInput {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.input
}

// SetInput sets the current RestoreTableFromBackupInput
func (r *RestoreTableOperation) SetInput(input *dynamodb.RestoreTableFromBackupInput) *RestoreTableOperation {
	if !r.IsPending() {
		panic(&InvalidState{})
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.input = input
	return r
}

// ExecuteInBatch executes the RestoreTableOperation using given Request
// and returns a Result used for executing this operation in a batch
func (r *RestoreTableOperation) ExecuteInBatch(req *dyno.Request) Result {
	return r.Execute(req)
}

// GoExecute executes the RestoreTableOperation in a go routine and returns a channel
// that will pass a RestoreTableResult when execution completes
func (r *RestoreTableOperation) GoExecute(req *dyno.Request) <-chan *RestoreTableResult {
	outCh := make(chan *RestoreTableResult)
	go func() {
		defer close(outCh)
		outCh <- r.Execute(req)
	}()
	return outCh
}

// Execute runs the RestoreTableOperation and returns a RestoreTableResult
func (r *RestoreTableOperation) Execute(req *dyno.Request) (out *RestoreTableResult) {
	out = &RestoreTableResult{}
	r.setRunning()
	defer r.setDone(out)
	out.output, out.err = req.RestoreTableFromBackup(r.input)
	return
}
