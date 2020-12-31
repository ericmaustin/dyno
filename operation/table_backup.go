package operation

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ericmaustin/dyno"
)

// BackupTableResult is the result of a BackUp table operation
type BackupTableResult struct {
	resultBase
	output *dynamodb.CreateBackupOutput
}

// OutputInterface returns the CreateBackupOutput from the DeleteResult as an interface
func (b *BackupTableResult) OutputInterface() interface{} {
	return b.output
}

// Output returns the CreateBackupOutput from the BackupTableResult
func (b *BackupTableResult) Output() *dynamodb.CreateBackupOutput {
	return b.output
}

// OutputError returns the res and the error from the BackupTableResult for convenience
func (b *BackupTableResult) OutputError() (*dynamodb.CreateBackupOutput, error) {
	return b.output, b.err
}

type BackupTableOperation struct {
	*baseOperation
	input *dynamodb.CreateBackupInput
}

// BackupTable creates a new BackupTableOperation with optional BackupTableInput
func BackupTable(tableName, backupName string) *BackupTableOperation {
	input := &dynamodb.CreateBackupInput{
		TableName: &tableName,
		BackupName: &backupName,
	}
	b := &BackupTableOperation{
		baseOperation: newBase(),
		input:         input,
	}
	return b
}

// SetInput sets the inputBackupTableOperation
// panics with InvalidState error if operation is not pending
func (b *BackupTableOperation) SetInput(input *dynamodb.CreateBackupInput) *BackupTableOperation {
	if !b.IsPending() {
		panic(&InvalidState{})
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.input = input
	return b
}

// Input returns the current Input for the operation
// returns InvalidState if the operation is not done
func (b *BackupTableOperation) Input() *dynamodb.CreateBackupInput {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.input
}

// ExecuteInBatch executes the BackupTableOperation using given Request
// and returns a Result used for executing this operation in a batch
func (b *BackupTableOperation) ExecuteInBatch(req *dyno.Request) Result {
	return b.Execute(req)
}

// GoExecute executes the BackupTableOperation request in a go routine and returns a channel
// that will pass a DeleteResult when execution completes
func (b *BackupTableOperation) GoExecute(req *dyno.Request) <-chan *BackupTableResult {
	outCh := make(chan *BackupTableResult)
	go func() {
		defer close(outCh)
		outCh <- b.Execute(req)
	}()
	return outCh
}

// Execute executes the BackupTableOperation
func (b *BackupTableOperation) Execute(req *dyno.Request) (out *BackupTableResult) {
	out = &BackupTableResult{}
	b.setRunning()
	defer b.setDone(out)
	// call the api
	out.output, out.err = req.CreateBackup(b.input)
	return
}
