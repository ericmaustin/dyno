package operation

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ericmaustin/dyno"
)

// BackupTableResult is the result of a BackUp table operation
type BackupTableResult struct {
	ResultBase
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
	return b.output, b.Err
}

//BackupTableOperation represents a BackupTable operation
type BackupTableOperation struct {
	*BaseOperation
	input *dynamodb.CreateBackupInput
}

// BackupTable creates a new BackupTableOperation with optional BackupTableInput
func BackupTable(tableName, backupName string) *BackupTableOperation {
	input := &dynamodb.CreateBackupInput{
		TableName:  &tableName,
		BackupName: &backupName,
	}
	b := &BackupTableOperation{
		BaseOperation: NewBase(),
		input:         input,
	}
	return b
}

// SetInput sets the inputBackupTableOperation
// panics with ErrInvalidState error if operation is not pending
func (b *BackupTableOperation) SetInput(input *dynamodb.CreateBackupInput) *BackupTableOperation {
	if !b.IsPending() {
		panic(&ErrInvalidState{})
	}
	b.Mu.Lock()
	defer b.Mu.Unlock()
	b.input = input
	return b
}

// Input returns the current Input for the operation
// returns ErrInvalidState if the operation is not done
func (b *BackupTableOperation) Input() *dynamodb.CreateBackupInput {
	b.Mu.RLock()
	defer b.Mu.RUnlock()
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
	b.SetRunning()
	defer b.SetDone(out)
	// call the api
	out.output, out.Err = req.CreateBackup(b.input)
	return
}
