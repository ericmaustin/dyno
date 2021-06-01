package operation

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ericmaustin/dyno"
)

// DescribeBackupResult is returned by GoExecute in a channel when operation completes
type DescribeBackupResult struct {
	ResultBase
	output *dynamodb.DescribeBackupOutput
}

// OutputInterface returns the DescribeBackupOutput as an interface from the DescribeBackupResult
func (d *DescribeBackupResult) OutputInterface() (interface{}, error) {
	return d.output, d.Err
}

// Output returns the DescribeBackupOutput from the DescribeBackupResult for convenience
func (d *DescribeBackupResult) Output() (*dynamodb.DescribeBackupOutput, error) {
	return d.output, d.Err
}

// DescribeBackupOperation represents an operation that performs a DescribeBackup operation
type DescribeBackupOperation struct {
	*BaseOperation
	input *dynamodb.DescribeBackupInput
}

// DescribeBackup creates a new DescribeBackupOperation with optional DescribeBackupInput to be executed later
func DescribeBackup(arn string) *DescribeBackupOperation {
	d := &DescribeBackupOperation{
		BaseOperation: NewBase(),
		input:         &dynamodb.DescribeBackupInput{},
	}

	if len(arn) > 0 {
		d.input.SetBackupArn(arn)
	}

	return d
}

// ExecuteInBatch executes the DescribeBackupOperation using given Request
// and returns a Result used for executing this operation in a batch
func (d *DescribeBackupOperation) ExecuteInBatch(req *dyno.Request) Result {
	return d.Execute(req)
}

// Execute executes the DescribeTableOperation request
func (d *DescribeBackupOperation) Execute(req *dyno.Request) (out *DescribeBackupResult) {
	out = &DescribeBackupResult{}
	d.SetRunning()
	defer d.SetDone()
	out.output, out.Err = req.DescribeBackup(d.input)
	return
}

// GoExecute executes the DescribeBackupOperation request in a go routine and returns a channel
// that will return a DescribeBackupResult when operation is done
func (d *DescribeBackupOperation) GoExecute(req *dyno.Request) <-chan *DescribeBackupResult {
	outCh := make(chan *DescribeBackupResult)
	go func() {
		defer close(outCh)
		outCh <- d.Execute(req)
	}()
	return outCh
}
