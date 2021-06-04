package dyno

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ericmaustin/dyno/timer"
	"github.com/google/uuid"
)

// ExecutionFunction is a function that can be executed by a Request
type ExecutionFunction func(ctx context.Context) error

type execution struct {
	ctx     context.Context
	cancel  context.CancelFunc
	sleeper *timer.Sleeper
}

func (e *execution) checkError(err error) bool {
	if err == nil {
		return false
	}
	if awsErr, ok := err.(awserr.Error); ok {

		switch awsErr.Code() {
		case dynamodb.ErrCodeProvisionedThroughputExceededException,
			dynamodb.ErrCodeRequestLimitExceeded,
			dynamodb.ErrCodeTransactionConflictException:
			return true
		default:
			// unknown error, return here
			return false
		}
	}
	return false
}

func (e *execution) do(execFunc ExecutionFunction) error {
	// if context was cancelled then context cancelled error
	select {
	case <-e.ctx.Done():
		return Error{
			Code:    ErrRequestExecutionContextCancelled,
			Message: "context cancelled",
		}
	default:
		// exec nothing
	}
	err := execFunc(e.ctx)
	retry := e.checkError(err)
	if retry {
		if err = <-e.sleeper.Sleep(); err != nil {
			return err
		}
		return execFunc(e.ctx)
	}
	return err
}

const initialSleepTime = time.Millisecond * 50

// Request is a Session that has a context and a cancel function
type Request struct {
	*Session
	requestID string
	ctx       context.Context
	cancel    context.CancelFunc
	mu        *sync.RWMutex
}

// newRequest creates a new request
func newRequest(ctx context.Context, cancel context.CancelFunc, s *Session) *Request {
	r := &Request{
		mu:        &sync.RWMutex{},
		Session:   s,
		requestID: uuid.New().String(),
		ctx:       ctx,
		cancel:    cancel,
	}
	return r
}

// Cancel the request
func (r *Request) Cancel() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cancel()
}

// Context returns the context for this request
func (r *Request) Context() context.Context {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.ctx
}

// SetTimeout sets a timeout on the context
// This does not replace any existing timeouts as it uses the existing context if there is one
func (r *Request) SetTimeout(timeout time.Duration) *Request {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ctx == nil {
		r.ctx = context.Background()
	}
	r.ctx, r.cancel = context.WithTimeout(r.ctx, timeout)
	return r
}

// SetContext sets the request context
func (r *Request) SetContext(ctx context.Context) *Request {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ctx = ctx
	return r
}

// Execute executes a given execution function with exponential back-off on dynamodb error
func (r *Request) Execute(execFunc ExecutionFunction) error {
	var (
		ctx    context.Context
		cancel context.CancelFunc
	)
	r.mu.RLock()
	if _, ok := r.ctx.Deadline(); !ok {
		// no deadline set, so use max timeout
		ctx, cancel = context.WithTimeout(r.Context(), r.MaxTimeout())
	} else {
		ctx, cancel = r.ctx, r.cancel
	}
	r.mu.RUnlock()
	exec := &execution{
		ctx:    ctx,
		cancel: cancel,
		sleeper: timer.NewExponentialSleeper(initialSleepTime).
			WithContext(ctx).
			WithTimeout(r.MaxTimeout()).
			WithAddRandom(time.Millisecond * 100),
	}
	return exec.do(execFunc)
}

// Scan executes a scan api call with a dynamodb.ScanInput
func (r *Request) Scan(in *dynamodb.ScanInput, handler ScanHandler) (out *dynamodb.ScanOutput, err error) {
	err = r.Execute(func(ctx context.Context) error {
		out, err = r.DynamoClient().ScanWithContext(ctx, in)
		if err != nil {
			return err
		}
		if handler != nil {
			return handler(out)
		}
		return err
	})
	return
}

// ScanAll executes a scan api call with a dynamodb.ScanInput and keeps scanning until there are no more
// LastEvaluatedKey values
func (r *Request) ScanAll(in *dynamodb.ScanInput, handler ScanHandler) (out []*dynamodb.ScanOutput, err error) {
	var o *dynamodb.ScanOutput
	err = r.Execute(func(ctx context.Context) error {
		for {
			o, err = r.DynamoClient().ScanWithContext(ctx, in)
			out = append(out, o)
			if err != nil {
				return err
			}
			if handler != nil {
				if err = handler(o); err != nil {
					return err
				}
			}
			if o.LastEvaluatedKey == nil {
				// no more work
				break
			}
			in.ExclusiveStartKey = o.LastEvaluatedKey
		}
		return err
	})
	return
}

// Query executes a query api call with a dynamodb.QueryInput
func (r *Request) Query(in *dynamodb.QueryInput, handler QueryHandler) (out *dynamodb.QueryOutput, err error) {
	err = r.Execute(func(ctx context.Context) error {
		out, err = r.DynamoClient().QueryWithContext(ctx, in)
		if err != nil {
			return err
		}
		if handler != nil {
			return handler(out)
		}
		return err
	})
	return
}

// QueryAll executes a query api call with a dynamodb.QueryInput and keeps querying until there are no more
// LastEvaluatedKey values
func (r *Request) QueryAll(in *dynamodb.QueryInput, handler QueryHandler) (out []*dynamodb.QueryOutput, err error) {
	var o *dynamodb.QueryOutput
	err = r.Execute(func(ctx context.Context) error {
		for {
			o, err = r.DynamoClient().QueryWithContext(ctx, in)
			out = append(out, o)
			if err != nil {
				return err
			}
			if handler != nil {
				if err = handler(o); err != nil {
					return err
				}
			}
			if o.LastEvaluatedKey == nil {
				// no more work
				break
			}
			in.ExclusiveStartKey = o.LastEvaluatedKey
		}
		return err
	})
	return
}

// PutItem runs a put item api call with a dynamodb.PutItemInput
func (r *Request) PutItem(in *dynamodb.PutItemInput, handler PutItemHandler) (out *dynamodb.PutItemOutput, err error) {
	err = r.Execute(func(ctx context.Context) error {
		out, err = r.DynamoClient().PutItemWithContext(ctx, in)
		if err != nil {
			return err
		}
		if handler != nil {
			return handler(out)
		}
		return nil
	})
	return
}

//GetItem runs a GetItem dynamodb operation with a dynamodb.GetItemInput
func (r *Request) GetItem(in *dynamodb.GetItemInput, handler GetItemHandler) (out *dynamodb.GetItemOutput, err error) {
	err = r.Execute(func(ctx context.Context) error {
		out, err = r.DynamoClient().GetItemWithContext(ctx, in)
		if err != nil {
			return err
		}
		if handler != nil {
			return handler(out)
		}
		return nil
	})
	return
}

// UpdateItem runs an update item api call with a dynamodb.UpdateItemInput
func (r *Request) UpdateItem(in *dynamodb.UpdateItemInput, handler UpdateItemHandler) (out *dynamodb.UpdateItemOutput, err error) {
	err = r.Execute(func(ctx context.Context) error {
		out, err = r.DynamoClient().UpdateItemWithContext(ctx, in)
		if err != nil {
			return err
		}
		if handler == nil {
			return handler(out)
		}
		return nil
	})
	return
}

// DeleteItem runs a delete item api call with a dynamodb.DeleteItemInput
func (r *Request) DeleteItem(in *dynamodb.DeleteItemInput, handler DeleteItemHandler) (out *dynamodb.DeleteItemOutput, err error) {
	err = r.Execute(func(ctx context.Context) error {
		out, err = r.DynamoClient().DeleteItemWithContext(ctx, in)
		if err != nil {
			return err
		}
		if handler == nil {
			return handler(out)
		}
		return nil
	})
	return
}

// BatchGetItem runs a batch get item api call with a dynamodb.BatchGetItemInput
func (r *Request) BatchGetItem(in *dynamodb.BatchGetItemInput, handler BatchGetItemHandler) (out *dynamodb.BatchGetItemOutput, err error) {
	err = r.Execute(func(ctx context.Context) error {
		out, err = r.DynamoClient().BatchGetItemWithContext(ctx, in)
		if err != nil {
			return err
		}
		if handler == nil {
			return handler(out)
		}
		return err
	})
	return
}

// BatchGetItemAll runs a batch get item api call with a dynamodb.BatchGetItemInput
// and continues to run batch get item requests until no unprocessed keys remain
func (r *Request) BatchGetItemAll(in *dynamodb.BatchGetItemInput, handler BatchGetItemHandler) (out []*dynamodb.BatchGetItemOutput, err error) {
	var o *dynamodb.BatchGetItemOutput
	err = r.Execute(func(ctx context.Context) error {
		for {
			o, err = r.DynamoClient().BatchGetItemWithContext(ctx, in)
			out = append(out, o)
			if err != nil {
				return err
			}
			if handler != nil {
				if err = handler(o); err != nil {
					return err
				}
			}
			if o.UnprocessedKeys == nil || len(o.UnprocessedKeys) < 1 {
				break
			}
			in.RequestItems = o.UnprocessedKeys
		}
		return err
	})
	return
}

//BatchWriteItem runs a batch write item api call with a dynamodb.BatchWriteItemInput
func (r *Request) BatchWriteItem(in *dynamodb.BatchWriteItemInput) (out *dynamodb.BatchWriteItemOutput, err error) {
	err = r.Execute(func(ctx context.Context) error {
		out, err = r.DynamoClient().BatchWriteItemWithContext(ctx, in)
		return err
	})
	return
}

//CreateTable runs a create table api call with a dynamodb.CreateTableInput
func (r *Request) CreateTable(in *dynamodb.CreateTableInput, handler CreateTableHandler) (out *dynamodb.CreateTableOutput, err error) {
	err = r.Execute(func(ctx context.Context) error {
		out, err = r.DynamoClient().CreateTableWithContext(ctx, in)
		if err != nil {
			return err
		}
		if handler != nil {
			return handler(out)
		}
		return nil
	})
	return
}

//CreateGlobalTable runs a create global table api call with a dynamodb.CreateGlobalTableInput
func (r *Request) CreateGlobalTable(in *dynamodb.CreateGlobalTableInput, handler CreateGlobalTableHandler) (out *dynamodb.CreateGlobalTableOutput, err error) {
	err = r.Execute(func(ctx context.Context) error {
		out, err = r.DynamoClient().CreateGlobalTableWithContext(ctx, in)
		if err != nil {
			return err
		}
		if handler != nil {
			return handler(out)
		}
		return nil
	})
	return
}

//DescribeTable runs a describe table api call with a dynamodb.DescribeTableInput
func (r *Request) DescribeTable(in *dynamodb.DescribeTableInput, handler DescribeTableHandler) (out *dynamodb.DescribeTableOutput, err error) {
	err = r.Execute(func(ctx context.Context) error {
		out, err = r.DynamoClient().DescribeTableWithContext(ctx, in)
		if err != nil {
			return err
		}
		if handler != nil {
			return handler(out)
		}
		return nil
	})
	return
}

//UpdateTable runs an update table api call with a dynamodb.UpdateTableInput
func (r *Request) UpdateTable(in *dynamodb.UpdateTableInput) (out *dynamodb.UpdateTableOutput, err error) {
	err = r.Execute(func(ctx context.Context) error {
		out, err = r.DynamoClient().UpdateTableWithContext(ctx, in)
		return err
	})
	return
}

//ListTables runs a list table api call with a dynamodb.ListTablesInput
func (r *Request) ListTables(in *dynamodb.ListTablesInput) (out *dynamodb.ListTablesOutput, err error) {
	err = r.Execute(func(ctx context.Context) error {
		out, err = r.DynamoClient().ListTablesWithContext(ctx, in)
		return err
	})
	return
}

//CreateBackup runs a create table backup api call with a dynamodb.CreateBackupInput
func (r *Request) CreateBackup(in *dynamodb.CreateBackupInput, handler CreateBackupHandler) (out *dynamodb.CreateBackupOutput, err error) {
	err = r.Execute(func(ctx context.Context) error {
		out, err = r.DynamoClient().CreateBackupWithContext(ctx, in)
		if err != nil {
			return err
		}
		if handler != nil {
			return handler(out)
		}
		return nil
	})
	return
}

//DescribeBackup runs a describe table backup api call with a dynamodb.DescribeBackupInput
func (r *Request) DescribeBackup(in *dynamodb.DescribeBackupInput, handler DescribeBackupHandler) (out *dynamodb.DescribeBackupOutput, err error) {
	err = r.Execute(func(ctx context.Context) error {
		out, err = r.DynamoClient().DescribeBackupWithContext(ctx, in)
		if err != nil {
			return err
		}
		if handler != nil {
			return handler(out)
		}
		return nil
	})
	return
}

//DeleteBackup runs a delete table backup api call with a dynamodb.DeleteBackupInput
func (r *Request) DeleteBackup(in *dynamodb.DeleteBackupInput) (out *dynamodb.DeleteBackupOutput, err error) {
	err = r.Execute(func(ctx context.Context) error {
		out, err = r.DynamoClient().DeleteBackupWithContext(ctx, in)
		return err
	})
	return
}

//ListBackups runs a delete list backups api call with a dynamodb.ListBackupsInput
func (r *Request) ListBackups(in *dynamodb.ListBackupsInput) (out *dynamodb.ListBackupsOutput, err error) {
	err = r.Execute(func(ctx context.Context) error {
		out, err = r.DynamoClient().ListBackupsWithContext(ctx, in)
		return err
	})
	return
}

//DeleteTable runs a delete table api call with a dynamodb.DeleteTableInput
func (r *Request) DeleteTable(in *dynamodb.DeleteTableInput, handler DeleteTableHandler) (out *dynamodb.DeleteTableOutput, err error) {
	err = r.Execute(func(ctx context.Context) error {
		out, err = r.DynamoClient().DeleteTableWithContext(ctx, in)
		if err != nil {
			return err
		}
		if handler != nil {
			return handler(out)
		}
		return nil
	})
	return
}

//RestoreTableFromBackup runs a restore table from backup api call with a dynamodb.RestoreTableFromBackupInput
func (r *Request) RestoreTableFromBackup(in *dynamodb.RestoreTableFromBackupInput) (out *dynamodb.RestoreTableFromBackupOutput, err error) {
	err = r.Execute(func(ctx context.Context) error {
		out, err = r.DynamoClient().RestoreTableFromBackupWithContext(ctx, in)
		return err
	})
	return
}

//ListenForTableDeletion returns a channel that will be passed an error when
// the table is either deleted (will return nil) or errors during waiting
// The table must be already deleted or either active or being deleted or else an error will be returned
func (r *Request) ListenForTableDeletion(tableName string) <-chan error {
	out := make(chan error)

	ticker := time.NewTicker(time.Millisecond * 100)

	go func() {
		var (
			err, apiErr      error
			tableDescription *dynamodb.DescribeTableOutput
		)
		defer func() {
			out <- err
			close(out)
		}()
		for {
			select {
			case <-r.ctx.Done():
				err = fmt.Errorf("timed out listening for table %s ready state", tableName)
			case <-ticker.C:
				tableDescription, apiErr = r.DescribeTable(&dynamodb.DescribeTableInput{
					TableName: &tableName,
				}, nil)

				// set the error if there was one
				if apiErr != nil {
					if IsAwsErrorCode(apiErr, dynamodb.ErrCodeResourceNotFoundException) {
						// this error means successful deletion
						return
					}
					// all other errors are a problem
					err = apiErr
					return
				}

				if tableDescription.Table != nil {
					switch *tableDescription.Table.TableStatus {
					case dynamodb.TableStatusDeleting, dynamodb.TableStatusActive:
						continue
					default:
						err = fmt.Errorf("table %s is in an invalid state: '%v'", tableName, *tableDescription.Table.TableStatus)
						return
					}
				}
			}
		}
	}()
	return out
}

//TableReadyOutput used as output for Request.ListenForTableReady
// will hold an error value in Err if Request.ListenForTableReady failed or timed out
type TableReadyOutput struct {
	*dynamodb.DescribeTableOutput
	Err error
}

//ListenForTableReady returns a channel that will be passed a TableReadyOutput when listener is triggered
// or encounters an error
func (r *Request) ListenForTableReady(tableName string) <-chan *TableReadyOutput {
	outCh := make(chan *TableReadyOutput)

	ticker := time.NewTicker(time.Millisecond * 100)

	go func() {
		var (
			apiErr           error
			tableDescription *dynamodb.DescribeTableOutput
		)
		out := new(TableReadyOutput)
		defer func() {
			outCh <- out
			close(outCh)
		}()
		for {
			select {
			case <-r.ctx.Done():
				out.Err = fmt.Errorf("timed out listening for table %s ready state", tableName)
			case <-ticker.C:
				tableDescription, apiErr = r.DescribeTable(&dynamodb.DescribeTableInput{
					TableName: &tableName,
				}, nil)

				if apiErr != nil {
					if !IsAwsErrorCode(apiErr, dynamodb.ErrCodeResourceNotFoundException) {
						// not found is fine, but anything else is a problem
						out.Err = apiErr
						return
					}
					continue
				}

				if tableDescription.Table != nil {
					switch *tableDescription.Table.TableStatus {
					case dynamodb.TableStatusActive:
						out.DescribeTableOutput = tableDescription
						return
					case dynamodb.TableStatusCreating:
						continue
					default:
						out.Err = fmt.Errorf("table %s is in an invalid state: '%v'", tableName, *tableDescription.Table.TableStatus)
						return
					}
				}
			}
		}
	}()
	return outCh
}

// BackupCompletionOutput used as output for Request.ListenForBackupCompleted
// will hold an error value in Err if Request.ListenForBackupCompleted failed or timed out
type BackupCompletionOutput struct {
	*dynamodb.DescribeBackupOutput
	Err error
}

//ListenForBackupCompleted returns a channel that will be passed a BackupCompletionOutput when listener is triggered
// or encounters an error
func (r *Request) ListenForBackupCompleted(backupArn string) <-chan *BackupCompletionOutput {
	outCh := make(chan *BackupCompletionOutput)

	ticker := time.NewTicker(time.Millisecond * 100)

	go func() {
		var (
			err        error
			backupDesc *dynamodb.DescribeBackupOutput
		)
		out := new(BackupCompletionOutput)
		defer func() {
			outCh <- out
			close(outCh)
		}()
		for {
			select {
			case <-r.ctx.Done():
				out.Err = fmt.Errorf("timed out listening for backup %s completion", backupArn)
			case <-ticker.C:
				backupDesc, err = r.DescribeBackup(&dynamodb.DescribeBackupInput{
					BackupArn: &backupArn,
				}, nil)

				if err != nil {
					if !IsAwsErrorCode(err, dynamodb.ErrCodeBackupNotFoundException) {
						// not found is fine, but anything else is a problem
						out.Err = err
						return
					}
					// not found, backup hasn't been created yet. so keep looping
					continue
				}

				if backupDesc.BackupDescription != nil {
					switch *backupDesc.BackupDescription.BackupDetails.BackupStatus {
					case dynamodb.BackupStatusAvailable:
						// backup is ready
						out.DescribeBackupOutput = backupDesc
						return
					case dynamodb.BackupStatusCreating:
						// backup being create. keep looping...
						continue
					default:
						out.Err = fmt.Errorf("backup %s is in an invalid state: '%v'", backupArn,
							*backupDesc.BackupDescription.BackupDetails.BackupStatus)
						return
					}
				}
			}
		}
	}()
	return outCh
}
