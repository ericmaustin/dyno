package dyno

import (
	"context"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ericmaustin/dyno/timer"
	"github.com/google/uuid"
	"sync"
	"time"
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
	if _, ok := r.ctx.Deadline(); !ok {
		// no deadline set, so use max timeout
		ctx, cancel = context.WithTimeout(r.Context(), r.MaxTimeout())
	} else {
		r.mu.RLock()
		ctx, cancel = r.ctx, r.cancel
		r.mu.RUnlock()
	}
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
func (r *Request) Scan(in *dynamodb.ScanInput) (out *dynamodb.ScanOutput, err error) {
	err = r.Execute(func(ctx context.Context) error {
		out, err = r.DynamoClient().ScanWithContext(ctx, in)
		return err
	})
	return
}

// Query executes a query api call with a dynamodb.QueryInput
func (r *Request) Query(in *dynamodb.QueryInput) (out *dynamodb.QueryOutput, err error) {
	err = r.Execute(func(ctx context.Context) error {
		out, err = r.DynamoClient().QueryWithContext(ctx, in)
		return err
	})
	return
}

// PutItem runs a put item api call with a dynamodb.PutItemInput
func (r *Request) PutItem(in *dynamodb.PutItemInput) (out *dynamodb.PutItemOutput, err error) {
	err = r.Execute(func(ctx context.Context) error {
		out, err = r.DynamoClient().PutItemWithContext(ctx, in)
		return err
	})
	return
}

//GetItem runs a GetItem dynamodb operation with a dynamodb.GetItemInput
func (r *Request) GetItem(in *dynamodb.GetItemInput) (out *dynamodb.GetItemOutput, err error) {
	err = r.Execute(func(ctx context.Context) error {
		out, err = r.DynamoClient().GetItemWithContext(ctx, in)
		return err
	})
	return
}

// UpdateItem runs an update item api call with a dynamodb.UpdateItemInput
func (r *Request) UpdateItem(in *dynamodb.UpdateItemInput) (out *dynamodb.UpdateItemOutput, err error) {
	err = r.Execute(func(ctx context.Context) error {
		out, err = r.DynamoClient().UpdateItemWithContext(ctx, in)
		return err
	})
	return
}

// DeleteItem runs a delete item api call with a dynamodb.DeleteItemInput
func (r *Request) DeleteItem(in *dynamodb.DeleteItemInput) (out *dynamodb.DeleteItemOutput, err error) {
	err = r.Execute(func(ctx context.Context) error {
		out, err = r.DynamoClient().DeleteItemWithContext(ctx, in)
		return err
	})
	return
}

// BatchGetItem runs a batch get item api call with a dynamodb.BatchGetItemInput
func (r *Request) BatchGetItem(in *dynamodb.BatchGetItemInput) (out *dynamodb.BatchGetItemOutput, err error) {
	err = r.Execute(func(ctx context.Context) error {
		out, err = r.DynamoClient().BatchGetItemWithContext(ctx, in)
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
func (r *Request) CreateTable(in *dynamodb.CreateTableInput) (out *dynamodb.CreateTableOutput, err error) {
	err = r.Execute(func(ctx context.Context) error {
		out, err = r.DynamoClient().CreateTableWithContext(ctx, in)
		return err
	})
	return
}

//CreateGlobalTable runs a create global table api call with a dynamodb.CreateGlobalTableInput
func (r *Request) CreateGlobalTable(in *dynamodb.CreateGlobalTableInput) (out *dynamodb.CreateGlobalTableOutput, err error) {
	err = r.Execute(func(ctx context.Context) error {
		out, err = r.DynamoClient().CreateGlobalTableWithContext(ctx, in)
		return err
	})
	return
}

//DescribeTable runs a describe table api call with a dynamodb.DescribeTableInput
func (r *Request) DescribeTable(in *dynamodb.DescribeTableInput) (out *dynamodb.DescribeTableOutput, err error) {
	err = r.Execute(func(ctx context.Context) error {
		out, err = r.DynamoClient().DescribeTableWithContext(ctx, in)
		return err
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
func (r *Request) CreateBackup(in *dynamodb.CreateBackupInput) (out *dynamodb.CreateBackupOutput, err error) {
	err = r.Execute(func(ctx context.Context) error {
		out, err = r.DynamoClient().CreateBackupWithContext(ctx, in)
		return err
	})
	return
}

//DescribeBackup runs a describe table backup api call with a dynamodb.DescribeBackupInput
func (r *Request) DescribeBackup(in *dynamodb.DescribeBackupInput) (out *dynamodb.DescribeBackupOutput, err error) {
	err = r.Execute(func(ctx context.Context) error {
		out, err = r.DynamoClient().DescribeBackupWithContext(ctx, in)
		return err
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
func (r *Request) DeleteTable(in *dynamodb.DeleteTableInput) (out *dynamodb.DeleteTableOutput, err error) {
	err = r.Execute(func(ctx context.Context) error {
		out, err = r.DynamoClient().DeleteTableWithContext(ctx, in)
		return err
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
