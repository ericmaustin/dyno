package dyno

import (
	"github.com/segmentio/ksuid"
	"sync"
	"sync/atomic"
	"time"
)

//Promise represents a request promise
// a Promise is returned from any DefaultClient Execution
// use <-Promise.Done() to wait for a promise to be done
// use Promise.Await() to wait for and receive the output from a promise
type Promise struct {
	id        string
	mu        sync.RWMutex
	startTime time.Time
	endTime   time.Time
	flag      uint32
	value     interface{}
	err       error
	doneCh    chan struct{}
}

//NewPromise creates a new Promise with a startTime of Now
func NewPromise() *Promise {
	return &Promise{
		id:        ksuid.New().String(),
	}
}

func (p *Promise) Start() {
	p.mu.Lock()
	p.startTime = time.Now()
	p.mu.Unlock()
}

// Done returns a channel that will emit a struct{}{} when Promise contains a result, an error is encountered,
// or context was cancelled
func (p *Promise) Done() <-chan struct{} {
	p.mu.Lock()

	if p.doneCh == nil {

		p.doneCh = make(chan struct{})

		go func() {
			defer func() {
				p.doneCh <- struct{}{}
				close(p.doneCh)
			}()

			for {
				if atomic.LoadUint32(&p.flag) > 0 {
					// done!
					return
				}
			}
		}()
	}

	p.mu.Unlock()

	return p.doneCh
}

func (p *Promise) ID() string {
	return p.id
}

// SetResponse sets the response value interface and error
// and sets the flag to the done state
func (p *Promise) SetResponse(val interface{}, err error) {
	p.mu.Lock()
	p.value = val
	p.err = err
	p.endTime = time.Now()
	p.mu.Unlock()
	atomic.StoreUint32(&p.flag, 1)
}

// Await waits for the Promise's result to complete or for the context to be cancelled
// whichever comes first
func (p *Promise) Await() (interface{}, error) {
	<-p.Done()
	p.mu.Lock()
	err := p.err
	out := p.value
	p.mu.Unlock()

	return out, err
}

// Duration returns the duration of the promise execution
func (p *Promise) Duration() time.Duration {
	p.mu.Lock()

	defer p.mu.Unlock()

	if p.endTime.IsZero() {
		return time.Since(p.startTime)
	}

	return p.endTime.Sub(p.startTime)
}
//
////ScanPromise represents a Scan Promise
//type ScanPromise struct {
//	*Promise
//}
//
////Await returns the dynamodb.ScanOutput and error when the output is ready
//func (p *ScanPromise) Await() (*dynamodb.ScanOutput, error) {
//	out, err := p.Promise.Await()
//	if out == nil {
//		return nil, err
//	}
//	return out.(*dynamodb.ScanOutput), err
//}
//
////ScanAllPromise represents a ScanAllPromise Promise
//type ScanAllPromise struct {
//	*Promise
//}
//
////Await returns the dynamodb.ScanOutput and error when the output is ready
//func (p *ScanAllPromise) Await() ([]*dynamodb.ScanOutput, error) {
//	out, err := p.Promise.Await()
//	if out == nil {
//		return nil, err
//	}
//	return out.([]*dynamodb.ScanOutput), err
//}
//
////QueryAllPromise represents a ScanAllPromise Promise
//type QueryAllPromise struct {
//	*Promise
//}
//
////Await returns the dynamodb.QueryOutput and error when the output is ready
//func (p *QueryAllPromise) Await() ([]*dynamodb.QueryOutput, error) {
//	out, err := p.Promise.Await()
//
//	if out == nil {
//		return nil, err
//	}
//
//	return out.([]*dynamodb.QueryOutput), err
//}
//
////PutItemPromise represents a PutItem Promise
//type PutItemPromise struct {
//	*Promise
//}
//
////Await returns the dynamodb.QueryPromise and error when the output is ready
//func (promise *PutItemPromise) Await() (*dynamodb.PutItemOutput, error) {
//	out, err := promise.Promise.Await()
//	if out == nil {
//		return nil, err
//	}
//	return out.(*dynamodb.PutItemOutput), err
//}
//
////GetItemPromise represents a GetItem Promise
//type GetItemPromise struct {
//	*Promise
//}
//
////Await returns the dynamodb.QueryPromise and error when the output is ready
//func (p *GetItemPromise) Await() (*dynamodb.GetItemOutput, error) {
//	out, err := p.Promise.Await()
//	if out == nil {
//		return nil, err
//	}
//	return out.(*dynamodb.GetItemOutput), err
//}
//
////UpdateItemPromise represents an UpdateItem Promise
//type UpdateItemPromise struct {
//	*Promise
//}
//
////Await returns the dynamodb.QueryPromise and error when the output is ready
//func (p *UpdateItemPromise) Await() (*dynamodb.UpdateItemOutput, error) {
//	out, err := p.Promise.Await()
//	if out == nil {
//		return nil, err
//	}
//	return out.(*dynamodb.UpdateItemOutput), err
//}
//
////DeleteItemPromise represents an DeleteItem Promise
//type DeleteItemPromise struct {
//	*Promise
//}
//
////Await returns the dynamodb.QueryPromise and error when the output is ready
//func (p *DeleteItemPromise) Await() (*dynamodb.DeleteItemOutput, error) {
//	out, err := p.Promise.Await()
//	if out == nil {
//		return nil, err
//	}
//	return out.(*dynamodb.DeleteItemOutput), err
//}
//
////BatchGetItemPromise represents an BatchGetItem Promise
//type BatchGetItemPromise struct {
//	*Promise
//}
//
////Await returns the dynamodb.QueryPromise and error when the output is ready
//func (p *BatchGetItemPromise) Await() (*dynamodb.BatchGetItemOutput, error) {
//	out, err := p.Promise.Await()
//	if out == nil {
//		return nil, err
//	}
//	return out.(*dynamodb.BatchGetItemOutput), err
//}
//
////BatchGetItemAllPromise represents an BatchGetItemAllFunc Promise
//type BatchGetItemAllPromise struct {
//	*Promise
//}
//
////Await returns the dynamodb.QueryPromise and error when the output is ready
//func (promise *BatchGetItemAllPromise) Await() ([]*dynamodb.BatchGetItemOutput, error) {
//	out, err := promise.Promise.Await()
//	if out == nil {
//		return nil, err
//	}
//	return out.([]*dynamodb.BatchGetItemOutput), err
//}
//
////BatchWriteItemPromise represents a dynamodb.BatchWriteItemPromise and error
//type BatchWriteItemPromise struct {
//	*Promise
//}
//
////Await returns the dynamodb.QueryPromise and error when the output is ready
//func (p *BatchWriteItemPromise) Await() (*dynamodb.BatchWriteItemOutput, error) {
//	out, err := p.Promise.Await()
//	if out == nil {
//		return nil, err
//	}
//	return out.(*dynamodb.BatchWriteItemOutput), err
//}
//
////BatchWriteItemAllPromise represents a dynamodb.BatchWriteItemPromise and error
//type BatchWriteItemAllPromise struct {
//	*Promise
//}
//
////Await returns the dynamodb.QueryPromise and error when the output is ready
//func (p *BatchWriteItemAllPromise) Await() ([]*dynamodb.BatchWriteItemOutput, error) {
//	out, err := p.Promise.Await()
//	if out == nil {
//		return nil, err
//	}
//	return out.([]*dynamodb.BatchWriteItemOutput), err
//}
//
////CreateTablePromise represents a dynamodb.CreateTablePromise and error
//type CreateTablePromise struct {
//	*Promise
//}
//
////Await returns the dynamodb.QueryPromise and error when the output is ready
//func (p *CreateTablePromise) Await() (*dynamodb.CreateTableOutput, error) {
//	out, err := p.Promise.Await()
//	if out == nil {
//		return nil, err
//	}
//	return out.(*dynamodb.CreateTableOutput), err
//}
//
////CreateGlobalTablePromise represents a dynamodb.CreateTablePromise and error
//type CreateGlobalTablePromise struct {
//	*Promise
//}
//
////Await returns the dynamodb.CreateGlobalTableOutput and error when the output is ready
//func (p *CreateGlobalTablePromise) Await() (*dynamodb.CreateGlobalTableOutput, error) {
//	out, err := p.Promise.Await()
//	if out == nil {
//		return nil, err
//	}
//	return out.(*dynamodb.CreateGlobalTableOutput), err
//}
//
////UpdateTablePromise represents a dynamodb.UpdateTable and error
//type UpdateTablePromise struct {
//	*Promise
//}
//
////Await returns the dynamodb.UpdateTableOutput and error when the output is ready
//func (p *UpdateTablePromise) Await() (*dynamodb.UpdateTableOutput, error) {
//	out, err := p.Promise.Await()
//	if out == nil {
//		return nil, err
//	}
//	return out.(*dynamodb.UpdateTableOutput), err
//}
//
////ListTablePromise represents a dynamodb.ListTableOutput and error
//type ListTablePromise struct {
//	*Promise
//}
//
////Await returns the dynamodb.ListTablesOutput and error when the output is ready
//func (p *ListTablePromise) Await() (*dynamodb.ListTablesOutput, error) {
//	out, err := p.Promise.Await()
//	if out == nil {
//		return nil, err
//	}
//	return out.(*dynamodb.ListTablesOutput), err
//}
//
////CreateBackupPromise represents a dynamodb.CreateBackupOutput and error
//type CreateBackupPromise struct {
//	*Promise
//}
//
////Await returns the dynamodb.CreateBackupOutput and error when the output is ready
//func (p *CreateBackupPromise) Await() (*dynamodb.CreateBackupOutput, error) {
//	out, err := p.Promise.Await()
//	if out == nil {
//		return nil, err
//	}
//	return out.(*dynamodb.CreateBackupOutput), err
//}
//
////DescribeBackupPromise represents a dynamodb.DescribeBackupOutput and error
//type DescribeBackupPromise struct {
//	*Promise
//}
//
////Await returns the dynamodb.DescribeBackupOutput and error when the output is ready
//func (p *DescribeBackupPromise) Await() (*dynamodb.DescribeBackupOutput, error) {
//	out, err := p.Promise.Await()
//	if out == nil {
//		return nil, err
//	}
//	return out.(*dynamodb.DescribeBackupOutput), err
//}
//
////ListBackupsPromise represents a dynamodb.ListBackupsOutput and error
//type ListBackupsPromise struct {
//	*Promise
//}
//
////Await returns the dynamodb.ListBackupsOutput and error when the output is ready
//func (p *ListBackupsPromise) Await() (*dynamodb.ListBackupsOutput, error) {
//	out, err := p.Promise.Await()
//	if out == nil {
//		return nil, err
//	}
//	return out.(*dynamodb.ListBackupsOutput), err
//}
//
////RestoreTableFromBackupPromise represents a dynamodb.RestoreTableFromBackupOutput and error
//type RestoreTableFromBackupPromise struct {
//	*Promise
//}
//
////Await returns the dynamodb.v and error when the output is ready
//func (p *RestoreTableFromBackupPromise) Await() (*dynamodb.RestoreTableFromBackupOutput, error) {
//	out, err := p.Promise.Await()
//	if out == nil {
//		return nil, err
//	}
//	return out.(*dynamodb.RestoreTableFromBackupOutput), err
//}
//
////QueryPromise represents a Query Promise
//type QueryPromise struct {
//	*Promise
//}
//
////Await returns the dynamodb.QueryOutput and error when the output is ready
//func (p *QueryPromise) Await() (*dynamodb.QueryOutput, error) {
//	out, err := p.Promise.Await()
//	if out == nil {
//		return nil, err
//	}
//	return out.(*dynamodb.QueryOutput), err
//}
//
////DescribeTablePromise represents a DescribeTable Promise
//type DescribeTablePromise struct {
//	*Promise
//}
//
////Await returns the dynamodb.QueryOutput and error when the output is ready
//func (p *DescribeTablePromise) Await() (*dynamodb.DescribeTableOutput, error) {
//	out, err := p.Promise.Await()
//	if out == nil {
//		return nil, err
//	}
//	return out.(*dynamodb.DescribeTableOutput), err
//}
//
////DeleteTablePromise represents a DeleteTable Promise
//type DeleteTablePromise struct {
//	*Promise
//}
//
////Await returns the dynamodb.QueryOutput and error when the output is ready
//func (p *DeleteTablePromise) Await() (*dynamodb.DeleteTableOutput, error) {
//	out, err := p.Promise.Await()
//	if out == nil {
//		return nil, err
//	}
//	return out.(*dynamodb.DeleteTableOutput), err
//}
//
////WaitPromise represents a promise for all Wait operations
//type WaitPromise struct {
//	*Promise
//}
//
////Await returns the dynamodb.QueryOutput and error when the output is ready
//func (p *WaitPromise) Await() error {
//	_, err := p.Promise.Await()
//	return err
//}
