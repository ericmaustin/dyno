package dyno

import (
	"context"
	"fmt"
	"github.com/segmentio/ksuid"
	"sync/atomic"
)

//NewPool creates a new pool with a context Client connection and limit
// the context is used for all Executions
func NewPool(ctx context.Context, limit uint64) *Pool {
	return &Pool{
		ctx:      ctx,
		limit:    limit,
		id:       ksuid.New().String(),
	}
}

// Pool is a batch request handler that spawns a number of workers to handle requests
type Pool struct {
	ctx       context.Context
	dynamoDB  *Client
	limit     uint64
	taskCount uint64
	id        string
}

//Limit returns the Pool limit
func (p *Pool) Limit() uint64 {
	return p.limit
}

//ID returns the Pool id
func (p *Pool) ID() string {
	return p.id
}

//String implements Stringer
func (p *Pool) String() string {
	cnt := p.ActiveCount()
	return fmt.Sprintf("Pool <%s> %d running", p.id, cnt)
}

//addActive adds a cnt to the active process count Pool.active
func (p *Pool) addActive(cnt uint64) {
	atomic.AddUint64(&p.taskCount, cnt)
}

//subActive subtracts a cnt from the active process count Pool.active
func (p *Pool) subActive(cnt uint64) {
	atomic.AddUint64(&p.taskCount, -cnt)
}

//claimSlot waits for an available
func (p *Pool) claimSlot() error {
	readyCh := make(chan struct{})
	go func() {
		for {
			if atomic.LoadUint64(&p.taskCount) < p.limit {
				readyCh <- struct{}{}
				close(readyCh)
				return
			}
		}
	}()
	select {
	case <-readyCh:
		p.addActive(1)
		return nil
	case <-p.ctx.Done():
		return fmt.Errorf("could not claim pool slot: %v", p.ctx.Err())
	}
}

// ActiveCount returns number of active operations
func (p *Pool) ActiveCount() uint64 {
	return atomic.LoadUint64(&p.taskCount)
}

//Do executes a given Operation
func (p *Pool) Do(op Operation) (err error) {
	if err = p.claimSlot(); err != nil {
		return
	}
	go func() {
		defer p.subActive(1)
		op.DynoInvoke(p.ctx)
	}()
	return
}
//
//// Scan executes a scan api call with a ScanInput
//func (p *Pool) Scan(in *ScanInput) *ScanPromise {
//	return &ScanPromise{p.Do(ScanFunc(in))}
//}
//
//// ScanAll executes a scan api call with a dynamodb.ScanInput and keeps scanning until there are no more
//// LastEvaluatedKey values
//func (p *Pool) ScanAll(in *ScanInput) *ScanAllPromise {
//	return &ScanAllPromise{p.Do(ScanAllFunc(in))}
//}
//
////Query executes a query api call with a QueryInput
//func (p *Pool) Query(in *QueryInput) *QueryPromise {
//	return &QueryPromise{p.Do(QueryFunc(in))}
//}
//
//// QueryAll executes a query api call with a QueryInput and keeps querying until there are no more
//// LastEvaluatedKey values
//func (p *Pool) QueryAll(in *QueryInput) *QueryAllPromise {
//	return &QueryAllPromise{p.Do(QueryAllFunc(in))}
//}
//
//// PutItem runs a put item api call with a PutItemInput and returns PutItemPromise
//func (p *Pool) PutItem(in *PutItemInput) *PutItemPromise {
//	return &PutItemPromise{p.Do(PutItemFunc(in))}
//}
//
////GetItem runs a GetItem dynamodb operation with a GetItemInput and returns a GetItemPromise
//func (p *Pool) GetItem(in *GetItemInput) *GetItemPromise {
//	return &GetItemPromise{p.Do(GetItemFunc(in))}
//}
//
//// BatchGetItem runs a batch get item api call with a dynamodb.BatchGetItemBuilder
//func (p *Pool) BatchGetItem(in *BatchGetItemBuilder) *BatchGetItemPromise {
//	return &BatchGetItemPromise{p.Do(BatchGetItemFunc(in))}
//}
//
//// BatchGetItemAll runs a batch get item api call with a dynamodb.BatchGetItemBuilder
////and keeps querying until there are no more UnprocessedKeys
//func (p *Pool) BatchGetItemAll(in *BatchGetItemBuilder) *BatchGetItemAllPromise {
//	return &BatchGetItemAllPromise{p.Do(BatchGetItemAllFunc(in))}
//}
//
//// UpdateItem runs an update item api call with an UpdateItemInput and returns a UpdateItemPromise
//func (p *Pool) UpdateItem(in *UpdateItemInput) *UpdateItemPromise {
//	return &UpdateItemPromise{p.Do(UpdateItemFunc(in))}
//}
//
//// DeleteItem runs an update item api call with an DeleteItemInput and returns a DeleteItemPromise
//func (p *Pool) DeleteItem(in *DeleteItemInput) *DeleteItemPromise {
//	return &DeleteItemPromise{p.Do(DeleteItemFunc(in))}
//}
//
//// BatchWriteItem runs an BatchWriteItem api call with an BatchWriteItemInput and returns a BatchWriteItemPromise
//func (p *Pool) BatchWriteItem(in *BatchWriteItemInput) *BatchWriteItemPromise {
//	return &BatchWriteItemPromise{p.Do(BatchWriteItemFunc(in))}
//}
//
//// BatchWriteItemAll runs an BatchWriteItem api call with an BatchWriteItemInput and returns a BatchWriteItemPromise
//func (p *Pool) BatchWriteItemAll(in *BatchWriteItemInput) *BatchWriteItemAllPromise {
//	return &BatchWriteItemAllPromise{p.Do(BatchWriteItemAllFunc(in))}
//}
//
//// CreateTable runs an CreateTable item api call with an DeleteItemInput and returns a DeleteItemPromise
//func (p *Pool) CreateTable(in *CreateTableBuilder) *CreateTablePromise {
//	return &CreateTablePromise{p.Do(CreateTableFunc(in))}
//}
//
//// DescribeTable runs an DescribeTable item api call with an DescribeTableInput and returns a DescribeTablePromise
//func (p *Pool) DescribeTable(in *DescribeTableInput) *DescribeTablePromise {
//	return &DescribeTablePromise{p.Do(DescribeTableFunc(in))}
//}
//
//// DeleteTable runs an DeleteTable item api call with a DeleteTableInput and returns a DeleteTablePromise
//func (p *Pool) DeleteTable(in *DeleteTableInput) *DeleteTablePromise {
//	return &DeleteTablePromise{p.Do(DeleteTableFunc(in))}
//}
//
//// CreateBackup runs an CreateBackup item api call with a CreateBackupInput and returns a CreateBackupPromise
//func (p *Pool) CreateBackup(in *CreateBackupInput) *CreateBackupPromise {
//	return &CreateBackupPromise{p.Do(CreateBackupFunc(in))}
//}
//
////WaitUntilTableExists runs a WaitUntilTableExistsFunc with a DescribeTableInput and returns a WaitPromise
//func (p *Pool) WaitUntilTableExists(in *DescribeTableInput) *WaitPromise {
//	return &WaitPromise{p.Do(WaitUntilTableExistsFunc(in))}
//}
//
////WaitUntilTableNotExists runs a WaitUntilTableNotExistsFunc that returns a WaitPromise
//func (p *Pool) WaitUntilTableNotExists(in *DescribeTableInput) *WaitPromise {
//	return &WaitPromise{p.Do(WaitUntilTableNotExistsFunc(in))}
//}
//
////WaitUntilBackupExists runs a WaitUntilBackupExists that returns a WaitPromise
//func (p *Pool) WaitUntilBackupExists(in *DescribeBackupInput) *WaitPromise {
//	return &WaitPromise{p.Do(WaitUntilBackupExists(in))}
//}
