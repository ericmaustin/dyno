package dyno

import (
	"context"
	"fmt"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/segmentio/ksuid"
	"sync/atomic"
)

// NewPool creates a new pool with a context DefaultClient connection and limit
// the context is used for all Executions
func NewPool(ctx context.Context, client *ddb.Client, limit uint64) *Pool {
	return &Pool{
		ctx:      ctx,
		client:   client,
		limit:    limit,
		id:       ksuid.New().String(),
	}
}

// Pool is a batch request handler that spawns a number of workers to handle requests
type Pool struct {
	ctx       context.Context
	client    *ddb.Client // dynamoDB is the client
	limit     uint64 // limit is the max number of active tasks that can be running at the same time
	taskCount uint64 // taskCount holds the count of active tasks and uses sync.atomic for getting and setting
	id        string // id allows us to identify pools easily (e.g. for logging purposes)
}

// Limit returns the Pool limit
func (p *Pool) Limit() uint64 {
	return p.limit
}

// ID returns the Pool id
func (p *Pool) ID() string {
	return p.id
}

// String implements Stringer
func (p *Pool) String() string {
	cnt := p.ActiveCount()
	return fmt.Sprintf("Pool <%s> %d running", p.id, cnt)
}

// addActive adds a cnt to the active process countCh Pool.active
func (p *Pool) addActive(cnt uint64) {
	atomic.AddUint64(&p.taskCount, cnt)
}

// subActive subtracts a cnt from the active process countCh Pool.active
func (p *Pool) subActive(cnt uint64) {
	atomic.AddUint64(&p.taskCount, -cnt)
}

// claimSlot waits for an available execution slot
func (p *Pool) claimSlot() error {
	for {
		select {
		case <-p.ctx.Done():
			return fmt.Errorf("could not claim pool slot: %v", p.ctx.Err())
		default:
			if atomic.LoadUint64(&p.taskCount) < p.limit {
				p.addActive(1) // increment the active count since we got a slot
				return nil
			}
		}
	}
}

// ActiveCount returns number of active operations
func (p *Pool) ActiveCount() uint64 {
	return atomic.LoadUint64(&p.taskCount)
}

// Do executes one or more Operations
func (p *Pool) Do(ops ...Operation) (err error) {

	for _, op := range ops {
		if err = p.claimSlot(); err != nil {
			return
		}

		go func(op Operation) {
			defer func() {
				p.subActive(1) // decrement the active count
			}()

			op.DynoInvoke(p.ctx, p.client)
		}(op)
	}

	return
}

// Wait returns a chan that will be passed a struct when the pool has zero active tasks
// useful for when a pool is meant to be short lived with a set number of operations to run in a batch
func (p *Pool) Wait() <-chan struct{} {
	out := make(chan struct{})

	go func(){
		defer func(){
			out <- struct{}{}
			close(out)
		}()

		for {
			select {
			case <-p.ctx.Done():
				return
			default:
				if atomic.LoadUint64(&p.taskCount) == 0 {
					return
				}
			}
		}
	}()

	return out
}


//// ScanAll executes a scan api call with a ScanInput
//func (p *Scan) ScanAll(input *ddb.ScanInput, optFns ...func(*ScanOptions)) ([]*ddb.ScanOutput, error) {
//	opt := NewScanAll(input, optFns...)
//	opt.DynoInvoke(ctx, c.ddb)
//
//	return opt.Await()
//}
