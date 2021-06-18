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
		ctx:   ctx,
		limit: limit,
		id:    ksuid.New().String(),
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
