package dyno

import (
	"context"
	"errors"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ericmaustin/dyno/timer"
	"sync"
	"time"
)

// DescribeTable executes DescribeTable operation and returns a DescribeTablePromise
func (c *Client) DescribeTable(ctx context.Context, input *ddb.DescribeTableInput, mw ...DescribeTableMiddleWare) *DescribeTablePromise {
	return NewDescribeTable(input, mw...).Invoke(ctx, c.ddb)
}

// TableExistsWaiter executes TableExistsWaiter operation and returns a TableWaiterPromise
func (c *Client) TableExistsWaiter(ctx context.Context, input *ddb.DescribeTableInput, mw ...DescribeTableMiddleWare) *DescribeTablePromise {
	return NewTableExistsWaiter(input, mw...).Invoke(ctx, c.ddb)
}

// TableNotExistsWaiter executes TableNotExistsWaiter operation and returns a TableNotExistsWaiterPromise
func (c *Client) TableNotExistsWaiter(ctx context.Context, input *ddb.DescribeTableInput, mw ...DescribeTableMiddleWare) *DescribeTablePromise {
	return NewTableNotExistsWaiter(input, mw...).Invoke(ctx, c.ddb)
}

// DescribeTable executes a DescribeTable operation with a DescribeTableInput in this pool and returns the DescribeTablePromise
func (p *Pool) DescribeTable(input *ddb.DescribeTableInput, mw ...DescribeTableMiddleWare) *DescribeTablePromise {
	op := NewDescribeTable(input, mw...)

	if err := p.Do(op); err != nil {
		op.promise.SetResponse(nil, err)
	}

	return op.promise
}

// TableExistsWaiter executes a TableExistsWaiter operation with a TableExistsWaiterInput in this pool and returns the TableWaiterPromise
func (p *Pool) TableExistsWaiter(input *ddb.DescribeTableInput, mw ...DescribeTableMiddleWare) *DescribeTablePromise {
	op := NewTableExistsWaiter(input, mw...)

	if err := p.Do(op); err != nil {
		op.promise.SetResponse(nil, err)
	}

	return op.promise
}

// TableNotExistsWaiter executes a TableNotExistsWaiter operation with a TableNotExistsWaiterInput in this pool and returns the TableNotExistsWaiterPromise
func (p *Pool) TableNotExistsWaiter(input *ddb.DescribeTableInput, mw ...DescribeTableMiddleWare) *DescribeTablePromise {
	op := NewTableNotExistsWaiter(input, mw...)

	if err := p.Do(op); err != nil {
		op.promise.SetResponse(nil, err)
	}

	return op.promise
}

// DescribeTableContext represents an exhaustive DescribeTable operation request context
type DescribeTableContext struct {
	context.Context
	input  *ddb.DescribeTableInput
	client *ddb.Client
}

// DescribeTableOutput represents the output for the DescribeTable opration
type DescribeTableOutput struct {
	out *ddb.DescribeTableOutput
	err error
	mu sync.RWMutex
}

// Set sets the output
func (o *DescribeTableOutput) Set(out *ddb.DescribeTableOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *DescribeTableOutput) Get() (out *ddb.DescribeTableOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()
	return
}

// DescribeTablePromise represents a promise for the DescribeTable
type DescribeTablePromise struct {
	*Promise
}

// GetResponse returns the GetResponse output and error
// if Output has not been set yet nil is returned
func (p *DescribeTablePromise) GetResponse() (*ddb.DescribeTableOutput, error) {
	out, err := p.Promise.GetResponse()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.DescribeTableOutput), err
}

// Await waits for the DescribeTablePromise to be fulfilled and then returns a DescribeTableOutput and error
func (p *DescribeTablePromise) Await() (*ddb.DescribeTableOutput, error) {
	out, err := p.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.DescribeTableOutput), err
}

// newDescribeTablePromise returns a new DescribeTablePromise
func newDescribeTablePromise() *DescribeTablePromise {
	return &DescribeTablePromise{NewPromise()}
}

// DescribeTableHandler represents a handler for DescribeTable requests
type DescribeTableHandler interface {
	HandleDescribeTable(ctx *DescribeTableContext, output *DescribeTableOutput)
}

// DescribeTableHandlerFunc is a DescribeTableHandler function
type DescribeTableHandlerFunc func(ctx *DescribeTableContext, output *DescribeTableOutput)

// HandleDescribeTable implements DescribeTableHandler
func (h DescribeTableHandlerFunc) HandleDescribeTable(ctx *DescribeTableContext, output *DescribeTableOutput) {
	h(ctx, output)
}

// DescribeTableMiddleWare is a middleware function use for wrapping DescribeTableHandler requests
type DescribeTableMiddleWare interface {
	DescribeTableMiddleWare(next DescribeTableHandler) DescribeTableHandler
}

// DescribeTableMiddleWareFunc is a functional DescribeTableMiddleWare
type DescribeTableMiddleWareFunc func(next DescribeTableHandler) DescribeTableHandler

// DescribeTableMiddleWare implements the DescribeTableMiddleWare interface
func (mw DescribeTableMiddleWareFunc) DescribeTableMiddleWare(next DescribeTableHandler) DescribeTableHandler {
	return mw(next)
}

// DescribeTableFinalHandler is the final DescribeTableHandler that executes a dynamodb DescribeTable operation
type DescribeTableFinalHandler struct {}

// HandleDescribeTable implements the DescribeTableHandler
func (h *DescribeTableFinalHandler) HandleDescribeTable(ctx *DescribeTableContext, output *DescribeTableOutput) {
	output.Set(ctx.client.DescribeTable(ctx, ctx.input))
}

// DescribeTable represents a DescribeTable operation
type DescribeTable struct {
	promise     *DescribeTablePromise
	input       *ddb.DescribeTableInput
	middleWares []DescribeTableMiddleWare
}

// NewDescribeTable creates a new DescribeTable
func NewDescribeTable(input *ddb.DescribeTableInput, mws ...DescribeTableMiddleWare) *DescribeTable {
	return &DescribeTable{
		input:       input,
		middleWares: mws,
		promise:     newDescribeTablePromise(),
	}
}

// Invoke invokes the DescribeTable operation and returns a DescribeTablePromise
func (op *DescribeTable) Invoke(ctx context.Context, client *ddb.Client) *DescribeTablePromise {
	go op.DynoInvoke(ctx, client)

	return op.promise
}

// DynoInvoke implements the Operation interface
func (op *DescribeTable) DynoInvoke(ctx context.Context, client *ddb.Client) {
	invokeDescribeTableWithHandler(ctx, client, op.input, new(DescribeTableFinalHandler), op.middleWares, op.promise)
}

// TableExistsWaiterFinalHandler is the final TableWaiterHandler that executes a dynamodb TableExistsWaiter operation
type TableExistsWaiterFinalHandler struct {}

// HandleDescribeTable implements the DescribeTableHandler
func (h *TableExistsWaiterFinalHandler) HandleDescribeTable(ctx *DescribeTableContext, output *DescribeTableOutput) {
	var (
		out     *ddb.DescribeTableOutput
		sleeper *timer.Sleeper
		retry   bool
		err     error
	)

	defer func() { output.Set(out, err) }()


	sleeper = timer.NewLinearSleeper(time.Millisecond * 100, 2).WithContext(ctx)

	for {
		out, err = ctx.client.DescribeTable(ctx, ctx.input)
		retry, err = tableExistsRetryState(out, err)

		if !retry || err != nil  {
			return
		}

		if err = <-sleeper.Sleep(); err != nil {
			return
		}
	}
}

// TableExistsWaiter represents an operation that waits for a table to exist
type TableExistsWaiter struct {
	promise     *DescribeTablePromise
	input       *ddb.DescribeTableInput
	middleWares []DescribeTableMiddleWare
}

// NewTableExistsWaiter creates a new TableExistsWaiter operation on the given client with a given DescribeTableInput and options
func NewTableExistsWaiter(input *ddb.DescribeTableInput, mws ...DescribeTableMiddleWare) *TableExistsWaiter {
	return &TableExistsWaiter{
		input:       input,
		middleWares: mws,
		promise:     newDescribeTablePromise(),
	}
}

// Invoke invokes the TableExistsWaiter operation
func (op *TableExistsWaiter) Invoke(ctx context.Context, client *ddb.Client) *DescribeTablePromise {
	go op.DynoInvoke(ctx, client)
	
	return op.promise
}

// DynoInvoke implements the Operation interface
func (op *TableExistsWaiter) DynoInvoke(ctx context.Context, client *ddb.Client) {
	invokeDescribeTableWithHandler(ctx, client, op.input, new(TableExistsWaiterFinalHandler), op.middleWares, op.promise)
}

// TableNotExistsWaiterFinalHandler is the final TableNotExistsWaiterHandler that executes a dynamodb TableNotExistsWaiter operation
type TableNotExistsWaiterFinalHandler struct {}

// HandleDescribeTable implements the DescribeTableHandler
func (h *TableNotExistsWaiterFinalHandler) HandleDescribeTable(ctx *DescribeTableContext, output *DescribeTableOutput) {
	var (
		out     *ddb.DescribeTableOutput
		sleeper *timer.Sleeper
		retry   bool
		err     error
	)

	defer func() { output.Set(out, err) }()

	sleeper = timer.NewLinearSleeper(time.Millisecond * 100, 2).WithContext(ctx)

	for {
		out, err = ctx.client.DescribeTable(ctx, ctx.input)
		retry, err = tableNotExistsRetryState(out, err)

		if !retry || err != nil {
			return
		}

		if err = <-sleeper.Sleep(); err != nil {
			return
		}
	}
}


// TableNotExistsWaiter represents an operation that waits for a table to exist
type TableNotExistsWaiter struct {
	promise     *DescribeTablePromise
	input       *ddb.DescribeTableInput
	middleWares []DescribeTableMiddleWare
}

// NewTableNotExistsWaiter creates a new TableNotExistsWaiter operation on the given client with a given DescribeTableInput and options
func NewTableNotExistsWaiter(input *ddb.DescribeTableInput, mws ...DescribeTableMiddleWare) *TableNotExistsWaiter {
	return &TableNotExistsWaiter{
		input:       input,
		middleWares: mws,
		promise:     newDescribeTablePromise(),
	}
}

// Invoke invokes the TableNotExistsWaiter operation
func (op *TableNotExistsWaiter) Invoke(ctx context.Context, client *ddb.Client) *DescribeTablePromise {
	go op.DynoInvoke(ctx, client)

	return op.promise
}

// DynoInvoke implements the Operation interface
func (op *TableNotExistsWaiter) DynoInvoke(ctx context.Context, client *ddb.Client) {
	invokeDescribeTableWithHandler(ctx, client, op.input, new(TableNotExistsWaiterFinalHandler), op.middleWares, op.promise)
}
// invokeDescribeTableWithHandler invokes a describe table operation with a specific handler
func invokeDescribeTableWithHandler(ctx context.Context, client *ddb.Client, input *ddb.DescribeTableInput, handler DescribeTableHandler, mws []DescribeTableMiddleWare, promise *DescribeTablePromise) {
	output := new(DescribeTableOutput)

	defer func() { promise.SetResponse(output.Get()) }()

	requestCtx := &DescribeTableContext{
		Context: ctx,
		client:  client,
		input:   input,
	}

	// no middlewares
	if len(mws) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(mws) - 1; i >= 0; i-- {
			handler = mws[i].DescribeTableMiddleWare(handler)
		}
	}

	handler.HandleDescribeTable(requestCtx, output)
}

// NewDescribeTableInput creates a new DescribeTableInput
func NewDescribeTableInput(tableName *string) *ddb.DescribeTableInput {
	return &ddb.DescribeTableInput{TableName: tableName}
}

func tableExistsRetryState(output *ddb.DescribeTableOutput, err error) (bool, error) {
	if err != nil {
		var resourceNotFound *types.ResourceNotFoundException
		if errors.As(err, &resourceNotFound) {
			// not found, retry
			return true, nil
		}
		// unexpected error
		return false, err
	}

	if output == nil || output.Table == nil {
		return false, errors.New("table output is nil")
	}

	switch output.Table.TableStatus {
	case types.TableStatusActive:
		return false, nil
	case types.TableStatusCreating, types.TableStatusUpdating:
		return true, nil
	}

	return false, errors.New("table is in an invalid state")
}

func tableNotExistsRetryState(output *ddb.DescribeTableOutput, err error) (bool, error) {
	if err != nil {
		var resourceNotFound *types.ResourceNotFoundException
		if errors.As(err, &resourceNotFound) {
			// not found is what we want to get
			return false, nil
		}
		// unexpected error
		return false, err
	}

	if output == nil || output.Table == nil {
		return false, errors.New("table output is nil")
	}

	switch output.Table.TableStatus {
	case types.TableStatusDeleting, types.TableStatusActive:
		// still deleting or have yet to delete
		return true, nil
	}

	return false, errors.New("table is in an invalid state")
}
