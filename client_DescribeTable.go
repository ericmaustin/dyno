package dyno

import (
	"context"
	"errors"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ericmaustin/dyno/timer"
	"time"
)

// DescribeTable executes DescribeTable operation and returns a DescribeTablePromise
func (c *Client) DescribeTable(ctx context.Context, input *ddb.DescribeTableInput, mw ...DescribeTableMiddleWare) *DescribeTablePromise {
	return NewDescribeTable(input, mw...).Invoke(ctx, c.ddb)
}

// TableExistsWaiter executes TableExistsWaiter operation and returns a TableExistsWaiterPromise
func (c *Client) TableExistsWaiter(ctx context.Context, input *ddb.DescribeTableInput, mw ...TableExistsWaiterMiddleWare) *TableExistsWaiterPromise {
	return NewTableExistsWaiter(input, mw...).Invoke(ctx, c.ddb)
}

// TableNotExistsWaiter executes TableNotExistsWaiter operation and returns a TableNotExistsWaiterPromise
func (c *Client) TableNotExistsWaiter(ctx context.Context, input *ddb.DescribeTableInput, mw ...TableNotExistsWaiterMiddleWare) *TableNotExistsWaiterPromise {
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

// TableExistsWaiter executes a TableExistsWaiter operation with a TableExistsWaiterInput in this pool and returns the TableExistsWaiterPromise
func (p *Pool) TableExistsWaiter(input *ddb.DescribeTableInput, mw ...TableExistsWaiterMiddleWare) *TableExistsWaiterPromise {
	op := NewTableExistsWaiter(input, mw...)

	if err := p.Do(op); err != nil {
		op.promise.SetResponse(nil, err)
	}

	return op.promise
}

// TableNotExistsWaiter executes a TableNotExistsWaiter operation with a TableNotExistsWaiterInput in this pool and returns the TableNotExistsWaiterPromise
func (p *Pool) TableNotExistsWaiter(input *ddb.DescribeTableInput, mw ...TableNotExistsWaiterMiddleWare) *TableNotExistsWaiterPromise {
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
	HandleDescribeTable(ctx *DescribeTableContext, promise *DescribeTablePromise)
}

// DescribeTableHandlerFunc is a DescribeTableHandler function
type DescribeTableHandlerFunc func(ctx *DescribeTableContext, promise *DescribeTablePromise)

// HandleDescribeTable implements DescribeTableHandler
func (h DescribeTableHandlerFunc) HandleDescribeTable(ctx *DescribeTableContext, promise *DescribeTablePromise) {
	h(ctx, promise)
}

// DescribeTableMiddleWare is a middleware function use for wrapping DescribeTableHandler requests
type DescribeTableMiddleWare func(next DescribeTableHandler) DescribeTableHandler

// DescribeTableFinalHandler returns the final DescribeTableHandler that executes a dynamodb DescribeTable operation
func DescribeTableFinalHandler() DescribeTableHandler {
	return DescribeTableHandlerFunc(func(ctx *DescribeTableContext, promise *DescribeTablePromise) {
		promise.SetResponse(ctx.client.DescribeTable(ctx, ctx.input))
	})
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

	requestCtx := &DescribeTableContext{
		Context: ctx,
		client:  client,
		input:   op.input,
	}

	h := DescribeTableFinalHandler()

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i](h)
		}
	}

	h.HandleDescribeTable(requestCtx, op.promise)
}


// TableExistsWaiterPromise represents a promise for the TableExistsWaiter
type TableExistsWaiterPromise struct {
	*Promise
}

// GetResponse returns the GetResponse output and error
// if Output has not been set yet nil is returned
func (p *TableExistsWaiterPromise) GetResponse() (*ddb.DescribeTableOutput, error) {
	out, err := p.Promise.GetResponse()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.DescribeTableOutput), err
}

// Await waits for the DescribeTablePromise to be fulfilled and then returns a DescribeTableOutput and error
func (p *TableExistsWaiterPromise) Await() (*ddb.DescribeTableOutput, error) {
	out, err := p.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.DescribeTableOutput), err
}

// newTableExistsWaiterPromise returns a new TableExistsWaiterPromise
func newTableExistsWaiterPromise() *TableExistsWaiterPromise {
	return &TableExistsWaiterPromise{NewPromise()}
}

// TableExistsWaiterHandler represents a handler for TableExistsWaiter requests
type TableExistsWaiterHandler interface {
	HandleTableExistsWaiter(ctx *DescribeTableContext, promise *TableExistsWaiterPromise)
}

// TableExistsWaiterHandlerFunc is a TableExistsWaiterHandler function
type TableExistsWaiterHandlerFunc func(ctx *DescribeTableContext, promise *TableExistsWaiterPromise)

// HandleTableExistsWaiter implements TableExistsWaiterHandler
func (h TableExistsWaiterHandlerFunc) HandleTableExistsWaiter(ctx *DescribeTableContext, promise *TableExistsWaiterPromise) {
	h(ctx, promise)
}

// TableExistsWaiterMiddleWare is a middleware function use for wrapping TableExistsWaiterHandler requests
type TableExistsWaiterMiddleWare func(handler TableExistsWaiterHandler) TableExistsWaiterHandler

// TableExistsWaiterFinalHandler returns the final TableExistsWaiterHandler that executes a dynamodb TableExistsWaiter operation
func TableExistsWaiterFinalHandler() TableExistsWaiterHandler {
	return TableExistsWaiterHandlerFunc(func(ctx *DescribeTableContext, promise *TableExistsWaiterPromise) {
		var (
			out     *ddb.DescribeTableOutput
			sleeper *timer.Sleeper
			retry   bool
			err     error
		)

		defer func() { promise.SetResponse(out, err) }()


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
	})
}

// TableExistsWaiter represents an operation that waits for a table to exist
type TableExistsWaiter struct {
	promise     *TableExistsWaiterPromise
	input       *ddb.DescribeTableInput
	middleWares []TableExistsWaiterMiddleWare
}

// NewTableExistsWaiter creates a new TableExistsWaiter operation on the given client with a given DescribeTableInput and options
func NewTableExistsWaiter(input *ddb.DescribeTableInput, mws ...TableExistsWaiterMiddleWare) *TableExistsWaiter {
	return &TableExistsWaiter{
		input:       input,
		middleWares: mws,
		promise:     newTableExistsWaiterPromise(),
	}
}

// Invoke invokes the TableExistsWaiter operation
func (op *TableExistsWaiter) Invoke(ctx context.Context, client *ddb.Client) *TableExistsWaiterPromise {
	go op.DynoInvoke(ctx, client)
	
	return op.promise
}

// DynoInvoke implements the Operation interface
func (op *TableExistsWaiter) DynoInvoke(ctx context.Context, client *ddb.Client) {
	requestCtx := &DescribeTableContext{
		Context: ctx,
		client:  client,
		input:   op.input,
	}

	h := TableExistsWaiterFinalHandler()

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i](h)
		}
	}

	h.HandleTableExistsWaiter(requestCtx, op.promise)
}


// TableNotExistsWaiterPromise represents a promise for the TableNotExistsWaiter
type TableNotExistsWaiterPromise struct {
	*Promise
}

// Await waits for the DescribeTablePromise to be fulfilled and then returns a DescribeTableOutput and error
func (p *TableNotExistsWaiterPromise) Await() error {
	_, err := p.Promise.Await()

	return err
}

// newTableNotExistsWaiterPromise returns a new TableNotExistsWaiterPromise
func newTableNotExistsWaiterPromise() *TableNotExistsWaiterPromise {
	return &TableNotExistsWaiterPromise{NewPromise()}
}

// TableNotExistsWaiterHandler represents a handler for TableNotExistsWaiter requests
type TableNotExistsWaiterHandler interface {
	HandleTableNotExistsWaiter(ctx *DescribeTableContext, promise *TableNotExistsWaiterPromise)
}

// TableNotExistsWaiterHandlerFunc is a TableNotExistsWaiterHandler function
type TableNotExistsWaiterHandlerFunc func(ctx *DescribeTableContext, promise *TableNotExistsWaiterPromise)

// HandleTableNotExistsWaiter implements TableNotExistsWaiterHandler
func (h TableNotExistsWaiterHandlerFunc) HandleTableNotExistsWaiter(ctx *DescribeTableContext, promise *TableNotExistsWaiterPromise) {
	h(ctx, promise)
}

// TableNotExistsWaiterMiddleWare is a middleware function use for wrapping TableNotExistsWaiterHandler requests
type TableNotExistsWaiterMiddleWare func(handler TableNotExistsWaiterHandler) TableNotExistsWaiterHandler

// TableNotExistsWaiterFinalHandler returns the final TableNotExistsWaiterHandler that executes a dynamodb TableNotExistsWaiter operation
func TableNotExistsWaiterFinalHandler() TableNotExistsWaiterHandler {
	return TableNotExistsWaiterHandlerFunc(func(ctx *DescribeTableContext, promise *TableNotExistsWaiterPromise) {
		var (
			out     *ddb.DescribeTableOutput
			sleeper *timer.Sleeper
			retry   bool
			err     error
		)

		defer func() { promise.SetResponse(nil, err) }()
		
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
	})
}

// TableNotExistsWaiter represents an operation that waits for a table to exist
type TableNotExistsWaiter struct {
	promise     *TableNotExistsWaiterPromise
	input       *ddb.DescribeTableInput
	middleWares []TableNotExistsWaiterMiddleWare
}

// NewTableNotExistsWaiter creates a new TableNotExistsWaiter operation on the given client with a given DescribeTableInput and options
func NewTableNotExistsWaiter(input *ddb.DescribeTableInput, mws ...TableNotExistsWaiterMiddleWare) *TableNotExistsWaiter {
	return &TableNotExistsWaiter{
		input:       input,
		middleWares: mws,
		promise:     newTableNotExistsWaiterPromise(),
	}
}

// Invoke invokes the TableNotExistsWaiter operation
func (op *TableNotExistsWaiter) Invoke(ctx context.Context, client *ddb.Client) *TableNotExistsWaiterPromise {
	go op.DynoInvoke(ctx, client)

	return op.promise
}

// DynoInvoke implements the Operation interface
func (op *TableNotExistsWaiter) DynoInvoke(ctx context.Context, client *ddb.Client) {
	requestCtx := &DescribeTableContext{
		Context: ctx,
		client:  client,
		input:   op.input,
	}

	h := TableNotExistsWaiterFinalHandler()

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i](h)
		}
	}

	h.HandleTableNotExistsWaiter(requestCtx, op.promise)
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
