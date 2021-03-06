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

// DescribeTable executes DescribeTable operation and returns a DescribeTable operation
func (s *Session) DescribeTable(input *ddb.DescribeTableInput, mw ...DescribeTableMiddleWare) *DescribeTable {
	return NewDescribeTable(input, mw...).Invoke(s.ctx, s.ddb)
}

// TableExistsWaiter executes TableExistsWaiter operation and returns a TableExistsWaiter operation
func (s *Session) TableExistsWaiter(input *ddb.DescribeTableInput, mw ...DescribeTableMiddleWare) *TableExistsWaiter {
	return NewTableExistsWaiter(input, mw...).Invoke(s.ctx, s.ddb)
}

// TableNotExistsWaiter executes TableNotExistsWaiter operation and returns a TableNotExistsWaiter operation
func (s *Session) TableNotExistsWaiter(input *ddb.DescribeTableInput, mw ...DescribeTableMiddleWare) *TableNotExistsWaiter {
	return NewTableNotExistsWaiter(input, mw...).Invoke(s.ctx, s.ddb)
}

// DescribeTable executes a DescribeTable operation with a DescribeTableInput in this pool and returns the DescribeTable operation
func (p *Pool) DescribeTable(input *ddb.DescribeTableInput, mw ...DescribeTableMiddleWare) *DescribeTable {
	op := NewDescribeTable(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// TableExistsWaiter executes a TableExistsWaiter operation with a DescribeTableInput in this pool and returns the TableExistsWaiter operation
func (p *Pool) TableExistsWaiter(input *ddb.DescribeTableInput, mw ...DescribeTableMiddleWare) *TableExistsWaiter {
	op := NewTableExistsWaiter(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// TableNotExistsWaiter executes a TableNotExistsWaiter operation with a DescribeTableInput in this pool and returns the TableNotExistsWaiter operation
func (p *Pool) TableNotExistsWaiter(input *ddb.DescribeTableInput, mw ...DescribeTableMiddleWare) *TableNotExistsWaiter {
	op := NewTableNotExistsWaiter(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// DescribeTableContext represents an exhaustive DescribeTable operation request context
type DescribeTableContext struct {
	context.Context
	Input  *ddb.DescribeTableInput
	Client *ddb.Client
}

// DescribeTableOutput represents the output for the DescribeTable opration
type DescribeTableOutput struct {
	out *ddb.DescribeTableOutput
	err error
	mu  sync.RWMutex
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
type DescribeTableFinalHandler struct{}

// HandleDescribeTable implements the DescribeTableHandler
func (h *DescribeTableFinalHandler) HandleDescribeTable(ctx *DescribeTableContext, output *DescribeTableOutput) {
	output.Set(ctx.Client.DescribeTable(ctx, ctx.Input))
}

// DescribeTable represents a DescribeTable operation
type DescribeTable struct {
	*BaseOperation
	input       *ddb.DescribeTableInput
	middleWares []DescribeTableMiddleWare
}

// NewDescribeTable creates a new DescribeTable
func NewDescribeTable(input *ddb.DescribeTableInput, mws ...DescribeTableMiddleWare) *DescribeTable {
	return &DescribeTable{
		BaseOperation: NewOperation(),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the DescribeTable operation and this DescribeTable operation
func (op *DescribeTable) Invoke(ctx context.Context, client *ddb.Client) *DescribeTable {
	op.SetRunning() // operation now waiting for a response

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the DescribeLimits operation
func (op *DescribeTable) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	invokeDescribeTableWithHandler(ctx, client, op.input, new(DescribeTableFinalHandler), op.middleWares, op.BaseOperation)
}

// Await waits for the DescribeTable operation to be fulfilled and then returns a DescribeTableOutput and error
func (op *DescribeTable) Await() (*ddb.DescribeTableOutput, error) {
	out, err := op.BaseOperation.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.DescribeTableOutput), err
}

// TableExistsWaiterFinalHandler is the final TableWaiterHandler that executes a dynamodb TableExistsWaiter operation
type TableExistsWaiterFinalHandler struct{}

// HandleDescribeTable implements the DescribeTableHandler
func (h *TableExistsWaiterFinalHandler) HandleDescribeTable(ctx *DescribeTableContext, output *DescribeTableOutput) {
	var (
		out     *ddb.DescribeTableOutput
		sleeper *timer.Sleeper
		retry   bool
		err     error
	)

	defer func() { output.Set(out, err) }()

	sleeper = timer.NewLinearSleeper(time.Millisecond*100, 2).WithContext(ctx)

	for {
		out, err = ctx.Client.DescribeTable(ctx, ctx.Input)
		retry, err = tableExistsRetryState(out, err)

		if !retry || err != nil {
			return
		}

		if err = <-sleeper.Sleep(); err != nil {
			return
		}
	}
}

// TableExistsWaiter represents an operation that waits for a table to exist
type TableExistsWaiter struct {
	*BaseOperation
	input       *ddb.DescribeTableInput
	middleWares []DescribeTableMiddleWare
}

// NewTableExistsWaiter creates a new TableExistsWaiter operation on the given client with a given DescribeTableInput and options
func NewTableExistsWaiter(input *ddb.DescribeTableInput, mws ...DescribeTableMiddleWare) *TableExistsWaiter {
	return &TableExistsWaiter{
		BaseOperation: NewOperation(),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the TableExistsWaiter operation in a goroutine and returns a TableExistsWaiter operation
func (op *TableExistsWaiter) Invoke(ctx context.Context, client *ddb.Client) *TableExistsWaiter {
	op.SetRunning() // operation now waiting for a response

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation implements the Operation interface
func (op *TableExistsWaiter) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	invokeDescribeTableWithHandler(ctx, client, op.input, new(TableExistsWaiterFinalHandler), op.middleWares, op.BaseOperation)
}

// TableNotExistsWaiterFinalHandler is the final TableNotExistsWaiterHandler that executes a dynamodb TableNotExistsWaiter operation
type TableNotExistsWaiterFinalHandler struct{}

// HandleDescribeTable implements the DescribeTableHandler
func (h *TableNotExistsWaiterFinalHandler) HandleDescribeTable(ctx *DescribeTableContext, output *DescribeTableOutput) {
	var (
		out     *ddb.DescribeTableOutput
		sleeper *timer.Sleeper
		retry   bool
		err     error
	)

	defer func() { output.Set(out, err) }()

	sleeper = timer.NewLinearSleeper(time.Millisecond*100, 2).WithContext(ctx)

	for {
		out, err = ctx.Client.DescribeTable(ctx, ctx.Input)
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
	*BaseOperation
	input       *ddb.DescribeTableInput
	middleWares []DescribeTableMiddleWare
}

// NewTableNotExistsWaiter creates a new TableNotExistsWaiter operation on the given client with a given DescribeTableInput and options
func NewTableNotExistsWaiter(input *ddb.DescribeTableInput, mws ...DescribeTableMiddleWare) *TableNotExistsWaiter {
	return &TableNotExistsWaiter{
		BaseOperation: NewOperation(),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the TableNotExistsWaiter operation in a goroutine and returns a TableNotExistsWaiter operation
func (op *TableNotExistsWaiter) Invoke(ctx context.Context, client *ddb.Client) *TableNotExistsWaiter {
	op.SetRunning() // operation now waiting for a response

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the TableNotExistsWaiter operation
func (op *TableNotExistsWaiter) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	invokeDescribeTableWithHandler(ctx, client, op.input, new(TableNotExistsWaiterFinalHandler), op.middleWares, op.BaseOperation)
}

// invokeDescribeTableWithHandler invokes a describe table operation with a specific handler
func invokeDescribeTableWithHandler(ctx context.Context, client *ddb.Client, input *ddb.DescribeTableInput, handler DescribeTableHandler, mws []DescribeTableMiddleWare, op *BaseOperation) {
	output := new(DescribeTableOutput)

	defer func() { op.SetResponse(output.Get()) }()

	// loop in reverse to preserve middleware order
	for i := len(mws) - 1; i >= 0; i-- {
		handler = mws[i].DescribeTableMiddleWare(handler)
	}

	requestCtx := &DescribeTableContext{
		Context: ctx,
		Client:  client,
		Input:   input,
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
