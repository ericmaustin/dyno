package dyno

import (
	"context"
	"errors"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ericmaustin/dyno/timer"
	"time"
)

// DescribeTable executes a scan api call with a DescribeTableInput
func (c *Client) DescribeTable(ctx context.Context, input *ddb.DescribeTableInput, optFns ...func(*DescribeTableOptions)) (*ddb.DescribeTableOutput, error) {
	op := NewDescribeTable(input, optFns...)
	op.DynoInvoke(ctx, c.ddb)

	return op.Await()
}

// WaitForTableExists waits for a table to exist
func (c *Client) WaitForTableExists(ctx context.Context, input *ddb.DescribeTableInput, optFns ...func(*TableExistsWaiterOptions)) error {
	waiter := NewTableExistsWaiter(input, optFns...)
	waiter.DynoInvoke(ctx, c.ddb)

	return waiter.Await()
}

// WaitForTableNotExists waits for a table to not exist
func (c *Client) WaitForTableNotExists(ctx context.Context, input *ddb.DescribeTableInput, optFns ...func(*TableNotExistsWaiterOptions)) error {
	waiter := NewTableNotExistsWaiter(input, optFns...)
	waiter.DynoInvoke(ctx, c.ddb)

	return waiter.Await()
}

// DescribeTableInputCallback is a callback that is called on a given DescribeTableInput before a DescribeTable operation api call executes
type DescribeTableInputCallback interface {
	DescribeTableInputCallback(context.Context, *ddb.DescribeTableInput) (*ddb.DescribeTableOutput, error)
}

// DescribeTableOutputCallback is a callback that is called on a given DescribeTableOutput after a DescribeTable operation api call executes
type DescribeTableOutputCallback interface {
	DescribeTableOutputCallback(context.Context, *ddb.DescribeTableOutput) error
}

// DescribeTableInputCallbackF is DescribeTableOutputCallback function
type DescribeTableInputCallbackF func(context.Context, *ddb.DescribeTableInput) (*ddb.DescribeTableOutput, error)

// DescribeTableInputCallback implements the DescribeTableOutputCallback interface
func (cb DescribeTableInputCallbackF) DescribeTableInputCallback(ctx context.Context, input *ddb.DescribeTableInput) (*ddb.DescribeTableOutput, error) {
	return cb(ctx, input)
}

// DescribeTableOutputCallbackF is DescribeTableOutputCallback function
type DescribeTableOutputCallbackF func(context.Context, *ddb.DescribeTableOutput) error

// DescribeTableOutputCallback implements the DescribeTableOutputCallback interface
func (cb DescribeTableOutputCallbackF) DescribeTableOutputCallback(ctx context.Context, input *ddb.DescribeTableOutput) error {
	return cb(ctx, input)
}

// DescribeTableOptions represents options passed to the DescribeTable operation
type DescribeTableOptions struct {
	// InputCallbacks are called before the DescribeTable dynamodb api operation with the dynamodb.DescribeTableInput
	InputCallbacks []DescribeTableInputCallback
	// OutputCallbacks are called after the DescribeTable dynamodb api operation with the dynamodb.DescribeTableOutput
	OutputCallbacks []DescribeTableOutputCallback
}

// DescribeTableWithInputCallback adds a DescribeTableInputCallbackF to the InputCallbacks
func DescribeTableWithInputCallback(cb DescribeTableInputCallbackF) func(*DescribeTableOptions) {
	return func(opt *DescribeTableOptions) {
		opt.InputCallbacks = append(opt.InputCallbacks, cb)
	}
}

// DescribeTableWithOutputCallback adds a DescribeTableOutputCallback to the OutputCallbacks
func DescribeTableWithOutputCallback(cb DescribeTableOutputCallback) func(*DescribeTableOptions) {
	return func(opt *DescribeTableOptions) {
		opt.OutputCallbacks = append(opt.OutputCallbacks, cb)
	}
}

// DescribeTable represents a DescribeTable operation
type DescribeTable struct {
	*Promise
	input   *ddb.DescribeTableInput
	options DescribeTableOptions
}

// NewDescribeTable creates a new DescribeTable operation on the given client with a given DescribeTableInput and options
func NewDescribeTable(input *ddb.DescribeTableInput, optFns ...func(*DescribeTableOptions)) *DescribeTable {
	opts := DescribeTableOptions{}

	for _, opt := range optFns {
		opt(&opts)
	}

	return &DescribeTable{
		Promise: NewPromise(),
		input:   input,
		options: opts,
	}
}

// Await waits for the Operation to be complete and then returns a DescribeTableOutput and error
func (op *DescribeTable) Await() (*ddb.DescribeTableOutput, error) {
	out, err := op.Promise.Await()

	if out == nil {
		return nil, err
	}

	return out.(*ddb.DescribeTableOutput), err
}

// Invoke invokes the DescribeTable operation
func (op *DescribeTable) Invoke(ctx context.Context, client *ddb.Client) *DescribeTable {
	go op.DynoInvoke(ctx, client)

	return op
}

// DynoInvoke implements the Operation interface
func (op *DescribeTable) DynoInvoke(ctx context.Context, client *ddb.Client) {
	var (
		out *ddb.DescribeTableOutput
		err error
	)

	defer func() { op.SetResponse(out, err) }()

	for _, cb := range op.options.InputCallbacks {
		if out, err = cb.DescribeTableInputCallback(ctx, op.input); out != nil || err != nil {
			return
		}
	}

	if out, err = client.DescribeTable(ctx, op.input); err != nil {
		return
	}

	for _, cb := range op.options.OutputCallbacks {
		if err = cb.DescribeTableOutputCallback(ctx, out); err != nil {
			return
		}
	}

	return
}

// TableExistsWaiterOptions represents options passed to the TableExistsWaiter operation
type TableExistsWaiterOptions struct {
	Timeout      time.Duration
	RetryDelay   time.Duration
	RetrySleeper *timer.Sleeper

	// OutputCallbacks are called after the DescribeTable dynamodb api operation with the dynamodb.DescribeTableOutput
	OutputCallbacks []DescribeTableOutputCallback
}

// TableExistsWaiterWithTimeout adds a timeout to the TableExistsWaiterOptions
func TableExistsWaiterWithTimeout(timeout time.Duration) func(*TableExistsWaiterOptions) {
	return func(opt *TableExistsWaiterOptions) {
		opt.Timeout = timeout
	}
}

// TableExistsWaiterWithOutputCallback adds a DescribeTableOutputCallback to the OutputCallbacks
func TableExistsWaiterWithOutputCallback(cb DescribeTableOutputCallback) func(*TableExistsWaiterOptions) {
	return func(opt *TableExistsWaiterOptions) {
		opt.OutputCallbacks = append(opt.OutputCallbacks, cb)
	}
}

// TableExistsWaiter represents an operation that waits for a table to exist
type TableExistsWaiter struct {
	*Promise
	input   *ddb.DescribeTableInput
	options TableExistsWaiterOptions
}

// NewTableExistsWaiter creates a new TableExistsWaiter operation on the given client with a given DescribeTableInput and options
func NewTableExistsWaiter(input *ddb.DescribeTableInput, optFns ...func(*TableExistsWaiterOptions)) *TableExistsWaiter {
	opts := TableExistsWaiterOptions{
		Timeout:    time.Minute * 5,
		RetryDelay: time.Second,
	}

	for _, opt := range optFns {
		opt(&opts)
	}

	return &TableExistsWaiter{
		Promise: NewPromise(),
		input:   input,
		options: opts,
	}
}

// Await waits for the Operation to be complete and then returns a DescribeTableOutput and error
func (op *TableExistsWaiter) Await() error {
	_, err := op.Promise.Await()
	return err
}

// Invoke invokes the TableExistsWaiter operation
func (op *TableExistsWaiter) Invoke(ctx context.Context, client *ddb.Client) *TableExistsWaiter {
	go op.DynoInvoke(ctx, client)
	return op
}

// DynoInvoke implements the Operation interface
func (op *TableExistsWaiter) DynoInvoke(ctx context.Context, client *ddb.Client) {
	var (
		out     *ddb.DescribeTableOutput
		sleeper *timer.Sleeper
		retry   bool
		err     error
	)

	defer func() {
		op.SetResponse(nil, err)

		if out == nil {
			return
		}

		for _, cb := range op.options.OutputCallbacks {
			if err = cb.DescribeTableOutputCallback(ctx, out); err != nil {
				return
			}
		}
	}()

	ctx, cancel := context.WithTimeout(ctx, op.options.Timeout)

	defer cancel()

	if op.options.RetrySleeper != nil {
		sleeper = op.options.RetrySleeper.WithContext(ctx)
	} else {
		sleeper = timer.NewSleeper(op.options.RetryDelay).WithContext(ctx)
	}

	for {
		out, err = client.DescribeTable(ctx, op.input)
		retry, err = tableExistsRetryState(out, err)

		if !retry || err != nil  {
			return
		}

		if err = <-sleeper.Sleep(); err != nil {
			return
		}
	}
}

// TableNotExistsWaiterOptions represents options passed to the TableNotExistsWaiter operation
type TableNotExistsWaiterOptions struct {
	Timeout      time.Duration
	RetryDelay   time.Duration
	RetrySleeper *timer.Sleeper
}

// TableNotExistsWaiterWithTimeout adds a timeout to the TableNotExistsWaiterOptions
func TableNotExistsWaiterWithTimeout(timeout time.Duration) func(*TableNotExistsWaiterOptions) {
	return func(opt *TableNotExistsWaiterOptions) {
		opt.Timeout = timeout
	}
}

// TableNotExistsWaiterWithRetrySleeper sets the retry sleeper on TableNotExistsWaiterOptions
func TableNotExistsWaiterWithRetrySleeper(sleeper *timer.Sleeper) func(*TableNotExistsWaiterOptions) {
	return func(opt *TableNotExistsWaiterOptions) {
		opt.RetrySleeper = sleeper
	}
}

// TableNotExistsWaiter represents an operation that waits for a table to exist
type TableNotExistsWaiter struct {
	*Promise
	input   *ddb.DescribeTableInput
	options TableNotExistsWaiterOptions
}

// NewTableNotExistsWaiter creates a new TableNotExistsWaiter operation on the given client with a given DescribeTableInput and options
func NewTableNotExistsWaiter(input *ddb.DescribeTableInput, optFns ...func(*TableNotExistsWaiterOptions)) *TableNotExistsWaiter {
	opts := TableNotExistsWaiterOptions{
		Timeout:      time.Minute * 5,
		RetryDelay:   time.Second,
		RetrySleeper: nil,
	}

	for _, opt := range optFns {
		opt(&opts)
	}

	return &TableNotExistsWaiter{
		Promise: NewPromise(),
		input:   input,
		options: opts,
	}
}

// Await waits for the Operation to be complete and then returns a DescribeTableOutput and error
func (op *TableNotExistsWaiter) Await() error {
	_, err := op.Promise.Await()

	return err
}

// Invoke invokes the TableNotExistsWaiter operation
func (op *TableNotExistsWaiter) Invoke(ctx context.Context, client *ddb.Client) *TableNotExistsWaiter {
	go op.DynoInvoke(ctx, client)

	return op
}

// DynoInvoke implements the Operation interface
func (op *TableNotExistsWaiter) DynoInvoke(ctx context.Context, client *ddb.Client) {
	var (
		out     *ddb.DescribeTableOutput
		retry   bool
		err     error
		sleeper *timer.Sleeper
	)

	defer func() { op.SetResponse(nil, err) }()

	ctx, cancel := context.WithTimeout(ctx, op.options.Timeout)

	defer cancel()

	if op.options.RetrySleeper != nil {
		sleeper = op.options.RetrySleeper.WithContext(ctx)
	} else {
		sleeper = timer.NewSleeper(op.options.RetryDelay).WithContext(ctx)
	}

	for {
		out, err = client.DescribeTable(ctx, op.input)
		retry, err = tableNotExistsRetryState(out, err)

		if !retry || err != nil {
			return
		}

		if err = <-sleeper.Sleep(); err != nil {
			return
		}
	}
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
