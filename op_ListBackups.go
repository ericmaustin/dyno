package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
	"time"
)

// ListBackups executes ListBackups operation and returns a ListBackups operation
func (s *Session) ListBackups(input *ddb.ListBackupsInput, mw ...ListBackupsMiddleWare) *ListBackups {
	return NewListBackups(input, mw...).Invoke(s.ctx, s.ddb)
}

// ListBackups executes a ListBackups operation with a ListBackupsInput in this pool and returns the ListBackups operation
func (p *Pool) ListBackups(input *ddb.ListBackupsInput, mw ...ListBackupsMiddleWare) *ListBackups {
	op := NewListBackups(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// ListAllBackups executes ListBackups operation and returns a ListBackups operation
func (s *Session) ListAllBackups(input *ddb.ListBackupsInput, mw ...ListBackupsMiddleWare) *ListBackups {
	return NewListAllBackups(input, mw...).Invoke(s.ctx, s.ddb)
}

// ListAllBackups executes a ListBackups operation with a ListBackupsInput in this pool and returns the ListBackups operation
func (p *Pool) ListAllBackups(input *ddb.ListBackupsInput, mw ...ListBackupsMiddleWare) *ListBackups {
	op := NewListAllBackups(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// ListBackupsContext represents an exhaustive ListBackups operation request context
type ListBackupsContext struct {
	context.Context
	Input  *ddb.ListBackupsInput
	Client *ddb.Client
}

// ListBackupsOutput represents the output for the ListBackups operation
type ListBackupsOutput struct {
	out *ddb.ListBackupsOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *ListBackupsOutput) Set(out *ddb.ListBackupsOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *ListBackupsOutput) Get() (out *ddb.ListBackupsOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()

	return
}

// ListBackupsHandler represents a handler for ListBackups requests
type ListBackupsHandler interface {
	HandleListBackups(ctx *ListBackupsContext, output *ListBackupsOutput)
}

// ListBackupsHandlerFunc is a ListBackupsHandler function
type ListBackupsHandlerFunc func(ctx *ListBackupsContext, output *ListBackupsOutput)

// HandleListBackups implements ListBackupsHandler
func (h ListBackupsHandlerFunc) HandleListBackups(ctx *ListBackupsContext, output *ListBackupsOutput) {
	h(ctx, output)
}

// ListBackupsFinalHandler is the final ListBackupsHandler that executes a dynamodb ListBackups operation
type ListBackupsFinalHandler struct{}

// HandleListBackups implements the ListBackupsHandler
func (h *ListBackupsFinalHandler) HandleListBackups(ctx *ListBackupsContext, output *ListBackupsOutput) {
	output.Set(ctx.Client.ListBackups(ctx, ctx.Input))
}

// ListAllBackupsFinalHandler is the final ListBackupsHandler that executes a dynamodb ListBackups operation
type ListAllBackupsFinalHandler struct{}

// HandleListBackups implements the ListBackupsHandler
func (h *ListAllBackupsFinalHandler) HandleListBackups(ctx *ListBackupsContext, output *ListBackupsOutput) {
	var (
		err error
		out, finalOutput *ddb.ListBackupsOutput
	)

	input := CopyListBackupsInput(ctx.Input)
	finalOutput = new(ddb.ListBackupsOutput)

	defer func() { output.Set(finalOutput, err) }()

	for {
		out, err = ctx.Client.ListBackups(ctx, input)
		if err != nil {
			output.Set(nil, err)
		}

		finalOutput.BackupSummaries = append(finalOutput.BackupSummaries, out.BackupSummaries...)

		if out.LastEvaluatedBackupArn == nil || len(*out.LastEvaluatedBackupArn) == 0 {
			return
		}

		*input.ExclusiveStartBackupArn = *out.LastEvaluatedBackupArn
	}
}

// ListBackupsMiddleWare is a middleware function use for wrapping ListBackupsHandler requests
type ListBackupsMiddleWare interface {
	ListBackupsMiddleWare(next ListBackupsHandler) ListBackupsHandler
}

// ListBackupsMiddleWareFunc is a functional ListBackupsMiddleWare
type ListBackupsMiddleWareFunc func(next ListBackupsHandler) ListBackupsHandler

// ListBackupsMiddleWare implements the ListBackupsMiddleWare interface
func (mw ListBackupsMiddleWareFunc) ListBackupsMiddleWare(next ListBackupsHandler) ListBackupsHandler {
	return mw(next)
}

// ListBackups represents a ListBackups operation
type ListBackups struct {
	*BaseOperation
	input       *ddb.ListBackupsInput
	middleWares []ListBackupsMiddleWare
	handler     ListBackupsHandler
}

// NewListBackups creates a new ListBackups operation
func NewListBackups(input *ddb.ListBackupsInput, mws ...ListBackupsMiddleWare) *ListBackups {
	return &ListBackups{
		BaseOperation: NewOperation(),
		input:         input,
		middleWares:   mws,
		handler:       new(ListBackupsFinalHandler),
	}
}

// NewListAllBackups creates a new ListBackups operation
func NewListAllBackups(input *ddb.ListBackupsInput, mws ...ListBackupsMiddleWare) *ListBackups {
	return &ListBackups{
		BaseOperation: NewOperation(),
		input:         input,
		middleWares:   mws,
		handler:       new(ListAllBackupsFinalHandler),
	}
}

// Invoke invokes the ListBackups operation in a goroutine and returns a ListBackups operation
func (op *ListBackups) Invoke(ctx context.Context, client *ddb.Client) *ListBackups {
	op.SetRunning() // operation now waiting for a response

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the ListBackups operation
func (op *ListBackups) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	output := new(ListBackupsOutput)

	defer func() { op.SetResponse(output.Get()) }()

	var h ListBackupsHandler

	h = op.handler

	// loop in reverse to preserve middleware order
	for i := len(op.middleWares) - 1; i >= 0; i-- {
		h = op.middleWares[i].ListBackupsMiddleWare(h)
	}

	requestCtx := &ListBackupsContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	h.HandleListBackups(requestCtx, output)
}

// Await waits for the ListBackups operation to be fulfilled and then returns a ListBackupsOutput and error
func (op *ListBackups) Await() (*ddb.ListBackupsOutput, error) {
	out, err := op.BaseOperation.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.ListBackupsOutput), err
}

// NewListBackupsInput creates a new ListBackupsInput
func NewListBackupsInput() *ddb.ListBackupsInput {
	return &ddb.ListBackupsInput{}
}

// CopyListBackupsInput makes a copy of the input
func CopyListBackupsInput(input *ddb.ListBackupsInput) *ddb.ListBackupsInput {
	out := &ddb.ListBackupsInput{
		BackupType: input.BackupType,
	}

	if input.Limit != nil {
		input.Limit = new(int32)
		*out.Limit = *input.Limit
	}

	if input.TableName != nil {
		input.TableName = new(string)
		*out.TableName = *input.TableName
	}

	if input.ExclusiveStartBackupArn != nil {
		input.ExclusiveStartBackupArn = new(string)
		*out.ExclusiveStartBackupArn = *input.ExclusiveStartBackupArn
	}

	if input.TimeRangeLowerBound != nil {
		input.TimeRangeLowerBound = new(time.Time)
		*out.TimeRangeLowerBound = *input.TimeRangeLowerBound
	}

	if input.TimeRangeUpperBound != nil {
		input.TimeRangeUpperBound = new(time.Time)
		*out.TimeRangeUpperBound = *input.TimeRangeUpperBound
	}

	return out
}