package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// RestoreTableToPointInTime executes RestoreTableToPointInTime operation and returns a RestoreTableToPointInTime operation
func (s *Session) RestoreTableToPointInTime(input *ddb.RestoreTableToPointInTimeInput, mw ...RestoreTableToPointInTimeMiddleWare) *RestoreTableToPointInTime {
	return NewRestoreTableToPointInTime(input, mw...).Invoke(s.ctx, s.ddb)
}

// RestoreTableToPointInTime executes a RestoreTableToPointInTime operation with a RestoreTableToPointInTimeInput in this pool and returns the RestoreTableToPointInTime operation
func (p *Pool) RestoreTableToPointInTime(input *ddb.RestoreTableToPointInTimeInput, mw ...RestoreTableToPointInTimeMiddleWare) *RestoreTableToPointInTime {
	op := NewRestoreTableToPointInTime(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// RestoreTableToPointInTimeContext represents an exhaustive RestoreTableToPointInTime operation request context
type RestoreTableToPointInTimeContext struct {
	context.Context
	Input  *ddb.RestoreTableToPointInTimeInput
	Client *ddb.Client
}

// RestoreTableToPointInTimeOutput represents the output for the RestoreTableToPointInTime operation
type RestoreTableToPointInTimeOutput struct {
	out *ddb.RestoreTableToPointInTimeOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *RestoreTableToPointInTimeOutput) Set(out *ddb.RestoreTableToPointInTimeOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *RestoreTableToPointInTimeOutput) Get() (out *ddb.RestoreTableToPointInTimeOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()

	return
}

// RestoreTableToPointInTimeHandler represents a handler for RestoreTableToPointInTime requests
type RestoreTableToPointInTimeHandler interface {
	HandleRestoreTableToPointInTime(ctx *RestoreTableToPointInTimeContext, output *RestoreTableToPointInTimeOutput)
}

// RestoreTableToPointInTimeHandlerFunc is a RestoreTableToPointInTimeHandler function
type RestoreTableToPointInTimeHandlerFunc func(ctx *RestoreTableToPointInTimeContext, output *RestoreTableToPointInTimeOutput)

// HandleRestoreTableToPointInTime implements RestoreTableToPointInTimeHandler
func (h RestoreTableToPointInTimeHandlerFunc) HandleRestoreTableToPointInTime(ctx *RestoreTableToPointInTimeContext, output *RestoreTableToPointInTimeOutput) {
	h(ctx, output)
}

// RestoreTableToPointInTimeFinalHandler is the final RestoreTableToPointInTimeHandler that executes a dynamodb RestoreTableToPointInTime operation
type RestoreTableToPointInTimeFinalHandler struct{}

// HandleRestoreTableToPointInTime implements the RestoreTableToPointInTimeHandler
func (h *RestoreTableToPointInTimeFinalHandler) HandleRestoreTableToPointInTime(ctx *RestoreTableToPointInTimeContext, output *RestoreTableToPointInTimeOutput) {
	output.Set(ctx.Client.RestoreTableToPointInTime(ctx, ctx.Input))
}

// RestoreTableToPointInTimeMiddleWare is a middleware function use for wrapping RestoreTableToPointInTimeHandler requests
type RestoreTableToPointInTimeMiddleWare interface {
	RestoreTableToPointInTimeMiddleWare(next RestoreTableToPointInTimeHandler) RestoreTableToPointInTimeHandler
}

// RestoreTableToPointInTimeMiddleWareFunc is a functional RestoreTableToPointInTimeMiddleWare
type RestoreTableToPointInTimeMiddleWareFunc func(next RestoreTableToPointInTimeHandler) RestoreTableToPointInTimeHandler

// RestoreTableToPointInTimeMiddleWare implements the RestoreTableToPointInTimeMiddleWare interface
func (mw RestoreTableToPointInTimeMiddleWareFunc) RestoreTableToPointInTimeMiddleWare(next RestoreTableToPointInTimeHandler) RestoreTableToPointInTimeHandler {
	return mw(next)
}

// RestoreTableToPointInTime represents a RestoreTableToPointInTime operation
type RestoreTableToPointInTime struct {
	*BaseOperation
	input       *ddb.RestoreTableToPointInTimeInput
	middleWares []RestoreTableToPointInTimeMiddleWare
}

// NewRestoreTableToPointInTime creates a new RestoreTableToPointInTime operation
func NewRestoreTableToPointInTime(input *ddb.RestoreTableToPointInTimeInput, mws ...RestoreTableToPointInTimeMiddleWare) *RestoreTableToPointInTime {
	return &RestoreTableToPointInTime{
		BaseOperation: NewOperation(),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the RestoreTableToPointInTime operation in a goroutine and returns a RestoreTableToPointInTime operation
func (op *RestoreTableToPointInTime) Invoke(ctx context.Context, client *ddb.Client) *RestoreTableToPointInTime {
	op.SetRunning() // operation now waiting for a response

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the RestoreTableToPointInTime operation
func (op *RestoreTableToPointInTime) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	output := new(RestoreTableToPointInTimeOutput)

	defer func() { op.SetResponse(output.Get()) }()

	var h RestoreTableToPointInTimeHandler

	h = new(RestoreTableToPointInTimeFinalHandler)

	// loop in reverse to preserve middleware order
	for i := len(op.middleWares) - 1; i >= 0; i-- {
		h = op.middleWares[i].RestoreTableToPointInTimeMiddleWare(h)
	}

	requestCtx := &RestoreTableToPointInTimeContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	h.HandleRestoreTableToPointInTime(requestCtx, output)
}

// Await waits for the RestoreTableToPointInTime operation to be fulfilled and then returns a RestoreTableToPointInTimeOutput and error
func (op *RestoreTableToPointInTime) Await() (*ddb.RestoreTableToPointInTimeOutput, error) {
	out, err := op.BaseOperation.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.RestoreTableToPointInTimeOutput), err
}

// NewRestoreTableToPointInTimeInput creates a RestoreTableToPointInTimeInput with a given table name and key
func NewRestoreTableToPointInTimeInput(tableName *string) *ddb.RestoreTableToPointInTimeInput {
	return &ddb.RestoreTableToPointInTimeInput{
		TargetTableName: tableName,
	}
}
