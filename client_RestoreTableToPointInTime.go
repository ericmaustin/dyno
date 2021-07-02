package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// RestoreTableToPointInTime executes RestoreTableToPointInTime operation and returns a RestoreTableToPointInTimePromise
func (c *Client) RestoreTableToPointInTime(ctx context.Context, input *ddb.RestoreTableToPointInTimeInput, mw ...RestoreTableToPointInTimeMiddleWare) *RestoreTableToPointInTime {
	return NewRestoreTableToPointInTime(input, mw...).Invoke(ctx, c.ddb)
}

// RestoreTableToPointInTime executes a RestoreTableToPointInTime operation with a RestoreTableToPointInTimeInput in this pool and returns the RestoreTableToPointInTimePromise
func (p *Pool) RestoreTableToPointInTime(input *ddb.RestoreTableToPointInTimeInput, mw ...RestoreTableToPointInTimeMiddleWare) *RestoreTableToPointInTime {
	op := NewRestoreTableToPointInTime(input, mw...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// RestoreTableToPointInTimeContext represents an exhaustive RestoreTableToPointInTime operation request context
type RestoreTableToPointInTimeContext struct {
	context.Context
	Input  *ddb.RestoreTableToPointInTimeInput
	Client *ddb.Client
}

// RestoreTableToPointInTimeOutput represents the output for the RestoreTableToPointInTime opration
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
	*Promise
	input       *ddb.RestoreTableToPointInTimeInput
	middleWares []RestoreTableToPointInTimeMiddleWare
}

// NewRestoreTableToPointInTime creates a new RestoreTableToPointInTime
func NewRestoreTableToPointInTime(input *ddb.RestoreTableToPointInTimeInput, mws ...RestoreTableToPointInTimeMiddleWare) *RestoreTableToPointInTime {
	return &RestoreTableToPointInTime{
		Promise: NewPromise(),
		input:       input,
		middleWares: mws,
	}
}

// Invoke invokes the RestoreTableToPointInTime operation and returns a RestoreTableToPointInTimePromise
func (op *RestoreTableToPointInTime) Invoke(ctx context.Context, client *ddb.Client) *RestoreTableToPointInTime {
	go op.DynoInvoke(ctx, client)

	return op
}

// DynoInvoke implements the Operation interface
func (op *RestoreTableToPointInTime) DynoInvoke(ctx context.Context, client *ddb.Client) {
	output := new(RestoreTableToPointInTimeOutput)

	defer func() { op.SetResponse(output.Get()) }()

	requestCtx := &RestoreTableToPointInTimeContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	var h RestoreTableToPointInTimeHandler

	h = new(RestoreTableToPointInTimeFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].RestoreTableToPointInTimeMiddleWare(h)
		}
	}

	h.HandleRestoreTableToPointInTime(requestCtx, output)
}

// Await waits for the RestoreTableToPointInTimePromise to be fulfilled and then returns a RestoreTableToPointInTimeOutput and error
func (op *RestoreTableToPointInTime) Await() (*ddb.RestoreTableToPointInTimeOutput, error) {
	out, err := op.Promise.Await()
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
