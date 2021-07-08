package dyno

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"sync"
)

// UpdateContinuousBackups executes UpdateContinuousBackups operation and returns a UpdateContinuousBackupsPromise
func (c *Client) UpdateContinuousBackups(ctx context.Context, input *ddb.UpdateContinuousBackupsInput, mw ...UpdateContinuousBackupsMiddleWare) *UpdateContinuousBackups {
	return NewUpdateContinuousBackups(input, mw...).Invoke(ctx, c.ddb)
}

// UpdateContinuousBackups executes a UpdateContinuousBackups operation with a UpdateContinuousBackupsInput in this pool and returns the UpdateContinuousBackupsPromise
func (p *Pool) UpdateContinuousBackups(input *ddb.UpdateContinuousBackupsInput, mw ...UpdateContinuousBackupsMiddleWare) *UpdateContinuousBackups {
	op := NewUpdateContinuousBackups(input, mw...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// UpdateContinuousBackupsContext represents an exhaustive UpdateContinuousBackups operation request context
type UpdateContinuousBackupsContext struct {
	context.Context
	Input  *ddb.UpdateContinuousBackupsInput
	Client *ddb.Client
}

// UpdateContinuousBackupsOutput represents the output for the UpdateContinuousBackups operation
type UpdateContinuousBackupsOutput struct {
	out *ddb.UpdateContinuousBackupsOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *UpdateContinuousBackupsOutput) Set(out *ddb.UpdateContinuousBackupsOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *UpdateContinuousBackupsOutput) Get() (out *ddb.UpdateContinuousBackupsOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()
	
	return
}

// UpdateContinuousBackupsHandler represents a handler for UpdateContinuousBackups requests
type UpdateContinuousBackupsHandler interface {
	HandleUpdateContinuousBackups(ctx *UpdateContinuousBackupsContext, output *UpdateContinuousBackupsOutput)
}

// UpdateContinuousBackupsHandlerFunc is a UpdateContinuousBackupsHandler function
type UpdateContinuousBackupsHandlerFunc func(ctx *UpdateContinuousBackupsContext, output *UpdateContinuousBackupsOutput)

// HandleUpdateContinuousBackups implements UpdateContinuousBackupsHandler
func (h UpdateContinuousBackupsHandlerFunc) HandleUpdateContinuousBackups(ctx *UpdateContinuousBackupsContext, output *UpdateContinuousBackupsOutput) {
	h(ctx, output)
}

// UpdateContinuousBackupsFinalHandler is the final UpdateContinuousBackupsHandler that executes a dynamodb UpdateContinuousBackups operation
type UpdateContinuousBackupsFinalHandler struct{}

// HandleUpdateContinuousBackups implements the UpdateContinuousBackupsHandler
func (h *UpdateContinuousBackupsFinalHandler) HandleUpdateContinuousBackups(ctx *UpdateContinuousBackupsContext, output *UpdateContinuousBackupsOutput) {
	output.Set(ctx.Client.UpdateContinuousBackups(ctx, ctx.Input))
}

// UpdateContinuousBackupsMiddleWare is a middleware function use for wrapping UpdateContinuousBackupsHandler requests
type UpdateContinuousBackupsMiddleWare interface {
	UpdateContinuousBackupsMiddleWare(next UpdateContinuousBackupsHandler) UpdateContinuousBackupsHandler
}

// UpdateContinuousBackupsMiddleWareFunc is a functional UpdateContinuousBackupsMiddleWare
type UpdateContinuousBackupsMiddleWareFunc func(next UpdateContinuousBackupsHandler) UpdateContinuousBackupsHandler

// UpdateContinuousBackupsMiddleWare implements the UpdateContinuousBackupsMiddleWare interface
func (mw UpdateContinuousBackupsMiddleWareFunc) UpdateContinuousBackupsMiddleWare(next UpdateContinuousBackupsHandler) UpdateContinuousBackupsHandler {
	return mw(next)
}

// UpdateContinuousBackups represents a UpdateContinuousBackups operation
type UpdateContinuousBackups struct {
	*Promise
	input       *ddb.UpdateContinuousBackupsInput
	middleWares []UpdateContinuousBackupsMiddleWare
}

// NewUpdateContinuousBackups creates a new UpdateContinuousBackups
func NewUpdateContinuousBackups(input *ddb.UpdateContinuousBackupsInput, mws ...UpdateContinuousBackupsMiddleWare) *UpdateContinuousBackups {
	return &UpdateContinuousBackups{
		Promise:     NewPromise(),
		input:       input,
		middleWares: mws,
	}
}

// Invoke invokes the UpdateContinuousBackups operation in a goroutine and returns a BatchGetItemAllPromise
func (op *UpdateContinuousBackups) Invoke(ctx context.Context, client *ddb.Client) *UpdateContinuousBackups {
	op.SetWaiting() // promise now waiting for a response
	go op.invoke(ctx, client)

	return op
}

// DynoInvoke implements the Operation interface
func (op *UpdateContinuousBackups) DynoInvoke(ctx context.Context, client *ddb.Client) {
	op.SetWaiting() // promise now waiting for a response
	op.invoke(ctx, client)
}

// invoke invokes the UpdateContinuousBackups operation
func (op *UpdateContinuousBackups) invoke(ctx context.Context, client *ddb.Client) {
	output := new(UpdateContinuousBackupsOutput)

	defer func() { op.SetResponse(output.Get()) }()

	requestCtx := &UpdateContinuousBackupsContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	var h UpdateContinuousBackupsHandler

	h = new(UpdateContinuousBackupsFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].UpdateContinuousBackupsMiddleWare(h)
		}
	}

	h.HandleUpdateContinuousBackups(requestCtx, output)
}

// Await waits for the UpdateContinuousBackupsPromise to be fulfilled and then returns a UpdateContinuousBackupsOutput and error
func (op *UpdateContinuousBackups) Await() (*ddb.UpdateContinuousBackupsOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.UpdateContinuousBackupsOutput), err
}

// UpdateContinuousBackupsBuilder is used to dynamically build a UpdateContinuousBackupsInput request
type UpdateContinuousBackupsBuilder struct {
	*ddb.UpdateContinuousBackupsInput
	projection *expression.ProjectionBuilder
}

// NewUpdateContinuousBackupsInput creates a new UpdateContinuousBackupsInput with a table name and key
func NewUpdateContinuousBackupsInput(tableName *string, recoveryEnabled bool) *ddb.UpdateContinuousBackupsInput {
	return &ddb.UpdateContinuousBackupsInput{
		TableName: tableName,
		PointInTimeRecoverySpecification: &ddbTypes.PointInTimeRecoverySpecification{
			PointInTimeRecoveryEnabled: &recoveryEnabled,
		},
	}
}
