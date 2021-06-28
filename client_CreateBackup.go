package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// CreateBackup executes CreateBackup operation and returns a CreateBackupPromise
func (c *Client) CreateBackup(ctx context.Context, input *ddb.CreateBackupInput, mw ...CreateBackupMiddleWare) *CreateBackupPromise {
	return NewCreateBackup(input, mw...).Invoke(ctx, c.ddb)
}

// CreateBackup executes a CreateBackup operation with a CreateBackupInput in this pool and returns the CreateBackupPromise
func (p *Pool) CreateBackup(input *ddb.CreateBackupInput, mw ...CreateBackupMiddleWare) *CreateBackupPromise {
	op := NewCreateBackup(input, mw...)

	if err := p.Do(op); err != nil {
		op.promise.SetResponse(nil, err)
	}

	return op.promise
}

// CreateBackupContext represents an exhaustive CreateBackup operation request context
type CreateBackupContext struct {
	context.Context
	input  *ddb.CreateBackupInput
	client *ddb.Client
}

// CreateBackupPromise represents a promise for the CreateBackup
type CreateBackupPromise struct {
	*Promise
}

// GetResponse returns the GetResponse output and error
// if Output has not been set yet nil is returned
func (p *CreateBackupPromise) GetResponse() (*ddb.CreateBackupOutput, error) {
	out, err := p.Promise.GetResponse()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.CreateBackupOutput), err
}

// Await waits for the CreateBackupPromise to be fulfilled and then returns a CreateBackupOutput and error
func (p *CreateBackupPromise) Await() (*ddb.CreateBackupOutput, error) {
	out, err := p.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.CreateBackupOutput), err
}

// newCreateBackupPromise returns a new CreateBackupPromise
func newCreateBackupPromise() *CreateBackupPromise {
	return &CreateBackupPromise{NewPromise()}
}

// CreateBackupHandler represents a handler for CreateBackup requests
type CreateBackupHandler interface {
	HandleCreateBackup(ctx *CreateBackupContext, promise *CreateBackupPromise)
}

// CreateBackupHandlerFunc is a CreateBackupHandler function
type CreateBackupHandlerFunc func(ctx *CreateBackupContext, promise *CreateBackupPromise)

// CreateBackupFinalHandler is the final CreateBackupHandler that executes a dynamodb CreateBackup operation
type CreateBackupFinalHandler struct {}

// HandleCreateBackup implements the CreateBackupHandler
func (h *CreateBackupFinalHandler) HandleCreateBackup(ctx *CreateBackupContext, promise *CreateBackupPromise) {
	promise.SetResponse(ctx.client.CreateBackup(ctx, ctx.input))
}

// HandleCreateBackup implements CreateBackupHandler
func (h CreateBackupHandlerFunc) HandleCreateBackup(ctx *CreateBackupContext, promise *CreateBackupPromise) {
	h(ctx, promise)
}

// CreateBackupMiddleWare is a middleware function use for wrapping CreateBackupHandler requests
type CreateBackupMiddleWare interface {
	CreateBackupMiddleWare(h CreateBackupHandler) CreateBackupHandler
}

// CreateBackupMiddleWareFunc is a functional CreateBackupMiddleWare
type CreateBackupMiddleWareFunc func(handler CreateBackupHandler) CreateBackupHandler

// CreateBackupMiddleWare implements the CreateBackupMiddleWare interface
func (mw CreateBackupMiddleWareFunc) CreateBackupMiddleWare(h CreateBackupHandler) CreateBackupHandler {
	return mw(h)
}

// CreateBackup represents a CreateBackup operation
type CreateBackup struct {
	promise     *CreateBackupPromise
	input       *ddb.CreateBackupInput
	middleWares []CreateBackupMiddleWare
}

// NewCreateBackup creates a new CreateBackup
func NewCreateBackup(input *ddb.CreateBackupInput, mws ...CreateBackupMiddleWare) *CreateBackup {
	return &CreateBackup{
		input:       input,
		middleWares: mws,
		promise:     newCreateBackupPromise(),
	}
}

// Invoke invokes the CreateBackup operation and returns a CreateBackupPromise
func (op *CreateBackup) Invoke(ctx context.Context, client *ddb.Client) *CreateBackupPromise {
	go op.DynoInvoke(ctx, client)

	return op.promise
}

// DynoInvoke implements the Operation interface
func (op *CreateBackup) DynoInvoke(ctx context.Context, client *ddb.Client) {

	requestCtx := &CreateBackupContext{
		Context: ctx,
		client:  client,
		input:   op.input,
	}

	var h CreateBackupHandler

	h = new(CreateBackupFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].CreateBackupMiddleWare(h)
		}
	}

	h.HandleCreateBackup(requestCtx, op.promise)
}

// NewCreateBackupInput creates a CreateBackupInput with a given table name and key
func NewCreateBackupInput(tableName *string, backupArn *string) *ddb.CreateBackupInput {
	return &ddb.CreateBackupInput{
		BackupName: backupArn,
		TableName:  tableName,
	}
}
