package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

// CreateBackup creates a new CreateBackup, invokes and returns it
func (s *Session) CreateBackup(input *ddb.CreateBackupInput, mw ...CreateBackupMiddleWare) *CreateBackup {
	return NewCreateBackup(input, mw...).Invoke(s.ctx, s.ddb)
}

// CreateBackup creates a new CreateBackup, passes it to the Pool and then returns the CreateBackup
func (p *Pool) CreateBackup(input *ddb.CreateBackupInput, mw ...CreateBackupMiddleWare) *CreateBackup {
	op := NewCreateBackup(input, mw...)

	p.Do(op) // run the operation in the pool

	return op
}

// CreateBackupContext represents an exhaustive CreateBackup operation request context
type CreateBackupContext struct {
	context.Context
	Input  *ddb.CreateBackupInput
	Client *ddb.Client
}

// CreateBackupOutput represents the output for the CreateBackup operation
type CreateBackupOutput struct {
	out *ddb.CreateBackupOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *CreateBackupOutput) Set(out *ddb.CreateBackupOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *CreateBackupOutput) Get() (out *ddb.CreateBackupOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()
	return
}

// CreateBackupHandler represents a handler for CreateBackup requests
type CreateBackupHandler interface {
	HandleCreateBackup(ctx *CreateBackupContext, output *CreateBackupOutput)
}

// CreateBackupHandlerFunc is a CreateBackupHandler function
type CreateBackupHandlerFunc func(ctx *CreateBackupContext, output *CreateBackupOutput)

// HandleCreateBackup implements CreateBackupHandler
func (h CreateBackupHandlerFunc) HandleCreateBackup(ctx *CreateBackupContext, output *CreateBackupOutput) {
	h(ctx, output)
}

// CreateBackupFinalHandler is the final CreateBackupHandler that executes a dynamodb CreateBackup operation
type CreateBackupFinalHandler struct{}

// HandleCreateBackup implements the CreateBackupHandler
func (h *CreateBackupFinalHandler) HandleCreateBackup(ctx *CreateBackupContext, output *CreateBackupOutput) {
	output.Set(ctx.Client.CreateBackup(ctx, ctx.Input))
}

// CreateBackupMiddleWare is a middleware function use for wrapping CreateBackupHandler requests
type CreateBackupMiddleWare interface {
	CreateBackupMiddleWare(next CreateBackupHandler) CreateBackupHandler
}

// CreateBackupMiddleWareFunc is a functional CreateBackupMiddleWare
type CreateBackupMiddleWareFunc func(next CreateBackupHandler) CreateBackupHandler

// CreateBackupMiddleWare implements the CreateBackupMiddleWare interface
func (mw CreateBackupMiddleWareFunc) CreateBackupMiddleWare(next CreateBackupHandler) CreateBackupHandler {
	return mw(next)
}

// CreateBackup represents a CreateBackup operation
type CreateBackup struct {
	*BaseOperation
	input       *ddb.CreateBackupInput
	middleWares []CreateBackupMiddleWare
}

// NewCreateBackup creates a new CreateBackup
func NewCreateBackup(input *ddb.CreateBackupInput, mws ...CreateBackupMiddleWare) *CreateBackup {
	return &CreateBackup{
		BaseOperation: NewOperation(),
		input:         input,
		middleWares:   mws,
	}
}

// Invoke invokes the CreateBackup operation in a goroutine and returns a BatchGetItemAllOperation
func (op *CreateBackup) Invoke(ctx context.Context, client *ddb.Client) *CreateBackup {
	op.SetRunning() // operation now running

	go op.InvokeDynoOperation(ctx, client)

	return op
}

// InvokeDynoOperation invokes the CreateBackup operation
func (op *CreateBackup) InvokeDynoOperation(ctx context.Context, client *ddb.Client) {
	output := new(CreateBackupOutput)

	defer func() { op.SetResponse(output.Get()) }()

	var h CreateBackupHandler

	h = new(CreateBackupFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].CreateBackupMiddleWare(h)
		}
	}

	requestCtx := &CreateBackupContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	h.HandleCreateBackup(requestCtx, output)
}

// Await waits for the CreateBackupOperation to be fulfilled and then returns a CreateBackupOutput and error
func (op *CreateBackup) Await() (*ddb.CreateBackupOutput, error) {
	out, err := op.BaseOperation.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.CreateBackupOutput), err
}

// NewCreateBackupInput creates a CreateBackupInput with a given table name and key
func NewCreateBackupInput(tableName *string, backupArn *string) *ddb.CreateBackupInput {
	return &ddb.CreateBackupInput{
		BackupName: backupArn,
		TableName:  tableName,
	}
}
