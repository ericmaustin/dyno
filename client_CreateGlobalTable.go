package dyno

import (
	"context"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"sync"
)

// CreateGlobalTable creates a new CreateGlobalTable, invokes and returns it
func (c *Client) CreateGlobalTable(ctx context.Context, input *ddb.CreateGlobalTableInput, mw ...CreateGlobalTableMiddleWare) *CreateGlobalTable {
	return NewCreateGlobalTable(input, mw...).Invoke(ctx, c.ddb)
}

// CreateGlobalTable creates a new CreateGlobalTable, passes it to the Pool and then returns the CreateGlobalTable
func (p *Pool) CreateGlobalTable(input *ddb.CreateGlobalTableInput, mw ...CreateGlobalTableMiddleWare) *CreateGlobalTable {
	op := NewCreateGlobalTable(input, mw...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// CreateGlobalTableContext represents an exhaustive CreateGlobalTable operation request context
type CreateGlobalTableContext struct {
	context.Context
	input  *ddb.CreateGlobalTableInput
	client *ddb.Client
}

// CreateGlobalTableOutput represents the output for the CreateGlobalTable opration
type CreateGlobalTableOutput struct {
	out *ddb.CreateGlobalTableOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *CreateGlobalTableOutput) Set(out *ddb.CreateGlobalTableOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *CreateGlobalTableOutput) Get() (out *ddb.CreateGlobalTableOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()
	return
}

// CreateGlobalTableHandler represents a handler for CreateGlobalTable requests
type CreateGlobalTableHandler interface {
	HandleCreateGlobalTable(ctx *CreateGlobalTableContext, output *CreateGlobalTableOutput)
}

// CreateGlobalTableHandlerFunc is a CreateGlobalTableHandler function
type CreateGlobalTableHandlerFunc func(ctx *CreateGlobalTableContext, output *CreateGlobalTableOutput)

// HandleCreateGlobalTable implements CreateGlobalTableHandler
func (h CreateGlobalTableHandlerFunc) HandleCreateGlobalTable(ctx *CreateGlobalTableContext, output *CreateGlobalTableOutput) {
	h(ctx, output)
}

// CreateGlobalTableFinalHandler is the final CreateGlobalTableHandler that executes a dynamodb CreateGlobalTable operation
type CreateGlobalTableFinalHandler struct{}

// HandleCreateGlobalTable implements the CreateGlobalTableHandler
func (h *CreateGlobalTableFinalHandler) HandleCreateGlobalTable(ctx *CreateGlobalTableContext, output *CreateGlobalTableOutput) {
	output.Set(ctx.client.CreateGlobalTable(ctx, ctx.input))
}

// CreateGlobalTableMiddleWare is a middleware function use for wrapping CreateGlobalTableHandler requests
type CreateGlobalTableMiddleWare interface {
	CreateGlobalTableMiddleWare(next CreateGlobalTableHandler) CreateGlobalTableHandler
}

// CreateGlobalTableMiddleWareFunc is a functional CreateGlobalTableMiddleWare
type CreateGlobalTableMiddleWareFunc func(next CreateGlobalTableHandler) CreateGlobalTableHandler

// CreateGlobalTableMiddleWare implements the CreateGlobalTableMiddleWare interface
func (mw CreateGlobalTableMiddleWareFunc) CreateGlobalTableMiddleWare(h CreateGlobalTableHandler) CreateGlobalTableHandler {
	return mw(h)
}

// CreateGlobalTable represents a CreateGlobalTable operation
type CreateGlobalTable struct {
	*Promise
	input       *ddb.CreateGlobalTableInput
	middleWares []CreateGlobalTableMiddleWare
}

// NewCreateGlobalTable creates a new CreateGlobalTable
func NewCreateGlobalTable(input *ddb.CreateGlobalTableInput, mws ...CreateGlobalTableMiddleWare) *CreateGlobalTable {
	return &CreateGlobalTable{
		Promise: NewPromise(),
		input:       input,
		middleWares: mws,
	}
}

// Invoke invokes the CreateGlobalTable operation and returns a CreateGlobalTablePromise
func (op *CreateGlobalTable) Invoke(ctx context.Context, client *ddb.Client) *CreateGlobalTable {
	go op.DynoInvoke(ctx, client)

	return op
}

// DynoInvoke implements the Operation interface
func (op *CreateGlobalTable) DynoInvoke(ctx context.Context, client *ddb.Client) {
	output := new(CreateGlobalTableOutput)

	defer func() { op.SetResponse(output.Get()) }()

	requestCtx := &CreateGlobalTableContext{
		Context: ctx,
		client:  client,
		input:   op.input,
	}

	var h CreateGlobalTableHandler
	h = new(CreateGlobalTableFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].CreateGlobalTableMiddleWare(h)
		}
	}

	h.HandleCreateGlobalTable(requestCtx, output)
}

// Await waits for the CreateGlobalTablePromise to be fulfilled and then returns a CreateGlobalTableOutput and error
func (op *CreateGlobalTable) Await() (*ddb.CreateGlobalTableOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.CreateGlobalTableOutput), err
}

// CreateGlobalTableBuilder is used to construct a CreateGlobalTableBuilder dynamically
type CreateGlobalTableBuilder struct {
	*ddb.CreateGlobalTableInput
	regions map[string]struct{}
}

// AddReplication adds a Replica to the ReplicationGroup
func (bld *CreateGlobalTableBuilder) AddReplication(r types.Replica)  *CreateGlobalTableBuilder{
	bld.ReplicationGroup = append(bld.ReplicationGroup, r)
	return bld
}

// AddReplicaInRegion adds a Replica with the provided region to the ReplicationGroup
func (bld *CreateGlobalTableBuilder) AddReplicaInRegion(region string) *CreateGlobalTableBuilder {
	if _, ok := bld.regions[region]; ok {
		return bld
	}

	bld.ReplicationGroup = append(bld.ReplicationGroup, types.Replica{RegionName: &region})

	return bld
}

// Build returns the CreateGlobalTableInput
func (bld *CreateGlobalTableBuilder) Build() *ddb.CreateGlobalTableInput {
	return bld.CreateGlobalTableInput
}
