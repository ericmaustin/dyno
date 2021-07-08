package dyno

import (
	"context"
	"fmt"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"sync"
)

// CreateTable creates a new CreateTable, invokes and returns it
func (c *Client) CreateTable(ctx context.Context, input *ddb.CreateTableInput, mw ...CreateTableMiddleWare) *CreateTable {
	return NewCreateTable(input, mw...).Invoke(ctx, c.ddb)
}

// CreateTable creates a new CreateTable, passes it to the Pool and then returns the CreateTable
func (p *Pool) CreateTable(input *ddb.CreateTableInput, mw ...CreateTableMiddleWare) *CreateTable {
	op := NewCreateTable(input, mw...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// CreateTableContext represents an exhaustive CreateTable operation request context
type CreateTableContext struct {
	context.Context
	Input  *ddb.CreateTableInput
	Client *ddb.Client
}

// CreateTableOutput represents the output for the CreateTable opration
type CreateTableOutput struct {
	out *ddb.CreateTableOutput
	err error
	mu  sync.RWMutex
}

// Set sets the output
func (o *CreateTableOutput) Set(out *ddb.CreateTableOutput, err error) {
	o.mu.Lock()
	o.out = out
	o.err = err
	o.mu.Unlock()
}

// Get gets the output
func (o *CreateTableOutput) Get() (out *ddb.CreateTableOutput, err error) {
	o.mu.Lock()
	out = o.out
	err = o.err
	o.mu.Unlock()

	return
}

// CreateTableHandler represents a handler for CreateTable requests
type CreateTableHandler interface {
	HandleCreateTable(ctx *CreateTableContext, output *CreateTableOutput)
}

// CreateTableHandlerFunc is a CreateTableHandler function
type CreateTableHandlerFunc func(ctx *CreateTableContext, output *CreateTableOutput)

// HandleCreateTable implements CreateTableHandler
func (h CreateTableHandlerFunc) HandleCreateTable(ctx *CreateTableContext, output *CreateTableOutput) {
	h(ctx, output)
}

// CreateTableFinalHandler is the final CreateTableHandler that executes a dynamodb CreateTable operation
type CreateTableFinalHandler struct{}

// HandleCreateTable implements the CreateTableHandler
func (h *CreateTableFinalHandler) HandleCreateTable(ctx *CreateTableContext, output *CreateTableOutput) {
	output.Set(ctx.Client.CreateTable(ctx, ctx.Input))
}

// CreateTableMiddleWare is a middleware function use for wrapping CreateTableHandler requests
type CreateTableMiddleWare interface {
	CreateTableMiddleWare(next CreateTableHandler) CreateTableHandler
}

// CreateTableMiddleWareFunc is a functional CreateTableMiddleWare
type CreateTableMiddleWareFunc func(next CreateTableHandler) CreateTableHandler

// CreateTableMiddleWare implements the CreateTableMiddleWare interface
func (mw CreateTableMiddleWareFunc) CreateTableMiddleWare(h CreateTableHandler) CreateTableHandler {
	return mw(h)
}

// CreateTable represents a CreateTable operation
type CreateTable struct {
	*Promise
	input       *ddb.CreateTableInput
	middleWares []CreateTableMiddleWare
}

// NewCreateTable creates a new CreateTable
func NewCreateTable(input *ddb.CreateTableInput, mws ...CreateTableMiddleWare) *CreateTable {
	return &CreateTable{
		Promise:     NewPromise(),
		input:       input,
		middleWares: mws,
	}
}

// Invoke invokes the CreateTable operation in a goroutine and returns a BatchGetItemAllPromise
func (op *CreateTable) Invoke(ctx context.Context, client *ddb.Client) *CreateTable {
	op.SetWaiting() // promise now waiting for a response

	go op.invoke(ctx, client)

	return op
}

// DynoInvoke implements the Operation interface
func (op *CreateTable) DynoInvoke(ctx context.Context, client *ddb.Client) {
	op.SetWaiting() // promise now waiting for a response
	op.invoke(ctx, client)
}

// invoke invokes the CreateTable operation
func (op *CreateTable) invoke(ctx context.Context, client *ddb.Client) {
	output := new(CreateTableOutput)

	defer func() { op.SetResponse(output.Get()) }()

	requestCtx := &CreateTableContext{
		Context: ctx,
		Client:  client,
		Input:   op.input,
	}

	var h CreateTableHandler

	h = new(CreateTableFinalHandler)

	// no middlewares
	if len(op.middleWares) > 0 {
		// loop in reverse to preserve middleware order
		for i := len(op.middleWares) - 1; i >= 0; i-- {
			h = op.middleWares[i].CreateTableMiddleWare(h)
		}
	}

	h.HandleCreateTable(requestCtx, output)
}

// Await waits for the CreateTablePromise to be fulfilled and then returns a CreateTableOutput and error
func (op *CreateTable) Await() (*ddb.CreateTableOutput, error) {
	out, err := op.Promise.Await()
	if out == nil {
		return nil, err
	}

	return out.(*ddb.CreateTableOutput), err
}

// CreateTableBuilder is used to construct a CreateTableBuilder dynamically
type CreateTableBuilder struct {
	*ddb.CreateTableInput
	pendingAttributeDefinitions []ddbTypes.AttributeDefinition
}

// NewCreateTableBuilder creates a new CreateTableBuilder
func NewCreateTableBuilder() *CreateTableBuilder {
	return &CreateTableBuilder{
		CreateTableInput: &ddb.CreateTableInput{
			BillingMode: ddbTypes.BillingModePayPerRequest,
		},
	}
}

// SetProvisionedThroughputCapacityUnits sets the provisioned write and read throughput for this table
func (bld *CreateTableBuilder) SetProvisionedThroughputCapacityUnits(rcu, wcu int64) *CreateTableBuilder {
	bld.SetProvisionedThroughput(&ddbTypes.ProvisionedThroughput{
		ReadCapacityUnits:  &rcu,
		WriteCapacityUnits: &wcu,
	})

	bld.BillingMode = ddbTypes.BillingModeProvisioned

	return bld
}

// AddAttributeDefinition adds an attribute definition to the builder
func (bld *CreateTableBuilder) AddAttributeDefinition(attribute ddbTypes.AttributeDefinition) *CreateTableBuilder {
	bld.pendingAttributeDefinitions = append(bld.pendingAttributeDefinitions, attribute)
	return bld
}

// AddGlobalIndex adds one or more local global indexes to the builder
func (bld *CreateTableBuilder) AddGlobalIndex(gsi ...ddbTypes.GlobalSecondaryIndex) *CreateTableBuilder {
	bld.GlobalSecondaryIndexes = append(bld.GlobalSecondaryIndexes, gsi...)
	return bld
}

// AddLocalIndex adds one or more local secondary indexes to the builder
func (bld *CreateTableBuilder) AddLocalIndex(lsi ...ddbTypes.LocalSecondaryIndex) *CreateTableBuilder {
	bld.LocalSecondaryIndexes = append(bld.LocalSecondaryIndexes, lsi...)
	return bld
}

// AddTag adds a tag to the CreateTableBuilder using a key value pair of strings
func (bld *CreateTableBuilder) AddTag(key, value string) *CreateTableBuilder {
	bld.Tags = append(bld.Tags, ddbTypes.Tag{
		Key:   &key,
		Value: &value,
	})

	return bld
}

// AddTags adds tags to the CreateTableBuilder
func (bld *CreateTableBuilder) AddTags(tags ...ddbTypes.Tag) *CreateTableBuilder {
	bld.Tags = append(bld.Tags, tags...)
	return bld
}

// AddTagsFromMap adds tags to the CreateTableBuilder using a map of strings
func (bld *CreateTableBuilder) AddTagsFromMap(tags map[string]string) *CreateTableBuilder {
	for key, value := range tags {
		bld.AddTag(key, value)
	}

	return bld
}

// SetAttributeDefinitions sets the AttributeDefinitions field's value.
func (bld *CreateTableBuilder) SetAttributeDefinitions(v []ddbTypes.AttributeDefinition) *CreateTableBuilder {
	bld.AttributeDefinitions = v
	return bld
}

// SetBillingMode sets the BillingMode field's value.
func (bld *CreateTableBuilder) SetBillingMode(v ddbTypes.BillingMode) *CreateTableBuilder {
	bld.BillingMode = v
	return bld
}

// SetGlobalSecondaryIndexes sets the GlobalSecondaryIndexes field's value.
func (bld *CreateTableBuilder) SetGlobalSecondaryIndexes(v []ddbTypes.GlobalSecondaryIndex) *CreateTableBuilder {
	bld.GlobalSecondaryIndexes = v
	return bld
}

// SetKeySchema sets the KeySchema field's value.
func (bld *CreateTableBuilder) SetKeySchema(v []ddbTypes.KeySchemaElement) *CreateTableBuilder {
	bld.KeySchema = v
	return bld
}

// SetLocalSecondaryIndexes sets the LocalSecondaryIndexes field's value.
func (bld *CreateTableBuilder) SetLocalSecondaryIndexes(v []ddbTypes.LocalSecondaryIndex) *CreateTableBuilder {
	bld.LocalSecondaryIndexes = v
	return bld
}

// SetProvisionedThroughput sets the ProvisionedThroughput field's value.
func (bld *CreateTableBuilder) SetProvisionedThroughput(v *ddbTypes.ProvisionedThroughput) *CreateTableBuilder {
	bld.ProvisionedThroughput = v
	return bld
}

// SetSSESpecification sets the SSESpecification field's value.
func (bld *CreateTableBuilder) SetSSESpecification(v *ddbTypes.SSESpecification) *CreateTableBuilder {
	bld.SSESpecification = v
	return bld
}

// SetStreamSpecification sets the StreamSpecification field's value.
func (bld *CreateTableBuilder) SetStreamSpecification(v *ddbTypes.StreamSpecification) *CreateTableBuilder {
	bld.StreamSpecification = v
	return bld
}

// SetTableName sets the TableName field's value.
func (bld *CreateTableBuilder) SetTableName(v string) *CreateTableBuilder {
	bld.TableName = &v
	return bld
}

// SetTags sets the Tags field's value.
func (bld *CreateTableBuilder) SetTags(v []ddbTypes.Tag) *CreateTableBuilder {
	bld.Tags = v
	return bld
}

// Build builds the dynamodb.CreateTableBuilder
func (bld *CreateTableBuilder) Build() (*ddb.CreateTableInput, error) {
	if bld.pendingAttributeDefinitions != nil {
		for _, ad := range bld.pendingAttributeDefinitions {
			for _, attr := range bld.AttributeDefinitions {
				// don't add duplicate attribute names
				if *attr.AttributeName == *ad.AttributeName {
					if attr.AttributeType == ad.AttributeType {
						continue
					}

					return nil, fmt.Errorf("cannot add duplicate attribute with mismatched type."+
						"attrubuteName = %s, attributeTypes = %s, %s",
						*attr.AttributeName, attr.AttributeType, ad.AttributeType)
				}
			}
		}
	}

	if bld.GlobalSecondaryIndexes != nil {
		for _, gsi := range bld.GlobalSecondaryIndexes {
			if bld.BillingMode == ddbTypes.BillingModePayPerRequest {
				gsi.ProvisionedThroughput = nil
			}
		}
	}

	return bld.CreateTableInput, nil
}
