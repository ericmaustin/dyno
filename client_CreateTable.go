package dyno

import (
	"context"
	"fmt"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// CreateTable executes a scan api call with a CreateTableInput
func (c *DefaultClient) CreateTable(ctx context.Context, input *ddb.CreateTableInput, optFns ...func(*CreateTableOptions)) (*ddb.CreateTableOutput, error) {
	op := NewCreateTable(input, optFns...)
	op.DynoInvoke(ctx, c.ddb)

	return op.Await()
}

// CreateTable executes a CreateTable operation with a CreateTableInput in this pool and returns the CreateTable for processing
func (p *Pool) CreateTable(input *ddb.CreateTableInput, optFns ...func(*CreateTableOptions)) *CreateTable {
	op := NewCreateTable(input, optFns...)

	if err := p.Do(op); err != nil {
		op.SetResponse(nil, err)
	}

	return op
}

// CreateTableInputCallback is a callback that is called on a given CreateTableInput before a CreateTable operation api call executes
type CreateTableInputCallback interface {
	CreateTableInputCallback(context.Context, *ddb.CreateTableInput) (*ddb.CreateTableOutput, error)
}

// CreateTableOutputCallback is a callback that is called on a given CreateTableOutput after a CreateTable operation api call executes
type CreateTableOutputCallback interface {
	CreateTableOutputCallback(context.Context, *ddb.CreateTableOutput) error
}

// CreateTableInputCallbackF is CreateTableOutputCallback function
type CreateTableInputCallbackF func(context.Context, *ddb.CreateTableInput) (*ddb.CreateTableOutput, error)

// CreateTableInputCallback implements the CreateTableOutputCallback interface
func (cb CreateTableInputCallbackF) CreateTableInputCallback(ctx context.Context, input *ddb.CreateTableInput) (*ddb.CreateTableOutput, error) {
	return cb(ctx, input)
}

// CreateTableOutputCallbackF is CreateTableOutputCallback function
type CreateTableOutputCallbackF func(context.Context, *ddb.CreateTableOutput) error

// CreateTableOutputCallback implements the CreateTableOutputCallback interface
func (cb CreateTableOutputCallbackF) CreateTableOutputCallback(ctx context.Context, input *ddb.CreateTableOutput) error {
	return cb(ctx, input)
}

// CreateTableOptions represents options passed to the CreateTable operation
type CreateTableOptions struct {
	// InputCallbacks are called before the CreateTable dynamodb api operation with the dynamodb.CreateTableInput
	InputCallbacks []CreateTableInputCallback
	// OutputCallbacks are called after the CreateTable dynamodb api operation with the dynamodb.CreateTableOutput
	OutputCallbacks []CreateTableOutputCallback
}

// CreateTableWithInputCallback adds a CreateTableInputCallbackF to the InputCallbacks
func CreateTableWithInputCallback(cb CreateTableInputCallbackF) func(*CreateTableOptions) {
	return func(opt *CreateTableOptions) {
		opt.InputCallbacks = append(opt.InputCallbacks, cb)
	}
}

// CreateTableWithOutputCallback adds a CreateTableOutputCallback to the OutputCallbacks
func CreateTableWithOutputCallback(cb CreateTableOutputCallback) func(*CreateTableOptions) {
	return func(opt *CreateTableOptions) {
		opt.OutputCallbacks = append(opt.OutputCallbacks, cb)
	}
}

// CreateTable represents a CreateTable operation
type CreateTable struct {
	*Promise
	input   *ddb.CreateTableInput
	options CreateTableOptions
}

// NewCreateTable creates a new CreateTable operation on the given client with a given CreateTableInput and options
func NewCreateTable(input *ddb.CreateTableInput, optFns ...func(*CreateTableOptions)) *CreateTable {
	opts := CreateTableOptions{}

	for _, opt := range optFns {
		opt(&opts)
	}

	return &CreateTable{
		Promise: NewPromise(),
		input:   input,
		options: opts,
	}
}

// Await waits for the Operation to be complete and then returns a CreateTableOutput and error
func (op *CreateTable) Await() (*ddb.CreateTableOutput, error) {
	out, err := op.Promise.Await()

	if out == nil {
		return nil, err
	}

	return out.(*ddb.CreateTableOutput), err
}

// Invoke invokes the CreateTable operation
func (op *CreateTable) Invoke(ctx context.Context, client *ddb.Client) *CreateTable {
	go op.DynoInvoke(ctx, client)
	return op
}

// DynoInvoke implements the Operation interface
func (op *CreateTable) DynoInvoke(ctx context.Context, client *ddb.Client) {
	var (
		out *ddb.CreateTableOutput
		err error
	)

	defer func(){ op.SetResponse(out, err) }()

	for _, cb := range op.options.InputCallbacks {
		if out, err = cb.CreateTableInputCallback(ctx, op.input); out != nil || err != nil {
			return
		}
	}

	if out, err = client.CreateTable(ctx, op.input); err != nil {
		return
	}

	for _, cb := range op.options.OutputCallbacks {
		if err = cb.CreateTableOutputCallback(ctx, out); err != nil {
			return
		}
	}
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
