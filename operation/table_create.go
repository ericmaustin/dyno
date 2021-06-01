package operation

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ericmaustin/dyno"
	"github.com/ericmaustin/dyno/encoding"
)

//TableBillingMode used to set what the table's billing mode should be
type TableBillingMode string

const (
	// TableBillingModeOnDemand is used as the billing mode when using PAY PER REQUEST or ON DEMAND billing
	TableBillingModeOnDemand = TableBillingMode("PAY_PER_REQUEST")
	// TableBillingModeProvisioned is used as the billing mode when Provisioned billing is used
	TableBillingModeProvisioned = TableBillingMode("PROVISIONED")
)

// CreateTableBuilder is used to construct a CreateTableInput dynamically
type CreateTableBuilder struct {
	input *dynamodb.CreateTableInput
}

// NewCreateTableBuilder creates a new CreateTableBuilder
func NewCreateTableBuilder() *CreateTableBuilder {
	b := &CreateTableBuilder{
		input: &dynamodb.CreateTableInput{},
	}
	if b.input.AttributeDefinitions == nil {
		b.input.AttributeDefinitions = []*dynamodb.AttributeDefinition{}
	}
	return b
}

// SetInput sets the CreateTableBuilder's dynamodb.CreateTableInput
func (b *CreateTableBuilder) SetInput(input *dynamodb.CreateTableInput) *CreateTableBuilder {
	b.input = input
	return b
}

// SetName sets the name for the table to be created
func (b *CreateTableBuilder) SetName(name interface{}) *CreateTableBuilder {
	nameStr := encoding.ToString(name)
	b.input.SetTableName(nameStr)
	return b
}

// SetProvisionedThroughput sets the provisioned throughput for this table
func (b *CreateTableBuilder) SetProvisionedThroughput(rcu, wcu int64) *CreateTableBuilder {
	b.input.SetProvisionedThroughput(&dynamodb.ProvisionedThroughput{
		ReadCapacityUnits:  &rcu,
		WriteCapacityUnits: &wcu,
	})
	return b
}

// AddAttributeDefinition adds an attribute definition to the builder
func (b *CreateTableBuilder) AddAttributeDefinition(attribute *dynamodb.AttributeDefinition) {
	for _, attr := range b.input.AttributeDefinitions {
		// don't add duplicate attribute names
		if *attr.AttributeName == *attribute.AttributeName {
			if *attr.AttributeType == *attribute.AttributeType {
				return
			}
			panic(fmt.Errorf("cannot add duplicate attribute with mismatched type."+
				"attrubuteName = %s, attributeTypes = %s, %s",
				*attr.AttributeName, *attr.AttributeType, *attribute.AttributeType))
		}
	}
	b.input.AttributeDefinitions = append(b.input.AttributeDefinitions, attribute)
}

// AddGlobalIndex adds one or more local global indexes to the builder
func (b *CreateTableBuilder) AddGlobalIndex(gsi ...*dynamodb.GlobalSecondaryIndex) *CreateTableBuilder {
	if b.input.GlobalSecondaryIndexes == nil {
		b.input.GlobalSecondaryIndexes = gsi
		return b
	}
	b.input.GlobalSecondaryIndexes = append(b.input.GlobalSecondaryIndexes, gsi...)
	return b
}

// AddLocalIndex adds one or more local secondary indexes to the builder
func (b *CreateTableBuilder) AddLocalIndex(lsi ...*dynamodb.LocalSecondaryIndex) *CreateTableBuilder {
	if b.input.LocalSecondaryIndexes == nil {
		b.input.LocalSecondaryIndexes = lsi
		return b
	}
	b.input.LocalSecondaryIndexes = append(b.input.LocalSecondaryIndexes, lsi...)
	return b
}

// SetKeySchema sets the key schema for the input
func (b *CreateTableBuilder) SetKeySchema(keySchema []*dynamodb.KeySchemaElement) {
	b.input.KeySchema = keySchema
}

// AddTag adds a tag to the CreateTableInput using a key value pair of strings
func (b *CreateTableBuilder) AddTag(key, value string) *CreateTableBuilder {
	tag := &dynamodb.Tag{
		Key:   &key,
		Value: &value,
	}

	if b.input.Tags == nil {
		b.input.Tags = []*dynamodb.Tag{tag}
		return b
	}
	b.input.Tags = append(b.input.Tags, tag)
	return b
}

// AddTags adds tags to the CreateTableInput
func (b *CreateTableBuilder) AddTags(tags ...*dynamodb.Tag) *CreateTableBuilder {
	if b.input.Tags == nil {
		b.input.Tags = tags
		return b
	}
	b.input.Tags = append(b.input.Tags, tags...)
	return b
}

// AddTagsFromMap adds tags to the CreateTableInput using a map of strings
func (b *CreateTableBuilder) AddTagsFromMap(tags map[string]string) *CreateTableBuilder {
	for key, value := range tags {
		b.AddTag(key, value)
	}
	return b
}

// Build builds the CreateTableInput
func (b *CreateTableBuilder) Build() *dynamodb.CreateTableInput {
	if b.input.ProvisionedThroughput != nil {
		b.input.SetBillingMode(string(TableBillingModeProvisioned))
	} else {
		b.input.SetBillingMode(string(TableBillingModeOnDemand))
	}
	if b.input.GlobalSecondaryIndexes != nil {
		for _, gsi := range b.input.GlobalSecondaryIndexes {
			if *b.input.BillingMode == string(TableBillingModeOnDemand) {
				gsi.ProvisionedThroughput = nil
			}
		}
	}
	return b.input
}

// Operation returns the CreateTableOperation using the builder's input
func (b *CreateTableBuilder) Operation() *CreateTableOperation {
	return CreateTable(b.Build())
}

// CreateTableResult returned by CreateTableOperation
type CreateTableResult struct {
	ResultBase
	output *dynamodb.CreateTableOutput
}

// OutputInterface returns the CreateTableOutput as an interface from the CreateTableResult
func (c *CreateTableResult) OutputInterface() interface{} {
	return c.output
}

// Output returns the CreateTableOutput from the CreateTableResult
func (c *CreateTableResult) Output() *dynamodb.CreateTableOutput {
	return c.output
}

// OutputError returns the CreateTableOutput and the error from the CreateTableResult for convenience
func (c *CreateTableResult) OutputError() (*dynamodb.CreateTableOutput, error) {
	return c.Output(), c.Err
}

//CreateTableOperation used as input to the CreateTable func
type CreateTableOperation struct {
	*BaseOperation
	wait  bool
	input *dynamodb.CreateTableInput
}

// CreateTable creates a new CreateTableOperation with optional CreateTableInput input
func CreateTable(input *dynamodb.CreateTableInput) *CreateTableOperation {
	c := &CreateTableOperation{
		BaseOperation: NewBase(),
		input:         input,
	}
	return c
}

// SetInput sets the input to be used for this CreateTableOperation
// panics with an ErrInvalidState error if operation isn't pending
func (c *CreateTableOperation) SetInput(input *dynamodb.CreateTableInput) *CreateTableOperation {
	if !c.IsPending() {
		panic(&ErrInvalidState{})
	}
	c.Mu.Lock()
	defer c.Mu.Unlock()
	c.input = input
	return c
}

// SetWait will set the wait flag to the the wait bool value
// if true, then the CreateTableOperation will wait for the table to become available before returning
// panics with an ErrInvalidState error if operation isn't pending
func (c *CreateTableOperation) SetWait(wait bool) *CreateTableOperation {
	if !c.IsPending() {
		panic(&ErrInvalidState{})
	}
	c.Mu.Lock()
	defer c.Mu.Unlock()
	c.wait = wait
	return c
}

// ExecuteInBatch executes the CreateTableOperation request
func (c *CreateTableOperation) ExecuteInBatch(req *dyno.Request) Result {
	return c.Execute(req)
}

// Execute executes the CreateTableOperation request
func (c *CreateTableOperation) Execute(req *dyno.Request) (out *CreateTableResult) {
	out = &CreateTableResult{}
	c.SetRunning()
	defer c.SetDone(out)
	out.output, out.Err = req.CreateTable(c.input)
	if c.wait {
		if out.Err != nil && dyno.IsAwsErrorCode(out.Error(), dynamodb.ErrCodeResourceInUseException) {
			tblDescOut, tblDescErr := DescribeTable(*c.input.TableName).Execute(req).OutputError()
			if tblDescErr != nil {
				out.Err = tblDescErr
			} else {
				out.output = &dynamodb.CreateTableOutput{TableDescription: tblDescOut.Table}
			}
		} else if out.Err != nil {
			return
		}
		_, out.Err = WaitForTableReady(req, *c.input.TableName, nil)
	}
	return
}

// GoExecute executes the CreateTableOperation request in a go routine and returns a channel
// that will return a CreateTableResult when operation is done
func (c *CreateTableOperation) GoExecute(req *dyno.Request) <-chan *CreateTableResult {
	outCh := make(chan *CreateTableResult)
	go func() {
		defer close(outCh)
		outCh <- c.Execute(req)
	}()
	return outCh
}
