package operation

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ericmaustin/dyno"
	"github.com/ericmaustin/dyno/encoding"
)

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
func NewCreateTableBuilder(input *dynamodb.CreateTableInput) *CreateTableBuilder {
	b := &CreateTableBuilder{}
	if input != nil {
		b.input = input
	} else {
		b.input = &dynamodb.CreateTableInput{}
	}
	if b.input.AttributeDefinitions == nil {
		b.input.AttributeDefinitions = []*dynamodb.AttributeDefinition{}
	}
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

// Input builds the CreateTableInput
func (b *CreateTableBuilder) Input() *dynamodb.CreateTableInput {
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
	return CreateTable(b.Input())
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
	return c.Output(), c.err
}

type CreateTableOperation struct {
	*Base
	wait  bool
	input *dynamodb.CreateTableInput
}

// CreateTable creates a new CreateTableOperation with optional CreateTableInput input
func CreateTable(input *dynamodb.CreateTableInput) *CreateTableOperation {
	c := &CreateTableOperation{
		Base:  newBase(),
		input: input,
	}
	return c
}

// SetInput sets the input to be used for this CreateTableOperation
// panics with an InvalidState error if operation isn't pending
func (c *CreateTableOperation) SetInput(input *dynamodb.CreateTableInput) *CreateTableOperation {
	if !c.IsPending() {
		panic(&InvalidState{})
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.input = input
	return c
}

// SetWait will set the wait flag to the the wait bool value
// if true, then the CreateTableOperation will wait for the table to become available before returning
// panics with an InvalidState error if operation isn't pending
func (c *CreateTableOperation) SetWait(wait bool) *CreateTableOperation {
	if !c.IsPending() {
		panic(&InvalidState{})
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.wait = wait
	return c
}

// Execute executes the CreateTableOperation request
func (c *CreateTableOperation) ExecuteInBatch(req *dyno.Request) Result {
	return c.Execute(req)
}

// Execute executes the CreateTableOperation request
func (c *CreateTableOperation) Execute(req *dyno.Request) (out *CreateTableResult) {
	out = &CreateTableResult{}
	c.setRunning()
	defer c.setDone(out)
	out.output, out.err = req.CreateTable(c.input)
	if c.wait {
		if out.err != nil && dyno.IsAwsErrorCode(out.Error(), dynamodb.ErrCodeResourceInUseException) {
			tblDescOut, tblDescErr := DescribeTable(*c.input.TableName).Execute(req).OutputError()
			if tblDescErr != nil {
				out.err = tblDescErr
			} else {
				out.output = &dynamodb.CreateTableOutput{TableDescription: tblDescOut.Table}
			}
		} else if out.err != nil {
			return
		}
		_, out.err = WaitForTableReady(req, *c.input.TableName, nil)
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
