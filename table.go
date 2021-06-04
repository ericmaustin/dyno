package dyno

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/ericmaustin/dyno/condition"
)

// Table represents a dynamodb table
type Table struct {
	*dynamodb.TableDescription
	LastSync time.Time
	// map of global secondary indexes
	GSIs map[string]*GSI
	// map of local secondary indexes
	LSIs                            map[string]*LSI
	PartitionKeyAttributeDefinition *dynamodb.AttributeDefinition
	SortKeyAttributeDefinition      *dynamodb.AttributeDefinition
	Tags                            []*dynamodb.Tag
}

// Name returns this table's name as a string
// will panic if name is nil
func (t *Table) Name() string {
	return *t.TableName
}

//IsOnDemand returns true if the table is set to On Demand pricing
func (t *Table) IsOnDemand() bool {
	if t.BillingModeSummary == nil {
		// default to pay per request
		t.BillingModeSummary = new(dynamodb.BillingModeSummary)
		t.BillingModeSummary.SetBillingMode(dynamodb.BillingModePayPerRequest)
	}
	return *t.BillingModeSummary.BillingMode == dynamodb.BillingModePayPerRequest
}

// RCUs returns the read cost units for this table
func (t *Table) RCUs() int64 {
	if t.IsOnDemand() || t.ProvisionedThroughput.ReadCapacityUnits == nil {
		return 0
	}
	return *t.ProvisionedThroughput.ReadCapacityUnits
}

// WCUs returns the write cost units for this table
func (t *Table) WCUs() int64 {
	if t.IsOnDemand() || t.ProvisionedThroughput.WriteCapacityUnits == nil {
		return 0
	}
	return *t.ProvisionedThroughput.WriteCapacityUnits
}

// Description returns the table description
func (t *Table) Description() *dynamodb.TableDescription {
	return t.TableDescription
}

// Sync updates this table's description with the remote dynamodb table
func (t *Table) Sync(req *Request) error {
	out, err := req.DescribeTable(&dynamodb.DescribeTableInput{TableName: t.TableName}, nil)
	if err != nil {
		return err
	}

	if out.Table != nil {
		t.Set(out.Table)
	}

	return nil
}

// Set sets the table description to the input value
func (t *Table) Set(input *dynamodb.TableDescription) {
	t.LastSync = time.Now()
	*t.TableDescription = *input
}

// SortKeyName returns the table's sort key
func (t *Table) SortKeyName() *string {
	if len(t.KeySchema) == 0 {
		return nil
	}
	return getSortKeyNameFromKeySchema(t.KeySchema)
}

// PartitionKeyName returns the partition key name
func (t *Table) PartitionKeyName() *string {
	if len(t.KeySchema) == 0 {
		return nil
	}
	return getPartitionKeyNameFromKeySchema(t.KeySchema)
}

// SetPartitionKey sets the partition key for this table
func (t *Table) SetPartitionKey(pkName string, attributeType string) *Table {
	if _, ok := validAttributeTypes[attributeType]; !ok {
		panic(fmt.Errorf("attribute type %s is not valid", attributeType))
	}

	t.KeySchema = addPartitionKeyToKeySchema(t.KeySchema, pkName)
	t.PartitionKeyAttributeDefinition = &dynamodb.AttributeDefinition{
		AttributeName: &pkName,
		AttributeType: &attributeType,
	}
	t.AttributeDefinitions = appendUniqueAttributeDefinitions(t.AttributeDefinitions, t.PartitionKeyAttributeDefinition)
	return t
}

// SetSortKey sets the sortKey key for this table
func (t *Table) SetSortKey(skName string, attributeType string) *Table {
	if _, ok := validAttributeTypes[attributeType]; !ok {
		panic(fmt.Errorf("attribute type %s is not valid", attributeType))
	}

	t.KeySchema = addSortKeyToKeySchema(t.KeySchema, skName)
	t.SortKeyAttributeDefinition = &dynamodb.AttributeDefinition{
		AttributeName: &skName,
		AttributeType: &attributeType,
	}
	t.AttributeDefinitions = appendUniqueAttributeDefinitions(t.AttributeDefinitions, t.SortKeyAttributeDefinition)
	return t
}

// AddGSI adds a new GSI for this table
// provided GSI must have an IndexName or this func will panic
func (t *Table) AddGSI(gsi ...*GSI) *Table {

	for _, g := range gsi {
		gsiDesc := &dynamodb.GlobalSecondaryIndexDescription{
			IndexName:  g.IndexName,
			KeySchema:  g.KeySchema,
			Projection: g.Projection,
		}

		if g.ProvisionedThroughput != nil {
			gsiDesc.ProvisionedThroughput = &dynamodb.ProvisionedThroughputDescription{
				ReadCapacityUnits:  g.ProvisionedThroughput.ReadCapacityUnits,
				WriteCapacityUnits: g.ProvisionedThroughput.WriteCapacityUnits,
			}
		}

		t.GlobalSecondaryIndexes = append(t.GlobalSecondaryIndexes, gsiDesc)

		if t.GSIs == nil {
			t.GSIs = make(map[string]*GSI)
		}

		t.GSIs[*g.IndexName] = g

		if g.PartitionKeyAttributeDefinition != nil {
			t.AttributeDefinitions = appendUniqueAttributeDefinitions(t.AttributeDefinitions, g.PartitionKeyAttributeDefinition)
		}
		if g.SortKeyAttributeDefinition != nil {
			t.AttributeDefinitions = appendUniqueAttributeDefinitions(t.AttributeDefinitions, g.SortKeyAttributeDefinition)
		}
	}

	return t
}

// AddLSI adds a new LSI attached to this table
// provided LSI must have an IndexName or this func will panic
func (t *Table) AddLSI(lsi ...*LSI) {

	for _, l := range lsi {
		lsiDesc := &dynamodb.LocalSecondaryIndexDescription{
			IndexName:  l.IndexName,
			KeySchema:  l.KeySchema,
			Projection: l.Projection,
		}

		t.LocalSecondaryIndexes = append(t.LocalSecondaryIndexes, lsiDesc)

		if t.LSIs == nil {
			t.LSIs = make(map[string]*LSI)
		}

		t.LSIs[*l.IndexName] = l

		if l.SortKeyAttributeDefinition != nil {
			t.AttributeDefinitions = appendUniqueAttributeDefinitions(t.AttributeDefinitions, l.SortKeyAttributeDefinition)
		}
	}
}

// ExtractKeys extracts key values from a dynamodb.AttributeValue map
func (t *Table) ExtractKeys(avMap map[string]*dynamodb.AttributeValue) map[string]*dynamodb.AttributeValue {
	return extractKeyAttributeValuesFromKeySchema(t.KeySchema, avMap)
}

// ExtractAllKeys extracts all key values from a slice of dynamodb.AttributeValue maps
func (t *Table) ExtractAllKeys(avMaps []map[string]*dynamodb.AttributeValue) []map[string]*dynamodb.AttributeValue {
	return extractAllKeyAttributeValuesFromKeySchema(t.KeySchema, avMaps)
}

// ExtractPartitionKeyValue extracts this table's partition key attribute value from a given input
// panics if this table does not have a partition key
func (t *Table) ExtractPartitionKeyValue(avMap map[string]*dynamodb.AttributeValue) *dynamodb.AttributeValue {
	return extractPartitionKeyAttributeValueFromKeySchema(t.KeySchema, avMap)
}

// ExtractSortKeyValue extracts this table's sort key attribute value from a given input
// panics if this table does not have a partition key
func (t *Table) ExtractSortKeyValue(avMap map[string]*dynamodb.AttributeValue) *dynamodb.AttributeValue {
	return extractSortKeyAttributeValueFromKeySchema(t.KeySchema, avMap)
}

//AddTag adds a tag with given key and value to the Table
func (t *Table) AddTag(key, value string) *Table {
	t.Tags = append(t.Tags, &dynamodb.Tag{
		Key:   &key,
		Value: &value,
	})
	return t
}

// GetCreateTableInput returns the table builder for this table
func (t *Table) GetCreateTableInput() (*dynamodb.CreateTableInput, error) {

	if t.PartitionKeyAttributeDefinition == nil {
		return nil, fmt.Errorf("PartitionKeyAttributeDefinition must not be nil")
	}

	if t.PartitionKeyAttributeDefinition.AttributeName == nil {
		return nil, fmt.Errorf("PartitionKeyAttributeDefinition.AttributeName must not be nil")
	}

	attributeDefinitionMap := map[string]*dynamodb.AttributeDefinition{
		*t.PartitionKeyAttributeDefinition.AttributeName: t.PartitionKeyAttributeDefinition,
	}

	in := &dynamodb.CreateTableInput{
		AttributeDefinitions: t.AttributeDefinitions,
		BillingMode:          t.BillingModeSummary.BillingMode,
		KeySchema:            t.KeySchema,
		StreamSpecification:  t.StreamSpecification,
		TableName:            t.TableName,
		Tags:                 t.Tags,
	}

	if t.ProvisionedThroughput != nil {
		in.ProvisionedThroughput = &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  t.ProvisionedThroughput.ReadCapacityUnits,
			WriteCapacityUnits: t.ProvisionedThroughput.WriteCapacityUnits,
		}
	}

	if t.SortKeyAttributeDefinition != nil {
		if t.SortKeyAttributeDefinition.AttributeName == nil {
			return nil, fmt.Errorf("SortKeyAttributeDefinition.AttributeName must not be nil")
		}
		attributeDefinitionMap[*t.SortKeyAttributeDefinition.AttributeName] = t.SortKeyAttributeDefinition
	}

	if len(t.GSIs) > 0 {
		for _, gsi := range t.GSIs {
			in.GlobalSecondaryIndexes = append(in.GlobalSecondaryIndexes, gsi.GlobalSecondaryIndex)
		}
	}

	if len(t.LSIs) > 0 {
		for _, lsi := range t.LSIs {
			in.LocalSecondaryIndexes = append(in.LocalSecondaryIndexes, lsi.LocalSecondaryIndex)
		}
	}

	return in, nil
}

// PublishTableOutput is returned by the TimeSpanMapper Publish method
type PublishTableOutput struct {
	CreateTableOutput *dynamodb.CreateTableOutput
	TableDescription  *dynamodb.TableDescription
	TableExists       bool
}

// NewTable creates new table with provided table name, table key, and and options
// Mapper key is required
func NewTable(name string) *Table {
	// create a table with given table key
	return &Table{
		TableDescription: &dynamodb.TableDescription{
			AttributeDefinitions: nil,
			BillingModeSummary: &dynamodb.BillingModeSummary{
				BillingMode: StringPtr(dynamodb.BillingModePayPerRequest),
			},
			TableName: &name,
		},
	}
}

// UpdateCostUnitsOutput is the output of the update cost units operation
type UpdateCostUnitsOutput struct {
	Changed bool
}

// GsiCostUpdate used as input to SetCostUnits when updating cost units for GSI's
type GsiCostUpdate struct {
	IndexName string
	Wcu       int64
	Rcu       int64
}

// SetName sets the name for this table
func (t *Table) SetName(name string) *Table {
	t.TableName = &name
	return t
}

//SetOnDemand sets the table to On Demand billling mode
func (t *Table) SetOnDemand() *Table {
	if t.BillingModeSummary == nil {
		t.BillingModeSummary = new(dynamodb.BillingModeSummary)
	}
	t.BillingModeSummary.SetBillingMode(dynamodb.BillingModePayPerRequest)
	return t
}

// SetReadCostUnits sets the read cost units for this table
func (t *Table) SetReadCostUnits(costUnits int64) *Table {
	if t.BillingModeSummary == nil {
		t.BillingModeSummary = new(dynamodb.BillingModeSummary)
	}
	if t.ProvisionedThroughput == nil {
		t.ProvisionedThroughput = new(dynamodb.ProvisionedThroughputDescription)
	}
	t.BillingModeSummary.SetBillingMode(dynamodb.BillingModeProvisioned)
	t.ProvisionedThroughput.SetReadCapacityUnits(costUnits)
	return t
}

// SetWriteCostUnits sets the write cost units for this table
func (t *Table) SetWriteCostUnits(costUnits int64) *Table {
	if t.BillingModeSummary == nil {
		t.BillingModeSummary = new(dynamodb.BillingModeSummary)
	}
	if t.ProvisionedThroughput == nil {
		t.ProvisionedThroughput = new(dynamodb.ProvisionedThroughputDescription)
	}
	t.BillingModeSummary.SetBillingMode(dynamodb.BillingModeProvisioned)
	t.ProvisionedThroughput.SetWriteCapacityUnits(costUnits)
	return t
}

// SetCostUnits sets both the read and write cost units
func (t *Table) SetCostUnits(rcus, wcus int64) *Table {
	if rcus == 0 && wcus == 0 {
		t.SetOnDemand()
		return t
	}
	if t.BillingModeSummary == nil {
		t.BillingModeSummary = new(dynamodb.BillingModeSummary)
	}
	if t.ProvisionedThroughput == nil {
		t.ProvisionedThroughput = new(dynamodb.ProvisionedThroughputDescription)
	}
	t.BillingModeSummary.SetBillingMode(dynamodb.BillingModeProvisioned)
	t.ProvisionedThroughput.SetReadCapacityUnits(rcus)
	t.ProvisionedThroughput.SetWriteCapacityUnits(wcus)
	return t
}

// UniqueKeyCondition returns a Builder that represents a unique key condition
func (t *Table) UniqueKeyCondition() *expression.ConditionBuilder {
	cndBuilder := new(condition.Builder)
	if t.PartitionKeyAttributeDefinition == nil ||
		t.PartitionKeyAttributeDefinition.AttributeName != nil {
		return nil
	}
	cndBuilder.And(condition.NotExists(*t.PartitionKeyAttributeDefinition.AttributeName))
	// if we have a sortKey key make sure document contains a key
	if t.SortKeyAttributeDefinition == nil ||
		t.SortKeyAttributeDefinition.AttributeName != nil {
		cndBuilder.And(condition.NotExists(*t.SortKeyAttributeDefinition.AttributeName))
	}
	builder := cndBuilder.Builder()
	return &builder
}

// CreateBackupInput creates a CreateBackupInput for this table
func (t *Table) CreateBackupInput(backupName string) *dynamodb.CreateBackupInput {
	return &dynamodb.CreateBackupInput{
		BackupName: t.TableName,
		TableName:  &backupName,
	}
}

// Publish creates this table in dynamodb with an api call
// returns a channel that will return a PublishResult when table is ready
func (t *Table) Publish(req *Request) <-chan *TableReadyOutput {
	doneCh := make(chan *TableReadyOutput)
	go func() {
		result := new(TableReadyOutput)
		defer func() {
			doneCh <- result
			close(doneCh)
		}()
		tblInput, err := t.GetCreateTableInput()
		if err != nil {
			result.Err = err
			return
		}
		_, err = req.CreateTable(tblInput, nil)
		if err != nil {
			if !IsAwsErrorCode(err, dynamodb.ErrCodeResourceInUseException) {
				result.Err = err
				return
			}

			// table already created
			tblDescOut, tblDescErr := req.DescribeTable(&dynamodb.DescribeTableInput{TableName: t.TableName}, nil)
			if tblDescErr != nil {
				result.Err = tblDescErr
				return
			}
			result.DescribeTableOutput = tblDescOut
			// set the table (heh)
			t.Set(result.Table)
			return
		}

		// wait for the table to be ready
		result = <-req.ListenForTableReady(*t.TableName)
	}()
	return doneCh
}

// Backup creates a backup of this table
// returns a channel that will return a BackupResult when backup completes
func (t *Table) Backup(req *Request, backupName string) <-chan *BackupCompletionOutput {
	doneCh := make(chan *BackupCompletionOutput)
	go func() {
		result := new(BackupCompletionOutput)
		defer func() {
			doneCh <- result
			close(doneCh)
		}()

		_, err := req.CreateBackup(&dynamodb.CreateBackupInput{
			BackupName: &backupName,
			TableName:  t.TableName,
		}, nil)

		if err != nil {
			result.Err = err
			return
		}

		// wait for the backup to be completed
		result = <-req.ListenForBackupCompleted(*t.TableName)
	}()
	return doneCh
}

// Delete deletes this table in dynamodb
// returns a channel that will return an error (nil if successful) when complete
func (t *Table) Delete(req *Request) <-chan error {
	doneCh := make(chan error)
	go func() {
		var err error
		defer func() {
			doneCh <- err
			close(doneCh)
		}()

		_, err = req.DeleteTable(&dynamodb.DeleteTableInput{
			TableName: t.TableName,
		}, nil)

		if err != nil {
			return
		}
		// wait for the table to be deleted
		err = <-req.ListenForTableDeletion(*t.TableName)
	}()
	return doneCh
}

//NewPutItemBuilder creates a new PutItemBuilder
func (t *Table) NewPutItemBuilder(item map[string]*dynamodb.AttributeValue) *PutItemBuilder {
	p := &PutItemBuilder{
		PutItemInput: &dynamodb.PutItemInput{
			Item:      item,
			TableName: t.TableName,
		},
		cnd: new(condition.Builder),
	}
	return p
}

//PutItem puts an item on this table with a given Request
func (t *Table) PutItem(req *Request, item map[string]*dynamodb.AttributeValue) (*dynamodb.PutItemOutput, error) {
	p := t.NewPutItemBuilder(item)
	input, err := p.Build()
	if err != nil {
		return nil, err
	}
	return req.PutItem(input, nil)
}
