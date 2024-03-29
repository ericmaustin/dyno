package dyno

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ericmaustin/dyno/condition"
	"sync"
	"time"
)

//TODO: method to update dynamodb table

// Table represents a dynamodb table
type Table struct {
	*ddbTypes.TableDescription
	LastSync                        time.Time
	GSIs                            map[string]*GSI               // map of global secondary indexes
	LSIs                            map[string]*LSI               // map of local secondary indexes
	PartitionKeyAttributeDefinition *ddbTypes.AttributeDefinition // the Partition Key's AttributeDefinition
	SortKeyAttributeDefinition      *ddbTypes.AttributeDefinition // the Sort Key's AttributeDefinition
	Tags                            []ddbTypes.Tag
	mu                              sync.RWMutex
}

// Name returns this table's name as a string
// will panic if name is nil
func (t *Table) Name() string {
	t.mu.RLock()
	name := *t.TableName
	t.mu.RUnlock()

	return name
}

// IsOnDemand returns true if the table is set to On Demand pricing
func (t *Table) IsOnDemand() bool {
	t.mu.Lock()

	if t.BillingModeSummary == nil {
		// default to pay per request
		t.BillingModeSummary = new(ddbTypes.BillingModeSummary)
		t.BillingModeSummary = &ddbTypes.BillingModeSummary{
			BillingMode: ddbTypes.BillingModePayPerRequest,
		}
	}

	out := t.BillingModeSummary.BillingMode == ddbTypes.BillingModePayPerRequest

	t.mu.Unlock()

	return out
}

// RCUs returns the read cost units for this table
func (t *Table) RCUs() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.IsOnDemand() || t.ProvisionedThroughput.ReadCapacityUnits == nil {
		return 0
	}

	return *t.ProvisionedThroughput.ReadCapacityUnits
}

// WCUs returns the write cost units for this table
func (t *Table) WCUs() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.IsOnDemand() || t.ProvisionedThroughput.WriteCapacityUnits == nil {
		return 0
	}

	return *t.ProvisionedThroughput.WriteCapacityUnits
}

// Description returns the table description
func (t *Table) Description() *ddbTypes.TableDescription {
	t.mu.RLock()
	out := t.TableDescription
	t.mu.RUnlock()

	return out
}

// DescribeTableInput gets the DescribeTableInput for this table
func (t *Table) DescribeTableInput() *ddb.DescribeTableInput {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return NewDescribeTableInput(t.TableName)
}

// TableExistsWaiter returns a new TableExistsWaiter for this table
func (t *Table) TableExistsWaiter() *TableExistsWaiter {
	return NewTableExistsWaiter(NewDescribeTableInput(t.TableName))
}

// TableNotExistsWaiter returns a new TableExistsWaiter for this table
func (t *Table) TableNotExistsWaiter() *TableNotExistsWaiter {
	return NewTableNotExistsWaiter(NewDescribeTableInput(t.TableName))
}

// UpdateWithRemote creates a new NewTableExistsWaiter with a callback that updates the table from the remote description
func (t *Table) UpdateWithRemote() *TableExistsWaiter {
	return NewTableExistsWaiter(NewDescribeTableInput(t.TableName), t)
}

// setTableDescription sets the table description to the input value
func (t *Table) setTableDescription(input *ddbTypes.TableDescription) {
	t.LastSync = time.Now()
	*t.TableDescription = *input
}

// UpdateWithTableDescription sets the table description to the input value
func (t *Table) UpdateWithTableDescription(input *ddbTypes.TableDescription) {
	t.mu.Lock()
	t.setTableDescription(input)
	t.mu.Unlock()
}

// SortKeyName returns the table's sort key
func (t *Table) SortKeyName() string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if len(t.KeySchema) == 0 {
		return ""
	}

	name := getSortKeyNameFromKeySchema(t.KeySchema)

	if name == nil {
		return ""
	}

	return *name
}

// PartitionKeyName returns the partition key name
func (t *Table) PartitionKeyName() string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if len(t.KeySchema) == 0 {
		return ""
	}

	name := getPartitionKeyNameFromKeySchema(t.KeySchema)

	if name == nil {
		return ""
	}

	return *name
}

// SetPartitionKey sets the partition key for this table
func (t *Table) SetPartitionKey(pkName string, attributeType ddbTypes.ScalarAttributeType) *Table {
	t.mu.Lock()

	t.KeySchema = addPartitionKeyToKeySchema(t.KeySchema, pkName)
	t.PartitionKeyAttributeDefinition = &ddbTypes.AttributeDefinition{
		AttributeName: &pkName,
		AttributeType: attributeType,
	}
	t.AttributeDefinitions = appendUniqueAttributeDefinitions(t.AttributeDefinitions, *t.PartitionKeyAttributeDefinition)

	t.mu.Unlock()

	return t
}

// SetPartitionKeyAttributeDefinition sets the partition key for this Table with an AttributeDefinition
func (t *Table) SetPartitionKeyAttributeDefinition(def *ddbTypes.AttributeDefinition) *Table {
	t.mu.Lock()

	t.KeySchema = addPartitionKeyToKeySchema(t.KeySchema, *def.AttributeName)
	t.PartitionKeyAttributeDefinition = def
	t.AttributeDefinitions = appendUniqueAttributeDefinitions(t.AttributeDefinitions, *def)

	t.mu.Unlock()

	return t
}

// SetSortKey sets the sortKey key for this table
func (t *Table) SetSortKey(skName string, attributeType ddbTypes.ScalarAttributeType) *Table {
	t.mu.Lock()

	t.KeySchema = addSortKeyToKeySchema(t.KeySchema, skName)
	t.SortKeyAttributeDefinition = &ddbTypes.AttributeDefinition{
		AttributeName: &skName,
		AttributeType: attributeType,
	}
	t.AttributeDefinitions = appendUniqueAttributeDefinitions(t.AttributeDefinitions, *t.SortKeyAttributeDefinition)

	t.mu.Unlock()

	return t
}

// SetSortKeyAttributeDefinition sets the sort key for this Table with an AttributeDefinition
func (t *Table) SetSortKeyAttributeDefinition(def *ddbTypes.AttributeDefinition) *Table {
	t.mu.Lock()

	t.KeySchema = addSortKeyToKeySchema(t.KeySchema, *def.AttributeName)
	t.AttributeDefinitions = appendUniqueAttributeDefinitions(t.AttributeDefinitions, *def)
	t.SortKeyAttributeDefinition = def

	t.mu.Unlock()

	return t
}

// AddGSI adds a new GSI for this table
// provided GSI must have an IndexName or this func will panic
func (t *Table) AddGSI(gsi ...*GSI) *Table {
	t.mu.Lock()

	if t.GSIs == nil {
		t.GSIs = make(map[string]*GSI)
	}

	for _, g := range gsi {
		gsiDesc := ddbTypes.GlobalSecondaryIndexDescription{
			IndexName:  &g.IndexName,
			KeySchema:  g.GetDynamoKeySchema(),
			Projection: g.GetDynamoProjection(),
		}

		if g.ProvisionedThroughput != nil {
			gsiDesc.ProvisionedThroughput = &ddbTypes.ProvisionedThroughputDescription{
				ReadCapacityUnits:  g.ProvisionedThroughput.ReadCapacityUnits,
				WriteCapacityUnits: g.ProvisionedThroughput.WriteCapacityUnits,
			}
		}

		t.GlobalSecondaryIndexes = append(t.GlobalSecondaryIndexes, gsiDesc)

		t.GSIs[g.IndexName] = g

		if g.PartitionKey != nil {
			t.AttributeDefinitions = appendUniqueAttributeDefinitions(t.AttributeDefinitions, *g.PartitionKey)
		}

		if g.SortKey != nil {
			t.AttributeDefinitions = appendUniqueAttributeDefinitions(t.AttributeDefinitions, *g.SortKey)
		}
	}

	t.mu.Unlock()

	return t
}

// AddLSI adds a new LSI attached to this table
// provided LSI must have an IndexName or this func will panic
func (t *Table) AddLSI(lsi ...*LSI) *Table {
	t.mu.Lock()

	if t.LSIs == nil {
		t.LSIs = make(map[string]*LSI)
	}

	for _, l := range lsi {
		lsiDesc := ddbTypes.LocalSecondaryIndexDescription{
			IndexName:  &l.IndexName,
			KeySchema:  l.GetDynamoKeySchema(),
			Projection: l.GetDynamoProjection(),
		}

		t.LocalSecondaryIndexes = append(t.LocalSecondaryIndexes, lsiDesc)

		if t.LSIs == nil {
			t.LSIs = make(map[string]*LSI)
		}

		t.LSIs[l.IndexName] = l

		if l.SortKey != nil {
			t.AttributeDefinitions = appendUniqueAttributeDefinitions(t.AttributeDefinitions, *l.SortKey)
		}
	}
	t.mu.Unlock()

	return t
}

// ExtractKeys extracts key values from a dynamodb.AttributeValue map
func (t *Table) ExtractKeys(avMap map[string]ddbTypes.AttributeValue) map[string]ddbTypes.AttributeValue {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return extractKeyAttributeValuesFromKeySchema(t.KeySchema, avMap)
}

// ExtractAllKeys extracts all key values from a slice of dynamodb.AttributeValue maps
func (t *Table) ExtractAllKeys(avMaps []map[string]ddbTypes.AttributeValue) []map[string]ddbTypes.AttributeValue {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return extractAllKeyAttributeValuesFromKeySchema(t.KeySchema, avMaps)
}

// ExtractPartitionKeyValue extracts this table's partition key attribute value from a given input
// panics if this table does not have a partition key
func (t *Table) ExtractPartitionKeyValue(avMap map[string]ddbTypes.AttributeValue) ddbTypes.AttributeValue {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return extractPartitionKeyAttributeValueFromKeySchema(t.KeySchema, avMap)
}

// ExtractSortKeyValue extracts this table's sort key attribute value from a given input
// panics if this table does not have a partition key
func (t *Table) ExtractSortKeyValue(avMap map[string]ddbTypes.AttributeValue) ddbTypes.AttributeValue {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return extractSortKeyAttributeValueFromKeySchema(t.KeySchema, avMap)
}

// AddTag adds a tag with given key and value to the Table
func (t *Table) AddTag(key, value string) *Table {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.Tags = append(t.Tags, ddbTypes.Tag{
		Key:   &key,
		Value: &value,
	})

	return t
}

// CreateTableInput returns the table builder for this table
func (t *Table) CreateTableInput() (*ddb.CreateTableInput, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.PartitionKeyAttributeDefinition == nil {
		return nil, fmt.Errorf("PartitionKeyAttributeDefinition must not be nil")
	}

	if t.PartitionKeyAttributeDefinition.AttributeName == nil {
		return nil, fmt.Errorf("PartitionKeyAttributeDefinition.AttributeName must not be nil")
	}

	attributeDefinitionMap := map[string]*ddbTypes.AttributeDefinition{
		*t.PartitionKeyAttributeDefinition.AttributeName: t.PartitionKeyAttributeDefinition,
	}

	in := &ddb.CreateTableInput{
		AttributeDefinitions: t.AttributeDefinitions,
		BillingMode:          t.BillingModeSummary.BillingMode,
		KeySchema:            t.KeySchema,
		StreamSpecification:  t.StreamSpecification,
		TableName:            t.TableName,
		Tags:                 t.Tags,
	}

	if t.ProvisionedThroughput != nil {
		in.ProvisionedThroughput = &ddbTypes.ProvisionedThroughput{
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
			in.GlobalSecondaryIndexes = append(in.GlobalSecondaryIndexes, gsi.GetDynamoGlobalSecondaryIndex())
		}
	}

	if len(t.LSIs) > 0 {
		for _, lsi := range t.LSIs {
			in.LocalSecondaryIndexes = append(in.LocalSecondaryIndexes, lsi.GetDynamoLocalSecondaryIndex())
		}
	}

	return in, nil
}

// NewTable creates new table with provided table name, table key, and and options
// Mapper key is required
func NewTable(name string) *Table {
	// create a table with given table key
	return &Table{
		TableDescription: &ddbTypes.TableDescription{
			AttributeDefinitions: nil,
			BillingModeSummary: &ddbTypes.BillingModeSummary{
				BillingMode: ddbTypes.BillingModePayPerRequest,
			},
			TableName: &name,
		},
	}
}

// SetName sets the name for this table
func (t *Table) SetName(name string) *Table {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.TableName = &name

	return t
}

//SetOnDemand sets the table to On Demand billing mode
func (t *Table) SetOnDemand() *Table {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.BillingModeSummary == nil {
		t.BillingModeSummary = new(ddbTypes.BillingModeSummary)
	}

	t.BillingModeSummary.BillingMode = ddbTypes.BillingModePayPerRequest

	return t
}

// SetReadCostUnits sets the read cost units for this table
func (t *Table) SetReadCostUnits(costUnits int64) *Table {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.BillingModeSummary == nil {
		t.BillingModeSummary = new(ddbTypes.BillingModeSummary)
	}

	if t.ProvisionedThroughput == nil {
		t.ProvisionedThroughput = new(ddbTypes.ProvisionedThroughputDescription)
	}

	t.BillingModeSummary.BillingMode = ddbTypes.BillingModeProvisioned
	t.ProvisionedThroughput.ReadCapacityUnits = &costUnits

	return t
}

// SetWriteCostUnits sets the write cost units for this table
func (t *Table) SetWriteCostUnits(costUnits int64) *Table {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.BillingModeSummary == nil {
		t.BillingModeSummary = new(ddbTypes.BillingModeSummary)
	}

	if t.ProvisionedThroughput == nil {
		t.ProvisionedThroughput = new(ddbTypes.ProvisionedThroughputDescription)
	}

	t.BillingModeSummary.BillingMode = ddbTypes.BillingModeProvisioned
	t.ProvisionedThroughput.WriteCapacityUnits = &costUnits

	return t
}

// SetCostUnits sets both the read and write cost units
func (t *Table) SetCostUnits(RCUs, WCUs int64) *Table {
	t.mu.Lock()
	defer t.mu.Unlock()

	if RCUs == 0 && WCUs == 0 {
		t.SetOnDemand()
		return t
	}

	if t.BillingModeSummary == nil {
		t.BillingModeSummary = new(ddbTypes.BillingModeSummary)
	}

	if t.ProvisionedThroughput == nil {
		t.ProvisionedThroughput = new(ddbTypes.ProvisionedThroughputDescription)
	}

	t.BillingModeSummary.BillingMode = ddbTypes.BillingModeProvisioned
	t.ProvisionedThroughput.ReadCapacityUnits = &RCUs
	t.ProvisionedThroughput.WriteCapacityUnits = &WCUs

	return t
}

// UniqueKeyCondition returns a Builder that represents a unique key condition
func (t *Table) UniqueKeyCondition() *expression.ConditionBuilder {
	t.mu.RLock()
	defer t.mu.RUnlock()

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

// Create creates a new CreateTable operation
func (t *Table) Create() (*CreateTable, error) {
	dynamodbInput, err := t.CreateTableInput()
	if err != nil {
		return nil, err
	}

	return NewCreateTable(dynamodbInput, t), nil
}

// CreateTableMiddleWare returns a CreateTableMiddleWare that will update this table from the create table
// operation output
func (t *Table) CreateTableMiddleWare(next CreateTableHandler) CreateTableHandler {
	return CreateTableHandlerFunc(func(ctx *CreateTableContext, output *CreateTableOutput) {
		next.HandleCreateTable(ctx, output)
		out, rErr := output.Get()
		if rErr == nil {
			t.UpdateWithTableDescription(out.TableDescription)
		}
	})
}

// DescribeTableMiddleWare updates this table with the output of the describe table output
func (t *Table) DescribeTableMiddleWare(next DescribeTableHandler) DescribeTableHandler {
	return DescribeTableHandlerFunc(func(ctx *DescribeTableContext, output *DescribeTableOutput) {
		next.HandleDescribeTable(ctx, output)
		out, rErr := output.Get()
		if rErr == nil {
			t.UpdateWithTableDescription(out.Table)
		}
	})
}

// BackupInput creates a CreateBackupInput for this table with a given backup name
func (t *Table) BackupInput(backupName string) *ddb.CreateBackupInput {
	t.mu.RLock()
	input := NewCreateBackupInput(t.TableName, &backupName)
	t.mu.RUnlock()

	return input
}

// NewBackup creates a backup operation for this table
// returns a channel that will return a BackupResult when backup completes
func (t *Table) NewBackup(backupName string) *CreateBackup {
	return NewCreateBackup(t.BackupInput(backupName))
}

// DeleteInput creates a DeleteTableInput for this table
func (t *Table) DeleteInput() *ddb.DeleteTableInput {
	t.mu.RLock()
	input := NewDeleteTableInput(t.TableName)
	t.mu.RUnlock()

	return input
}

// Delete returns a new DeleteTable operation for this table
func (t *Table) Delete() *DeleteTable {
	return NewDeleteTable(t.DeleteInput())
}

// ScanBuilder returns a new ScanBuilder for this table
func (t *Table) ScanBuilder() *ScanBuilder {
	return NewScanBuilder(nil).SetTableName(t.Name())
}

// QueryBuilder returns a new QueryBuilder for this table
func (t *Table) QueryBuilder() *QueryBuilder {
	return NewQueryBuilder(nil).SetTableName(t.Name())
}

// NewPutItemBuilder creates a PutBuilder for this table
func (t *Table) NewPutItemBuilder() *PutItemBuilder {
	return NewPutItemBuilder(nil).SetTableName(t.Name())
}

// NewBatchGetBuilder returns a new NewBatchGetBuilder for this table with given key items
func (t *Table) NewBatchGetBuilder(items []map[string]ddbTypes.AttributeValue, mws ...BatchGetItemMiddleWare) *BatchGetItemBuilder {
	keys := t.ExtractAllKeys(items)

	return NewBatchGetBuilder(nil).AddKey(t.Name(), keys...)
}

// NewBatchWriteBuilder returns a new BatchWriteItemBuilder
func (t *Table) NewBatchWriteBuilder(puts []map[string]ddbTypes.AttributeValue, deletes []map[string]ddbTypes.AttributeValue, mws ...BatchWriteItemMiddleWare) *BatchWriteItemBuilder {
	builder := NewBatchWriteItemBuilder(nil)

	if len(puts) > 0 {
		builder.AddPuts(t.Name(), puts...)
	}

	if len(deletes) > 0 {
		builder.AddDeletes(t.Name(), deletes...)
	}

	return builder
}
