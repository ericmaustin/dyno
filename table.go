package dyno

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ericmaustin/dyno/condition"
	"sync"
	"time"
)

// Table represents a dynamodb table
type Table struct {
	*types.TableDescription
	LastSync time.Time
	// map of global secondary indexes
	GSIs map[string]*GSI
	// map of local secondary indexes
	LSIs                            map[string]*LSI
	PartitionKeyAttributeDefinition *types.AttributeDefinition
	SortKeyAttributeDefinition      *types.AttributeDefinition
	Tags                            []types.Tag
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

//IsOnDemand returns true if the table is set to On Demand pricing
func (t *Table) IsOnDemand() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.BillingModeSummary == nil {
		// default to pay per request
		t.BillingModeSummary = new(types.BillingModeSummary)
		t.BillingModeSummary = &types.BillingModeSummary{
			BillingMode: types.BillingModePayPerRequest,
		}
	}
	return t.BillingModeSummary.BillingMode == types.BillingModePayPerRequest
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
func (t *Table) Description() *types.TableDescription {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.TableDescription
}

//DescribeTableInput gets the DescribeTableInput for this table
func (t *Table) DescribeTableInput() *ddb.DescribeTableInput {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return NewDescribeTableInput(t.TableName)
}

//WaitUntilExists returns an error that will be nil if table exists
func (t *Table) WaitUntilExists(ctx context.Context, client *ddb.Client) error {
	return NewTableExistsWaiter(client, NewDescribeTableInput(t.TableName)).Invoke(ctx).Await()
}

//WaitUntilNotExists returns  an error that will be nil if table no longer exists
func (t *Table) WaitUntilNotExists(ctx context.Context, client *ddb.Client) error {
	return NewTableNotExistsWaiter(client, NewDescribeTableInput(t.TableName)).Invoke(ctx).Await()
}

// Sync updates this table's description with the remote dynamodb table
// calls Table.syncWithContext after locking the table for writing
func (t *Table) Sync(ctx context.Context, client *ddb.Client) error {
	cb := DescribeTableOutputCallbackF(func(ctx context.Context, output *ddb.DescribeTableOutput) error {
		if output.Table != nil {
			t.UpdateWithTableDescription(output.Table)
		}
		return nil
	})
	waiter := NewTableExistsWaiter(client, NewDescribeTableInput(t.TableName),
		TableExistsWaiterWithOutputCallback(cb))
	waiter.DynoInvoke(ctx)
	return waiter.Await()
}

// setTableDescription sets the table description to the input value
func (t *Table) setTableDescription(input *types.TableDescription) {
	t.LastSync = time.Now()
	*t.TableDescription = *input
}

// UpdateWithTableDescription sets the table description to the input value
func (t *Table) UpdateWithTableDescription(input *types.TableDescription) {
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
func (t *Table) SetPartitionKey(pkName string, attributeType types.ScalarAttributeType) *Table {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.KeySchema = addPartitionKeyToKeySchema(t.KeySchema, pkName)
	t.PartitionKeyAttributeDefinition = &types.AttributeDefinition{
		AttributeName: &pkName,
		AttributeType: attributeType,
	}
	t.AttributeDefinitions = appendUniqueAttributeDefinitions(t.AttributeDefinitions, *t.PartitionKeyAttributeDefinition)
	return t
}

// SetSortKey sets the sortKey key for this table
func (t *Table) SetSortKey(skName string, attributeType types.ScalarAttributeType) *Table {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.KeySchema = addSortKeyToKeySchema(t.KeySchema, skName)
	t.SortKeyAttributeDefinition = &types.AttributeDefinition{
		AttributeName: &skName,
		AttributeType: attributeType,
	}
	t.AttributeDefinitions = appendUniqueAttributeDefinitions(t.AttributeDefinitions, *t.SortKeyAttributeDefinition)
	return t
}

// AddGSI adds a new GSI for this table
// provided GSI must have an IndexName or this func will panic
func (t *Table) AddGSI(gsi ...*GSI) *Table {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, g := range gsi {
		gsiDesc := types.GlobalSecondaryIndexDescription{
			IndexName:  g.IndexName,
			KeySchema:  g.KeySchema,
			Projection: g.Projection,
		}

		if g.ProvisionedThroughput != nil {
			gsiDesc.ProvisionedThroughput = &types.ProvisionedThroughputDescription{
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
			t.AttributeDefinitions = appendUniqueAttributeDefinitions(t.AttributeDefinitions, *g.PartitionKeyAttributeDefinition)
		}
		if g.SortKeyAttributeDefinition != nil {
			t.AttributeDefinitions = appendUniqueAttributeDefinitions(t.AttributeDefinitions, *g.SortKeyAttributeDefinition)
		}
	}

	return t
}

// AddLSI adds a new LSI attached to this table
// provided LSI must have an IndexName or this func will panic
func (t *Table) AddLSI(lsi ...*LSI) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, l := range lsi {
		lsiDesc := types.LocalSecondaryIndexDescription{
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
			t.AttributeDefinitions = appendUniqueAttributeDefinitions(t.AttributeDefinitions, *l.SortKeyAttributeDefinition)
		}
	}
}

// ExtractKeys extracts key values from a dynamodb.AttributeValue map
func (t *Table) ExtractKeys(avMap map[string]types.AttributeValue) map[string]types.AttributeValue {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return extractKeyAttributeValuesFromKeySchema(t.KeySchema, avMap)
}

// ExtractAllKeys extracts all key values from a slice of dynamodb.AttributeValue maps
func (t *Table) ExtractAllKeys(avMaps []map[string]types.AttributeValue) []map[string]types.AttributeValue {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return extractAllKeyAttributeValuesFromKeySchema(t.KeySchema, avMaps)
}

// ExtractPartitionKeyValue extracts this table's partition key attribute value from a given input
// panics if this table does not have a partition key
func (t *Table) ExtractPartitionKeyValue(avMap map[string]types.AttributeValue) types.AttributeValue {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return extractPartitionKeyAttributeValueFromKeySchema(t.KeySchema, avMap)
}

// ExtractSortKeyValue extracts this table's sort key attribute value from a given input
// panics if this table does not have a partition key
func (t *Table) ExtractSortKeyValue(avMap map[string]types.AttributeValue) types.AttributeValue {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return extractSortKeyAttributeValueFromKeySchema(t.KeySchema, avMap)
}

//AddTag adds a tag with given key and value to the Table
func (t *Table) AddTag(key, value string) *Table {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Tags = append(t.Tags, types.Tag{
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

	attributeDefinitionMap := map[string]*types.AttributeDefinition{
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
		in.ProvisionedThroughput = &types.ProvisionedThroughput{
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

// NewTable creates new table with provided table name, table key, and and options
// Mapper key is required
func NewTable(name string) *Table {
	// create a table with given table key
	return &Table{
		TableDescription: &types.TableDescription{
			AttributeDefinitions: nil,
			BillingModeSummary: &types.BillingModeSummary{
				BillingMode: types.BillingModePayPerRequest,
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
		t.BillingModeSummary = new(types.BillingModeSummary)
	}
	t.BillingModeSummary.BillingMode = types.BillingModePayPerRequest
	return t
}

// SetReadCostUnits sets the read cost units for this table
func (t *Table) SetReadCostUnits(costUnits int64) *Table {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.BillingModeSummary == nil {
		t.BillingModeSummary = new(types.BillingModeSummary)
	}
	if t.ProvisionedThroughput == nil {
		t.ProvisionedThroughput = new(types.ProvisionedThroughputDescription)
	}
	t.BillingModeSummary.BillingMode = types.BillingModeProvisioned
	t.ProvisionedThroughput.ReadCapacityUnits = &costUnits
	return t
}

// SetWriteCostUnits sets the write cost units for this table
func (t *Table) SetWriteCostUnits(costUnits int64) *Table {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.BillingModeSummary == nil {
		t.BillingModeSummary = new(types.BillingModeSummary)
	}
	if t.ProvisionedThroughput == nil {
		t.ProvisionedThroughput = new(types.ProvisionedThroughputDescription)
	}
	t.BillingModeSummary.BillingMode = types.BillingModeProvisioned
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
		t.BillingModeSummary = new(types.BillingModeSummary)
	}
	if t.ProvisionedThroughput == nil {
		t.ProvisionedThroughput = new(types.ProvisionedThroughputDescription)
	}
	t.BillingModeSummary.BillingMode = types.BillingModeProvisioned
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

//todo: finish GetTableUpdate method
//func (t *Table) GetTableUpdate(desc *dynamodb.TableDescription) {
//	update := dynamodb.UpdateTableInput{
//		AttributeDefinitions:        nil,
//		BillingMode:                 nil,
//		GlobalSecondaryIndexUpdates: nil,
//		ProvisionedThroughput:       nil,
//		ReplicaUpdates:              nil,
//		SSESpecification:            nil,
//		StreamSpecification:         nil,
//		TableName:                   nil,
//	}
//	//TableName
//	if desc.TableName == nil || *desc.TableName != *t.TableName {
//		update.TableName = t.TableName
//	}
//	//ProvisionedThroughput
//	if desc.ProvisionedThroughput == nil && t.ProvisionedThroughput != nil {
//		update.ProvisionedThroughput = &dynamodb.ProvisionedThroughput{
//			ReadCapacityUnits:  t.ProvisionedThroughput.ReadCapacityUnits,
//			WriteCapacityUnits: t.ProvisionedThroughput.WriteCapacityUnits,
//		}
//	} else if desc.ProvisionedThroughput != nil && t.ProvisionedThroughput != nil {
//		if *desc.ProvisionedThroughput.ReadCapacityUnits != *t.ProvisionedThroughput.ReadCapacityUnits {
//			update.ProvisionedThroughput = new(dynamodb.ProvisionedThroughput)
//			update.ProvisionedThroughput.ReadCapacityUnits = t.ProvisionedThroughput.ReadCapacityUnits
//		}
//		if *desc.ProvisionedThroughput.WriteCapacityUnits != *t.ProvisionedThroughput.WriteCapacityUnits {
//			if update.ProvisionedThroughput == nil {
//				update.ProvisionedThroughput = new(dynamodb.ProvisionedThroughput)
//			}
//			update.ProvisionedThroughput.WriteCapacityUnits = t.ProvisionedThroughput.WriteCapacityUnits
//		}
//	}
//	//Billing Mode
//	if desc.BillingModeSummary.BillingMode == nil && t.BillingModeSummary.BillingMode != nil {
//		update.BillingMode = t.BillingModeSummary.BillingMode
//	} else if t.BillingModeSummary.BillingMode != nil && *desc.BillingModeSummary.BillingMode != *t.BillingModeSummary.BillingMode {
//		update.BillingMode = t.BillingModeSummary.BillingMode
//	}
//	////StreamSpecification
//	//if desc.StreamSpecification == nil && t.StreamSpecification != nil {
//	//	update.StreamSpecification = t.StreamSpecification
//	//} else if desc.StreamSpecification != nil && t.StreamSpecification != nil {
//	//	if desc.StreamSpecification.StreamEnabled != nil
//	//}
//}

// Create creates this table in dynamodb with an api call
// returns a channel that will return a PublishResult when table is ready
func (t *Table) Create(ctx context.Context, client *ddb.Client) error {
	dynamodbInput, err := t.CreateTableInput()
	if err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	_, err = NewCreateTable(client, dynamodbInput, CreateTableWithOutputCallback(t)).Invoke(ctx).Await()
	return err
}

// CreateTableOutputCallback implements the CreateTableOutputCallback interface
func (t *Table) CreateTableOutputCallback(_ context.Context, output *ddb.CreateTableOutput) error {
	if output.TableDescription != nil {
		t.setTableDescription(output.TableDescription)
	}
	return nil
}

// BackupInput creates a CreateBackupInput for this table with a given backup name
func (t *Table) BackupInput(backupName string) *ddb.CreateBackupInput {
	t.mu.RLock()
	input := NewCreateBackupInput(t.TableName, &backupName)
	t.mu.RUnlock()
	return input
}

// Backup creates a backup of this table
// returns a channel that will return a BackupResult when backup completes
func (t *Table) Backup(ctx context.Context, client *ddb.Client, backupName string) (*ddb.CreateBackupOutput, error) {
	input := t.BackupInput(backupName)
	t.mu.Lock()
	defer t.mu.Unlock()
	return NewCreateBackup(client, input).Invoke(ctx).Await()
}

// DeleteInput creates a DeleteTableInput for this table
func (t *Table) DeleteInput() *ddb.DeleteTableInput {
	t.mu.RLock()
	input := NewDeleteTableInput(t.TableName)
	t.mu.RUnlock()
	return input
}

// Delete deletes this table in dynamodb
// returns a channel that will return an error (nil if successful) when complete
func (t *Table) Delete(ctx context.Context, client *ddb.Client) error {
	input := t.DeleteInput()
	t.mu.Lock()
	defer t.mu.Unlock()
	_, err := NewDeleteTable(client, input).Invoke(ctx).Await()
	return err
}

// NewScanBuilder creates a ScanBuilder with this table
func (t *Table) NewScanBuilder() *ScanBuilder {
	return NewScanBuilder(nil).SetTableName(t.Name())
}

// NewQueryBuilder creates a QueryBuilder with this table
func (t *Table) NewQueryBuilder() *QueryBuilder {
	return NewQueryBuilder(nil).SetTableName(t.Name())
}

// NewPutItemBuilder creates a PutBuilder with this table
func (t *Table) NewPutItemBuilder() *PutItemBuilder {
	return NewPutItemBuilder(nil).SetTableName(t.Name())
}
