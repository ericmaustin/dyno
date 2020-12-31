package table

import (
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/ericmaustin/dyno"
	"github.com/ericmaustin/dyno/condition"
	"github.com/ericmaustin/dyno/encoding"
	"github.com/ericmaustin/dyno/operation"
)

var (
	// DefaultWCUs the default Write Cost Units allocated to a table
	DefaultWCUs = int64(10)
	// DefaultRCUs the default PublishDone Cost Units allocated to a table
	DefaultRCUs = int64(10)
)

const (
	AttributeNumber = "N"
	AttributeBinary = "B"
	AttributeString = "S"
)

const LogFieldTableName = "table"

// TimeSpanMapper represents a dynamodb table
type Table struct {
	// the name of the table
	name string `validate:"required"`
	// the table key
	key *Key `validate:"required"`
	// map of global secondary indexes
	gsis map[string]*Gsi
	// map of local secondary indexes
	lsis map[string]*Lsi
	// write concurrency
	rcus int64
	// read concurrency
	wcus int64
	// is on demand
	onDemand bool
	// description
	description *dynamodb.TableDescription
	// modification mutex
	mu sync.RWMutex
	// arn that this table is associated with records aws
	arn string
}

// Key returns this table's name as a string
func (t *Table) Name() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.name
}

// Key returns this table's Key
func (t *Table) Key() *Key {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.key
}

func (t *Table) IsOnDemand() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.onDemand
}

// RCUs returns the read cost units for this table
func (t *Table) RCUs() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.rcus
}

// WCUs returns the write cost units for this table
func (t *Table) WCUs() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.wcus
}

// Description returns the table description
// if table hasn't been published or laoded yet, then a ErrTableNotLoaded error is returned
func (t *Table) Description() (*dynamodb.TableDescription, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.description == nil {
		return nil, &dyno.Error{
			Code:    dyno.ErrTableNotLoaded,
			Message: fmt.Sprintf("table %s does not have a description", t.name),
		}
	}

	return t.description, nil
}

// Sync updates this table's description with the remote dynamodb table
func (t *Table) Sync(req *dyno.Request) error {
	out, err := operation.DescribeTable(t.Name()).Execute(req).OutputError()
	if err != nil {
		return err
	}
	t.UpdateWithDescription(out.Table)
	return nil
}

// IsLoaded checks if this table was loaded
func (t *Table) IsLoaded() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.description != nil
}

func (t *Table) LogFields() map[string]interface{} {
	return map[string]interface{}{
		LogFieldTableName: t.Name,
		"table_ptr":       fmt.Sprintf("%p", t),
		"table_arn":       t.arn,
	}
}

// GetSortKey returns a sortKey key defined on this table either on the table itself or on any index attached to this table
func (t *Table) GetSortKey(fieldName string, indexName string) *SortKey {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if len(indexName) > 0 {
		if _, ok := t.lsis[indexName]; ok &&
			t.lsis[indexName].Key.sortKey != nil &&
			t.lsis[indexName].Key.sortKey.name == fieldName {
			return t.lsis[indexName].Key.sortKey
		}

		if _, ok := t.gsis[indexName]; ok &&
			t.gsis[indexName].Key.sortKey != nil &&
			t.gsis[indexName].Key.sortKey.name == fieldName {
			return t.gsis[indexName].Key.sortKey
		}

		return nil
	}

	if t.HasSortKey() && t.key.sortKey.name == fieldName {
		return t.key.sortKey
	}
	return nil
}

func (t *Table) IsKeyField(fieldName string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return (fieldName == t.PartitionKeyName()) || fieldName == t.SortKeyName()
}

// HasKey returns true if this table has a key
func (t *Table) HasKey() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.key != nil && t.key.partitionKey != nil
}

// HasSortKey returns true if this table's sortKey Key is set
func (t *Table) HasSortKey() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.key.sortKey != nil
}

// HasSortKey returns true if this table's sortKey Key is set
func (t *Table) SortKeyName() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.key.SortName()
}

func (t *Table) PartitionKeyName() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.key.PartitionName()
}

// Copy make a copy of this table with a new table name
//  this copies the ProjectionColumns, gsis, lsis, and Key
func (t *Table) Copy(name string) *Table {
	t.mu.RLock()
	defer t.mu.RUnlock()
	// create copy of the table's primary attributes
	table := &Table{
		name:     name,
		key:      t.key.Copy(),
		wcus:     t.wcus,
		rcus:     t.rcus,
		onDemand: t.onDemand,
	}

	// copy the lsis
	if t.lsis != nil {
		for name, lsi := range t.lsis {
			lsiCopy := &Lsi{
				Index: &Index{
					Name:           name,
					Key:            lsi.Key.Copy(),
					ProjectionType: lsi.ProjectionType,
				},
			}

			// copy the ProjectionColumns
			if lsi.ProjectionColumns != nil {
				lsiCopy.ProjectionColumns = append(lsiCopy.ProjectionColumns, lsi.ProjectionColumns...)
			}

			table.lsis[name] = lsiCopy
		}
	}

	// copy the gsis
	if t.gsis != nil {
		for name, gsi := range t.gsis {
			gsiCopy := &Gsi{
				Index: &Index{
					Name:           name,
					Key:            gsi.Key.Copy(),
					ProjectionType: gsi.ProjectionType,
				},
				WCUs: gsi.WCUs,
				RCUs: gsi.RCUs,
			}

			// copy the ProjectionColumns
			if gsi.ProjectionColumns != nil {
				gsiCopy.ProjectionColumns = append(gsiCopy.ProjectionColumns, gsi.ProjectionColumns...)
			}

			table.gsis[name] = gsiCopy
		}
	}

	return table
}

// AddGsi sets a new Gsi for this table
func (t *Table) AddGsi(gsi *Gsi) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	// validate the gsi
	if err := dyno.Validator().Struct(gsi); err != nil {
		return err
	}

	if _, ok := t.gsis[gsi.Name]; ok {
		return &dyno.Error{
			Code:    dyno.ErrGsiNameAlreadyExists,
			Message: fmt.Sprintf("An GSI with the name %s already exists on table %s", gsi.Name, t.name),
		}
	}
	t.gsis[gsi.Name] = gsi
	return nil
}

// AddLsi adds a new Lsi attached to this table
func (t *Table) AddLsi(lsi *Lsi) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	// validate the lsi
	if err := dyno.Validator().Struct(lsi); err != nil {
		return err
	}

	if lsi.Key.partitionKey.Name() != t.PartitionKeyName() {
		return &dyno.Error{
			Code: dyno.ErrPartitionKeyTableIndexMismatch,
			Message: fmt.Sprintf("partitionKey key column on lsi (%s) != table partitionKey's key (%s)",
				lsi.Key.partitionKey.Name(), t.PartitionKeyName()),
		}
	}

	if _, ok := t.lsis[lsi.Name]; ok {
		return &dyno.Error{
			Code:    dyno.ErrLsiNameAlreadyExists,
			Message: fmt.Sprintf("An LSI with the name %s already exists on table %s", lsi.Name, t.name),
		}
	}

	t.lsis[lsi.Name] = lsi

	return nil
}

// ExtractKey converts a item's key ProjectionColumns to a map of dynamodb attribute ProjectionColumns for a item
// belonging to this table
func (t *Table) ExtractKey(input interface{}) map[string]*dynamodb.AttributeValue {
	return t.key.extract(encoding.MustMarshalItem(input))
}

// ExtractKeys converts a list of records to a list of dynamodb attribute items
func (t *Table) ExtractKeys(input interface{}) []map[string]*dynamodb.AttributeValue {
	return t.key.extractAll(encoding.MustMarshalItems(input))
}

// ExtractPartitionKeyValue extracts this table's partition key attribute value from a given input
// panics if this table does not have a partition key
func (t *Table) ExtractPartitionKeyValue(input interface{}) *dynamodb.AttributeValue {
	return t.key.partitionKey.extractValue(encoding.MustMarshalItem(input))
}

// ExtractSortKeyValue extracts this table's sort key attribute value from a given input
// panics if this table does not have a partition key
func (t *Table) ExtractSortKeyValue(input interface{}) *dynamodb.AttributeValue {
	return t.key.sortKey.extractValue(encoding.MustMarshalItem(input))
}

// PublishTableOutput is returned by the TimeSpanMapper Publish method
type PublishTableOutput struct {
	CreateTableOutput *dynamodb.CreateTableOutput
	TableDescription  *dynamodb.TableDescription
	TableExists       bool
}

// CreateTableBuilder returns the table builder for this table
func (t *Table) CreateTableBuilder() *operation.CreateTableBuilder {
	t.mu.RLock()
	defer t.mu.RUnlock()

	builder := operation.NewCreateTableBuilder().
		SetName(t.name)

	if !t.onDemand {

		if t.rcus < 1 {
			t.rcus = DefaultRCUs
		}

		if t.wcus < 1 {
			t.wcus = DefaultWCUs
		}

		builder.SetProvisionedThroughput(t.rcus, t.wcus)
	}

	// if we have a key then create attribute definitions for all the keys
	if t.key != nil {
		builder.SetKeySchema(t.key.schema)
		for _, attr := range t.key.attributes {
			builder.AddAttributeDefinition(attr)
		}
	}

	// if we have lsis then add the lsis the the input
	if t.lsis != nil && len(t.lsis) > 0 {
		for _, lsi := range t.lsis {
			builder.AddLocalIndex(lsi.DynamoLocalSecondaryIndex())
			// add any missing definitions
			builder.AddAttributeDefinition(lsi.Key.PartitionAttributeDefinition())
			builder.AddAttributeDefinition(lsi.Key.SortAttributeDefinition())
		}
	}
	// if we have gsis then add the gsis to the input
	if t.gsis != nil && len(t.gsis) > 0 {
		for _, gsi := range t.gsis {
			builder.AddGlobalIndex(gsi.DynamoGlobalSecondaryIndex())
			builder.AddAttributeDefinition(gsi.Key.PartitionAttributeDefinition())
			// add any missing definitions
			if gsi.Key.sortKey != nil {
				builder.AddAttributeDefinition(gsi.Key.SortAttributeDefinition())
			}
		}
	}

	return builder
}

// NewTable creates new table with provided table name, table key, and and options
// Mapper key is required
func NewTable(name interface{}, key *Key) *Table {

	n, err := dyno.GetStringFromInterface(name)
	if err != nil {
		panic(err)
	}

	// create a table with given table key
	return &Table{
		name:     n,
		key:      key,
		wcus:     0,
		rcus:     0,
		mu:       sync.RWMutex{},
		lsis:     make(map[string]*Lsi),
		gsis:     make(map[string]*Gsi),
		onDemand: false,
	}
}

type UpdateCostUnitsOutput struct {
	Changed bool
}

// GsiCostUpdate used as input to SetCostUnits when updating cost units for Gsi's
type GsiCostUpdate struct {
	IndexName string
	Wcu       int64
	Rcu       int64
}

// UpdateWithDescription updates this table's settings with a dynamodb.TableDescription object
func (t *Table) UpdateWithDescription(desc *dynamodb.TableDescription) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.description = desc
	if desc.ProvisionedThroughput != nil || (desc.BillingModeSummary != nil && *desc.BillingModeSummary.BillingMode == "PROVISIONED") {
		// if throughput is provisioned then update the tables throughput values
		t.rcus = *desc.ProvisionedThroughput.ReadCapacityUnits
		t.wcus = *desc.ProvisionedThroughput.WriteCapacityUnits
	} else {
		t.onDemand = true
		t.rcus = 0
		t.wcus = 0
	}

	if len(desc.GlobalSecondaryIndexes) < 1 {
		return
	}

	// update global secondary indexes
	for _, gsiDescription := range desc.GlobalSecondaryIndexes {
		if gsi, ok := t.gsis[*gsiDescription.IndexName]; ok {
			gsi.Description = gsiDescription
			if *desc.BillingModeSummary.BillingMode == "PROVISIONED" {
				// if provisioned throughput then update throughput on this gsi
				gsi.WCUs = *gsiDescription.ProvisionedThroughput.WriteCapacityUnits
				gsi.RCUs = *gsiDescription.ProvisionedThroughput.ReadCapacityUnits
			} else {
				// update the onDemand flag and set throughput settings to 0
				gsi.WCUs = 0
				gsi.RCUs = 0
				gsi.OnDemand = true
			}
			if gsiDescription.Projection != nil {
				// if projection is set, update the gsi to match
				if gsi.ProjectionType != *gsiDescription.Projection.ProjectionType {
					gsi.ProjectionType = *gsiDescription.Projection.ProjectionType
				}
				if len(gsiDescription.Projection.NonKeyAttributes) > 0 {
					// if we have NonKeyAttributes update this gsi with these values
					gsi.ProjectionColumns = make([]string, len(gsiDescription.Projection.NonKeyAttributes))
					for i, col := range gsiDescription.Projection.NonKeyAttributes {
						gsi.ProjectionColumns[i] = *col
					}
				}
			} // if gsiDescription.Projection != nil {
		} // end if *desc.BillingModeSummary.BillingMode == "PROVISIONED"
	} // end for _, gsiDescription := range desc.GlobalSecondaryIndexes
	for _, lsiDescription := range desc.LocalSecondaryIndexes {
		if lsi, ok := t.lsis[*lsiDescription.IndexName]; ok {
			lsi.Description = lsiDescription
		}
	}
	t.arn = *desc.TableArn
}

// SetName sets the name for this table
func (t *Table) SetName(name string) *Table {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.name = name
	return t
}

// SetKey sets the key for this table
func (t *Table) SetKey(key *Key) *Table {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.key = key
	return t
}

// SetPartitionKey sets the partitionKey key for this table
func (t *Table) SetPartitionKey(pk *PartitionKey) *Table {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.key == nil {
		t.key = &Key{
			partitionKey: pk,
		}
		return t
	}
	t.key.partitionKey = pk
	return t
}

// SetSortKey sets the sortKey key for this table
func (t *Table) SetSortKey(sk *SortKey) *Table {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.key == nil {
		t.key = &Key{
			sortKey: sk,
		}
		return t
	}
	t.key.sortKey = sk
	return t
}

// SetRCUs sets the read cost units for this table
func (t *Table) SetRCUs(costUnits int64) *Table {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.rcus = costUnits
	return t
}

// SetWCUs sets the write cost units for this table
func (t *Table) SetWCUs(costUnits int64) *Table {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.wcus = costUnits
	return t
}

// SetOnDemand sets whether this table should be on demand cost based.
// if onDemand is true then wcus and rcus are set to 0
func (t *Table) SetOnDemand(onDemand bool) *Table {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.onDemand = onDemand
	if t.onDemand {
		t.wcus = 0
		t.rcus = 0
	}
	return t
}

// AddGSIs adds one or more gsis to this table
func (t *Table) AddGSIs(gsis ...*Gsi) *Table {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.gsis == nil {
		t.gsis = make(map[string]*Gsi)
		return t
	}
	for _, g := range gsis {
		if _, ok := t.gsis[g.Name]; ok {
			panic(fmt.Errorf("duplicate GSI with name '%s' cannot be added to table %s", g.Name, t.name))
		}
		t.gsis[g.Name] = g
	}
	return t
}

// Gsi gets gsi with given name, nil if not exists
func (t *Table) Gsi(gsiName string) *Gsi {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.gsis == nil {
		return nil
	}
	if _, ok := t.gsis[gsiName]; ok {
		return t.gsis[gsiName]
	}
	return nil
}

// AddLSIs adds one or more lsis to this table
func (t *Table) AddLSIs(lsis ...*Lsi) *Table {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.lsis == nil {
		t.lsis = make(map[string]*Lsi)
		return t
	}
	for _, l := range lsis {
		if _, ok := t.gsis[l.Name]; ok {
			panic(fmt.Errorf("duplicate LSI with name '%s' cannot be added to table %s", l.Name, t.name))
		}
		t.lsis[l.Name] = l
	}
	return t
}

// Lsi gets lsi with given name, nil if not exists
func (t *Table) Lsi(lsiName string) *Lsi {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.gsis == nil {
		return nil
	}
	if _, ok := t.lsis[lsiName]; ok {
		return t.lsis[lsiName]
	}
	return nil
}

// HasIndex checks if index with the given name exists in this table
func (t *Table) HasIndex(idxName string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.gsis != nil {
		if _, ok := t.gsis[idxName]; ok {
			return true
		}
	}
	if t.lsis != nil {
		if _, ok := t.lsis[idxName]; ok {
			return true
		}
	}
	return false
}

// UniqueKeyCondition returns a ConditionBuilder that represents a unique key condition
func (t *Table) UniqueKeyCondition() *expression.ConditionBuilder {
	t.mu.RLock()
	defer t.mu.RUnlock()
	cndSet := condition.NewSet()
	if !t.HasKey() {
		return nil
	}
	cndSet.AddAnd(condition.NotExists(t.PartitionKeyName()))
	// if we have a sortKey key make sure document contains a key
	if t.HasSortKey() {
		cndSet.AddAnd(condition.NotExists(t.SortKeyName()))
	}
	builder := cndSet.Builder()
	return &builder
}

// CreateBackupInput creates a CreateBackupInput for this table
func (t *Table) CreateBackupInput(backupName string) *dynamodb.CreateBackupInput {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return &dynamodb.CreateBackupInput{
		BackupName: &t.name,
		TableName:  &backupName,
	}
}

// CreateDeleteInput creates a DeleteItemInput for this table
func (t *Table) CreateDeleteInput(item interface{}, condition *expression.ConditionBuilder) (input *dynamodb.DeleteItemInput, err error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	// marshal the item
	var itemKey, dynoItem map[string]*dynamodb.AttributeValue

	dynoItem, err = encoding.MarshalItem(item)

	if err != nil {
		return
	}

	// get the key values
	itemKey = t.key.extract(dynoItem)

	return operation.CreateDeleteInput(t.name, itemKey, condition), nil
}

// CreateGetInput creates a new dynamodb.GetItemInput with the provided table and key object
func (t *Table) CreateGetInput(key interface{}, projection *expression.ProjectionBuilder) (input *dynamodb.GetItemInput, err error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var expr expression.Expression

	// encode the dynamo outputs
	keyItem, err := encoding.MarshalItem(key)
	if err != nil {
		return
	}

	// extract the key
	keyItem = t.key.extract(keyItem)

	// doPut the dynamo API call
	input = &dynamodb.GetItemInput{
		TableName: &t.name,
		Key:       keyItem,
	}

	if projection != nil {
		builder := expression.NewBuilder()
		builder = builder.WithProjection(*projection)
		expr, err = builder.Build()
		if err != nil {
			return
		}
		input.ExpressionAttributeNames = expr.Names()
		input.ProjectionExpression = expr.Projection()
	}
	return
}

// CreateBatchKeysAndAttributes creates a KeysAndAttributes map with the given table and items
func (t *Table) CreateBatchKeysAndAttributes(items interface{}, consistentRead bool, projection *expression.ProjectionBuilder) (input map[string]*dynamodb.KeysAndAttributes, err error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var (
		expr expression.Expression
		keys []map[string]*dynamodb.AttributeValue
	)

	keys = t.ExtractKeys(items)

	input = map[string]*dynamodb.KeysAndAttributes{
		t.name: {
			Keys:           make([]map[string]*dynamodb.AttributeValue, len(keys)),
			ConsistentRead: &consistentRead,
		},
	}

	if projection != nil {
		builder := expression.NewBuilder()
		builder = builder.WithProjection(*projection)
		expr, err = builder.Build()
		if err != nil {
			return
		}
		input[t.name].ExpressionAttributeNames = expr.Names()
		input[t.name].ProjectionExpression = expr.Projection()
	}

	return
}

type BackupResult struct {
	DescribeBackupOutput *dynamodb.DescribeBackupOutput
	Err                  error
}

// Backup creates a backup of this table
// returns a channel that will return a BackupResult when backup completes
func (t *Table) Backup(req *dyno.Request, backupName string, timeout *time.Duration) <-chan *BackupResult {
	doneCh := make(chan *BackupResult)
	go func() {
		result := &BackupResult{}
		defer func() {
			doneCh <- result
			close(doneCh)
		}()
		out, err := operation.BackupTable(t.name, backupName).
			Execute(req).
			OutputError()
		if err != nil {
			result.Err = err
			return
		}
		result.DescribeBackupOutput, result.Err = operation.WaitForBackupCompletion(req, *out.BackupDetails.BackupArn, timeout)
	}()
	return doneCh
}

// Publish creates this table in dynamodb
// returns a channel that will return a PublishResult when table is ready
func (t *Table) Publish(req *dyno.Request) <-chan *operation.CreateTableResult {
	doneCh := make(chan *operation.CreateTableResult)
	go func() {
		defer func() {
			close(doneCh)
		}()
		out := t.CreateTableBuilder().Operation().SetWait(true).Execute(req)
		if out.Error() == nil {
			t.UpdateWithDescription(out.Output().TableDescription)
		}
		doneCh <- out
	}()
	return doneCh
}

// Delete deletes this table in dynamodb
// returns a channel that will return a DeleteResult when table is ready
func (t *Table) Delete(req *dyno.Request, timeout *time.Duration) <-chan *operation.DeleteTableResult {
	doneCh := make(chan *operation.DeleteTableResult)
	go func() {
		t.mu.Lock()
		var out *operation.DeleteTableResult
		defer func() {
			t.mu.Unlock()
			doneCh <- out
			close(doneCh)
		}()
		out = operation.DeleteTable(t.name).Execute(req)
		if out.Error() != nil {
			return
		}
		// wait for the table to be deleted
		waitErr := operation.WaitForTableDeletion(req, *out.Output().TableDescription.TableName, timeout)
		if waitErr != nil {
			out.SetError(waitErr)
			return
		}
		// clear the description
		t.description = nil
	}()
	return doneCh
}

// BatchWriteBuilder creates a BatchWriteBuilder for this table with given puts and deletes
func (t *Table) BatchWriteBuilder(puts interface{}, deletes interface{}) *operation.BatchWriteBuilder {
	bw := operation.NewBatchWriteBuilder()
	if puts != nil {
		bw.AddPuts(t.Name(), puts)
	}
	if deletes != nil {
		bw.AddDeletes(t.Name(), deletes)
	}
	return bw
}

// GetItemBuilder creates a GetBuilder for this table with optional item
func (t *Table) GetItemBuilder(item interface{}) *operation.GetBuilder {
	b := operation.NewGetBuilder().SetTable(t.Name())
	if item != nil {
		keyItem := t.ExtractKey(item)
		b.SetKey(keyItem)
	}
	return b
}

// DeleteItemBuilder creates a DeleteItemBuilder for this table with optional item
func (t *Table) DeleteItemBuilder(item interface{}) *operation.DeleteItemBuilder {
	b := operation.NewDeleteBuilder().SetTable(t.Name())
	if item != nil {
		keyItem := t.ExtractKey(item)
		b.SetKey(keyItem)
	}
	return b
}

// UpdateItemBuilder creates an UpdateItemBuilder for this table with given item
func (t *Table) UpdateItemBuilder() *operation.UpdateItemBuilder {
	return operation.NewUpdateItemBuilder().SetTable(t.Name())
}

// ScanBuilder creates an ScanBuilder for this table
func (t *Table) ScanBuilder() *operation.ScanBuilder {
	return operation.NewScanBuilder().SetTable(t.Name())
}

// ScanIndexBuilder creates an ScanBuilder for this table with given index name
// returns index not found error if index doesnt exist
func (t *Table) ScanIndexBuilder(idx string) (*operation.ScanBuilder, error) {
	if !t.HasIndex(idx) {
		return nil, fmt.Errorf("%s is not a valid index on table %s", idx, t.Name())
	}
	return operation.NewScanBuilder().SetTable(t.Name()).SetIndex(idx), nil
}

// QueryBuilder creates an QueryBuilder for this table
func (t *Table) QueryBuilder() *operation.QueryBuilder {
	return operation.NewQueryBuilder().SetTable(t.Name())
}

// QueryIndexBuilder creates an QueryBuilder for this table with given index name
// returns index not found error if index doesnt exist
func (t *Table) QueryIndexBuilder(idx string) (*operation.QueryBuilder, error) {
	if !t.HasIndex(idx) {
		return nil, fmt.Errorf("%s is not a valid index on table %s", idx, t.Name())
	}
	return operation.NewQueryBuilder().SetTable(t.Name()).SetIndex(idx), nil
}

// PutItemBuilder creates an PutBuilder for this table with optional item
func (t *Table) PutItemBuilder(item interface{}) *operation.PutBuilder {
	return operation.NewPutBuilder().
		SetTable(t.Name()).
		SetItem(item)
}
