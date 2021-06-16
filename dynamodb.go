package dyno

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/segmentio/ksuid"
	"sync"
)

// New generates a new Client with provided aws session and a context
func New(cnf aws.Config, opts ...func(*dynamodb.Options)) *Client {
	return &Client{
		id:     ksuid.New().String(),
		Client: dynamodb.NewFromConfig(cnf, opts...),
	}
}

// Client represents a single dynamoDB session that includes an aws session
type Client struct {
	*dynamodb.Client
	id string
	mu sync.RWMutex
}

// DynamoDBClient gets a dynamo dynamodb.Client
func (db *Client) DynamoDBClient() *dynamodb.Client {
	db.mu.Lock()
	dy := db.Client
	db.mu.Unlock()
	return dy
}

// ID gets the session id string
func (db *Client) ID() string {
	db.mu.RLock()
	id := db.id
	db.mu.RUnlock()
	return id
}

// Do executes a given execution function
func (db *Client) Do(exe Executable) *Promise {
	return Execute(context.Background(), db.Client, exe)
}

// DoWithContext executes a given execution with a provided context.Context
func (db *Client) DoWithContext(ctx context.Context, exe Executable) *Promise {
	return Execute(ctx, db.DynamoDB, exe)
}

// Scan executes a scan api call with a ScanInput
func (db *Client) Scan(in *ScanInput) *ScanPromise {
	return &ScanPromise{db.Do(ScanFunc(in))}
}

// ScanWithContext executes a scan api call with a context.Context and ScanInput
func (db *Client) ScanWithContext(ctx context.Context, in *ScanInput) *ScanPromise {
	return &ScanPromise{db.DoWithContext(ctx, ScanFunc(in))}
}

// ScanAll executes a scan api call with a dynamodb.ScanInput and keeps scanning until there are no more
// LastEvaluatedKey values
func (db *Client) ScanAll(in *ScanInput) *ScanAllPromise {
	return &ScanAllPromise{db.Do(ScanAllFunc(in))}
}

// ScanAllWithContext executes a scan api call with a context.Context and dynamodb.ScanInput and keeps scanning until there are no more
// LastEvaluatedKey values
func (db *Client) ScanAllWithContext(ctx context.Context, in *ScanInput) *ScanAllPromise {
	return &ScanAllPromise{db.DoWithContext(ctx, ScanAllFunc(in))}
}

//Query executes a query api call with a QueryInput
func (db *Client) Query(in *QueryInput) *QueryPromise {
	return &QueryPromise{db.Do(QueryFunc(in))}
}

//QueryWithContext executes a query api call with a context.Context and QueryInput
func (db *Client) QueryWithContext(ctx context.Context, in *QueryInput) *QueryPromise {
	return &QueryPromise{db.DoWithContext(ctx, QueryFunc(in))}
}

// QueryAll executes a query api call with a QueryInput and keeps querying until there are no more
// LastEvaluatedKey values
func (db *Client) QueryAll(in *QueryInput) *QueryAllPromise {
	return &QueryAllPromise{db.Do(QueryAllFunc(in))}
}

// QueryAllWithContext executes a query api call with a context.Context and QueryInput
//and keeps querying until there are no more LastEvaluatedKey values
func (db *Client) QueryAllWithContext(ctx context.Context, in *QueryInput) *QueryAllPromise {
	return &QueryAllPromise{db.DoWithContext(ctx, QueryAllFunc(in))}
}

// PutItem runs a put item api call with a PutItemInput and returns PutItemPromise
func (db *Client) PutItem(in *PutItemInput) *PutItemPromise {
	return &PutItemPromise{db.Do(PutItemFunc(in))}
}

// PutItemWithContext runs a put item api call with a ctx context.Context and PutItemInput and returns PutItemPromise
func (db *Client) PutItemWithContext(ctx context.Context, in *PutItemInput) *PutItemPromise {
	return &PutItemPromise{db.DoWithContext(ctx, PutItemFunc(in))}
}

//GetItem runs a GetItem dynamodb operation with a GetItemInput and returns a GetItemPromise
func (db *Client) GetItem(in *GetItemInput) *GetItemPromise {
	return &GetItemPromise{db.Do(GetItemFunc(in))}
}

//GetItemWithContext runs a GetItem dynamodb operation with a ctx context.Context and GetItemInput and returns a GetItemPromise
func (db *Client) GetItemWithContext(ctx context.Context, in *GetItemInput) *GetItemPromise {
	return &GetItemPromise{db.Do(GetItemFunc(in))}
}

// BatchGetItem runs a batch get item api call with a dynamodb.BatchGetItemBuilder
func (db *Client) BatchGetItem(in *BatchGetItemBuilder) *BatchGetItemPromise {
	return &BatchGetItemPromise{db.Do(BatchGetItemFunc(in))}
}

// BatchGetItemWithContext runs a batch get item api call with a ctx context.Context and dynamodb.BatchGetItemBuilder
func (db *Client) BatchGetItemWithContext(ctx context.Context, in *BatchGetItemBuilder) *BatchGetItemPromise {
	return &BatchGetItemPromise{db.DoWithContext(ctx, BatchGetItemFunc(in))}
}

// BatchGetItemAll runs a batch get item api call with a dynamodb.BatchGetItemBuilder
//and keeps querying until there are no more UnprocessedKeys
func (db *Client) BatchGetItemAll(in *BatchGetItemBuilder) *BatchGetItemAllPromise {
	return &BatchGetItemAllPromise{db.Do(BatchGetItemAllFunc(in))}
}

// BatchGetItemAllWithContext runs a batch get item api call with a ctx context.Context and dynamodb.BatchGetItemBuilder
//and keeps querying until there are no more UnprocessedKeys
func (db *Client) BatchGetItemAllWithContext(ctx context.Context, in *BatchGetItemBuilder) *BatchGetItemAllPromise {
	return &BatchGetItemAllPromise{db.DoWithContext(ctx, BatchGetItemAllFunc(in))}
}

// UpdateItem runs an update item api call with an UpdateItemInput and returns a UpdateItemPromise
func (db *Client) UpdateItem(in *UpdateItemInput) *UpdateItemPromise {
	return &UpdateItemPromise{db.Do(UpdateItemFunc(in))}
}

// UpdateItemWithContext runs a batch get item api call with a ctx context.Context and dynamodb.BatchGetItemBuilder
func (db *Client) UpdateItemWithContext(ctx context.Context, in *UpdateItemInput) *UpdateItemPromise {
	return &UpdateItemPromise{db.DoWithContext(ctx, UpdateItemFunc(in))}
}

// DeleteItem runs an update item api call with an DeleteItemInput and returns a DeleteItemPromise
func (db *Client) DeleteItem(in *DeleteItemInput) *DeleteItemPromise {
	return &DeleteItemPromise{db.Do(DeleteItemFunc(in))}
}

// DeleteItemWithContext runs an update item api call with an DeleteItemInput and returns a DeleteItemPromise
func (db *Client) DeleteItemWithContext(ctx context.Context, in *DeleteItemInput) *DeleteItemPromise {
	return &DeleteItemPromise{db.DoWithContext(ctx, DeleteItemFunc(in))}
}

// BatchWriteItem runs an BatchWriteItem api call with an BatchWriteItemInput and returns a BatchWriteItemPromise
func (db *Client) BatchWriteItem(in *BatchWriteItemInput) *BatchWriteItemPromise {
	return &BatchWriteItemPromise{db.Do(BatchWriteItemFunc(in))}
}

// BatchWriteItemWithContext runs an BatchWriteItem api call with an DeleteItemInput and returns a DeleteItemPromise
func (db *Client) BatchWriteItemWithContext(ctx context.Context, in *BatchWriteItemInput) *BatchWriteItemPromise {
	return &BatchWriteItemPromise{db.DoWithContext(ctx, BatchWriteItemFunc(in))}
}

// BatchWriteItemAll runs an BatchWriteItem api call with an BatchWriteItemInput and returns a BatchWriteItemPromise
func (db *Client) BatchWriteItemAll(in *BatchWriteItemInput) *BatchWriteItemAllPromise {
	return &BatchWriteItemAllPromise{db.Do(BatchWriteItemAllFunc(in))}
}

// BatchWriteItemAllWithContext runs an BatchWriteItem api call with an DeleteItemInput and returns a DeleteItemPromise
func (db *Client) BatchWriteItemAllWithContext(ctx context.Context, in *BatchWriteItemInput) *BatchWriteItemAllPromise {
	return &BatchWriteItemAllPromise{db.DoWithContext(ctx, BatchWriteItemAllFunc(in))}
}

// CreateTable runs an CreateTable item api call with an DeleteItemInput and returns a DeleteItemPromise
func (db *Client) CreateTable(in *CreateTableBuilder) *CreateTablePromise {
	return &CreateTablePromise{db.Do(CreateTableFunc(in))}
}

// CreateTableWithContext runs an CreateTable item api call with a context.Context and CreateTableBuilder and returns a DeleteItemPromise
func (db *Client) CreateTableWithContext(ctx context.Context, in *CreateTableBuilder) *CreateTablePromise {
	return &CreateTablePromise{db.DoWithContext(ctx, CreateTableFunc(in))}
}

// DescribeTable runs an DescribeTable item api call with an DescribeTableInput and returns a DescribeTablePromise
func (db *Client) DescribeTable(in *DescribeTableInput) *DescribeTablePromise {
	return &DescribeTablePromise{db.Do(DescribeTableFunc(in))}
}

// DescribeTableWithContext runs an DescribeTable item api call with a context.Context and DescribeTableInput and returns a DeleteItemPromise
func (db *Client) DescribeTableWithContext(ctx context.Context, in *DescribeTableInput) *DescribeTablePromise {
	return &DescribeTablePromise{db.DoWithContext(ctx, DescribeTableFunc(in))}
}

// DeleteTable runs an DeleteTable item api call with a DeleteTableInput and returns a DeleteTablePromise
func (db *Client) DeleteTable(in *DeleteTableInput) *DeleteTablePromise {
	return &DeleteTablePromise{db.Do(DeleteTableFunc(in))}
}

// DeleteTableWithContext runs an DeleteTabl item api call with a context.Context and DeleteTableInput and returns a DeleteItemPromise
func (db *Client) DeleteTableWithContext(ctx context.Context, in *DeleteTableInput) *DeleteTablePromise {
	return &DeleteTablePromise{db.DoWithContext(ctx, DeleteTableFunc(in))}
}

// CreateBackup runs an CreateBackup item api call with a CreateBackupInput and returns a CreateBackupPromise
func (db *Client) CreateBackup(in *CreateBackupInput) *CreateBackupPromise {
	return &CreateBackupPromise{db.Do(CreateBackupFunc(in))}
}

// CreateBackupWithContext runs an CreateTable item api call with a context.Context and CreateTableBuilder and returns a DeleteItemPromise
func (db *Client) CreateBackupWithContext(ctx context.Context, in *CreateBackupInput) *CreateBackupPromise {
	return &CreateBackupPromise{db.DoWithContext(ctx, CreateBackupFunc(in))}
}

//WaitUntilTableExists runs a WaitUntilTableExistsFunc with a DescribeTableInput and returns a WaitPromise
func (db *Client) WaitUntilTableExists(in *DescribeTableInput) *WaitPromise {
	return &WaitPromise{db.Do(WaitUntilTableExistsFunc(in))}
}

//WaitUntilTableExistsWithContext runs a WaitUntilTableExistsFunc with a context and DescribeTableInput and returns a WaitPromise
func (db *Client) WaitUntilTableExistsWithContext(ctx context.Context, in *DescribeTableInput) *WaitPromise {
	return &WaitPromise{db.DoWithContext(ctx, WaitUntilTableExistsFunc(in))}
}

//WaitUntilTableNotExists runs a WaitUntilTableNotExistsFunc that returns a WaitPromise
func (db *Client) WaitUntilTableNotExists(in *DescribeTableInput) *WaitPromise {
	return &WaitPromise{db.Do(WaitUntilTableNotExistsFunc(in))}
}

//WaitUntilTableNotExistsWithContext runs a WaitUntilTableNotExistsFunc with a context that returns a WaitPromise
func (db *Client) WaitUntilTableNotExistsWithContext(ctx context.Context, in *DescribeTableInput) *WaitPromise {
	return &WaitPromise{db.DoWithContext(ctx, WaitUntilTableNotExistsFunc(in))}
}

//WaitUntilBackupExists runs a WaitUntilBackupExists that returns a WaitPromise
func (db *Client) WaitUntilBackupExists(in *DescribeBackupInput) *WaitPromise {
	return &WaitPromise{db.Do(WaitUntilBackupExists(in))}
}

//WaitUntilBackupExistsWithContext runs a WaitUntilBackupExists that returns a WaitPromise
func (db *Client) WaitUntilBackupExistsWithContext(ctx context.Context, in *DescribeBackupInput) *WaitPromise {
	return &WaitPromise{db.DoWithContext(ctx, WaitUntilBackupExists(in))}
}
