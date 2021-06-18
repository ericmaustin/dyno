package dyno

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// NewClient creates a new Client with provided dynamodb Client
func NewClient(client *ddb.Client) *Client {
	return &Client{
		ddb: client,
	}
}

// NewClientFromConfig creates a new Client with provided config
func NewClientFromConfig(config aws.Config, optFns ...func(*ddb.Options)) *Client {
	return &Client{
		ddb: ddb.NewFromConfig(config, optFns...),
	}
}

// Client represents a single dynamoDB session that includes an aws session
type Client struct {
	ddb *ddb.Client
	//mu sync.RWMutex
}

// DynamoDBClient gets a dynamo dynamodb.Client
func (c *Client) DynamoDBClient() *ddb.Client {
	return c.ddb
}

// Operation used to mark a type as being an Operation
type Operation interface {
	DynoInvoke(context.Context)
}

//
////GetItem runs a GetItem dynamodb operation with a GetItemInput and returns a GetItemPromise
//func (c *Client) GetItem(in *GetItemInput) *GetItemPromise {
//	return &GetItemPromise{c.Do(GetItemFunc(in))}
//}
//
////GetItemWithContext runs a GetItem dynamodb operation with a ctx context.Context and GetItemInput and returns a GetItemPromise
//func (c *Client) GetItemWithContext(ctx context.Context, in *GetItemInput) *GetItemPromise {
//	return &GetItemPromise{c.Do(GetItemFunc(in))}
//}
//
//// BatchGetItem runs a batch get item api call with a dynamodb.BatchGetItemBuilder
//func (c *Client) BatchGetItem(in *BatchGetItemBuilder) *BatchGetItemPromise {
//	return &BatchGetItemPromise{c.Do(BatchGetItemFunc(in))}
//}
//
//// BatchGetItemWithContext runs a batch get item api call with a ctx context.Context and dynamodb.BatchGetItemBuilder
//func (c *Client) BatchGetItemWithContext(ctx context.Context, in *BatchGetItemBuilder) *BatchGetItemPromise {
//	return &BatchGetItemPromise{c.DoWithContext(ctx, BatchGetItemFunc(in))}
//}
//
//// BatchGetItemAll runs a batch get item api call with a dynamodb.BatchGetItemBuilder
////and keeps querying until there are no more UnprocessedKeys
//func (c *Client) BatchGetItemAll(in *BatchGetItemBuilder) *BatchGetItemAllPromise {
//	return &BatchGetItemAllPromise{c.Do(BatchGetItemAllFunc(in))}
//}
//
//// BatchGetItemAllWithContext runs a batch get item api call with a ctx context.Context and dynamodb.BatchGetItemBuilder
////and keeps querying until there are no more UnprocessedKeys
//func (c *Client) BatchGetItemAllWithContext(ctx context.Context, in *BatchGetItemBuilder) *BatchGetItemAllPromise {
//	return &BatchGetItemAllPromise{c.DoWithContext(ctx, BatchGetItemAllFunc(in))}
//}
//
//// BatchWriteItem runs an BatchWriteItem api call with an BatchWriteItemInput and returns a BatchWriteItemPromise
//func (c *Client) BatchWriteItem(in *BatchWriteItemInput) *BatchWriteItemPromise {
//	return &BatchWriteItemPromise{c.Do(BatchWriteItemFunc(in))}
//}
//
//// BatchWriteItemWithContext runs an BatchWriteItem api call with an DeleteItemInput and returns a DeleteItemPromise
//func (c *Client) BatchWriteItemWithContext(ctx context.Context, in *BatchWriteItemInput) *BatchWriteItemPromise {
//	return &BatchWriteItemPromise{c.DoWithContext(ctx, BatchWriteItemFunc(in))}
//}
//
//// BatchWriteItemAll runs an BatchWriteItem api call with an BatchWriteItemInput and returns a BatchWriteItemPromise
//func (c *Client) BatchWriteItemAll(in *BatchWriteItemInput) *BatchWriteItemAllPromise {
//	return &BatchWriteItemAllPromise{c.Do(BatchWriteItemAllFunc(in))}
//}
//
//// BatchWriteItemAllWithContext runs an BatchWriteItem api call with an DeleteItemInput and returns a DeleteItemPromise
//func (c *Client) BatchWriteItemAllWithContext(ctx context.Context, in *BatchWriteItemInput) *BatchWriteItemAllPromise {
//	return &BatchWriteItemAllPromise{c.DoWithContext(ctx, BatchWriteItemAllFunc(in))}
//}
//
//// CreateTable runs an CreateTable item api call with an DeleteItemInput and returns a DeleteItemPromise
//func (c *Client) CreateTable(in *CreateTableBuilder) *CreateTablePromise {
//	return &CreateTablePromise{c.Do(CreateTableFunc(in))}
//}
//
//// CreateTableWithContext runs an CreateTable item api call with a context.Context and CreateTableBuilder and returns a DeleteItemPromise
//func (c *Client) CreateTableWithContext(ctx context.Context, in *CreateTableBuilder) *CreateTablePromise {
//	return &CreateTablePromise{c.DoWithContext(ctx, CreateTableFunc(in))}
//}
//
//// DescribeTable runs an DescribeTable item api call with an DescribeTableInput and returns a DescribeTablePromise
//func (c *Client) DescribeTable(in *DescribeTableInput) *DescribeTablePromise {
//	return &DescribeTablePromise{c.Do(DescribeTableFunc(in))}
//}
//
//// DescribeTableWithContext runs an DescribeTable item api call with a context.Context and DescribeTableInput and returns a DeleteItemPromise
//func (c *Client) DescribeTableWithContext(ctx context.Context, in *DescribeTableInput) *DescribeTablePromise {
//	return &DescribeTablePromise{c.DoWithContext(ctx, DescribeTableFunc(in))}
//}
//
//// DeleteTable runs an DeleteTable item api call with a DeleteTableInput and returns a DeleteTablePromise
//func (c *Client) DeleteTable(in *DeleteTableInput) *DeleteTablePromise {
//	return &DeleteTablePromise{c.Do(DeleteTableFunc(in))}
//}
//
//// DeleteTableWithContext runs an DeleteTabl item api call with a context.Context and DeleteTableInput and returns a DeleteItemPromise
//func (c *Client) DeleteTableWithContext(ctx context.Context, in *DeleteTableInput) *DeleteTablePromise {
//	return &DeleteTablePromise{c.DoWithContext(ctx, DeleteTableFunc(in))}
//}
//
//// CreateBackup runs an CreateBackup item api call with a CreateBackupInput and returns a CreateBackupPromise
//func (c *Client) CreateBackup(in *CreateBackupInput) *CreateBackupPromise {
//	return &CreateBackupPromise{c.Do(CreateBackupFunc(in))}
//}
//
//// CreateBackupWithContext runs an CreateTable item api call with a context.Context and CreateTableBuilder and returns a DeleteItemPromise
//func (c *Client) CreateBackupWithContext(ctx context.Context, in *CreateBackupInput) *CreateBackupPromise {
//	return &CreateBackupPromise{c.DoWithContext(ctx, CreateBackupFunc(in))}
//}
//
////WaitUntilTableExists runs a WaitUntilTableExistsFunc with a DescribeTableInput and returns a WaitPromise
//func (c *Client) WaitUntilTableExists(in *DescribeTableInput) *WaitPromise {
//	return &WaitPromise{c.Do(WaitUntilTableExistsFunc(in))}
//}
//
////WaitUntilTableExistsWithContext runs a WaitUntilTableExistsFunc with a context and DescribeTableInput and returns a WaitPromise
//func (c *Client) WaitUntilTableExistsWithContext(ctx context.Context, in *DescribeTableInput) *WaitPromise {
//	return &WaitPromise{c.DoWithContext(ctx, WaitUntilTableExistsFunc(in))}
//}
//
////WaitUntilTableNotExists runs a WaitUntilTableNotExistsFunc that returns a WaitPromise
//func (c *Client) WaitUntilTableNotExists(in *DescribeTableInput) *WaitPromise {
//	return &WaitPromise{c.Do(WaitUntilTableNotExistsFunc(in))}
//}
//
////WaitUntilTableNotExistsWithContext runs a WaitUntilTableNotExistsFunc with a context that returns a WaitPromise
//func (c *Client) WaitUntilTableNotExistsWithContext(ctx context.Context, in *DescribeTableInput) *WaitPromise {
//	return &WaitPromise{c.DoWithContext(ctx, WaitUntilTableNotExistsFunc(in))}
//}
//
////WaitUntilBackupExists runs a WaitUntilBackupExists that returns a WaitPromise
//func (c *Client) WaitUntilBackupExists(in *DescribeBackupInput) *WaitPromise {
//	return &WaitPromise{c.Do(WaitUntilBackupExists(in))}
//}
//
////WaitUntilBackupExistsWithContext runs a WaitUntilBackupExists that returns a WaitPromise
//func (c *Client) WaitUntilBackupExistsWithContext(ctx context.Context, in *DescribeBackupInput) *WaitPromise {
//	return &WaitPromise{c.DoWithContext(ctx, WaitUntilBackupExists(in))}
//}
