package dyno

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// TODO: add the rest of the interface methods to Client

type Client interface {
	BatchGetItem(ctx context.Context, input *ddb.BatchGetItemInput, optFns ...func(*BatchGetItemOptions)) (*ddb.BatchGetItemOutput, error)
	BatchWriteItem(ctx context.Context, input *ddb.BatchWriteItemInput, optFns ...func(*BatchWriteItemOptions)) (*ddb.BatchWriteItemOutput, error)
	CreateBackup(ctx context.Context, input *ddb.CreateBackupInput, optFns ...func(*CreateBackupOptions)) (*ddb.CreateBackupOutput, error)
	Scan(ctx context.Context, input *ddb.ScanInput, optFns ...func(*ScanOptions)) (*ddb.ScanOutput, error)
}

// NewDefaultClient creates a new DefaultClient with provided dynamodb DefaultClient
func NewDefaultClient(client *ddb.Client) *DefaultClient {
	return &DefaultClient{
		ddb: client,
	}
}

// NewClientFromConfig creates a new DefaultClient with provided config
func NewClientFromConfig(config aws.Config, optFns ...func(*ddb.Options)) *DefaultClient {
	return &DefaultClient{
		ddb: ddb.NewFromConfig(config, optFns...),
	}
}

// DefaultClient represents a client to interact with both dynamodb and dax endpoints
type DefaultClient struct {
	ddb *ddb.Client
}

// DynamoDBClient gets a dynamo dynamodb.Client
func (c *DefaultClient) DynamoDBClient() *ddb.Client {
	return c.ddb
}
// Operation used to mark a type as being an Operation
type Operation interface {
	DynoInvoke(context.Context, *ddb.Client)
}

// OperationF is an operation function that implements Operation
type OperationF func(context.Context, *ddb.Client)

// DynoInvoke implements Operation
func (op OperationF) DynoInvoke(ctx context.Context, client *ddb.Client) {
	op(ctx, client)
}
//
//
//type RedisClient struct {
//	*DefaultClient
//	key string
//	rd  *redis.DefaultClient
//	ttl time.Duration
//}
//
//// SetKey sets the RedisClient key
//func (c *RedisClient) SetKey(k string) {
//	c.key = k
//}
//
//// RedisCallback gets a new redis callback
//func (c *RedisClient) RedisCallback() *RedisCallback{
//	return &RedisCallback{
//		rd:        c.rd,
//		clientKey: c.key,
//		ttl:       c.ttl,
//	}
//}
//
//
//type RedisCallback struct {
//	rd *redis.DefaultClient
//	key string
//	clientKey string
//	ttl time.Duration
//}
//
//func (cb *RedisCallback) BatchGetItemInputCallback(ctx context.Context, input *ddb.BatchGetItemInput) (*ddb.BatchGetItemOutput, error) {
//	cb.key = cb.clientKey + hash.NewBuffer().WriteBatchGetItemInput(input).SHA256String()
//	raw, err := cb.rd.Get(ctx, cb.key).Bytes()
//
//	if err == redis.Nil {
//		return nil, nil
//	}
//
//	if err != nil {
//		return nil, err
//	}
//
//	out := new(ddb.BatchGetItemOutput)
//	err = msgpack.Unmarshal(raw, out)
//
//	return out, err
//}
//
//func (cb *RedisCallback) BatchGetItemOutputCallback(ctx context.Context, output *ddb.BatchGetItemOutput) error {
//	b, err := msgpack.Marshal(output)
//
//	if err != nil {
//		return err
//	}
//
//	return cb.rd.Set(ctx, cb.key, b, cb.ttl).Err()
//}
//
//
//// BatchGetItem executes a scan api call with a BatchGetItemInput
//func (c *RedisClient) BatchGetItem(ctx context.Context, input *ddb.BatchGetItemInput, optFns ...func(*BatchGetItemOptions)) (*ddb.BatchGetItemOutput, error) {
//
//	redisCallback := c.RedisCallback()
//
//	op := NewBatchGetItem(input, append(optFns, []func(*BatchGetItemOptions){
//		BatchGetItemWithInputCallback(redisCallback),
//		BatchGetItemWithOutputCallback(redisCallback),
//	}...)...)
//
//	op.DynoInvoke(ctx, c.ddb)
//	return op.Await()
//}