package dyno

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/ericmaustin/dyno/hash"
	"github.com/go-redis/redis/v8"
	"github.com/vmihailenco/msgpack/v5"
	"time"
)

// TODO: add the rest of the interface methods to Client

// NewDefaultClient creates a new Client with provided dynamodb Client
func NewDefaultClient(client *ddb.Client) *Client {
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

// Client represents a client to interact with both dynamodb and dax endpoints
type Client struct {
	ddb *ddb.Client
}

// DynamoDB gets a dynamo dynamodb.Client
func (c *Client) DynamoDB() *ddb.Client {
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

//type RedisClient struct {
//	*Client
//	key string
//	rd  *redis.Client
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

// RedisCallback wraps calls with redis and back-fills cache misses
type RedisCallback struct {
	rd        *redis.Client
	key       string
	clientKey string
	ttl       time.Duration
}

func (cb *RedisCallback) BatchGetItemInputCallback(ctx context.Context, input *ddb.BatchGetItemInput) (*ddb.BatchGetItemOutput, error) {
	cb.key = cb.clientKey + hash.NewBuffer().WriteBatchGetItemInput(input).SHA256String()
	raw, err := cb.rd.Get(ctx, cb.key).Bytes()

	if err == redis.Nil {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	out := new(ddb.BatchGetItemOutput)
	err = msgpack.Unmarshal(raw, out)

	return out, err
}

func (cb *RedisCallback) BatchGetItemOutputCallback(ctx context.Context, output *ddb.BatchGetItemOutput) error {
	b, err := msgpack.Marshal(output)

	if err != nil {
		return err
	}

	return cb.rd.Set(ctx, cb.key, b, cb.ttl).Err()
}
