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

// Invoke implements Operation
func (op OperationF) DynoInvoke(ctx context.Context, client *ddb.Client) {
	op(ctx, client)
}

