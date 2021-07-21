package dyno

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// NewSession creates a new Session with provided dynamodb Session
func NewSession(ctx context.Context, client *ddb.Client) *Session {
	return &Session{
		ctx: ctx,
		ddb: client,
	}
}

// NewSessionWithConfig creates a new Session with provided aws.Config
func NewSessionWithConfig(ctx context.Context, config aws.Config, optFns ...func(*ddb.Options)) *Session {
	return &Session{
		ctx: ctx,
		ddb: ddb.NewFromConfig(config, optFns...),
	}
}

// Session represents a client to interact with both dynamodb and dax endpoints
type Session struct {
	ctx context.Context
	ddb *ddb.Client
}

// WithContext creates a new Session from the existing session with a new context
func (s *Session) WithContext(ctx context.Context) *Session {
	return &Session{
		ctx: ctx,
		ddb: s.ddb,
	}
}

// DynamoDB gets a dynamo dynamodb.Client
func (s *Session) DynamoDB() *ddb.Client {
	return s.ddb
}

// Do runs an operation with this client
func (s *Session) Do(op Operation) {
	op.SetRunning()
	op.InvokeDynoOperation(s.ctx, s.ddb)
}
