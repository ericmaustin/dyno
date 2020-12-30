package dyno

import (
	"context"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ericmaustin/dyno/log"
	"github.com/google/uuid"
	"regexp"
	"sync"
	"time"
)

var (
	awsNameFilter     = regexp.MustCompile("[^a-zA-Z0-9._-]+")
	defaultMaxTimeout = time.Duration(5) * time.Minute
)

// Session represents a single dyno session that includes an aws session
type Session struct {
	instanceID string
	log        log.Logrus
	client     *dynamodb.DynamoDB
	mu         *sync.RWMutex
	awsSession *session.Session
	maxTimeout time.Duration
	ctx        context.Context
}

// Log returns the log for this session
func (s *Session) Log() log.Logrus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.log == nil {
		s.log = log.New().WithField("session_id", s.instanceID)
	}
	return s.log
}

// DynamoClient gets a dynamo client attached to this session
// lazy load a new session if one does not exist
func (s *Session) DynamoClient() *dynamodb.DynamoDB {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.client == nil {
		s.client = dynamodb.New(s.awsSession)
	}
	return s.client
}

// AWSSession returns the current AWS Session
func (s *Session) AWSSession() *session.Session {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.awsSession
}

// Ctx returns the context for this request
func (s *Session) Ctx() context.Context {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ctx
}

// SetContext sets the context
func (s *Session) SetContext(ctx context.Context) *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ctx = ctx
	return s
}

// SetLogger sets the logger for the session
func (s *Session) SetLogger(logger log.Logrus) *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log = logger.WithField("session_id", s.instanceID)
	return s
}

// MaxTimeout gets the maximum api call timeout for all requests made by this session
func (s *Session) MaxTimeout() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.maxTimeout
}

// SetMaxTimeout sets the maximum api call timeout for all requests made by this session
func (s *Session) SetMaxTimeout(timeout time.Duration) *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxTimeout = timeout
	return s
}

// New generates a new session with provided aws session
func New(awsSession *session.Session) *Session {
	return &Session{
		instanceID: uuid.New().String(),
		awsSession: awsSession,
		mu:         &sync.RWMutex{},
		maxTimeout: defaultMaxTimeout,
		ctx:        context.Background(),
	}
}

// NewWithContext generates a new session with provided aws session and a context
func NewWithContext(ctx context.Context, awsSession *session.Session) *Session {
	return &Session{
		instanceID: uuid.New().String(),
		awsSession: awsSession,
		mu:         &sync.RWMutex{},
		maxTimeout: defaultMaxTimeout,
		ctx:        ctx,
	}
}

// FilterName strips illegal characters from a string to conform to AWS limits
func FilterName(input string) string {
	filtered := awsNameFilter.ReplaceAllString(input, "")
	if len(filtered) > 255 {
		filtered = filtered[0:255]
	}
	return filtered
}

// Request creates a new request with a the given context
func (s *Session) Request() *Request {
	ctx, cancel := context.WithCancel(s.ctx)
	return newRequest(ctx, cancel, s)
}

// RequestWithTimeout creates a new session with the given timeout attached to the context
func (s *Session) RequestWithTimeout(timeout time.Duration) *Request {
	ctx, cancel := context.WithTimeout(s.ctx, timeout)
	return newRequest(ctx, cancel, s)
}
