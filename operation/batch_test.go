package operation

import (
	"context"
	"fmt"
	"github.com/ericmaustin/dyno"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

type BatchTestSuite struct {
	suite.Suite
	sess    *dyno.Session
	records []*testItem
	keys    []map[string]string
}

func (s *BatchTestSuite) SetupSuite() {
	s.sess = createTestSession()
	createTestTable(s.sess)
	s.records = putTestRecords(s.sess)
	s.keys = putTestKeys(s.records)
}

func (s *BatchTestSuite) TearDownSuite() {
	destroytestTable(s.sess)
}

func (s *BatchTestSuite) TestBatchGet() {
	batch := NewBatch(context.Background(), s.getGets()).
		SetWorkerCount(3)

	timerStart := time.Now()
	out := batch.Execute(s.sess)
	fmt.Printf("3 workers total time: %s\n", time.Since(timerStart))

	s.NoError(out.Error())
	s.Equal(5, out.Output().Success)
	fmt.Printf("%+v\n", out.Output())
}

func (s *BatchTestSuite) TestBatchGetOneWorker() {
	batch := NewBatch(context.Background(), s.getGets()).
		SetWorkerCount(1)

	timerStart := time.Now()
	out := batch.Execute(s.sess)
	fmt.Printf("1 worker total time: %s\n", time.Since(timerStart))

	s.NoError(out.Error())
	s.Equal(5, out.Output().Success)
	fmt.Printf("%+v\n", out.Output())
}

func (s *BatchTestSuite) getGets() []Operation {
	out := make([]Operation, len(s.records))
	for i, rec := range s.records {
		getInput := NewGetBuilder().
			SetTable(getTestTableName()).
			SetKey(map[string]string{
				"id": rec.ID,
			}).Input()
		out[i] = Get(getInput)
	}
	return out
}

// In order for 'go test' to runner this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestBatchTestSuite(t *testing.T) {
	suite.Run(t, new(BatchTestSuite))
}
