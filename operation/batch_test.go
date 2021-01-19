package operation

import (
	"context"
	"fmt"
	"github.com/ericmaustin/dyno"
	"github.com/stretchr/testify/suite"
	"sync"
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
	inputs, target := s.getGets()
	batch := NewBatch(context.Background(), 3).
		AddOperations(inputs...)

	timerStart := time.Now()
	err := batch.Execute(s.sess)
	fmt.Printf("3 workers total time: %s\n", time.Since(timerStart))

	s.NoError(err)
	s.Equal(len(s.records), len(*target))
	fmt.Printf("%+v\n", s.records)
}

func (s *BatchTestSuite) getGets() ([]Operation, *[]*testItem) {
	out := make([]Operation, len(s.records))
	mu := &sync.Mutex{}
	var target []*testItem
	for i, rec := range s.records {
		getInput := NewGetBuilder().
			SetTable(getTestTableName()).
			SetKey(map[string]string{
				"id": rec.ID,
			}).Input()
		out[i] = Get(getInput).SetHandler(LoadOneIntoSlice(&target, mu))
	}
	return out, &target
}

// In order for 'go test' to Execute this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestBatchTestSuite(t *testing.T) {
	suite.Run(t, new(BatchTestSuite))
}
