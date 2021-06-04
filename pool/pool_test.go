package operation

import (
	"fmt"
	"github.com/ericmaustin/dyno"
	"github.com/ericmaustin/dyno/internal/dynotest"
	"github.com/stretchr/testify/suite"
	"sync"
	"testing"
	"time"
)

type PoolTestSuite struct {
	suite.Suite
	sess    *dyno.Session
	records []*dyno.TestItem
	keys    []map[string]string
}

func (s *PoolTestSuite) SetupSuite() {
	s.sess = dyno.CreateTestSession()
	dyno.CreateTestTable(s.sess)
	s.records = dynotest.PutTestRecords(s.sess)
	s.keys = dynotest.PutTestKeys(s.records)
}

func (s *PoolTestSuite) TearDownSuite() {
	dynotest.DestroyTestTable(s.sess)
}

func (s *PoolTestSuite) TestBatchGet() {

	pool := NewPool(s.sess, 3)
	defer pool.Stop()
	mu := &sync.Mutex{}

	var target []*dyno.TestItem
	var outs []*PoolResult

	timerStart := time.Now()
	for _, rec := range s.records {
		getInput := NewGetBuilder().
			SetTable(getTestTableName()).
			SetKey(map[string]string{
				"id": rec.ID,
			}).Input()
		out, err := pool.Do(Get(getInput).SetHandler(LoadOneIntoSlice(&target, mu)), s.sess.Request())
		s.NoError(err)
		outs = append(outs, out)
	}

	for _, out := range outs {
		err := out.Error()
		s.NoError(err)
	}

	fmt.Printf("3 workers total time= %s\n", time.Since(timerStart))

	s.Equal(len(s.records), len(target))
	fmt.Printf("%+v\n", target)
	for _, t := range target {
		fmt.Printf("[%s] -> %s\n", t.ID, t.TestField)
	}
}

// In order for 'go test' to Execute this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestPool(t *testing.T) {
	suite.Run(t, new(PoolTestSuite))
}
