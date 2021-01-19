package operation

import (
	"context"
	"fmt"
	"github.com/ericmaustin/dyno"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"sync"
	"testing"
	"time"
)

type RunnerTestSuite struct {
	suite.Suite
	sess    *dyno.Session
	records []*testItem
	keys    []map[string]string
}

func (s *RunnerTestSuite) SetupSuite() {
	s.sess = createTestSession()
	createTestTable(s.sess)
	s.records = putTestRecords(s.sess)
	s.keys = putTestKeys(s.records)
}

func (s *RunnerTestSuite) TearDownSuite() {
	destroytestTable(s.sess)
}

func (s *RunnerTestSuite) TestBatchGet() {

	runner := NewRunner(context.Background(), s.sess, 3)
	mu := &sync.Mutex{}

	var target []*testItem

	timerStart := time.Now()
	for _, rec := range s.records {
		getInput := NewGetBuilder().
			SetTable(getTestTableName()).
			SetKey(map[string]string{
				"id": rec.ID,
			}).Input()
		err := runner.Run(Get(getInput).SetHandler(LoadOneIntoSlice(&target, mu)))
		s.NoError(err)
	}

	err := runner.Wait().Stop().Error()

	fmt.Printf("3 routines total time= %s\n", time.Since(timerStart))

	s.NoError(err)
	s.Equal(len(s.records), len(target))
	fmt.Printf("%+v\n", target)
	for _, t := range target {
		fmt.Printf("[%s] -> %s\n", t.ID, t.TestField)
	}
}

func (s *RunnerTestSuite) TestBatchGetOneRoutine() {

	runner := NewRunner(context.Background(), s.sess, 1)
	mu := &sync.Mutex{}

	var target []*testItem

	timerStart := time.Now()
	for _, rec := range s.records {
		getInput := NewGetBuilder().
			SetTable(getTestTableName()).
			SetKey(map[string]string{
				"id": rec.ID,
			}).Input()
		err := runner.Run(Get(getInput).SetHandler(LoadOneIntoSlice(&target, mu)))
		s.NoError(err)
	}

	err := runner.Wait().Stop().Error()

	fmt.Printf("1 routine total time= %s\n", time.Since(timerStart))

	s.NoError(err)
	s.Equal(len(s.records), len(target))
	fmt.Printf("%+v\n", target)
	for _, t := range target {
		fmt.Printf("[%s] -> %s\n", t.ID, t.TestField)
	}
}

func (s *RunnerTestSuite) TestBatchGetEarlyFailure() {

	runner := NewRunner(context.Background(), s.sess, 3)
	mu := &sync.Mutex{}

	var target []*testItem

	for i, rec := range s.records {
		getInput := NewGetBuilder().
			SetTable(getTestTableName()).
			SetKey(map[string]string{
				"id": rec.ID,
			}).Input()

		if i <= 1 {
			err := runner.Run(Get(getInput).SetHandler(LoadOneIntoSlice(&target, mu)))
			s.NoError(err)
		} else {
			runner.Stop()
			err := runner.Run(Get(getInput).SetHandler(LoadOneIntoSlice(&target, mu)))
			assert.Error(s.T(), err)
		}

	}

	err := runner.Wait().Stop().Error()

	s.NoError(err)
	s.LessOrEqual(len(target), 4)
	fmt.Printf("%+v\n", target)
	for _, t := range target {
		fmt.Printf("[%s] -> %s\n", t.ID, t.TestField)
	}
}


// In order for 'go test' to Execute this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestRunner(t *testing.T) {
	suite.Run(t, new(RunnerTestSuite))
}
