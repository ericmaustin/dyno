package operation

import (
	"fmt"
	"git-codecommit.us-east-1.amazonaws.com/v1/repos/dyno.git"
	"github.com/stretchr/testify/suite"
	"testing"
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

	batch := NewBatch(s.getGets()).
		SetWorkerCount(5)

	out := batch.Execute(s.sess.Request())

	s.NoError(out.Error())
	s.Equal(5, out.Output().Success)
	fmt.Printf("%+v", out.Output())
}

func (s *BatchTestSuite) getGets() []Operation {
	out := make([]Operation, len(s.records))
	for i, rec := range s.records {
		getInput := NewGetBuilder(nil).
			SetTable(testTableName).
			SetKey(map[string]string{
				"id": rec.ID,
			}).Input()
		out[i] = Get(getInput)
	}
	return out
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestBatchTestSuite(t *testing.T) {
	suite.Run(t, new(BatchTestSuite))
}
