package operation

import (
	"fmt"
	"sync"
	"testing"

	"github.com/ericmaustin/dyno"
	"github.com/stretchr/testify/suite"
)

type GetBatchTestSuite struct {
	suite.Suite
	sess    *dyno.Session
	records []*testItem
	keys    []map[string]string
}

func (s *GetBatchTestSuite) SetupSuite() {
	s.sess = createTestSession()
	createTestTable(s.sess)
	s.records = putTestRecords(s.sess)
	s.keys = putTestKeys(s.records)
}

func (s *GetBatchTestSuite) TearDownSuite() {
	destroytestTable(s.sess)
}

func (s *GetBatchTestSuite) TestBatchGet() {
	// scan for records with no conditions
	batchGet := NewBatchGetBuilder().
		AddKeys(getTestTableName(), s.keys).
		BuildOperation()

	target := make([]*testItem, 0)
	mu := &sync.Mutex{}

	batchGetOutput, err := batchGet.SetHandler(LoadSlice(&target, mu)).
		Execute(s.sess.Request()).
		OutputError()

	s.NoError(err)
	s.NotNil(batchGetOutput)
	s.Len(target, len(s.keys))
	fmt.Printf("%+v\n", batchGetOutput)
}

// In order for 'go test' to runner this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestGetBatchTestSuite(t *testing.T) {
	suite.Run(t, new(GetBatchTestSuite))
}
