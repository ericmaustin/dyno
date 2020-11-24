package operation

import (
	"testing"

	"github.com/ericmaustin/dyno"
	"github.com/ericmaustin/dyno/condition"
	"github.com/stretchr/testify/suite"
)

type QueryTestSuite struct {
	suite.Suite
	sess *dyno.Session
}

func (q *QueryTestSuite) SetupSuite() {
	q.sess = createTestSession()
	createTestTable(q.sess)
	putTestRecords(q.sess)
}

func (q *QueryTestSuite) TearDownSuite() {
	destroytestTable(q.sess)
}

func (q *QueryTestSuite) TestScanOperation() {

	target := make([]*testItem, 0)

	// scan for records with no conditions
	scanOutput, err := NewQueryBuilder(nil).
		SetTable(testTableName).
		AddKeyCondition(condition.KeyEqual("id", "A")).
		Operation().                               // get the operation
		SetHandler(ItemSliceUnmarshaler(&target)). // set the handler to unmarshal the target
		Execute(q.sess.Request()).
		OutputError()

	q.NoError(err)
	q.NotNil(scanOutput)
	q.Len(target, 1)
	q.Equal("A", target[0].ID)
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestQueryTestSuite(t *testing.T) {
	suite.Run(t, new(QueryTestSuite))
}
