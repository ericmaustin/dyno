package operation

import (
	"fmt"
	testing2 "github.com/ericmaustin/dyno/internal/dynotest"
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
	q.sess = testing2.createTestSession()
	testing2.createTestTable(q.sess)
	testing2.putTestRecords(q.sess)
}

func (q *QueryTestSuite) TearDownSuite() {
	testing2.destroytestTable(q.sess)
}

func (q *QueryTestSuite) TestQueryOperation() {

	target := make([]*testing2.testItem, 0)

	// scan for records with no conditions
	scanOutput, err := NewQueryBuilder().
		SetTable(testing2.getTestTableName()).
		AddKeyCondition(condition.KeyEqual("id", "A")).
		BuildOperation().                    // get the operation
		SetHandler(LoadSlice(&target, nil)). // set the handler to unmarshal the target
		Execute(q.sess.Request()).
		OutputError()

	q.NoError(err)
	q.NotNil(scanOutput)
	q.Len(target, 1)
	q.Equal("A", target[0].ID)
}

func (q *QueryTestSuite) TestQueryCountOperation() {

	// scan for records with no conditions
	scanOutput, err := NewQueryBuilder().
		SetTable(testing2.getTestTableName()).
		AddKeyCondition(condition.KeyEqual("id", "A")).
		BuildCountOperation(). // get the operation
		Execute(q.sess.Request()).
		OutputError()

	q.NoError(err)
	q.NotNil(scanOutput)
	q.Equal(scanOutput, int64(1))
	fmt.Println("scan count=", scanOutput)
}

// In order for 'go test' to Execute this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestQueryTestSuite(t *testing.T) {
	suite.Run(t, new(QueryTestSuite))
}
