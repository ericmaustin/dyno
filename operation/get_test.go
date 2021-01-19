package operation

import (
	"fmt"
	"testing"

	"github.com/ericmaustin/dyno"
	"github.com/stretchr/testify/suite"
)

type GetTestSuite struct {
	suite.Suite
	sess    *dyno.Session
	records []*testItem
	keys    []map[string]string
}

func (s *GetTestSuite) SetupSuite() {
	s.sess = createTestSession()
	createTestTable(s.sess)
	s.records = putTestRecords(s.sess)
	s.keys = putTestKeys(s.records)
}

func (s *GetTestSuite) TearDownSuite() {
	destroytestTable(s.sess)
}

func (s *GetTestSuite) TestBatchGet() {

	target := &testItem{}

	getOperation := NewGetBuilder().
		SetTable(getTestTableName()).
		SetKey(map[string]string{
			"id": "A",
		}).BuildOperation()

	getOutput, err := getOperation.SetHandler(LoadOne(target)).
		Execute(s.sess.Request()).
		OutputError()

	s.NoError(err)
	s.NotNil(getOutput)
	s.NotNil(target)
	s.Equal("A", target.ID)
	fmt.Printf("%+v\n", target)
}

// In order for 'go test' to Execute this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestGetTestSuite(t *testing.T) {
	suite.Run(t, new(GetTestSuite))
}
