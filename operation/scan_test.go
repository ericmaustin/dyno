package operation

import (
	"github.com/ericmaustin/dyno"
	"github.com/ericmaustin/dyno/condition"
	"github.com/stretchr/testify/suite"
	"testing"
)

type ScanTestSuite struct {
	suite.Suite
	sess *dyno.Session
}

func (s *ScanTestSuite) SetupSuite() {
	s.sess = createTestSession()
	createTestTable(s.sess)
	putTestRecords(s.sess)
}

func (s *ScanTestSuite) TearDownSuite() {
	destroytestTable(s.sess)
}

func (s *ScanTestSuite) TestScanOperation() {
	// scan for records with no conditions
	scanInput := NewScanBuilder(nil).
		SetTable(getTestTableName()).
		SetSelect(ScanSelectAllAttributes).
		Input()

	target := make([]*testItem, 0)

	scanOutput, err := Scan(scanInput).SetHandler(ItemSliceUnmarshaler(&target)).
		Execute(s.sess.Request()).
		OutputError()

	s.NoError(err)
	s.NotNil(scanOutput)
	s.Len(target, 5)
}

func (s *ScanTestSuite) TestScanOperationWithFilter() {
	target := make([]*testItem, 0)

	// scan for records with no conditions
	scanOutput, err := NewScanBuilder(nil).
		SetTable(getTestTableName()).
		SetSelect(ScanSelectAllAttributes).
		AddFilter(condition.Equal("id", "A")).
		Operation().
		SetHandler(ItemSliceUnmarshaler(&target)).
		Execute(s.sess.Request()).
		OutputError()

	s.NoError(err)
	s.NotNil(scanOutput)
	s.Len(target, 1)

	target = make([]*testItem, 0)

	scanInput := NewScanBuilder(nil).
		SetTable(getTestTableName()).
		SetSelect(ScanSelectAllAttributes).
		AddFilter(condition.GreaterThanEqual("SubID", 1)).
		Input()

	scanOutput, err = Scan(scanInput).
		SetHandler(ItemSliceUnmarshaler(&target)).
		Execute(s.sess.Request()).
		OutputError()

	s.NoError(err)
	s.NotNil(scanOutput)
	s.Len(target, 5)

}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestScanTestSuite(t *testing.T) {
	suite.Run(t, new(ScanTestSuite))
}
