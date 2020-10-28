package table

import (
	"fmt"
	"git-codecommit.us-east-1.amazonaws.com/v1/repos/dyno.git"
	"git-codecommit.us-east-1.amazonaws.com/v1/repos/dyno.git/logging"
	"git-codecommit.us-east-1.amazonaws.com/v1/repos/dyno.git/operation"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

// set up the fixtures

// Suite for testing creating a table
type CRUDTableTest struct {
	suite.Suite
	table   *Table
	sess    *dyno.Session
	records []*testItem
}

type testEmbeddedItem struct {
	SubID    int
	SubField string
}

type testItemKey struct {
	ID string `dyno:"id"`
}

type testItem struct {
	Key       *testItemKey      `dyno:",*"`
	TestField string            `dyno:"test_field"`
	Embedded  *testEmbeddedItem `dyno:",*"`
}

const testTableName = "__table_testing"

func createTestSession() *dyno.Session {
	log := logging.New()
	log.SetLevel(logrus.DebugLevel)

	// create the session
	awsSess, err := session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region: dyno.StringPtr("us-east-1"),
		},
		Profile:           "mt2_dev",
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		panic(err)
	}
	/* get a session */
	sess := dyno.New(awsSess).
		SetMaxTimeout(time.Minute).
		SetLogger(log)

	return sess
}

func createTestTable(sess *dyno.Session) *Table {
	tbl := NewTable(testTableName, NewKey(NewPartitionStringKey("id"), nil)).
		SetOnDemand(true)
	err := (<-tbl.Publish(sess.Request(), nil)).Error()
	if err != nil {
		panic(err)
	}
	return tbl
}

func putTestKeys(items []*testItem) []*testItemKey {
	out := make([]*testItemKey, len(items))
	for i, item := range items {
		out[i] = item.Key
	}
	return out
}

func putTestRecords(sess *dyno.Session) []*testItem {
	testRecords := []*testItem{
		{
			Key:       &testItemKey{"A"},
			TestField: "A field",
			Embedded: &testEmbeddedItem{
				SubID:    1,
				SubField: "SubA",
			},
		},
		{
			Key:       &testItemKey{"B"},
			TestField: "B field",
			Embedded: &testEmbeddedItem{
				SubID:    2,
				SubField: "SubB",
			},
		},
		{
			Key:       &testItemKey{"C"},
			TestField: "C field",
			Embedded: &testEmbeddedItem{
				SubID:    3,
				SubField: "SubC",
			},
		},
		{
			Key:       &testItemKey{"D"},
			TestField: "D field",
			Embedded: &testEmbeddedItem{
				SubID:    4,
				SubField: "SubD",
			},
		},
		{
			Key:       &testItemKey{"E"},
			TestField: "E field",
			Embedded: &testEmbeddedItem{
				SubID:    5,
				SubField: "SubE",
			},
		},
	}

	batchWriteInput := operation.NewBatchWriteBuilder(nil).AddPuts(testTableName, testRecords).Input()
	err := operation.BatchWrite(batchWriteInput).SetConcurrency(5).Execute(sess.Request()).Error()
	if err != nil {
		panic(err)
	}
	return testRecords
}

func TestTables(t *testing.T) {
	suite.Run(t, new(CRUDTableTest))
}

// SetupSuite creates the table for the suite
func (s *CRUDTableTest) SetupSuite() {
	s.sess = createTestSession()
	s.table = createTestTable(s.sess)
	s.records = putTestRecords(s.sess)
}

func (s *CRUDTableTest) TearDownSuite() {
	// delete items in the table
	out, err := (<-s.table.Delete(s.sess.Request(), nil)).OutputError()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Delete Table Output:\n%+v\n", out)
}

func (s *CRUDTableTest) TestScanTable() {
	target := make([]*testItem, 0)
	out, err := s.table.ScanBuilder().
		Operation().
		SetHandler(operation.ItemSliceUnmarshaler(&target)).
		Execute(s.sess.Request()).
		OutputError()
	if err != nil {
		panic(err)
	}
	s.NotNil(out)
	fmt.Printf("%+v", out)
	s.Equal(5, len(target))
	fmt.Printf("%+v\n", target[0])
	fmt.Printf("%+v\n", target[0].Key)
}
