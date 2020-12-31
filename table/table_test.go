package table

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/ericmaustin/dyno"
	"github.com/ericmaustin/dyno/log"
	"github.com/ericmaustin/dyno/operation"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
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
	SubID    int    `dyno:"sub_id"`
	SubField string `dyno:"sub_field"`
}

type testItemKey struct {
	ID string `dyno:"id"`
}

type testItem struct {
	Key       *testItemKey      `dyno:",*"`
	TestField string            `dyno:"test_field"`
	Embedded  *testEmbeddedItem `dyno:",*"`
}

var testTableName = ""

func getTestTableName() string {
	seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))
	if len(testTableName) < 1 {
		charset := "abcdefghijklmnopqrstuvwxyz" +
			"ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
			"0123456789_"

		// random string
		b := make([]byte, 50)
		for i := range b {
			b[i] = charset[seededRand.Intn(len(charset))]
		}
		testTableName = "__dyno_test_tables__" + string(b)
	}
	return testTableName
}

func createTestSession() *dyno.Session {
	logging := log.New()
	logging.SetLevel(logrus.DebugLevel)

	// create the session
	awsSess, err := session.NewSession()

	if err != nil {
		panic(err)
	}
	/* get a session */
	sess := dyno.New(awsSess).
		SetMaxTimeout(time.Minute).
		SetLogger(logging)

	return sess
}

func createTestTable(sess *dyno.Session) *Table {
	tbl := NewTable(getTestTableName(),
		NewKey(NewPartitionStringKey("id"), NewSortNumberKey("sub_id"))).
		SetOnDemand(true)
	err := (<-tbl.Publish(sess.Request())).Error()
	if err != nil {
		panic(err)
	}
	return tbl
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

	batchWriteInput := operation.NewBatchWriteBuilder(nil).
		AddPuts(getTestTableName(), testRecords).
		Build()

	err := operation.BatchWrite(batchWriteInput).
		SetConcurrency(5).
		Execute(sess.Request()).
		Error()

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
		SetHandler(operation.SliceLoader(&target)).
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
