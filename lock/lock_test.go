package lock

import (
	"fmt"
	awsSession "github.com/aws/aws-sdk-go/aws/session"
	"github.com/ericmaustin/dyno"
	"github.com/ericmaustin/dyno/logging"
	"github.com/ericmaustin/dyno/operation"
	"github.com/ericmaustin/dyno/table"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
	"time"
)

type testItem struct {
	ID        string `dyno:"id"`
	TestField string `dyno:"test_field"`
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
		testTableName = "__dyno_test_locks__" + string(b)
	}
	return testTableName
}

// TestLock tests locking functionality
func TestLock(t *testing.T) {

	log := logging.New()
	log.SetLevel(logrus.DebugLevel)
	// create the session
	// create the session
	awsSess, err := awsSession.NewSession()
	assert.NoError(t, err)

	/* get a session */
	sess := dyno.New(awsSess).
		SetMaxTimeout(time.Minute).
		SetLogger(log)

	// set up the table
	tbl := table.NewTable(getTestTableName(), table.NewKey(table.NewPartitionStringKey("id"), nil))

	pubRes := <-tbl.Publish(sess.RequestWithTimeout(time.Minute))
	sess.Log().Infof("pubRes: %v", pubRes)
	pubOut, err := pubRes.OutputError()
	assert.NoError(t, err)
	sess.Log().Infof("pub result: %v", pubOut)
	assert.NoError(t, err)

	items := []*testItem{
		{
			ID:        "a",
			TestField: "a field",
		},
		{
			ID:        "b",
			TestField: "b field",
		},
	}

	writeBatchInput := operation.NewBatchWriteBuilder(nil).
		AddPuts(tbl.Name(), items).
		Input()

	batchWriteOutput, err := operation.BatchWrite(writeBatchInput).
		Execute(sess.RequestWithTimeout(time.Minute)).
		OutputError()

	assert.NoError(t, err)

	sess.Log().Debugf("BatchPut cache execution time = %v", batchWriteOutput)

	queryInput := operation.NewQueryBuilder(nil).
		SetTable(tbl.Name()).
		AddKeyEquals(tbl.PartitionKeyName(), items[0].ID).
		Input()

	resultItems := make([]*testItem, 0)

	queryOut, err := operation.Query(queryInput).
		SetHandler(operation.ItemSliceUnmarshaler(&resultItems)).
		Execute(sess.RequestWithTimeout(time.Minute)).
		OutputError()

	assert.NoError(t, err)
	assert.NotZero(t, len(queryOut))

	for _, d := range resultItems {
		sess.Log().Debugf("QueryOperation id %v", d.ID)
	}

	/* Test locking a record */
	lock, err := Acquire(tbl, items[0], sess,
		OptHeartbeatFrequency(time.Millisecond*200),
		OptTimeout(time.Second),
		OptLeaseDuration(time.Second))

	assert.NoError(t, err)

	/* Test locking the same record - this should fail */
	_, err2 := Acquire(tbl, items[0], sess,
		OptHeartbeatFrequency(time.Millisecond*200),
		OptTimeout(time.Second*5),
		OptLeaseDuration(time.Second))

	sess.Log().Debugf("lock attempt #2 err: %s", err2)
	assert.Error(t, err2)

	// release
	err = lock.Release()

	assert.NoError(t, err)
	if err != nil {
		panic(err)
	}

	/* TEAR DOWN */
	delRes, err := (<-tbl.Delete(sess.Request(), nil)).OutputError()
	assert.NoError(t, err)
	fmt.Printf("DeleteTable output: %v", delRes)
}
