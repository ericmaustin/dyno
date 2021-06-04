package lock

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	awsSession "github.com/aws/aws-sdk-go/aws/session"
	"github.com/ericmaustin/dyno"
	"github.com/ericmaustin/dyno/operation"
	"github.com/ericmaustin/dyno/table"
	"github.com/stretchr/testify/assert"
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

	// create the session
	// create the session
	awsSess, err := awsSession.NewSession()
	assert.NoError(t, err)

	/* get a session */
	sess := dyno.New(awsSess).
		SetMaxTimeout(time.Minute)

	// set up the table
	tbl := dyno.NewTable(getTestTableName(), table.NewKey(table.NewPartitionStringKey("id"), nil))

	pubRes := <-tbl.Publish(sess.RequestWithTimeout(time.Minute))
	_, err = pubRes.OutputError()
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

	writeBatchInput := operation.NewBatchWriteBuilder().
		AddPuts(tbl.Name(), items).
		Build()

	_, err = operation.BatchWrite(writeBatchInput).
		Execute(sess.RequestWithTimeout(time.Minute)).
		OutputError()

	assert.NoError(t, err)

	queryInput := operation.NewQueryBuilder().
		SetTable(tbl.Name()).
		AddKeyEquals(tbl.PartitionKeyName(), items[0].ID).
		Build()

	resultItems := make([]*testItem, 0)

	queryOut, err := operation.Query(queryInput).
		SetHandler(operation.LoadSlice(&resultItems, nil)).
		Execute(sess.RequestWithTimeout(time.Minute)).
		OutputError()

	assert.NoError(t, err)
	assert.NotZero(t, len(queryOut))

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
