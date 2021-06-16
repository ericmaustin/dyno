package lock

import (
	"context"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ericmaustin/dyno/encoding"
	"testing"
	"time"

	"github.com/ericmaustin/dyno"
	"github.com/stretchr/testify/assert"
)

type testItem struct {
	ID        string `dyno:"id"`
	TestField string `dyno:"test_field"`
}

// TestLock tests locking functionality
func TestLock(t *testing.T) {

	// create the session
	// create the session
	db := dyno.CreateTestSession()

	// set up the table
	tbl := dyno.NewTable(dyno.TestTableName()).
		SetPartitionKey("id", "S")

	if err := tbl.Create(db); err != nil {
		panic(err)
	}

	defer func() {
		if err := tbl.Delete(db); err != nil {
			panic(err)
		}
	}()

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

	writeBatchInput := dyno.NewBatchWriteItemInput()
	for _, item := range items {
		writeBatchInput.AddPuts(tbl.Name(), encoding.MustMarshalMap(item))
	}

	if _, err := db.BatchWriteItem(writeBatchInput).Await(); err != nil {
		panic(err)
	}

	var resultItems []*testItem

	queryBuilder := dyno.NewQueryBuilder().
		SetTableName(tbl.Name()).
		AddKeyEquals(tbl.PartitionKeyName(), items[0].ID)

	queryBuilder.SetOutputCallback(func(ctx context.Context, output *dynamodb.QueryOutput) error {
		if len(output.Items) < 0 {
			return nil
		}
		for _, item := range output.Items {
			target := new(testItem)
			if err := encoding.UnmarshalMap(item, target); err != nil {
				return err
			}
			resultItems = append(resultItems, target)
		}
		return nil
	})

	queryInput, err := queryBuilder.Build()
	if err != nil {
		panic(err)
	}
	queryOutput, err := db.Query(queryInput).Await()
	assert.NoError(t, err)
	assert.NotZero(t, len(queryOutput.Items))

	itemMap := encoding.MustMarshalMap(items[0])
	key := tbl.ExtractKeys(itemMap)

	/* Test locking a record */
	lock := MustAcquire(tbl.Name(), key, db,
		OptHeartbeatFrequency(time.Millisecond*200),
		OptTimeout(time.Second),
		OptLeaseDuration(time.Second))

	assert.NoError(t, err)

	/* Test locking the same record - this should fail */
	_, err = Acquire(tbl.Name(), key, db,
		OptHeartbeatFrequency(time.Millisecond*200),
		OptTimeout(time.Second*5),
		OptLeaseDuration(time.Second))

	assert.Error(t, err)

	// release
	err = lock.Release()

	assert.NoError(t, err)
	if err != nil {
		panic(err)
	}
}
