package lock

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ericmaustin/dyno"
	"github.com/ericmaustin/dyno/encoding"
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
	client := dyno.CreateTestClient()

	// set up the table
	tbl := dyno.NewTable(dyno.TestTableName()).
		SetPartitionKey("id", types.ScalarAttributeTypeS)

	op, err := tbl.Create()
	if err != nil {
		panic(err)
	}

	_, err = op.Invoke(context.Background(), client.DynamoDB()).Await()
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := tbl.Delete().Invoke(context.Background(), client.DynamoDB()); err != nil {
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

	bld := dyno.NewBatchWriteItemBuilder(nil)
	for _, item := range items {
		bld.AddPuts(tbl.Name(), encoding.MustMarshalMap(item))
	}

	input := bld.Build()

	if _, err := client.BatchWriteItem(context.Background(), input).Await(); err != nil {
		panic(err)
	}

	queryBuilder := dyno.NewQueryBuilder(nil).
		SetTableName(tbl.Name()).
		AddKeyEquals(tbl.PartitionKeyName(), items[0].ID)

	queryInput, err := queryBuilder.Build()
	if err != nil {
		panic(err)
	}

	queryOutput, err := client.Query(context.Background(), queryInput).Await()
	assert.NoError(t, err)
	assert.NotZero(t, len(queryOutput.Items))

	itemMap := encoding.MustMarshalMap(items[0])
	key := tbl.ExtractKeys(itemMap)

	/* Test locking a record */
	lock := MustAcquire(tbl.Name(), key, client,
		OptHeartbeatFrequency(time.Millisecond*200),
		OptTimeout(time.Second),
		OptLeaseDuration(time.Second))

	assert.NoError(t, err)

	/* Test locking the same record - this should fail */
	_, err = Acquire(tbl.Name(), key, client,
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
