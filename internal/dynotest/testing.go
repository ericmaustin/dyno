package dynotest

import (
	"github.com/ericmaustin/dyno/operation"
	"math/rand"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ericmaustin/dyno"
)

type TestEmbeddedItem struct {
	SubID    int
	SubField string
}

type TestItem struct {
	ID        string            `dyno:"id"`
	TestField string            `dyno:"test_field"`
	Embedded  *TestEmbeddedItem `dyno:",*"`
}

type BaseTestSuite struct {

}

var testTableName = ""

func GetTestTableName() string {
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
		testTableName = "__dyno_test_operations__" + string(b)
	}
	return testTableName
}

func CreateTestSession() *dyno.Session {
	// create the session
	awsSess, err := session.NewSession()

	if err != nil {
		panic(err)
	}
	/* get a session */
	sess := dyno.New(awsSess).
		SetMaxTimeout(time.Minute)

	return sess
}

func CreateTestTable(sess *dyno.Session) {
	// set up the table
	tblInput := &dynamodb.CreateTableInput{
		TableName:   dyno.StringPtr(GetTestTableName()),
		BillingMode: dyno.StringPtr("PAY_PER_REQUEST"),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: dyno.StringPtr("id"),
				AttributeType: dyno.StringPtr("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: dyno.StringPtr("id"),
				KeyType:       dyno.StringPtr("HASH"),
			},
		},
		GlobalSecondaryIndexes: []*dynamodb.GlobalSecondaryIndex{

		},
	}

	err := operation.CreateTable(tblInput).SetWait(true).Execute(sess.Request()).Error()
	if err != nil {
		panic(err)
	}
}

func DestroyTestTable(sess *dyno.Session) {
	err := operation.DeleteTable(GetTestTableName()).
		Execute(sess.Request()).
		Error()

	if err != nil {
		panic(err)
	}

	// wait for table to be deleted
	err = operation.WaitForTableDeletion(sess.Request(), GetTestTableName(), nil)
	if err != nil {
		panic(err)
	}
}

func PutTestKeys(items []*TestItem) []map[string]string {
	out := make([]map[string]string, len(items))
	for i, item := range items {
		out[i] = map[string]string{
			"id": item.ID,
		}
	}
	return out
}

func PutTestRecords(sess *dyno.Session) []*TestItem {
	testRecords := []*TestItem{
		{
			ID:        "A",
			TestField: "A field",
			Embedded: &TestEmbeddedItem{
				SubID:    1,
				SubField: "SubA",
			},
		},
		{
			ID:        "B",
			TestField: "B field",
			Embedded: &TestEmbeddedItem{
				SubID:    2,
				SubField: "SubB",
			},
		},
		{
			ID:        "C",
			TestField: "C field",
			Embedded: &TestEmbeddedItem{
				SubID:    3,
				SubField: "SubC",
			},
		},
		{
			ID:        "D",
			TestField: "D field",
			Embedded: &TestEmbeddedItem{
				SubID:    4,
				SubField: "SubD",
			},
		},
		{
			ID:        "E",
			TestField: "E field",
			Embedded: &TestEmbeddedItem{
				SubID:    5,
				SubField: "SubE",
			},
		},
	}

	batchWriteInput := operation.NewBatchWriteBuilder().AddPuts(GetTestTableName(), testRecords).Build()
	err := operation.BatchWrite(batchWriteInput).SetConcurrency(5).Execute(sess.Request()).Error()
	if err != nil {
		panic(err)
	}
	return testRecords
}
