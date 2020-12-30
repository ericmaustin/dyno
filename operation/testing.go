package operation

import (
	"math/rand"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ericmaustin/dyno"
	"github.com/ericmaustin/dyno/log"
	"github.com/sirupsen/logrus"
)

type testEmbeddedItem struct {
	SubID    int
	SubField string
}

type testItem struct {
	ID        string            `dyno:"id"`
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
		testTableName = "__dyno_test_operations__" + string(b)
	}
	return testTableName
}

func createTestSession() *dyno.Session {
	logger := log.New()
	logger.SetLevel(logrus.DebugLevel)

	// create the session
	awsSess, err := session.NewSession()

	if err != nil {
		panic(err)
	}
	/* get a session */
	sess := dyno.New(awsSess).
		SetMaxTimeout(time.Minute).
		SetLogger(logger)

	return sess
}

func createTestTable(sess *dyno.Session) {
	// set up the table
	tblInput := &dynamodb.CreateTableInput{
		TableName:   dyno.StringPtr(getTestTableName()),
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
	}

	err := CreateTable(tblInput).SetWait(true).Execute(sess.Request()).Error()
	if err != nil {
		panic(err)
	}
}

func destroytestTable(sess *dyno.Session) {
	err := DeleteTable(getTestTableName()).
		Execute(sess.Request()).
		Error()

	if err != nil {
		panic(err)
	}

	// wait for table to be deleted
	err = WaitForTableDeletion(sess.Request(), getTestTableName(), nil)
	if err != nil {
		panic(err)
	}
}

func putTestKeys(items []*testItem) []map[string]string {
	out := make([]map[string]string, len(items))
	for i, item := range items {
		out[i] = map[string]string{
			"id": item.ID,
		}
	}
	return out
}

func putTestRecords(sess *dyno.Session) []*testItem {
	testRecords := []*testItem{
		{
			ID:        "A",
			TestField: "A field",
			Embedded: &testEmbeddedItem{
				SubID:    1,
				SubField: "SubA",
			},
		},
		{
			ID:        "B",
			TestField: "B field",
			Embedded: &testEmbeddedItem{
				SubID:    2,
				SubField: "SubB",
			},
		},
		{
			ID:        "C",
			TestField: "C field",
			Embedded: &testEmbeddedItem{
				SubID:    3,
				SubField: "SubC",
			},
		},
		{
			ID:        "D",
			TestField: "D field",
			Embedded: &testEmbeddedItem{
				SubID:    4,
				SubField: "SubD",
			},
		},
		{
			ID:        "E",
			TestField: "E field",
			Embedded: &testEmbeddedItem{
				SubID:    5,
				SubField: "SubE",
			},
		},
	}

	batchWriteInput := NewBatchWriteBuilder(nil).AddPuts(getTestTableName(), testRecords).Input()
	err := BatchWrite(batchWriteInput).SetConcurrency(5).Execute(sess.Request()).Error()
	if err != nil {
		panic(err)
	}
	return testRecords
}
