package dyno

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	ddbType "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ericmaustin/dyno/encoding"
	"github.com/segmentio/ksuid"
	"math/rand"
	"time"
)

const stringChars = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
	"0123456789_"

var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

// TestEmbeddedItem represents a simple embedded test record used for testing
type TestEmbeddedItem struct {
	Foo float64
	Bar int
	Baz []byte
}

// TestItem represents a simple test record used for testing
type TestItem struct {
	ID        string            `dynamodbav:"id"`
	TimeStamp int64             `dynamodbav:"timestamp"`
	Embedded  *TestEmbeddedItem `dynamodbav:",*"`
}

// NewTestItem creates a new random TestItem
func NewTestItem() *TestItem {
	return &TestItem{
		ID:        RandomString(100),
		TimeStamp: rand.Int63n(1e6),
		Embedded: &TestEmbeddedItem{
			Foo: rand.Float64(),
			Bar: rand.Intn(10),
			Baz: []byte(RandomString(4)),
		},
	}
}

// TestItemMarshaller represents a simple test record used for testing with marshalling
type TestItemMarshaller struct {
	ID        string
	TimeStamp time.Time
	Embedded  *TestEmbeddedItem
}

// MarshalAttributeValueMap implements the encoding.ItemMarshaller interface
func (m TestItemMarshaller) MarshalAttributeValueMap(av map[string]ddbType.AttributeValue) error {
	if m.Embedded == nil {
		m.Embedded = new(TestEmbeddedItem)
	}

	mm := encoding.ValueMarshalMap{
		"id":        encoding.StringMarshaler(&m.ID, encoding.NilNil),
		"timestamp": encoding.UnixNanoMarshaler(&m.TimeStamp, encoding.NilNil),
	}

	if m.Embedded != nil {
		mm["Foo"] = encoding.Float64Marshaler(&m.Embedded.Foo, encoding.NilNil)
		mm["Bar"] = encoding.IntMarshaler(&m.Embedded.Bar, encoding.NilNil)
		mm["Baz"] = encoding.BytesMarshaler(m.Embedded.Baz, encoding.NilNil)
	}

	return mm.MarshalToMap(av)
}

// UnmarshalAttributeValueMap implements the encoding.ItemUnmarshaller interface
func (m *TestItemMarshaller) UnmarshalAttributeValueMap(avMap map[string]ddbType.AttributeValue) error {
	if m.Embedded == nil {
		m.Embedded = new(TestEmbeddedItem)
	}

	mm := encoding.ValueUnmarshalerMap{
		"id":        encoding.StringUnmarshaler(&m.ID),
		"timestamp": encoding.UnixNanoUnmarshaler(&m.TimeStamp),
		"Foo":       encoding.Float64Unmarshaler(&m.Embedded.Foo),
		"Bar":       encoding.IntUnmarshaler(&m.Embedded.Bar),
		"Baz":       encoding.BytesUnmarshaler(&m.Embedded.Baz),
	}

	return mm.UnmarshalAttributeValueMap(avMap)
}

// NewMarshalledTestItem creates a new random TestItemMarshaller
func NewMarshalledTestItem() *TestItemMarshaller {
	return &TestItemMarshaller{
		ID:        RandomString(100),
		TimeStamp: time.Unix(rand.Int63n(1e6), 0),
		Embedded: &TestEmbeddedItem{
			Foo: rand.Float64(),
			Bar: rand.Intn(10),
			Baz: []byte(RandomString(4)),
		},
	}
}

// RandomString gets a random string from the charset
func RandomString(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = stringChars[seededRand.Intn(len(stringChars))]
	}

	return string(b)
}

// TestTableName generates a random test table name
func TestTableName() string {
	// testTableName = "__DYNO_TEST__1"
	return "__DYNO_TEST__" + ksuid.New().String()
}

// CreateTestClient crates a dyo session for testing
func CreateTestClient() *DefaultClient {
	// create the aws session
	c, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		panic(err)
	}

	return NewClientFromConfig(c)
}

// CreateTestTable creates the test table
func CreateTestTable(db *DefaultClient) *Table {
	// test table contains an example of a GSI and an LSI with
	table := NewTable(TestTableName())
	table.SetPartitionKey("id", "S")
	table.SetSortKey("timestamp", "N")
	table.AddGSI(NewGSI("gsi_test_idx").
		SetPartitionKey("Foo", "N"))
	table.AddLSI(NewLSI("lsi_test_idx").
		SetPartitionKey("id", "S").
		SetSortKey("Bar", "N"))

	input, err := table.Create()
	if err != nil {
		panic(err)
	}

	out, err := input.Invoke(context.Background(), db.DynamoDBClient()).Await()
	if err != nil {
		panic(err)
	}

	fmt.Println("create table output:\n", MustYamlString(out))

	// update the local table (also waits for table to exist)
	if err = table.UpdateWithRemote().Invoke(context.Background(), db.ddb).Await(); err != nil {
		panic(err)
	}

	return table
}

func GetTestItems(cnt int) []*TestItem {
	out := make([]*TestItem, cnt)
	for i := 0; i < cnt; i++ {
		out[i] = NewTestItem()
	}

	return out
}

func GetMarshalledTestRecords(cnt int) []*TestItemMarshaller {
	out := make([]*TestItemMarshaller, cnt)
	for i := 0; i < cnt; i++ {
		out[i] = NewMarshalledTestItem()
	}

	return out
}
