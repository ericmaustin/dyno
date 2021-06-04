package dyno

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ericmaustin/dyno/encoding/attribute"
	"math/rand"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
)

const stringChars = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
	"0123456789_"

var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

//TestEmbeddedItem represents a simple embedded test record used for testing
type TestEmbeddedItem struct {
	Foo float64
	Bar int
	Baz []byte
}

//TestItem represents a simple test record used for testing
type TestItem struct {
	ID        string            `dyno:"id"`
	TimeStamp int64             `dyno:"timestamp"`
	Embedded  *TestEmbeddedItem `dyno:",*"`
}

//NewTestItem creates a new random TestItem
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

//MarshalledTestItem represents a simple test record used for testing with marshalling
type MarshalledTestItem struct {
	ID        string
	TimeStamp time.Time
	Embedded  *TestEmbeddedItem
}

//UnmarshalItem implements the encoding.ItemUnmarshaller interface
func (m *MarshalledTestItem) UnmarshalItem(avMap map[string]*dynamodb.AttributeValue) error {
	var errs [5]error
	if av, ok := avMap["id"]; ok {
		errs[0] = attribute.UnmarshalString(av, &m.ID)
	}
	if av, ok := avMap["timestamp"]; ok {
		errs[1] = attribute.UnmarshalUnix(av, &m.TimeStamp)
	}
	if m.Embedded == nil {
		m.Embedded = new(TestEmbeddedItem)
	}
	if av, ok := avMap["Foo"]; ok {
		errs[2] = attribute.UnmarshalFloat(av, &m.Embedded.Foo)
	}
	if av, ok := avMap["Bar"]; ok {
		errs[3] = attribute.UnmarshalInt(av, &m.Embedded.Bar)
	}
	if av, ok := avMap["Baz"]; ok {
		errs[4] = attribute.UnmarshalBytes(av, &m.Embedded.Baz)
	}
	return NewErrSet(errs[:]).Err()
}

//NewMarshalledTestItem creates a new random MarshalledTestItem
func NewMarshalledTestItem() *MarshalledTestItem {
	return &MarshalledTestItem{
		ID:        RandomString(100),
		TimeStamp: time.Unix(rand.Int63n(1e6), 0),
		Embedded: &TestEmbeddedItem{
			Foo: rand.Float64(),
			Bar: rand.Intn(10),
			Baz: []byte(RandomString(4)),
		},
	}
}

//MarshalItem implements the encoding.ItemMarshaller interface
func (m MarshalledTestItem) MarshalItem(av map[string]*dynamodb.AttributeValue) error {
	av["id"] = attribute.EncodeString(&m.ID)
	av["timestamp"] = attribute.EncodeUnix(&m.TimeStamp)
	if m.Embedded != nil {
		av["Foo"] = attribute.EncodeFloat(&m.Embedded.Foo)
		av["Bar"] = attribute.EncodeInt(&m.Embedded.Bar)
		av["Baz"] = attribute.EncodeBytes(m.Embedded.Baz)
	}
	return nil
}

//RandomString gets a random string from the charset
func RandomString(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = stringChars[seededRand.Intn(len(stringChars))]
	}
	return string(b)
}

//TestTableName generates a random test table name
func TestTableName() string {
	var testTableName = ""
	testTableName = "__DYNO_TEST__1" + RandomString(50)
	//testTableName = "__DYNO_TEST__1"
	return testTableName
}

//CreateTestSession crates a dyo session for testing
func CreateTestSession() *Session {
	// create the session
	awsSess, err := session.NewSession()

	if err != nil {
		panic(err)
	}
	/* get a session */
	sess := New(awsSess).
		SetMaxTimeout(time.Minute * 5)

	return sess
}

//CreateTestTable creates the test table
func CreateTestTable(sess *Session) *Table {
	// test table contains an example of a GSI and an LSI with
	table := NewTable(TestTableName())
	table.SetPartitionKey("id", "S")
	table.SetSortKey("timestamp", "N")
	table.AddGSI(NewGSI("gsi_test_idx").
		SetPartitionKey("Foo", "N"))
	table.AddLSI(NewLSI("lsi_test_idx").
		SetPartitionKey("id", "S").
		SetSortKey("Bar", "N"))

	res := <-table.Publish(sess.Request())
	if res.Err != nil {
		panic(res.Err)
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

func GetMarshalledTestRecords(cnt int) []*MarshalledTestItem {
	out := make([]*MarshalledTestItem, cnt)
	for i := 0; i < cnt; i++ {
		out[i] = NewMarshalledTestItem()
	}
	return out
}
