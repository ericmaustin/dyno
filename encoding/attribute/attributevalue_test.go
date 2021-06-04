package attribute

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ericmaustin/dyno"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func TestEncodeDecodeBool(t *testing.T) {

	// encode
	b := true
	av := EncodeBool(&b)
	assert.Equal(t, *av.BOOL, b)

	// decode
	av = &dynamodb.AttributeValue{BOOL: &b}
	bPtr := new(bool)
	err := UnmarshalBool(av, bPtr)
	if err != nil {
		panic(err)
	}
	assert.Equal(t, b, *bPtr)
}

func TestEncodeDecodeString(t *testing.T) {

	// encode
	srcString := dyno.RandomString(10)
	av := EncodeString(&srcString)
	assert.Equal(t, srcString, *av.S)

	// decode
	avStr := dyno.RandomString(10)
	av = &dynamodb.AttributeValue{S: &avStr}
	str := new(string)
	err := UnmarshalString(av, str)
	if err != nil {
		panic(err)
	}
	assert.Equal(t, avStr, *str)
}

func TestEncodeDecodeInt(t *testing.T) {

	// encode
	i := rand.Intn(1e6)
	av := EncodeInt(&i)
	iStr := strconv.Itoa(i)
	assert.Equal(t, iStr, *av.N)

	// decode
	av = &dynamodb.AttributeValue{N: &iStr}
	iPtr := new(int)
	err := UnmarshalInt(av, iPtr)
	if err != nil {
		panic(err)
	}
	assert.Equal(t, i, *iPtr)
}

func TestEncodeDecodeInt64(t *testing.T) {

	// encode
	i := rand.Int63n(1e6)
	fmt.Println("int:", i)
	av := EncodeInt64(&i)
	iStr := strconv.FormatInt(i, 10)
	assert.Equal(t, iStr, *av.N)

	// decode
	av = &dynamodb.AttributeValue{N: &iStr}
	iPtr := new(int64)
	err := UnmarshalInt64(av, iPtr)
	if err != nil {
		panic(err)
	}
	assert.Equal(t, i, *iPtr)
}

func TestEncodeDecodeFloat(t *testing.T) {

	// encode
	f := rand.Float64()
	fmt.Println("float:", f)
	av := EncodeFloat(&f)
	fStr := strconv.FormatFloat(f, 'f', -1, 64)
	fmt.Println("float str:", fStr)
	assert.Equal(t, fStr, *av.N)

	// decode
	av = &dynamodb.AttributeValue{N: &fStr}
	fPtr := new(float64)
	err := UnmarshalFloat(av, fPtr)
	if err != nil {
		panic(err)
	}
	assert.Equal(t, f, *fPtr)
}

func TestEncodeDecodeByte(t *testing.T) {
	// encode
	b := []byte(dyno.RandomString(10))
	av := EncodeBytes(b)
	assert.Equal(t, b, av.B)

	// decode
	av = &dynamodb.AttributeValue{B: b}
	var bTarget []byte
	err := UnmarshalBytes(av, &bTarget)
	if err != nil {
		panic(err)
	}
	assert.Equal(t, b, bTarget)
}

func TestEncodeDecodeUnixTime(t *testing.T) {
	// encode
	tm := time.Now()
	av := EncodeUnix(&tm)
	tmStr := strconv.FormatInt(tm.Unix(), 10)
	assert.Equal(t, tmStr, *av.N)

	// decode
	av = &dynamodb.AttributeValue{N: &tmStr}
	tPtr := new(time.Time)
	err := UnmarshalUnix(av, tPtr)
	if err != nil {
		panic(err)
	}
	assert.Equal(t, tm.Unix(), (*tPtr).Unix())
}

func TestEncodeDecodeUnixNanoTime(t *testing.T) {
	// encode
	tm := time.Now()
	av := EncodeUnixNano(&tm)
	tmStr := strconv.FormatInt(tm.UnixNano(), 10)
	assert.Equal(t, tmStr, *av.N)

	// decode
	av = &dynamodb.AttributeValue{N: &tmStr}
	tPtr := new(time.Time)
	err := UnmarshalUnixNano(av, tPtr)
	if err != nil {
		panic(err)
	}
	assert.Equal(t, tm.UnixNano(), (*tPtr).UnixNano())
}

func TestEncodeDecodeStringSlice(t *testing.T) {
	// encode
	ss := []string{"foo", "bar"}
	av := EncodeStringSlice(ss)
	assert.True(t, *av.SS[0] == ss[0])
	assert.True(t, *av.SS[1] == ss[1])

	fmt.Println(av.String())

	// decode
	sPtrs := make([]*string, len(ss))
	for i := range ss {
		sPtrs[i] = &ss[i]
	}
	av = &dynamodb.AttributeValue{SS: sPtrs}
	var target []string
	err := UnmarshalStringSlice(av, &target)
	if err != nil {
		panic(err)
	}
	assert.True(t, *av.SS[0] == ss[0])
	assert.True(t, *av.SS[1] == ss[1])
}

func TestEncodeDecodeFloatSlice(t *testing.T) {
	// encode
	fs := []float64{0.1, 0.2}
	av := EncodeFloatSlice(fs)
	ss := []string{"0.1", "0.2"}
	assert.True(t, *av.NS[0] == ss[0])
	assert.True(t, *av.NS[1] == ss[1])

	fmt.Println(av.String())
	// decode
	av = &dynamodb.AttributeValue{NS: []*string{
		&ss[0],
		&ss[1],
	}}
	var target []float64
	err := UnmarshalFloatSlice(av, &target)
	if err != nil {
		panic(err)
	}
	assert.True(t, 0.1 == fs[0])
	assert.True(t, 0.2 == fs[1])
}

func TestEncodeDecodeIntSlice(t *testing.T) {
	// encode
	is := []int{1, 2}
	av := EncodeIntSlice(is)
	ss := []string{"1", "2"}
	assert.True(t, *av.NS[0] == ss[0])
	assert.True(t, *av.NS[1] == ss[1])

	fmt.Println(av.String())
	// decode
	av = &dynamodb.AttributeValue{NS: []*string{
		&ss[0],
		&ss[1],
	}}
	var target []int
	err := UnmarshalIntSlice(av, &target)
	if err != nil {
		panic(err)
	}
	assert.True(t, 1 == is[0])
	assert.True(t, 2 == is[1])
}

func TestEncodeDecodeInt64Slice(t *testing.T) {
	// encode
	is := []int64{1, 2}
	av := EncodeInt64Slice(is)
	ss := []string{"1", "2"}
	assert.True(t, *av.NS[0] == ss[0])
	assert.True(t, *av.NS[1] == ss[1])

	fmt.Println(av.String())
	// decode
	av = &dynamodb.AttributeValue{NS: []*string{
		&ss[0],
		&ss[1],
	}}
	var target []int64
	err := UnmarshalInt64Slice(av, &target)
	if err != nil {
		panic(err)
	}
	assert.True(t, 1 == is[0])
	assert.True(t, 2 == is[1])
}

func TestGetAttributeType(t *testing.T) {
	stringVal := "foo"
	av := &dynamodb.AttributeValue{S: &stringVal}
	assert.Equal(t, GetAttributeType(av), TypeString)

	boolVal := true
	av = &dynamodb.AttributeValue{BOOL: &boolVal}
	assert.Equal(t, GetAttributeType(av), TypeBOOL)

	intVal := 1
	av = EncodeInt(&intVal)
	assert.Equal(t, GetAttributeType(av), TypeNumber)

	av = &dynamodb.AttributeValue{M: map[string]*dynamodb.AttributeValue{
		"foo": EncodeInt(&intVal),
	}}
	assert.Equal(t, GetAttributeType(av), TypeMap)

	av = &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{
		EncodeInt(&intVal),
	}}
	assert.Equal(t, GetAttributeType(av), TypeList)

	av = &dynamodb.AttributeValue{SS: make([]*string, 1)}
	assert.Equal(t, GetAttributeType(av), TypeStringSet)

	av = &dynamodb.AttributeValue{NS: make([]*string, 1)}
	assert.Equal(t, GetAttributeType(av), TypeNumberSet)

	av = &dynamodb.AttributeValue{BS: make([][]byte, 1)}
	assert.Equal(t, GetAttributeType(av), TypeByteSet)

	nullVal := true
	av = &dynamodb.AttributeValue{NULL: &nullVal}
	assert.Equal(t, GetAttributeType(av), TypeNull)

	bs := []byte("bar")
	av = &dynamodb.AttributeValue{B: bs}
	assert.Equal(t, GetAttributeType(av), TypeBinary)
}
