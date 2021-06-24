package hash

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"io"
	"sort"
)

// TODO: doc comments

type Buffer struct {
	*bytes.Buffer
}

func NewBuffer() *Buffer {
	return &Buffer{
		Buffer: new(bytes.Buffer),
	}
}

// SHA256 creates a SHA256 hash for this buffer
func (b *Buffer) SHA256() [32]byte {
	return sha256.Sum256(b.Bytes())
}

// SHA256String creates a SHA256 hash string for this buffer
func (b *Buffer) SHA256String() string {
	sha := sha256.Sum256(b.Bytes())
	return hex.EncodeToString(sha[:])
}

// MustWrite panics if buffer cannot be written to
func (b *Buffer) MustWrite(bt []byte) *Buffer {
	if _, err := b.Buffer.Write(bt); err != nil {
		panic(err)
	}

	return b
}

// MustWriteString panics if buffer cannot be written to
func (b *Buffer) MustWriteString(s string) *Buffer {
	if _, err := b.Buffer.WriteString(s); err != nil {
		panic(err)
	}

	return b
}

// WriteAttributeValue converts an attribute value to a simple hash string that will be the same for the same values of a given
// attribute value
func (b *Buffer) WriteAttributeValue(av types.AttributeValue) *Buffer {

	switch v := av.(type) {
	case *types.AttributeValueMemberS:
		MustFprintf(b, "S:{%s}", v.Value)
	case *types.AttributeValueMemberN:
		MustFprintf(b, "N:{%s}", v.Value)
	case *types.AttributeValueMemberB:
		MustFprintf(b, "B:{%s}", string(v.Value))
	case *types.AttributeValueMemberNULL:
		MustFprintf(b, "NULL:{%v}", v.Value)
	case *types.AttributeValueMemberBOOL:
		MustFprintf(b, "BOOL:{%v}", v.Value)
	case *types.AttributeValueMemberL:
		b.MustWriteString("L:{").
			WriteAttributeValueSlice(v.Value).
			MustWriteString("}")
	case *types.AttributeValueMemberSS:
		b.MustWriteString("SS:{").
			WriteStringSlice(v.Value).
			MustWriteString("}")
	case *types.AttributeValueMemberNS:
		b.MustWriteString("NS:{").
			WriteStringSlice(v.Value).
			MustWriteString("}")
	case *types.AttributeValueMemberBS:
		b.MustWriteString("BS:{").
			WriteBytesSlice(v.Value).
			MustWriteString("}")
	case *types.AttributeValueMemberM:
		b.MustWriteString("M:{").
			WriteAttributeValueMap(v.Value).
			MustWriteString("}")
	default:
		panic("invalid attribute value")
	}

	return b
}

// WriteAttributeValueMap writes the attribute value map to the Buffer
func (b *Buffer) WriteAttributeValueMap(avm map[string]types.AttributeValue) *Buffer {
	if avm == nil {
		return b
	}

	avKeys := make([]string, len(avm))
	i := 0

	for k := range avm {
		avKeys[i] = k
		i++
	}

	sort.Strings(avKeys)
	end := len(avKeys) - 1

	for i, k := range avKeys {
		b.MustWriteString(k + ":")
		b.WriteAttributeValue(avm[k])

		if i < end {
			b.MustWriteString(",")
		}
	}

	return b
}

// WriteConditionMap inserts encoded bytes for a dynamodb.Condition to the Buffer
func (b *Buffer) WriteConditionMap(cnds map[string]types.Condition) *Buffer {
	if cnds == nil {
		return b
	}

	cndKeys := make([]string, len(cnds))
	i := 0

	for k := range cnds {
		cndKeys[i] = k
		i++
	}

	sort.Strings(cndKeys)
	end := len(cndKeys) - 1

	for i, k := range cndKeys {
		b.MustWriteString(k + ":").WriteCondition(cnds[k])

		if i < end {
			b.MustWriteString(",")
		}
	}

	return b
}

// WriteKeysAndAttributes writes the dynamodb.KeysAndAttributes value to the Buffer
func (b *Buffer) WriteKeysAndAttributes(ka types.KeysAndAttributes) *Buffer {
	if ka.AttributesToGet != nil {
		b.MustWriteString("AttributesToGet:{")

		for i, a := range ka.AttributesToGet {
			b.MustWriteString(a)

			if i < len(ka.AttributesToGet)-1 {
				b.MustWriteString(",")
			}
		}

		b.MustWriteString("}")
	}

	if ka.ExpressionAttributeNames != nil {
		b.MustWriteString("ExpressionAttributeNames:{")
		b.WriteMapOfStrings(ka.ExpressionAttributeNames)
		b.MustWriteString("}")
	}

	if ka.Keys != nil {
		b.MustWriteString("Keys:{")
		b.WriteSliceOfAttributeValueMaps(ka.Keys)
		b.MustWriteString("}")
	}

	if ka.ProjectionExpression != nil {
		MustFprintf(b, "ProjectionExpression{%s}", *ka.ProjectionExpression)
	}

	return b
}

func (b *Buffer) WriteStringPtr(str *string) *Buffer {
	if str == nil {
		b.MustWriteString("<NULL>")
		return b
	}

	b.MustWriteString(*str)

	return b
}

// WriteMapOfKeysAndAttributes writes a map of dynamodb.KeysAndAttributes value to the Buffer
func (b *Buffer) WriteMapOfKeysAndAttributes(km map[string]types.KeysAndAttributes) *Buffer {
	end := len(km) - 1
	keys := make([]string, len(km))
	i := 0

	for k := range km {
		keys[i] = k
		i++
	}

	sort.Strings(keys)

	for i, k := range keys {
		b.MustWriteString(k + ":")
		b.WriteKeysAndAttributes(km[k])

		if i < end {
			b.MustWriteString(",")
		}
	}

	return b
}

// WriteAttributeValueSlice writes a slice of dynamodb.AttributeValue to the BUffer
func (b *Buffer) WriteAttributeValueSlice(avs []types.AttributeValue) *Buffer {
	end := len(avs) - 1

	for i, v := range avs {
		b.WriteAttributeValue(v)

		if i < end {
			b.Buffer.WriteString(",")
		}
	}

	return b
}

// WriteCondition writes a dynamodb.Condition condition to the Buffer
func (b *Buffer) WriteCondition(c types.Condition) *Buffer {
	if c.AttributeValueList != nil {
		b.MustWriteString("AttributeValueList:{")
		b.WriteAttributeValueSlice(c.AttributeValueList)
		b.MustWriteString("}")
	}

	MustFprintf(b, "ComparisonOperator:{%s}", c.ComparisonOperator)

	return b
}

// WriteBytesSlice writes a []string to the Buffer
func (b *Buffer) WriteBytesSlice(bs [][]byte) *Buffer {
	end := len(bs) - 1
	for i, bt := range bs {

		b.MustWrite(bt)
		if i < end {
			b.MustWriteString(",")
		}
	}

	return b
}

// WriteStringSlice writes a []string to the Buffer
func (b *Buffer) WriteStringSlice(ss []string) *Buffer {
	end := len(ss) - 1

	for i, s := range ss {
		b.MustWriteString(s)

		if i < end {
			b.MustWriteString(",")
		}
	}

	return b
}

// WriteSliceOfStringPtrs writes a []*string to the Buffer
func (b *Buffer) WriteSliceOfStringPtrs(ss []*string) *Buffer {
	end := len(ss) - 1

	for i, s := range ss {
		b.WriteStringPtr(s)

		if i < end {
			b.MustWriteString(",")
		}
	}

	return b
}

// WriteSliceOfAttributeValueMaps writes a []map[string]*dynamodb.AttributeValue to the Buffer
func (b *Buffer) WriteSliceOfAttributeValueMaps(avs []map[string]types.AttributeValue) *Buffer {
	end := len(avs) - 1

	for i, av := range avs {
		if av == nil {
			b.MustWriteString("<NULL>")
		} else {
			b.WriteAttributeValueMap(av)
		}

		if i < end {
			b.MustWriteString(",")
		}
	}

	return b
}

// WriteMapOfStringPtrs writes a map[string]*string to the Buffer
func (b *Buffer) WriteMapOfStringPtrs(ms map[string]*string) *Buffer {
	if ms == nil {
		return b
	}

	end := len(ms) - 1
	keys := make([]string, len(ms))
	i := 0

	for k := range ms {
		keys[i] = k
		i++
	}

	sort.Strings(keys)

	for i, k := range keys {
		b.MustWriteString(k + ":")
		b.WriteStringPtr(ms[k])

		if i < end {
			b.MustWriteString(",")
		}
	}

	return b
}

// WriteMapOfStrings writes a map[string]*string to the Buffer
func (b *Buffer) WriteMapOfStrings(ms map[string]string) *Buffer {
	if ms == nil {
		return b
	}

	end := len(ms) - 1
	keys := make([]string, len(ms))
	i := 0

	for k := range ms {
		keys[i] = k
		i++
	}

	sort.Strings(keys)

	for i, k := range keys {
		b.MustWriteString(k + ":")
		b.MustWriteString(ms[k])

		if i < end {
			b.MustWriteString(",")
		}
	}

	return b
}

func (b *Buffer) WriteExpressionAttributeNames(v map[string]string) *Buffer {
	if v == nil {
		return b
	}

	b.MustWriteString("ExpressionAttributeNames:{")
	b.WriteMapOfStrings(v)
	b.MustWriteString("}")

	return b
}

func (b *Buffer) WriteExpressionAttributeValues(v map[string]types.AttributeValue) *Buffer {
	if v == nil {
		return b
	}

	b.MustWriteString("ExpressionAttributeValues:{")
	b.WriteAttributeValueMap(v)
	b.MustWriteString("}")

	return b
}

func (b *Buffer) WriteExclusiveStartKey(v map[string]types.AttributeValue) *Buffer {
	if v == nil {
		return b
	}

	b.MustWriteString("ExclusiveStartKey:{")
	b.WriteAttributeValueMap(v)
	b.MustWriteString("}")

	return b
}

func (b *Buffer) WriteConsistentRead(v *bool) *Buffer {
	if v == nil {
		return b
	}

	MustFprintf(b, "ConsistentRead:{%v}", *v)

	return b
}

func (b *Buffer) WriteScanIndexForward(v *bool) *Buffer {
	if v == nil {
		return b
	}
	MustFprintf(b, "ScanIndexForward:{%v}", *v)

	return b
}

func (b *Buffer) WriteTableName(v *string) {
	if v == nil {
		return
	}
	MustFprintf(b, "TableName:{%s}", *v)
}

func (b *Buffer) WriteFilterExpression(v *string) *Buffer {
	if v == nil {
		return b
	}
	MustFprintf(b, "FilterExpression:{%s}", *v)

	return b
}

func (b *Buffer) WriteReturnConsumedCapacity(v types.ReturnConsumedCapacity) *Buffer {
	MustFprintf(b, "ReturnConsumedCapacity:{%s}", v)

	return b
}

func (b *Buffer) WriteSelect(v types.Select) *Buffer {
	MustFprintf(b, "Select:{%s}", v)

	return b
}

func (b *Buffer) WriteProjectionExpression(v *string) *Buffer {
	if v == nil {
		return b
	}

	MustFprintf(b, "ProjectionExpression:{%s}", *v)

	return b
}

func (b *Buffer) WriteIndexName(v *string) *Buffer {
	if v == nil {
		return b
	}

	MustFprintf(b, "IndexName:{%s}", *v)

	return b
}

func (b *Buffer) WriteKeyConditionExpression(v *string) *Buffer {
	if v == nil {
		return b
	}

	MustFprintf(b, "KeyConditionExpression:{%s}", *v)

	return b
}

func (b *Buffer) WriteLimit(v *int32) *Buffer {
	if v == nil {
		return b
	}

	MustFprintf(b, "Limit:{%d}", *v)

	return b
}

func (b *Buffer) WriteKeyConditions(v map[string]types.Condition) *Buffer {
	if v == nil {
		return b
	}

	b.MustWriteString("KeyConditions:{")
	b.WriteConditionMap(v)
	b.MustWriteString("}")

	return b
}

func (b *Buffer) WriteRequestItems(v map[string]types.KeysAndAttributes) *Buffer {
	if v == nil {
		return b
	}

	b.MustWriteString("RequestItems:{")
	b.WriteMapOfKeysAndAttributes(v)
	b.MustWriteString("}")

	return b
}

// WriteQueryInput writes a dynamodb.QueryInput to the Buffer
func (b *Buffer) WriteQueryInput(in *dynamodb.QueryInput) *Buffer {
	b.WriteConsistentRead(in.ConsistentRead)
	b.WriteExclusiveStartKey(in.ExclusiveStartKey)
	b.WriteExpressionAttributeNames(in.ExpressionAttributeNames)
	b.WriteExpressionAttributeValues(in.ExpressionAttributeValues)
	b.WriteFilterExpression(in.FilterExpression)
	b.WriteIndexName(in.IndexName)
	b.WriteKeyConditionExpression(in.KeyConditionExpression)
	b.WriteKeyConditions(in.KeyConditions)
	b.WriteLimit(in.Limit)
	b.WriteProjectionExpression(in.ProjectionExpression)

	if in.QueryFilter != nil {
		b.MustWriteString("QueryFilter:{")
		b.WriteConditionMap(in.KeyConditions)
		b.MustWriteString("}")
	}

	b.WriteReturnConsumedCapacity(in.ReturnConsumedCapacity)
	b.WriteScanIndexForward(in.ScanIndexForward)
	b.WriteSelect(in.Select)
	b.WriteTableName(in.TableName)

	return b
}

// WriteScanInput writes a dynamodb.ScanInput to the Buffer
func (b *Buffer) WriteScanInput(in *dynamodb.ScanInput) *Buffer {
	b.WriteConsistentRead(in.ConsistentRead)
	b.WriteExclusiveStartKey(in.ExclusiveStartKey)
	b.WriteExpressionAttributeNames(in.ExpressionAttributeNames)
	b.WriteExpressionAttributeValues(in.ExpressionAttributeValues)
	b.WriteFilterExpression(in.FilterExpression)
	b.WriteIndexName(in.IndexName)
	b.WriteLimit(in.Limit)
	b.WriteProjectionExpression(in.ProjectionExpression)
	b.WriteReturnConsumedCapacity(in.ReturnConsumedCapacity)

	if in.ScanFilter != nil {
		b.MustWriteString("ScanFilter:{")
		b.WriteConditionMap(in.ScanFilter)
		b.MustWriteString("}")
	}

	if in.Segment != nil {
		MustFprintf(b, "Segment:{%d}", *in.Segment)
	}

	b.WriteSelect(in.Select)
	b.WriteTableName(in.TableName)

	if in.TotalSegments != nil {
		MustFprintf(b, "TotalSegments:{%d}", *in.TotalSegments)
	}

	return b
}

// WriteGetItemInput writes a dynamodb.GetItemInput to the Buffer
func (b *Buffer) WriteGetItemInput(in *dynamodb.GetItemInput) *Buffer {
	b.WriteConsistentRead(in.ConsistentRead)
	b.WriteExpressionAttributeNames(in.ExpressionAttributeNames)

	if in.Key != nil {
		b.MustWriteString("Key:{")
		b.WriteAttributeValueMap(in.Key)
		b.MustWriteString("}")
	}

	b.WriteProjectionExpression(in.ProjectionExpression)
	b.WriteReturnConsumedCapacity(in.ReturnConsumedCapacity)
	b.WriteTableName(in.TableName)

	return b
}

// WriteBatchGetItemInput writes a dynamodb.BatchGetItemInput to the Buffer
func (b *Buffer) WriteBatchGetItemInput(in *dynamodb.BatchGetItemInput) *Buffer {
	b.WriteRequestItems(in.RequestItems)
	b.WriteReturnConsumedCapacity(in.ReturnConsumedCapacity)

	return b
}

// MustFprintf panics if MustFprintf returns an error
func MustFprintf(w io.Writer, format string, a ...interface{}) {
	if _, err := fmt.Fprintf(w, format, a...); err != nil {
		panic(err)
	}
}
