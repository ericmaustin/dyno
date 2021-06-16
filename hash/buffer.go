package hash

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"sort"
)

type Buffer struct {
	*bytes.Buffer
}

func NewBuffer() *Buffer {
	return &Buffer{
		Buffer: new(bytes.Buffer),
	}
}

//SHA256 creates a SHA256 hash for this buffer
func (b *Buffer) SHA256() [32]byte {
	return sha256.Sum256(b.Bytes())
}

//SHA256String creates a SHA256 hash string for this buffer
func (b *Buffer) SHA256String() string {
	sha := sha256.Sum256(b.Bytes())
	return hex.EncodeToString(sha[:])
}

//WriteMapOfAttributeValues writes the attribute value map to the Buffer
func (b *Buffer) WriteMapOfAttributeValues(avm map[string]*dynamodb.AttributeValue) {
	if avm == nil {
		return
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
		b.WriteString(k + ":")
		b.WriteAttributeValue(avm[k])
		if i < end {
			b.WriteString(",")
		}
	}
}

//WriteConditionMap inserts encoded bytes for a dynamodb.Condition to the Buffer
func (b *Buffer) WriteConditionMap(cnds map[string]*dynamodb.Condition) {
	if cnds == nil {
		return
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
		b.WriteString(k + ":")
		b.WriteCondition(cnds[k])
		if i < end {
			b.WriteString(",")
		}
	}
}

//WriteKeysAndAttributes writes the dynamodb.KeysAndAttributes value to the Buffer
func (b *Buffer) WriteKeysAndAttributes(ka *dynamodb.KeysAndAttributes) {
	if ka.AttributesToGet != nil {
		b.WriteString("AttributesToGet:{")
		for i, a := range ka.AttributesToGet {
			b.WriteStringPtr(a)
			if i < len(ka.AttributesToGet)-1 {
				b.WriteString(",")
			}
		}
		b.WriteString("}")
	}
	if ka.ExpressionAttributeNames != nil {
		b.WriteString("ExpressionAttributeNames:{")
		b.WriteMapOfStringPtrs(ka.ExpressionAttributeNames)
		b.WriteString("}")
	}
	if ka.Keys != nil {
		b.WriteString("Keys:{")
		b.WriteSliceOfAttributeValues(ka.Keys)
		b.WriteString("}")
	}
	if ka.ProjectionExpression != nil {
		fmt.Fprintf(b, "ProjectionExpression{%s}", *ka.ProjectionExpression)
	}
}

func (b *Buffer) WriteStringPtr(str *string) {
	if str == nil {
		b.WriteString("<NULL>")
		return
	}
	b.WriteString(*str)
}

//WriteMapOfKeysAndAttributes writes a map of dynamodb.KeysAndAttributes value to the Buffer
func (b *Buffer) WriteMapOfKeysAndAttributes(km map[string]*dynamodb.KeysAndAttributes) {
	end := len(km) - 1
	keys := make([]string, len(km))
	i := 0
	for k := range km {
		keys[i] = k
		i++
	}
	sort.Strings(keys)
	for i, k := range keys {
		b.WriteString(k + ":")
		b.WriteKeysAndAttributes(km[k])
		if i < end {
			b.WriteString(",")
		}
	}
}

//WriteAttributeValueSlice writes a slice of dynamodb.AttributeValue to the BUffer
func (b *Buffer) WriteAttributeValueSlice(avs []*dynamodb.AttributeValue) {
	end := len(avs) - 1
	for i, v := range avs {
		b.WriteAttributeValue(v)
		if i < end {
			b.Buffer.WriteString(",")
		}
	}
}

//WriteCondition writes a dynamodb.Condition condition to the Buffer
func (b *Buffer) WriteCondition(c *dynamodb.Condition) {
	if c.AttributeValueList != nil {
		b.WriteString("AttributeValueList:{")
		b.WriteAttributeValueSlice(c.AttributeValueList)
		b.WriteString("}")
	}
	if c.ComparisonOperator != nil {
		fmt.Fprintf(b, "ComparisonOperator:{%s}", *c.ComparisonOperator)
	}
}

//WriteSliceOfStringPtrs writes a []*string to the Buffer
func (b *Buffer) WriteSliceOfStringPtrs(ss []*string) {
	end := len(ss) - 1
	for i, s := range ss {
		b.WriteStringPtr(s)
		if i < end {
			b.WriteString(",")
		}
	}
}

//WriteSliceOfAttributeValues writes a []map[string]*dynamodb.AttributeValue to the Buffer
func (b *Buffer) WriteSliceOfAttributeValues(avs []map[string]*dynamodb.AttributeValue) {
	end := len(avs) - 1
	for i, av := range avs {
		if av == nil {
			b.WriteString("<NULL>")
		} else {
			b.WriteMapOfAttributeValues(av)
		}
		if i < end {
			b.WriteString(",")
		}
	}
}

//WriteMapOfStringPtrs writes a map[string]*string to the Buffer
func (b *Buffer) WriteMapOfStringPtrs(ms map[string]*string) {
	if ms == nil {
		return
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
		b.WriteString(k + ":")
		b.WriteStringPtr(ms[k])
		if i < end {
			b.WriteString(",")
		}
	}
}

func (b *Buffer) WriteExpressionAttributeNames(v map[string]*string) {
	if v == nil {
		return
	}
	b.WriteString("ExpressionAttributeNames:{")
	b.WriteMapOfStringPtrs(v)
	b.WriteString("}")
}

func (b *Buffer) WriteExpressionAttributeValues(v map[string]*dynamodb.AttributeValue) {
	if v == nil {
		return
	}
	b.WriteString("ExpressionAttributeValues:{")
	b.WriteMapOfAttributeValues(v)
	b.WriteString("}")
}

func (b *Buffer) WriteExclusiveStartKey(v map[string]*dynamodb.AttributeValue) {
	if v == nil {
		return
	}
	b.WriteString("ExclusiveStartKey:{")
	b.WriteMapOfAttributeValues(v)
	b.WriteString("}")
}

func (b *Buffer) WriteConsistentRead(v *bool) {
	if v == nil {
		return
	}
	fmt.Fprintf(b, "ConsistentRead:{%v}", *v)
}

func (b *Buffer) WriteScanIndexForward(v *bool) {
	if v == nil {
		return
	}
	fmt.Fprintf(b, "ScanIndexForward:{%v}", *v)
}

func (b *Buffer) WriteTableName(v *string) {
	if v == nil {
		return
	}
	fmt.Fprintf(b, "TableName:{%s}", *v)
}

func (b *Buffer) WriteFilterExpression(v *string) {
	if v == nil {
		return
	}
	fmt.Fprintf(b, "FilterExpression:{%s}", *v)
}

func (b *Buffer) WriteReturnConsumedCapacity(v *string) {
	if v == nil {
		return
	}
	fmt.Fprintf(b, "ReturnConsumedCapacity:{%s}", *v)
}

func (b *Buffer) WriteSelect(v *string) {
	if v == nil {
		return
	}
	fmt.Fprintf(b, "Select:{%s}", *v)
}

func (b *Buffer) WriteProjectionExpression(v *string) {
	if v == nil {
		return
	}
	fmt.Fprintf(b, "ProjectionExpression:{%s}", *v)
}

func (b *Buffer) WriteIndexName(v *string) {
	if v == nil {
		return
	}
	fmt.Fprintf(b, "IndexName:{%s}", *v)
}

func (b *Buffer) WriteKeyConditionExpression(v *string) {
	if v == nil {
		return
	}
	fmt.Fprintf(b, "KeyConditionExpression:{%s}", *v)
}

func (b *Buffer) WriteLimit(v *int64) {
	if v == nil {
		return
	}
	fmt.Fprintf(b, "Limit:{%d}", *v)
}

func (b *Buffer) WriteKeyConditions(v map[string]*dynamodb.Condition) {
	if v == nil {
		return
	}
	b.WriteString("KeyConditions:{")
	b.WriteConditionMap(v)
	b.WriteString("}")
}

func (b *Buffer) WriteRequestItems(v map[string]*dynamodb.KeysAndAttributes) {
	if v == nil {
		return
	}
	b.WriteString("RequestItems:{")
	b.WriteMapOfKeysAndAttributes(v)
	b.WriteString("}")
}

//WriteAttributeValue converts an attribute value to a simple hash string that will be the same for the same values of a given
// attribute value
func (b *Buffer) WriteAttributeValue(av *dynamodb.AttributeValue) {
	if av.B != nil && len(av.B) > 0 {
		fmt.Fprintf(b, "B:{%s}", av.B)
	}
	if av.BOOL != nil {
		fmt.Fprintf(b, "BOOL:{%v}", *av.BOOL)
	}
	if av.BS != nil {
		end := len(av.BS) - 1
		for i, bs := range av.BS {
			b.Write(bs)
			if i < end {
				b.WriteString(",")
			}
		}
	}
	if av.L != nil {
		b.WriteString("L:{")
		b.WriteAttributeValueSlice(av.L)
		b.WriteString("}")
	}
	if av.M != nil {
		b.Buffer.WriteString("M:{")
		b.WriteMapOfAttributeValues(av.M)
		b.Buffer.WriteString("}")
	}
	if av.N != nil {
		fmt.Fprintf(b, "N:{%s}", *av.N)
	}
	if av.NS != nil {
		b.WriteString("NS:{")
		b.WriteSliceOfStringPtrs(av.NS)
		b.WriteString("}")
	}
	if av.NULL != nil {
		fmt.Fprintf(b, "NULL:{%v}", *av.NULL)
	}
	if av.S != nil {
		fmt.Fprintf(b, "S:{%s}", *av.S)
	}
	if av.SS != nil {
		b.WriteString("SS:{")
		b.WriteSliceOfStringPtrs(av.SS)
		b.WriteString("}")
	}
}

//WriteQueryInput writes a dynamodb.QueryInput to the Buffer
func (b *Buffer) WriteQueryInput(in *dynamodb.QueryInput) {
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
		b.WriteString("QueryFilter:{")
		b.WriteConditionMap(in.KeyConditions)
		b.WriteString("}")
	}
	b.WriteReturnConsumedCapacity(in.ReturnConsumedCapacity)
	b.WriteScanIndexForward(in.ScanIndexForward)
	b.WriteSelect(in.Select)
	b.WriteTableName(in.TableName)
}

//WriteScanInput writes a dynamodb.ScanInput to the Buffer
func (b *Buffer) WriteScanInput(in *dynamodb.ScanInput) {
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
		b.WriteString("ScanFilter:{")
		b.WriteConditionMap(in.ScanFilter)
		b.WriteString("}")
	}
	if in.Segment != nil {
		fmt.Fprintf(b, "Segment:{%d}", *in.Segment)
	}
	b.WriteSelect(in.Select)
	b.WriteTableName(in.TableName)
	if in.TotalSegments != nil {
		fmt.Fprintf(b, "TotalSegments:{%d}", *in.TotalSegments)
	}
}

//WriteGetItemInput writes a dynamodb.GetItemInput to the Buffer
func (b *Buffer) WriteGetItemInput(in *dynamodb.GetItemInput) {
	b.WriteConsistentRead(in.ConsistentRead)
	b.WriteExpressionAttributeNames(in.ExpressionAttributeNames)
	if in.Key != nil {
		b.WriteString("Key:{")
		b.WriteMapOfAttributeValues(in.Key)
		b.WriteString("}")
	}
	b.WriteProjectionExpression(in.ProjectionExpression)
	b.WriteReturnConsumedCapacity(in.ReturnConsumedCapacity)
	b.WriteTableName(in.TableName)
}

//WriteBatchGetItemInput writes a dynamodb.BatchGetItemInput to the Buffer
func (b *Buffer) WriteBatchGetItemInput(in *dynamodb.BatchGetItemInput) {
	b.WriteRequestItems(in.RequestItems)
	b.WriteReturnConsumedCapacity(in.ReturnConsumedCapacity)
}
