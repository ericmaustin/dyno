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

//WriteAttributeValueMap writes the attribute value map to the Buffer
func (b *Buffer) WriteAttributeValueMap(avm map[string]*dynamodb.AttributeValue) {
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
		b.WriteString(k+":")
		b.WriteAttributeValue(avm[k])
		if i < end {
			b.WriteString(",")
		}
	}
}

//WriteConditionMap inserts encoded bytes for a dynamodb.Condition to the Buffer
func (b *Buffer)WriteConditionMap(cnds map[string]*dynamodb.Condition) {
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
		b.WriteString(k+":")
		b.WriteCondition(cnds[k])
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
		if s == nil {
			b.WriteString("<NULL>")
		} else {
			b.WriteString(*s)
		}
		if i < end {
			b.WriteString(",")
		}
	}
}

//WriteMapOfStringPtrs writes a map[string]*string to the Buffer
func (b *Buffer) WriteMapOfStringPtrs(ms map[string]*string) {
	end := len(ms) - 1
	keys := make([]string, len(ms))
	i := 0
	for k := range ms {
		keys[i] = k
		i++
	}
	sort.Strings(keys)

	for i, s := range keys {
		b.WriteString("k:")
		if ms[s] == nil {
			b.WriteString("<NULL>")
		} else {
			b.WriteString(*ms[s])
		}
		if i < end {
			b.WriteString(",")
		}
	}
}

//WriteAttributeValue converts an attribute value to a simple hash string that will be the same for the same values of a given
// attribute value
func (b *Buffer) WriteAttributeValue(av *dynamodb.AttributeValue) {
	if av.B != nil && len(av.B) > 0{
		fmt.Fprintf(b, "B:{%s}", av.B)
	}
	if av.BOOL != nil{
		fmt.Fprintf(b, "BOOL:{%v}", *av.BOOL)
	}
	if av.BS != nil{
		end := len(av.BS) - 1
		for i, bs := range av.BS {
			b.Write(bs)
			if i < end {
				b.WriteString(",")
			}
		}
	}
	if av.L != nil{
		b.WriteString("L:{")
		b.WriteAttributeValueSlice(av.L)
		b.WriteString("}")
	}
	if av.M != nil {
		b.Buffer.WriteString("M:{")
		b.WriteAttributeValueMap(av.M)
		b.Buffer.WriteString("}")
	}
	if av.N != nil {
		fmt.Fprintf(b,"N:{%s}", *av.N)
	}
	if av.NS != nil {
		b.WriteString("NS:{")
		b.WriteSliceOfStringPtrs(av.NS)
		b.WriteString("}")
	}
	if av.NULL != nil {
		fmt.Fprintf(b,"NULL:{%v}", *av.NULL)
	}
	if av.S != nil {
		fmt.Fprintf(b,"S:{%s}", *av.S)
	}
	if av.SS != nil {
		b.WriteString("SS:{")
		b.WriteSliceOfStringPtrs(av.SS)
		b.WriteString("}")
	}
}

//WriteQueryInput writes a dynamodb.QueryInput to the Buffer
func (b *Buffer) WriteQueryInput(in *dynamodb.QueryInput) {

	if in.ConsistentRead != nil {
		fmt.Fprintf(b, "ConsistentRead:{%v}", *in.ConsistentRead)
	}

	if in.ExclusiveStartKey != nil {
		b.WriteString("ExclusiveStartKey:{")
		b.WriteAttributeValueMap(in.ExclusiveStartKey)
		b.WriteString("}")
	}

	if in.ExpressionAttributeNames != nil {
		b.WriteString("ExpressionAttributeNames:{")
		b.WriteMapOfStringPtrs(in.ExpressionAttributeNames)
		b.WriteString("}")

	}

	if in.ExpressionAttributeValues != nil {
		b.WriteString("ExpressionAttributeValues:{")
		b.WriteAttributeValueMap(in.ExpressionAttributeValues)
		b.WriteString("}")
	}

	if in.FilterExpression != nil {
		fmt.Fprintf(b, "FilterExpression:{%s}", *in.FilterExpression)
	}

	if in.IndexName != nil {
		fmt.Fprintf(b, "IndexName:{%s}", *in.IndexName)
	}

	if in.KeyConditionExpression != nil {
		fmt.Fprintf(b, "KeyConditionExpression:{%s}", *in.KeyConditionExpression)
	}

	if in.KeyConditions != nil {
		b.WriteString("KeyConditions:{")
		b.WriteConditionMap(in.KeyConditions)
		b.WriteString("}")
	}

	if in.Limit != nil {
		fmt.Fprintf(b, "Limit:{%d}", *in.Limit)
	}

	if in.ProjectionExpression != nil {
		fmt.Fprintf(b, "ProjectionExpression:{%s}", *in.ProjectionExpression)
	}

	if in.QueryFilter != nil {
		b.WriteString("QueryFilter:{")
		b.WriteConditionMap(in.KeyConditions)
		b.WriteString("}")
	}

	if in.ReturnConsumedCapacity != nil {
		fmt.Fprintf(b, "ReturnConsumedCapacity:{%s}", *in.ReturnConsumedCapacity)
	}

	if in.ScanIndexForward != nil {
		fmt.Fprintf(b, "ScanIndexForward:{%v}", in.ScanIndexForward)
	}

	if in.Select != nil {
		fmt.Fprintf(b, "Select:{%s}", *in.Select)
	}

	if in.TableName != nil {
		fmt.Fprintf(b, "TableName:{%s}", *in.TableName)
	}
}


