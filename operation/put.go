package operation

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/ericmaustin/dyno"
	"github.com/ericmaustin/dyno/condition"
	"github.com/ericmaustin/dyno/encoding"
)

//PutReturnValues is used to tell the operation what return values should be included
type PutReturnValues string

const (
	//PutReturnNone used to return NO values
	PutReturnNone = PutReturnValues("NONE")
	//PutReturnOld used to return only OLD values
	PutReturnOld = PutReturnValues("ALL_OLD")
)

// PutResult is the result of a PutOperation
type PutResult struct {
	resultBase
	output *dynamodb.PutItemOutput
}

// OutputInterface returns the PutItemOutput as an interface from the PutResult
func (p *PutResult) OutputInterface() interface{} {
	return p.output
}

// Output returns the PutItemOutput from the PutResult
func (p *PutResult) Output() *dynamodb.PutItemOutput {
	if p.output == nil {
		return nil
	}
	return p.output
}

// OutputError returns the PutItemOutput and the error from the PutResult for convenience
func (p *PutResult) OutputError() (*dynamodb.PutItemOutput, error) {
	return p.Output(), p.err
}

// PutBuilder allows for dynamic building of a PutItem input
type PutBuilder struct {
	input *dynamodb.PutItemInput
	cnd   *expression.ConditionBuilder
}

// NewPutBuilder creates a new PutBuilder
func NewPutBuilder() *PutBuilder {
	return &PutBuilder{
		input: &dynamodb.PutItemInput{},
	}
}

// SetInput sets the PutBuilder's dynamodb.PutItemInput explicitly
func (p *PutBuilder) SetInput(input *dynamodb.PutItemInput) *PutBuilder {
	p.input = input
	return p
}

// SetItem sets the item that will be used to build the put input
func (p *PutBuilder) SetItem(item interface{}) *PutBuilder {
	return p.SetAttributeValueMap(encoding.MustMarshalItem(item))
}

// SetAttributeValueMap sets the item with an attribute value map
// this bypasses the marshaller from the encoding module
func (p *PutBuilder) SetAttributeValueMap(item map[string]*dynamodb.AttributeValue) *PutBuilder {
	p.input.Item = item
	return p
}

// AddCondition adds a condition to this put
// adding multiple conditions by calling this multiple times will join the conditions with
// an AND
func (p *PutBuilder) AddCondition(cnd expression.ConditionBuilder) *PutBuilder {
	if p.cnd == nil {
		p.cnd = &cnd
	} else {
		cnd = condition.And(*p.cnd, cnd)
		p.cnd = &cnd
	}
	return p
}

// SetTable sets the table name
func (p *PutBuilder) SetTable(table string) *PutBuilder {
	p.input.TableName = &table
	return p
}

// SetReturnValues sets the returnValues
func (p *PutBuilder) SetReturnValues(returnValues PutReturnValues) *PutBuilder {
	p.input.SetReturnValues(string(returnValues))
	return p
}

// Build builds the put input
func (p *PutBuilder) Build() *dynamodb.PutItemInput {
	if p.input.ReturnValues == nil {
		p.input.SetReturnValues(string(PutReturnNone))
	}
	if p.cnd != nil {
		// build the Expression
		b, buildErr := expression.NewBuilder().WithCondition(*p.cnd).Build()
		// make sure build didn't throw an error
		if buildErr != nil {
			panic(buildErr)
		}
		p.input.ConditionExpression = b.Condition()
		p.input.ExpressionAttributeNames = b.Names()
		p.input.ExpressionAttributeValues = b.Values()
	}
	return p.input
}

// BuildOperation returns a PutOperation using this builder's input
func (p *PutBuilder) BuildOperation() *PutOperation {
	return Put(p.Build())
}

// PutOperation used as input for PutOperation
type PutOperation struct {
	*baseOperation
	input *dynamodb.PutItemInput
}

// Put creates a New PutOperation with optional PutInput
func Put(input *dynamodb.PutItemInput) *PutOperation {
	p := &PutOperation{
		baseOperation: newBase(),
		input:         input,
	}
	return p
}

// SetInput sets the PutItemInput
// panics with an ErrInvalidState error if operation isn't pending
func (p *PutOperation) SetInput(input *dynamodb.PutItemInput) *PutOperation {
	if !p.IsPending() {
		panic(&ErrInvalidState{})
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.input = input
	return p
}

// ExecuteInBatch executes the PutOperation using given Request
// and returns a Result used for executing this operation in a batch
func (p *PutOperation) ExecuteInBatch(req *dyno.Request) Result {
	return p.Execute(req)
}

// Execute executes the PutOperation
func (p *PutOperation) Execute(req *dyno.Request) (out *PutResult) {
	out = &PutResult{}
	p.setRunning()
	defer p.setDone(out)
	out.output, out.err = req.PutItem(p.input)
	return
}

// GoExecute executes the PutOperation in a go routine and returns a channel
// that will return a PutResult when operation is done
func (p *PutOperation) GoExecute(req *dyno.Request) <-chan *PutResult {
	outCh := make(chan *PutResult)
	go func() {
		defer close(outCh)
		outCh <- p.Execute(req)
	}()
	return outCh
}
