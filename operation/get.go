package operation

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/ericmaustin/dyno"
	"github.com/ericmaustin/dyno/encoding"
)

// GetResult is returned by the GetOperation Execution in a channel when operation completes
type GetResult struct {
	ResultBase
	output *dynamodb.GetItemOutput
}

// OutputInterface returns the GetItemOutput as an interface from the GetResult
func (g *GetResult) OutputInterface() interface{} {
	return g.output
}

// Output returns the GetItemOutput from the GetResult
func (g *GetResult) Output() *dynamodb.GetItemOutput {
	return g.output
}

// OutputError returns the GetItemOutput and the error from the GetResult for convenience
func (g *GetResult) OutputError() (*dynamodb.GetItemOutput, error) {
	return g.Output(), g.err
}

// GetBuilder is used to dynamically build a get request
type GetBuilder struct {
	input *dynamodb.GetItemInput
}

// NewGetBuilder returns a new GetBuilder
func NewGetBuilder(input *dynamodb.GetItemInput) *GetBuilder {
	g := &GetBuilder{}
	if input != nil {
		g.input = input
	} else {
		g.input = &dynamodb.GetItemInput{}
	}
	return g
}

// SetTable sets the table for the get input
func (g *GetBuilder) SetTable(table interface{}) *GetBuilder {
	tableName := encoding.ToString(table)
	g.input.TableName = &tableName
	return g
}

// SetKey sets the key for the get input
func (g *GetBuilder) SetKey(key interface{}) *GetBuilder {
	keyItem := encoding.MustMarshalItem(key)
	g.input.Key = keyItem
	return g
}

// SetConsistentRead sets consistent read for the get input
func (g *GetBuilder) SetConsistentRead(consistentRead bool) *GetBuilder {
	g.input.SetConsistentRead(consistentRead)
	return g
}

// SetProjection sets the projection for the get input
func (g *GetBuilder) SetProjection(projection interface{}) *GetBuilder {
	builder := expression.NewBuilder().
		WithProjection(*encoding.ProjectionBuilder(projection))
	expr, err := builder.Build()
	if err != nil {
		panic(err)
	}
	g.input.ExpressionAttributeNames = expr.Names()
	g.input.ProjectionExpression = expr.Projection()
	return g
}

// Input returns the get item input
func (g *GetBuilder) Input() *dynamodb.GetItemInput {
	return g.input
}

// Operation returns a GetOperation using the buidler's input
func (g *GetBuilder) Operation() *GetOperation {
	return Get(g.input)
}

// GetOperation used for running a get operation on dynamodb
type GetOperation struct {
	*Base
	input     *dynamodb.GetItemInput
	handler   ItemHandler
}

// Get creates a new GetOperation with optional input and ItemHandler
func Get(input *dynamodb.GetItemInput) *GetOperation {
	g := &GetOperation{
		Base:  newBase(),
		input: input,
	}
	return g
}

// SetHandler sets the target object to unmarshal the results into
// panics with an InvalidState error if operation isn't pending
func (g *GetOperation) SetHandler(handler ItemHandler) *GetOperation {
	if !g.IsPending() {
		panic(&InvalidState{})
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.handler = handler
	return g
}

// ExecuteInBatch executes the GetOperation request
func (g *GetOperation) ExecuteInBatch(req *dyno.Request) Result {
	return g.Execute(req)
}

// GoExecute executes the GetOperation request in a go routine
func (g *GetOperation) GoExecute(req *dyno.Request) <-chan *GetResult {
	outCh := make(chan *GetResult)
	go func() {
		defer close(outCh)
		outCh <- g.Execute(req)
	}()
	return outCh
}

// Execute executes the GetOperation and returns a GetResult
func (g *GetOperation) Execute(req *dyno.Request) (out *GetResult) {
	g.setRunning()
	out = &GetResult{}
	defer g.setDone(out)

	out.output, out.err = req.GetItem(g.input)

	if out.err != nil {
		return
	}

	// apply the handler
	if g.handler != nil {
		out.err = g.handler(out.output.Item)
	}
	return
}
