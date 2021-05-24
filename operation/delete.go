package operation

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/ericmaustin/dyno"
	"github.com/ericmaustin/dyno/condition"
	"github.com/ericmaustin/dyno/encoding"
)

// DeleteResult is returned as the result of a DeleteOperation
type DeleteResult struct {
	resultBase
	output *dynamodb.DeleteItemOutput
}

// OutputInterface returns the DeleteItemOutput from the DeleteResult as an interface
func (d *DeleteResult) OutputInterface() interface{} {
	return d.output
}

// Output returns the DeleteItemOutput from the DeleteResult
func (d *DeleteResult) Output() *dynamodb.DeleteItemOutput {
	return d.output
}

// OutputError returns the DeleteItemOutput the error from the DeleteResult for convenience
func (d *DeleteResult) OutputError() (*dynamodb.DeleteItemOutput, error) {
	return d.output, d.err
}

// DeleteItemBuilder builds DeleteItemInputs for use with the DeleteItemOperation
type DeleteItemBuilder struct {
	input *dynamodb.DeleteItemInput
	cnd   *expression.ConditionBuilder
}

// NewDeleteBuilder creates a new DeleteItemBuilder with optional existing UpdateItemInput as the baseOperation
func NewDeleteBuilder() *DeleteItemBuilder {
	return &DeleteItemBuilder{
		input: &dynamodb.DeleteItemInput{},
	}
}

// SetInput sets the DeleteItemBuilder's dynamodb.DeleteItemInput
func (d *DeleteItemBuilder) SetInput(input *dynamodb.DeleteItemInput) *DeleteItemBuilder {
	d.input = input
	return d
}

// SetKey sets the target key for the item to tbe deleted
func (d *DeleteItemBuilder) SetKey(key interface{}) *DeleteItemBuilder {
	return d.SetKeyAttributeValues(encoding.MustMarshalItem(key))
}

// SetKeyAttributeValues sets the target key for the item to tbe deleted
func (d *DeleteItemBuilder) SetKeyAttributeValues(key map[string]*dynamodb.AttributeValue) *DeleteItemBuilder {
	d.input.SetKey(key)
	return d
}

// AddCondition adds a condition to this update
// adding multiple conditions by calling this multiple times will join the conditions with
// an AND
func (d *DeleteItemBuilder) AddCondition(cnd expression.ConditionBuilder) *DeleteItemBuilder {
	if d.cnd == nil {
		d.cnd = &cnd
	} else {
		cnd = condition.And(*d.cnd, cnd)
		d.cnd = &cnd
	}
	return d
}

// SetTable sets the table name
func (d *DeleteItemBuilder) SetTable(table string) *DeleteItemBuilder {
	d.input.SetTableName(table)
	return d
}

// Build builds the update Build
func (d *DeleteItemBuilder) Build() *dynamodb.DeleteItemInput {
	if d.cnd != nil {
		expr := expression.NewBuilder().WithCondition(*d.cnd)
		b, buildErr := expr.Build()
		if buildErr != nil {
			panic(buildErr)
		}
		d.input.ConditionExpression = b.Condition()
		d.input.ExpressionAttributeNames = b.Names()
		d.input.ExpressionAttributeValues = b.Values()
	}
	return d.input
}

// BuildOperation returns a new DeleteOperation
func (d *DeleteItemBuilder) BuildOperation() *DeleteOperation {
	return Delete(d.Build())
}

// CreateDeleteInput creates a DeleteItemInput for the given table, key item and condition
func CreateDeleteInput(tableName string,
	keyItem interface{},
	cnd *expression.ConditionBuilder) *dynamodb.DeleteItemInput {
	db := NewDeleteBuilder().
		SetTable(tableName)
	if keyItem != nil {
		db.SetKey(keyItem)
	}
	if cnd != nil {
		db.AddCondition(*cnd)
	}
	return db.Build()
}

// DeleteOperation runs a deleteItem Input
type DeleteOperation struct {
	*baseOperation
	input *dynamodb.DeleteItemInput
}

// Delete creates a new ``DeleteOperation`` object that will delete the given input when executed
func Delete(input *dynamodb.DeleteItemInput) *DeleteOperation {
	d := &DeleteOperation{
		baseOperation: newBase(),
		input:         input,
	}
	return d
}

// Input returns current DeleteItemInput
func (d *DeleteOperation) Input() *dynamodb.DeleteItemInput {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.input
}

// SetInput sets the current DeleteItemInput
func (d *DeleteOperation) SetInput(input *dynamodb.DeleteItemInput) *DeleteOperation {
	if !d.IsPending() {
		panic(&ErrInvalidState{})
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.input = input
	return d
}

// ExecuteInBatch executes the DeleteOperation using given Request
// and returns a Result used for executing this operation in a batch
func (d *DeleteOperation) ExecuteInBatch(req *dyno.Request) Result {
	return d.Execute(req)
}

// GoExecute executes the DeleteOperation in a go routine and returns a channel
// that will pass a DeleteResult when execution completes
func (d *DeleteOperation) GoExecute(req *dyno.Request) <-chan *DeleteResult {
	outCh := make(chan *DeleteResult)
	go func() {
		defer close(outCh)
		outCh <- d.Execute(req)
	}()
	return outCh
}

// Execute runs the DeleteOperation and returns a DeleteResult
func (d *DeleteOperation) Execute(req *dyno.Request) (out *DeleteResult) {
	out = &DeleteResult{}
	d.setRunning()
	defer d.setDone(out)
	// do the DeleteItem the operation
	out.output, out.err = req.DeleteItem(d.input)
	return
}
