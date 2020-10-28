package operation

import (
	"git-codecommit.us-east-1.amazonaws.com/v1/repos/dyno.git"
	"git-codecommit.us-east-1.amazonaws.com/v1/repos/dyno.git/condition"
	"git-codecommit.us-east-1.amazonaws.com/v1/repos/dyno.git/encoding"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

// DeleteResult is returned as the result of a DeleteOperation
type DeleteResult struct {
	ResultBase
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
	deleteInput *dynamodb.DeleteItemInput
	cnd         *expression.ConditionBuilder
}

// NewDeleteBuilder creates a new DeleteItemBuilder with optional existing UpdateItemInput as the Base
func NewDeleteBuilder(input *dynamodb.DeleteItemInput) *DeleteItemBuilder {
	d := &DeleteItemBuilder{}
	if input != nil {
		d.deleteInput = input
	} else {
		d.deleteInput = &dynamodb.DeleteItemInput{}
	}
	return d
}

// SetKey sets the target key for the item to tbe deleted
func (d *DeleteItemBuilder) SetKey(key interface{}) *DeleteItemBuilder {
	keyItem, err := encoding.MarshalItem(key)
	if err != nil {
		panic(err)
	}
	d.deleteInput.SetKey(keyItem)
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
func (d *DeleteItemBuilder) SetTable(table interface{}) *DeleteItemBuilder {
	d.deleteInput.SetTableName(encoding.ToString(table))
	return d
}

// Input builds the update Input
func (d *DeleteItemBuilder) Input() *dynamodb.DeleteItemInput {
	if d.cnd != nil {
		expr := expression.NewBuilder().WithCondition(*d.cnd)
		b, buildErr := expr.Build()
		if buildErr != nil {
			panic(buildErr)
		}
		d.deleteInput.ConditionExpression = b.Condition()
		d.deleteInput.ExpressionAttributeNames = b.Names()
		d.deleteInput.ExpressionAttributeValues = b.Values()
	}
	return d.deleteInput
}

// Operation returns a new DeleteOperation
func (d *DeleteItemBuilder) Operation() *DeleteOperation {
	return Delete(d.Input())
}

// CreateDeleteInput creates a DeleteItemInput for the given table, key item and condition
func CreateDeleteInput(tableName string,
	keyItem interface{},
	cnd *expression.ConditionBuilder) *dynamodb.DeleteItemInput {
	input := &dynamodb.DeleteItemInput{
		TableName: &tableName,
	}
	db := NewDeleteBuilder(input)
	if keyItem != nil {
		db.SetKey(keyItem)
	}
	if cnd != nil {
		db.AddCondition(*cnd)
	}
	return db.Input()
}

// DeleteOperation runs a deleteItem Input
type DeleteOperation struct {
	*Base
	input *dynamodb.DeleteItemInput
}

// Delete creates a new ``DeleteOperation`` object that will delete the given input when executed
func Delete(input *dynamodb.DeleteItemInput) *DeleteOperation {
	d := &DeleteOperation{
		Base:  newBase(),
		input: input,
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
		panic(&InvalidState{})
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
