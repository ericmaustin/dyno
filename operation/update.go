package operation

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/ericmaustin/dyno"
	"github.com/ericmaustin/dyno/condition"
	"github.com/ericmaustin/dyno/encoding"
)

type UpdateReturnValues string

const (
	// UpdateReturnNone nothing is returned. (This setting is the default for ReturnValues.)
	UpdateReturnNone = UpdateReturnValues("NONE")
	// UpdateReturnOld Returns only the updated attributes, as they appeared
	// before the UpdateItem operation.
	UpdateReturnOld = UpdateReturnValues("UPDATE_OLD")
	// UpdateReturnAllNew - Returns all of the attributes of the item, as they appear
	// before the UpdateItem operation.
	UpdateReturnAllNew = UpdateReturnValues("ALL_NEW")
	// UpdateReturnNew - Returns all of the attributes of the item, as they appear
	// after the UpdateItem operation.
	UpdateReturnNew = UpdateReturnValues("ALL_NEW")
	// UpdatereturnUpdatedNew Returns only the updated attributes, as they appear after
	// the UpdateItem operation.
	UpdatereturnUpdatedNew = UpdateReturnValues("UPDATED_NEW")
)

// UpdateResult is returned as the result of a UpdateOperation
type UpdateResult struct {
	ResultBase
	output *dynamodb.UpdateItemOutput
}

// OutputInterface returns the UpdateItemOutput as an interface from the UpdateResult
func (u *UpdateResult) OutputInterface() interface{} {
	return u.output
}

// Output returns the UpdateItemOutput from the UpdateResult for convenience
func (u *UpdateResult) Output() *dynamodb.UpdateItemOutput {
	return u.output
}

// OutputError returns the UpdateItemOutput and the error from the UpdateResult for convenience
func (u *UpdateResult) OutputError() (*dynamodb.UpdateItemOutput, error) {
	return u.output, u.err
}

// UpdateItemBuilder is used to build a dynamodb UpdateItemInput
type UpdateItemBuilder struct {
	updateInput   *dynamodb.UpdateItemInput
	updateBuilder expression.UpdateBuilder
	cnd           *expression.ConditionBuilder
}

// NewUpdateItemBuilder creates a new UpdateItemBuilder with optional existing UpdateItemInput as the Base
func NewUpdateItemBuilder(input *dynamodb.UpdateItemInput) *UpdateItemBuilder {
	u := &UpdateItemBuilder{}
	if input != nil {
		u.updateInput = input
	} else {
		u.updateInput = &dynamodb.UpdateItemInput{}
	}
	return u
}

// Add adds an Add operation on this update with the given field name and value
func (u *UpdateItemBuilder) Add(field interface{}, value interface{}) *UpdateItemBuilder {
	u.updateBuilder = u.updateBuilder.Add(expression.Name(encoding.ToString(field)), expression.Value(value))
	return u
}

// AddItem adds an add operation on this update with the given fields and values from an item
func (u *UpdateItemBuilder) AddItem(item interface{}) *UpdateItemBuilder {
	encodedItem := encoding.MustMarshalItem(item)
	for key, value := range encodedItem {
		u.updateBuilder = u.updateBuilder.Add(expression.Name(key), expression.Value(value))
	}
	return u
}

// Delete adds a Delete operation on this update with the given field name and value
func (u *UpdateItemBuilder) Delete(field string, value interface{}) *UpdateItemBuilder {
	u.updateBuilder = u.updateBuilder.Delete(expression.Name(encoding.ToString(field)), expression.Value(value))
	return u
}

// DeleteItem adds a delete operation on this update with the given fields and values from an item
func (u *UpdateItemBuilder) DeleteItem(item interface{}) *UpdateItemBuilder {
	encodedItem := encoding.MustMarshalItem(item)
	for key, value := range encodedItem {
		u.updateBuilder = u.updateBuilder.Delete(expression.Name(key), expression.Value(value))
	}
	return u
}

// Remove adds one or more Remove operations on this update with the given field name
func (u *UpdateItemBuilder) Remove(fields ...interface{}) *UpdateItemBuilder {
	for _, field := range fields {
		u.updateBuilder = u.updateBuilder.Remove(expression.Name(encoding.ToString(field)))
	}
	return u
}

// Set adds a set operation on this update with the given field and value
func (u *UpdateItemBuilder) Set(field string, value interface{}) *UpdateItemBuilder {
	u.updateBuilder = u.updateBuilder.Set(expression.Name(encoding.ToString(field)), expression.Value(value))
	return u
}

// SetItem adds a set operation on this update with the given fields and values from an item
func (u *UpdateItemBuilder) SetItem(item interface{}) *UpdateItemBuilder {
	encodedItem := encoding.MustMarshalItem(item)
	for key, value := range encodedItem {
		u.updateBuilder = u.updateBuilder.Set(expression.Name(key), expression.Value(value))
	}
	return u
}

// AddCondition adds a condition to this update
// adding multiple conditions by calling this multiple times will join the conditions with
// an AND
func (u *UpdateItemBuilder) AddCondition(cnd expression.ConditionBuilder) *UpdateItemBuilder {
	if u.cnd == nil {
		u.cnd = &cnd
	} else {
		cnd = condition.And(*u.cnd, cnd)
		u.cnd = &cnd
	}
	return u
}

// SetKey sets the key for this update
func (u *UpdateItemBuilder) SetKey(item interface{}) *UpdateItemBuilder {
	u.updateInput.SetKey(encoding.MustMarshalItem(item))
	return u
}

// SetTable sets the table name
func (u *UpdateItemBuilder) SetTable(table interface{}) *UpdateItemBuilder {
	u.updateInput.SetTableName(encoding.ToString(table))
	return u
}

// SetReturnValues sets the returnValues
func (u *UpdateItemBuilder) SetReturnValues(returnValues UpdateReturnValues) *UpdateItemBuilder {
	u.updateInput.SetReturnValues(string(returnValues))
	return u
}

// Input builds the update input
func (u *UpdateItemBuilder) Input() *dynamodb.UpdateItemInput {
	expr := expression.NewBuilder().WithUpdate(u.updateBuilder)
	if u.cnd != nil {
		expr.WithCondition(*u.cnd)
	}
	b, buildErr := expr.Build()
	if buildErr != nil {
		panic(buildErr)
	}
	u.updateInput.ConditionExpression = b.Condition()
	u.updateInput.ExpressionAttributeNames = b.Names()
	u.updateInput.ExpressionAttributeValues = b.Values()
	u.updateInput.UpdateExpression = b.Update()
	return u.updateInput
}

// Operation returns a new UpdateOperation with this builder's input
func (u *UpdateItemBuilder) Operation() *UpdateOperation {
	return Update(u.Input())
}

/*
UpdateOperation used as Input for UpdateOperation
*/
type UpdateOperation struct {
	*Base
	input *dynamodb.UpdateItemInput
}

// Update returns a a new UpdateOperation with optional update item input
func Update(input *dynamodb.UpdateItemInput) *UpdateOperation {
	return &UpdateOperation{
		Base:  newBase(),
		input: input,
	}
}

// Input returns current UpdateItemInput
func (u *UpdateOperation) Input() *dynamodb.UpdateItemInput {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.input
}

// SetInput sets the current DeleteItemInput
func (u *UpdateOperation) SetInput(input *dynamodb.UpdateItemInput) *UpdateOperation {
	if !u.IsPending() {
		panic(&InvalidState{})
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	u.input = input
	return u
}

// ExecuteInBatch executes the UpdateOperation using given Request
// and returns a Result used for executing this operation in a batch
func (u *UpdateOperation) ExecuteInBatch(req *dyno.Request) Result {
	return u.Execute(req)
}

// GoExecute executes the UpdateOperation in a go routine and returns a channel
// that will pass a UpdateResult when execution completes
func (u *UpdateOperation) GoExecute(req *dyno.Request) <-chan *UpdateResult {
	outCh := make(chan *UpdateResult)
	go func() {
		defer close(outCh)
		outCh <- u.Execute(req)
	}()
	return outCh
}

// Execute runs the UpdateOperation and returns a UpdateResult
func (u *UpdateOperation) Execute(req *dyno.Request) (out *UpdateResult) {
	out = &UpdateResult{}
	u.setRunning()
	defer u.setDone(out)
	out.output, out.err = req.UpdateItem(u.input)
	return
}
