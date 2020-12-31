package operation

import (
	"math"
	"sync"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/ericmaustin/dyno"
	"github.com/ericmaustin/dyno/encoding"
)

const dynamoBatchGetItemLimit = 100

// BatchGetResult is returned by GoExecute in a channel when operation completes
type BatchGetResult struct {
	resultBase
	output []*dynamodb.BatchGetItemOutput
}

// OutputInterface returns the BatchGetItemOutput slice from the BatchGetResult
// as an interface
func (b *BatchGetResult) OutputInterface() interface{} {
	return b.output
}

// Output returns the BatchGetItemOutput slice from the BatchGetResult
func (b *BatchGetResult) Output() []*dynamodb.BatchGetItemOutput {
	if b.output == nil {
		return nil
	}
	return b.output
}

// OutputError returns the BatchGetItemOutput slice and the error from the DeleteResult for convenience
func (b *BatchGetResult) OutputError() ([]*dynamodb.BatchGetItemOutput, error) {
	return b.output, b.err
}

// BatchGetBuilder used to dynamically build a BatchGetItemInput
type BatchGetBuilder struct {
	input *dynamodb.BatchGetItemInput
}

// NewBatchGetBuilder creates a new BatchGetBuilder
func NewBatchGetBuilder() *BatchGetBuilder {
	return &BatchGetBuilder{
		input: &dynamodb.BatchGetItemInput{
			RequestItems: make(map[string]*dynamodb.KeysAndAttributes),
		},
	}
}

// SetInput sets the BatchGetBuilder's dynamodb.BatchGetItemInput explicitly
func (b *BatchGetBuilder) SetInput(input *dynamodb.BatchGetItemInput) *BatchGetBuilder {
	b.input = input
	return b
}

	// KeysAndAttributes used to create a dynamodb KeysAndAttributes struct easily
func KeysAndAttributes(itemKeys interface{}, projection interface{}, consistentRead bool) *dynamodb.KeysAndAttributes {
	item := encoding.MustMarshalItems(itemKeys)
	k := &dynamodb.KeysAndAttributes{
		Keys:           item,
		ConsistentRead: &consistentRead,
	}

	if projection != nil {
		builder := expression.NewBuilder().
			WithProjection(*encoding.ProjectionBuilder(projection))
		expr, err := builder.Build()
		if err != nil {
			panic(err)
		}
		k.ExpressionAttributeNames = expr.Names()
		k.ProjectionExpression = expr.Projection()
	}
	return k
}

func (b *BatchGetBuilder) initTable(tableName string) {
	if _, ok := b.input.RequestItems[tableName]; !ok {
		b.input.RequestItems[tableName] = &dynamodb.KeysAndAttributes{}
	}
}

// SetProjection sets the projection for the given table name
func (b *BatchGetBuilder) SetProjection(tableName string, projection interface{}) *BatchGetBuilder {
	b.initTable(tableName)
	if projection != nil {
		builder := expression.NewBuilder().
			WithProjection(*encoding.ProjectionBuilder(projection))
		expr, err := builder.Build()
		if err != nil {
			panic(err)
		}
		b.input.RequestItems[tableName].ExpressionAttributeNames = expr.Names()
		b.input.RequestItems[tableName].ProjectionExpression = expr.Projection()
	}
	return b
}

// SetConsistentRead sets the projection for the given table name
func (b *BatchGetBuilder) SetConsistentRead(tableName string, consistentRead bool) *BatchGetBuilder {
	b.initTable(tableName)
	b.input.RequestItems[tableName].ConsistentRead = &consistentRead
	return b
}

// SetKeysAndAttributes sets the keys and attributes to get from the given table
func (b *BatchGetBuilder) SetKeysAndAttributes(tableName string, keysAndAttributes *dynamodb.KeysAndAttributes) *BatchGetBuilder {
	b.input.RequestItems[tableName] = keysAndAttributes
	return b
}

// AddKeys adds keys to the request item map
func (b *BatchGetBuilder) AddKeys(tableName string, keys interface{}) *BatchGetBuilder {
	b.initTable(tableName)
	b.input.RequestItems[tableName].Keys = encoding.MustMarshalItems(keys)
	return b
}

// Build finalizes and returns the BatchGetItemInput
func (b *BatchGetBuilder) Build() *dynamodb.BatchGetItemInput {
	// remove all inputs that don't have any keys associated
	for tableName, keys := range b.input.RequestItems {
		if keys.Keys == nil || len(keys.Keys) < 1 {
			delete(b.input.RequestItems, tableName)
		}
	}
	if b.input.ReturnConsumedCapacity == nil {
		b.input.ReturnConsumedCapacity = dyno.StringPtr("NONE")
	}
	return b.input
}

// BuildOperation returns a new operation with this builder's input
func (b *BatchGetBuilder) BuildOperation() *BatchGetOperation {
	return BatchGet(b.Build())
}

// BatchGetOperation calls the dynamodb batch get api endpoint
type BatchGetOperation struct {
	*baseOperation
	input   *dynamodb.BatchGetItemInput
	handler ItemSliceHandler
	workers int
}

// BatchGet creates a new BatchGetOperation with optional BatchGetInput
func BatchGet(input *dynamodb.BatchGetItemInput) *BatchGetOperation {
	g := &BatchGetOperation{
		baseOperation: newBase(),
		input:         input,
	}
	return g
}

// SetInput sets the map of KeysAndAttributes for this BatchGetOperation
func (bg *BatchGetOperation) SetInput(input *dynamodb.BatchGetItemInput) *BatchGetOperation {
	if !bg.IsPending() {
		panic(&InvalidState{})
	}
	bg.mu.Lock()
	defer bg.mu.Unlock()
	bg.input = input
	return bg
}

// SetHandler sets the target object to unmarshal the results into
// panics with an InvalidState error if operation isn't pending
func (bg *BatchGetOperation) SetHandler(handler ItemSliceHandler) *BatchGetOperation {
	if !bg.IsPending() {
		panic(&InvalidState{})
	}
	bg.mu.Lock()
	defer bg.mu.Unlock()
	bg.handler = handler
	return bg
}

// SetConcurrency sets the workers for the batch request
// panics with an InvalidState error if operation isn't pending
func (bg *BatchGetOperation) SetConcurrency(concurrency int) *BatchGetOperation {
	if !bg.IsPending() {
		panic(&InvalidState{})
	}
	bg.mu.Lock()
	defer bg.mu.Unlock()
	bg.workers = concurrency
	return bg
}

// ExecuteInBatch executes the BatchGetOperation using given Request
// and returns a Result used for executing this operation in a batch
func (bg *BatchGetOperation) ExecuteInBatch(req *dyno.Request) Result {
	return bg.Execute(req)
}

// Execute executes the BatchGetOperation request
func (bg *BatchGetOperation) Execute(req *dyno.Request) (out *BatchGetResult) {
	bg.setRunning()

	out = &BatchGetResult{}

	defer bg.setDone(out)

	if bg.workers == 0 {
		// automatically set the workers to number of processes needed to run all at once
		cnt := 0
		for _, keysAndAttributes := range bg.input.RequestItems {
			cnt += len(keysAndAttributes.Keys)
		}
		bg.workers = int(math.Ceil(float64(cnt) / dynamoBatchGetItemLimit))
	}

	chunks := chunkBatchGetInputs(bg.input, dynamoBatchGetItemLimit)

	// set up the sem and wait groups
	sem := make(chan struct{}, bg.workers) // semaphore for workers limiting
	wg := sync.WaitGroup{}
	state := &batchGetState{
		mu: sync.RWMutex{},
	}

	for _, input := range chunks {

		if state.error() != nil {
			break
		}

		wg.Add(1)
		sem <- struct{}{}
		// doPut the request in a goroutine
		go bg.doBatchGetRequest(req, input, sem, &wg, state)
		// reset the key slice for the next loop
	}

	wg.Wait()
	close(sem)
	out.output = state.outputs
	out.err = state.err
	return
}

// GoExecute executes the BatchGetOperation in a go routine
// returns a BatchGetResult in a channel when operation completes
func (bg *BatchGetOperation) GoExecute(req *dyno.Request) <-chan *BatchGetResult {
	outCh := make(chan *BatchGetResult)
	go func() {
		defer close(outCh)
		outCh <- bg.Execute(req)
	}()
	return outCh
}

type batchGetState struct {
	outputs []*dynamodb.BatchGetItemOutput
	err     error
	mu      sync.RWMutex
}

func (s *batchGetState) SetError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

func (s *batchGetState) error() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.err
}

func (s *batchGetState) addOutput(out *dynamodb.BatchGetItemOutput) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.outputs == nil {
		s.outputs = []*dynamodb.BatchGetItemOutput{out}
		return
	}
	s.outputs = append(s.outputs, out)
}

func (bg *BatchGetOperation) doBatchGetRequest(req *dyno.Request,
	input *dynamodb.BatchGetItemInput,
	sem <-chan struct{},
	wg *sync.WaitGroup,
	state *batchGetState) {

	defer func() {
		<-sem
		wg.Done()
	}()

	// keep looping until no more keys are left
	for {
		if len(input.RequestItems) < 1 {
			return
		}

		// call the API endpoint
		out, err := req.BatchGetItem(input)

		if err != nil {
			state.SetError(err)
			return
		}

		if out.Responses == nil {
			continue
		}

		for tableName := range input.RequestItems {
			if _, ok := out.Responses[tableName]; ok {
				if bg.handler != nil {
					if err := bg.handler(out.Responses[tableName]); err != nil {
						state.SetError(err)
						return
					}
				}
				// if we have dynamo values
				state.addOutput(out)
			}
		}
		if out.UnprocessedKeys == nil || len(out.UnprocessedKeys) < 1 {
			return
		}
		input.RequestItems = out.UnprocessedKeys
	}
}

// chunkBatchGetInputs chunks the input map of KeysAndAttributes into chunks of the given chunkSize
func chunkBatchGetInputs(input *dynamodb.BatchGetItemInput, chunkSize int) (out []*dynamodb.BatchGetItemInput) {
	out = make([]*dynamodb.BatchGetItemInput, 0)

	var returnConsumedCapacity string
	if input.ReturnConsumedCapacity != nil {
		returnConsumedCapacity = *input.ReturnConsumedCapacity
	}

	for tableName, keysAndAttributes := range input.RequestItems {
		sliceOfKeys := make([][]map[string]*dynamodb.AttributeValue, 0)

		for i := 0; i < len(keysAndAttributes.Keys); i += chunkSize {
			end := i + chunkSize
			if end > len(keysAndAttributes.Keys) {
				end = len(keysAndAttributes.Keys)
			}
			sliceOfKeys = append(sliceOfKeys, keysAndAttributes.Keys[i:end])
		}

		for _, slice := range sliceOfKeys {
			out = append(out, &dynamodb.BatchGetItemInput{
				RequestItems: map[string]*dynamodb.KeysAndAttributes{
					tableName: {
						AttributesToGet:          keysAndAttributes.AttributesToGet,
						ConsistentRead:           keysAndAttributes.ConsistentRead,
						ExpressionAttributeNames: keysAndAttributes.ExpressionAttributeNames,
						Keys:                     slice,
						ProjectionExpression:     keysAndAttributes.ProjectionExpression,
					},
				},
				ReturnConsumedCapacity: &returnConsumedCapacity,
			})
		}
	}
	return
}
