package operation

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ericmaustin/dyno"
	"github.com/ericmaustin/dyno/encoding"
	"math"
	"sync"
)

const dynamoBatchWriteLimit = 25

// BatchWriteResult is returned as the result of a BatchWriteOperation
type BatchWriteResult struct {
	resultBase
	output []*dynamodb.BatchWriteItemOutput
}

// OutputInterface returns the BatchWriteItemOutput slice from the BatchWriteResult as an interface
func (b *BatchWriteResult) OutputInterface() interface{} {
	return b.output
}

// Output returns the BatchWriteItemOutput slice from the BatchWriteResult
func (b *BatchWriteResult) Output() []*dynamodb.BatchWriteItemOutput {
	return b.output
}

// OutputError returns the BatchWriteItemOutput slice and the error from the BatchWriteResult for convenience
func (b *BatchWriteResult) OutputError() ([]*dynamodb.BatchWriteItemOutput, error) {
	return b.output, b.err
}

// BatchWriteBuilder used to build a BatchWriteItemInput dynamically with encoding support
type BatchWriteBuilder struct {
	input *dynamodb.BatchWriteItemInput
}

// NewBatchWriteBuilder creates a new BatchWriteBuilder
func NewBatchWriteBuilder() *BatchWriteBuilder {
	b := &BatchWriteBuilder{
		input: &dynamodb.BatchWriteItemInput{
			RequestItems: make(map[string][]*dynamodb.WriteRequest),
		},
	}
	return b
}

func (b *BatchWriteBuilder) SetInput(input *dynamodb.BatchWriteItemInput) *BatchWriteBuilder {
	b.input = input
	return  b
}

// AddWriteRequests adds one or more WriteRequests for a given table to the input
func (b *BatchWriteBuilder) AddWriteRequests(tableName string, requests ...*dynamodb.WriteRequest) {
	if _, ok := b.input.RequestItems[tableName]; !ok {
		b.input.RequestItems[tableName] = []*dynamodb.WriteRequest{}
	}
	b.input.RequestItems[tableName] = append(b.input.RequestItems[tableName], requests...)
}

// AddPut adds a put write request to the input
func (b *BatchWriteBuilder) AddPut(table, item interface{}) *BatchWriteBuilder {
	tableName := encoding.ToString(table)
	itemMap, err := encoding.MarshalItem(item)
	if err != nil {
		panic(err)
	}
	b.AddWriteRequests(tableName, &dynamodb.WriteRequest{
		PutRequest: &dynamodb.PutRequest{
			Item: itemMap,
		},
	})

	return b
}

// AddPuts adds multiple put requests from a given input that should be a slice of structs or maps
func (b *BatchWriteBuilder) AddPuts(table, itemSlice interface{}) *BatchWriteBuilder {
	tableName := encoding.ToString(table)
	itemMaps, err := encoding.MarshalItems(itemSlice)
	if err != nil {
		panic(err)
	}

	for _, item := range itemMaps {
		b.AddWriteRequests(tableName, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: item,
			},
		})
	}
	return b
}

// AddPut adds a put write request to the input
func (b *BatchWriteBuilder) AddDelete(table, key interface{}) *BatchWriteBuilder {
	tableName := encoding.ToString(table)
	keyItem, err := encoding.MarshalItem(key)
	if err != nil {
		panic(err)
	}
	b.AddWriteRequests(tableName, &dynamodb.WriteRequest{
		DeleteRequest: &dynamodb.DeleteRequest{
			Key: keyItem,
		},
	})

	return b
}

// AddPuts adds multiple delete requests from a given input that should be a slice of structs or maps
func (b *BatchWriteBuilder) AddDeletes(table, keySlice interface{}) *BatchWriteBuilder {
	tableName := encoding.ToString(table)
	keyItems, err := encoding.MarshalItems(keySlice)
	if err != nil {
		panic(err)
	}

	for _, k := range keyItems {
		b.AddWriteRequests(tableName, &dynamodb.WriteRequest{
			DeleteRequest: &dynamodb.DeleteRequest{
				Key: k,
			},
		})
	}
	return b
}

// Build returns the input.
// while build doesn't actually build anything here, the name is kept as it matches other builder semantics
func (b *BatchWriteBuilder) Build() *dynamodb.BatchWriteItemInput {
	// return the already built input
	return b.input
}

// Operation returns a new BatchWriteOperation with this builder's input
func (b *BatchWriteBuilder) Operation() *BatchWriteOperation {
	return BatchWrite(b.Build())
}

// BatchWriteOperation is used to write a slice of records to a table
// if workers is 0 then there is no limit to how many go routines are spawned
type BatchWriteOperation struct {
	*baseOperation
	input       *dynamodb.BatchWriteItemInput
	concurrency int
}

// BatchWrite creates a new BatchWriteOperation with optional BatchWriteItemInput
func BatchWrite(input *dynamodb.BatchWriteItemInput) *BatchWriteOperation {
	bw := &BatchWriteOperation{
		baseOperation: newBase(),
		input:         input,
	}
	return bw
}

// SetInput sets the BatchWriteItemInput for this BatchWriteOperation
// panics with an InvalidState error if operation isn't pending
func (bw *BatchWriteOperation) SetInput(input *dynamodb.BatchWriteItemInput) *BatchWriteOperation {
	if !bw.IsPending() {
		panic(&InvalidState{})
	}
	bw.mu.Lock()
	defer bw.mu.Unlock()
	bw.input = input
	return bw
}

// Input returns the current BatchWriteItemInput for this BatchWriteOperation
func (bw *BatchWriteOperation) Input() *dynamodb.BatchWriteItemInput {
	bw.mu.RLock()
	defer bw.mu.RUnlock()
	return bw.input
}

// SetConcurrency sets the workers for the batch write request
// panics with an InvalidState error if operation isn't pending
func (bw *BatchWriteOperation) SetConcurrency(concurrency int) *BatchWriteOperation {
	if !bw.IsPending() {
		panic(&InvalidState{})
	}
	bw.mu.Lock()
	defer bw.mu.Unlock()
	bw.concurrency = concurrency
	return bw
}

// ExecuteInBatch executes the BatchWriteOperation using given Request
// and returns a Result used for executing this operation in a batch
func (bw *BatchWriteOperation) ExecuteInBatch(req *dyno.Request) Result {
	return bw.Execute(req)
}

// execute the batch write request
func (bw *BatchWriteOperation) Execute(req *dyno.Request) (out *BatchWriteResult) {
	out = &BatchWriteResult{}
	bw.setRunning()
	defer bw.setDone(out)

	if bw.concurrency == 0 {
		// automatically set the workers to number of processes needed to run all at once
		bw.concurrency = int(math.Ceil(float64(len(bw.input.RequestItems)) / dynamoBatchWriteLimit))
	}

	chunks := chunkBatchWrite(bw.input, dynamoBatchWriteLimit)

	// set up the sem and wait groups
	sem := make(chan struct{}, bw.concurrency) // semaphore for workers limiting
	wg := sync.WaitGroup{}
	state := &batchWriteState{
		mu: sync.RWMutex{},
	}

	for _, input := range chunks {

		if state.error() != nil {
			break
		}

		wg.Add(1)
		sem <- struct{}{}
		// doPut the request in a goroutine
		go doBatchWriteRequest(req, input, sem, &wg, state)
		// reset the key slice for the next loop
	}

	wg.Wait()
	close(sem)
	out.output = state.outputs
	out.err = state.err
	return
}

// GoExecute executes the BatchWriteOperation request in a go routine
func (bw *BatchWriteOperation) GoExecute(req *dyno.Request) <-chan *BatchWriteResult {
	outCh := make(chan *BatchWriteResult)
	go func() {
		defer close(outCh)
		outCh <- bw.Execute(req)
	}()
	return outCh
}

// chunkBatchWrite takes a single batch write input and splits it based on the max size
func chunkBatchWrite(input *dynamodb.BatchWriteItemInput, max int) []*dynamodb.BatchWriteItemInput {
	out := make([]*dynamodb.BatchWriteItemInput, 0)

	idx := 0
	cnt := 0

	for tableName, requests := range input.RequestItems {
		if len(out)-1 < idx {
			out = append(out, &dynamodb.BatchWriteItemInput{
				RequestItems:                map[string][]*dynamodb.WriteRequest{},
				ReturnConsumedCapacity:      input.ReturnConsumedCapacity,
				ReturnItemCollectionMetrics: input.ReturnItemCollectionMetrics,
			})
		}

		if _, ok := out[idx].RequestItems[tableName]; !ok {
			out[idx].RequestItems[tableName] = []*dynamodb.WriteRequest{}
		}

		for _, req := range requests {
			out[idx].RequestItems[tableName] = append(out[idx].RequestItems[tableName], req)
			cnt += 1
			if cnt >= max {
				cnt = 0
				idx += 1
				break
			}
		}
	}

	return out
}

type batchWriteState struct {
	outputs []*dynamodb.BatchWriteItemOutput
	err     error
	mu      sync.RWMutex
}

func (s *batchWriteState) setError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

func (s *batchWriteState) error() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.err
}

func (s *batchWriteState) addOutput(out *dynamodb.BatchWriteItemOutput) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.outputs == nil {
		s.outputs = []*dynamodb.BatchWriteItemOutput{out}
		return
	}
	s.outputs = append(s.outputs, out)
}

func doBatchWriteRequest(req *dyno.Request,
	input *dynamodb.BatchWriteItemInput,
	sem <-chan struct{},
	wg *sync.WaitGroup,
	state *batchWriteState) {

	defer func() {
		<-sem
		wg.Done()
	}()

	// keep looping until no more keys are left
	for {
		if input == nil {
			return
		}
		// call the API endpoint
		out, err := req.BatchWriteItem(input)

		if err != nil {
			state.setError(err)
			return
		}

		state.addOutput(out)

		if out.UnprocessedItems == nil || len(out.UnprocessedItems) < 1 {
			return
		}

		input.RequestItems = out.UnprocessedItems
	}
}
