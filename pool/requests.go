package operation

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ericmaustin/dyno"
	"strings"
)

//MultiError represents an error slice returned as a single error
type MultiError struct {
	Errs []error
}

func NewMultiError(err ...error) *MultiError {
	return &MultiError{err}
}

func (m *MultiError) AddError(err error) {
	if m == nil {
		// is this stupid?
		*m = MultiError{}
	}
	if err != nil {
		m.Errs = append(m.Errs, err)
	}
}

func (m *MultiError) Return() error {
	if len(m.Errs) > 0 {
		return m
	}
	return nil
}

func (m *MultiError) Error() string {
	outStr := make([]string, len(m.Errs))
	for i, e := range m.Errs {
		outStr[i] = e.Error()
	}
	return fmt.Sprintf("%d Errors: %s", len(m.Errs), strings.Join(outStr, "; "))
}

// ScanResult used used as the Scan result from a pool operation
type ScanResult struct {
	*PoolResult
}

// Output waits for the output and the error to be returned
func (s *ScanResult) Output() (*dynamodb.ScanOutput, error) {
	out, err := s.output()
	return out.(*dynamodb.ScanOutput), err
}

// ScanAllResult used used as the ScanAll result from a pool operation
type ScanAllResult struct {
	results []*ScanResult
}

// Output waits for the output and the error to be returned
func (s *ScanAllResult) Output() ([]*dynamodb.ScanOutput, error) {
	var (
		merr MultiError
		err  error
	)

	out := make([]*dynamodb.ScanOutput, len(s.results))

	for i, r := range s.results {
		out[i], err = r.Output()
		merr.AddError(err)
	}

	return out, merr.Return()
}

func ScanFunc(in *dynamodb.ScanInput, handler dyno.ScanHandler) PoolFunc {
	return func(req *dyno.Request) (interface{}, error) {
		return req.ScanAll(in, handler)
	}
}

// ScanCountResult used used as the Scan result from a pool operation
type ScanCountResult struct {
	*PoolResult
}

// Output waits for the output and the error to be returned
func (s *ScanCountResult) Output() (int64, error) {
	out, err := s.output()
	if err != nil {
		return 0, err
	}
	scanOut := out.(*dynamodb.ScanOutput)
	if scanOut.Count == nil {
		return 0, nil
	}
	return *scanOut.Count, err
}

// ScanCountAllResult used used as the ScanCountAll result from a pool operation
type ScanCountAllResult struct {
	results []*ScanCountResult
}

// Output waits for the output and the error to be returned
func (s *ScanCountAllResult) Output() (int64, error) {
	var (
		merr       MultiError
		err        error
		cnt, total int64
	)

	for _, r := range s.results {
		cnt, err = r.Output()
		merr.AddError(err)
		total += cnt
	}

	return total, merr.Return()
}

//func ScanFuncs(inputs []*dynamodb.ScanInput, handler dyno.ScanHandler) []PoolFunc {
//	out := make([]PoolFunc, len(inputs))
//	for i, in := range inputs {
//		out[i] = ScanFunc(in, handler)
//	}
//	return out
//}

// GetItemResult used used as the GetItem result from a pool operation
type GetItemResult struct {
	*PoolResult
}

// Output waits for the output and the error to be returned
func (r *GetItemResult) Output() (*dynamodb.GetItemOutput, error) {
	out, err := r.output()
	return out.(*dynamodb.GetItemOutput), err
}

func GetItemFunc(in *dynamodb.GetItemInput, handler dyno.GetItemHandler) PoolFunc {
	return func(req *dyno.Request) (interface{}, error) {
		return req.GetItem(in, handler)
	}
}

// BatchGetItemResult used used as the GetBatch result from a pool operation
type BatchGetItemResult struct {
	*PoolResult
}

// Output waits for the output and the error to be returned
func (r *BatchGetItemResult) Output() (*dynamodb.BatchGetItemOutput, error) {
	out, err := r.output()
	return out.(*dynamodb.BatchGetItemOutput), err
}

func BatchGetItemFunc(in *dynamodb.BatchGetItemInput, handler dyno.BatchGetItemHandler) PoolFunc {
	return func(req *dyno.Request) (interface{}, error) {
		return req.BatchGetItemAll(in, handler)
	}
}

// BatchGetItemAllResult used used as the BatchGetItemAll result from a pool operation
type BatchGetItemAllResult struct {
	results []*BatchGetItemResult
}

// Output waits for the output and the error to be returned
func (r *BatchGetItemAllResult) Output() ([]*dynamodb.BatchGetItemOutput, error) {
	var (
		merr MultiError
		err  error
	)

	out := make([]*dynamodb.BatchGetItemOutput, len(r.results))

	for i, r := range r.results {
		out[i], err = r.Output()
		merr.AddError(err)
	}

	return out, merr.Return()
}

// CreateTableResult used used as the Pool.CreateTable result
type CreateTableResult struct {
	*PoolResult
}

// Output waits for the output and the error to be returned
func (r *CreateTableResult) Output() (*dynamodb.CreateTableOutput, error) {
	out, err := r.output()
	return out.(*dynamodb.CreateTableOutput), err
}

//CreateTableFunc returns a PoolFunc for executing a Request.CreateTable operation in the Pool
func CreateTableFunc(in *dynamodb.CreateTableInput, handler dyno.CreateTableHandler) PoolFunc {
	return func(req *dyno.Request) (interface{}, error) {
		return req.CreateTable(in, handler)
	}
}

// DeleteTableResult used used as the Pool.DeleteTable result
type DeleteTableResult struct {
	*PoolResult
}

// Output waits for the output and the error to be returned
func (r *DeleteTableResult) Output() (*dynamodb.DeleteTableOutput, error) {
	out, err := r.output()
	return out.(*dynamodb.DeleteTableOutput), err
}

//DeleteTableFunc returns a PoolFunc for executing a Request.DeleteTable operation in the Pool
func DeleteTableFunc(in *dynamodb.DeleteTableInput, handler dyno.DeleteTableHandler) PoolFunc {
	return func(req *dyno.Request) (interface{}, error) {
		return req.DeleteTable(in, handler)
	}
}

//ListenForTableReadyResult used used as the Pool.DeleteTable result
type ListenForTableReadyResult struct {
	*PoolResult
}

// Output waits for the dyno.TableReadyOutput returned by Request.ListenForTableReady
func (r *ListenForTableReadyResult) Output() *dyno.TableReadyOutput {
	// no error is returned. See: ListenForTableDeletionFunc
	out, _ := r.output()
	return out.(*dyno.TableReadyOutput)
}

//ListenForTableReadyFunc returns a PoolFunc for executing a Request.ListenForTableReady operation in the Pool
func ListenForTableReadyFunc(tableName string) PoolFunc {
	return func(req *dyno.Request) (interface{}, error) {
		// no error returned
		return req.ListenForTableDeletion(tableName), nil
	}
}

// ListenForTableDeletionResult used used as the Pool.DeleteTable result
type ListenForTableDeletionResult struct {
	*PoolResult
}

// Output waits for the error chan returned by Request.ListenForTableDeletion
func (r *ListenForTableDeletionResult) Output() <-chan error {
	// no error is returned. See: ListenForTableDeletionFunc
	out, _ := r.output()
	return out.(<-chan error)
}

//ListenForTableDeletionFunc returns a PoolFunc for executing a Request.ListenForTableDeletion operation in the Pool
func ListenForTableDeletionFunc(tableName string) PoolFunc {
	return func(req *dyno.Request) (interface{}, error) {
		// no error returned
		return req.ListenForTableDeletion(tableName), nil
	}
}
