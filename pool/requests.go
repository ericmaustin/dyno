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
		err error
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
		merr MultiError
		err error
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

// GetResult used used as the Get result from a pool operation
type GetResult struct {
	*PoolResult
}

// Output waits for the output and the error to be returned
func (r *GetResult) Output() (*dynamodb.GetItemOutput, error) {
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
		err error
	)

	out := make([]*dynamodb.BatchGetItemOutput, len(r.results))

	for i, r := range r.results {
		out[i], err = r.Output()
		merr.AddError(err)
	}

	return out, merr.Return()
}