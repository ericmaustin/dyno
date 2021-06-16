package dyno

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go/aws/request"
	"time"
)

//Execute executes a given Executable with provided context.Context and dynamodb.DynamoDB client and returns a *Promise
func Execute(ctx context.Context, db *dynamodb.Client, exe Executable) *Promise {
	promise := NewPromise(ctx)
	go execute(ctx, db, exe, promise)
	return promise
}

//execute executes a given Executable with provided context.Context and dynamodb.DynamoDB client
func execute(ctx context.Context, db *dynamodb.Client, exe Executable, promise *Promise) {
	promise.SetResponse(exe.DynoExecute(ctx, db))
}

// ExecutionFunction is a function that can be executed by a Request
type ExecutionFunction func(ctx context.Context, db *dynamodb.Client) (interface{}, error)

//DynoExecute implements the Executable interface
func (e ExecutionFunction) DynoExecute(ctx context.Context, db *dynamodb.Client) (interface{}, error) {
	return e(ctx, db)
}

//Executable should be implemented for types that can be executed in a Request
type Executable interface {
	DynoExecute(ctx context.Context, db *dynamodb.Client) (interface{}, error)
}

//ScanFunc returns an ExecutionFunction that executes a Scan api call
func ScanFunc(input *ScanInput) ExecutionFunction {
	return func(ctx context.Context, db *dynamodb.Client) (interface{}, error) {
		var (
			out *dynamodb.ScanOutput
			err error
		)
		if input.InputCallback != nil {
			if out, err = input.InputCallback(ctx, input.ScanInput); out != nil || err != nil {
				return out, err
			}
		}
		if out, err = db.ScanWithContext(ctx, input.ScanInput); err != nil {
			return out, err
		}
		if input.OutputCallback != nil {
			err = input.OutputCallback(ctx, out)
		}
		return out, err
	}
}

//ScanAllFunc returns an ExecutionFunction that executes a Scan api call
// and keeps executing until there are no more LastEvaluatedKey left
func ScanAllFunc(input *ScanInput) ExecutionFunction {
	return func(ctx context.Context, db *dynamodb.DynamoDB) (interface{}, error) {
		var (
			outs []*dynamodb.ScanOutput
			out  *dynamodb.ScanOutput
			err  error
		)
		for {
			if input.InputCallback != nil {
				if out, err = input.InputCallback(ctx, input.ScanInput); out != nil || err != nil {
					if out != nil {
						outs = append(outs, out)
					}
					return outs, err
				}
			}
			if out, err = db.ScanWithContext(ctx, input.ScanInput); err != nil {
				return nil, err
			}
			if input.OutputCallback != nil {
				if err = input.OutputCallback(ctx, out); err != nil {
					return nil, err
				}
			}
			outs = append(outs, out)
			if out.LastEvaluatedKey == nil {
				// no more work
				break
			}
			input.ExclusiveStartKey = out.LastEvaluatedKey
		}
		return outs, err
	}
}

//QueryFunc returns an ExecutionFunction that executes a Query api call
func QueryFunc(input *QueryInput) ExecutionFunction {
	return func(ctx context.Context, db *dynamodb.DynamoDB) (interface{}, error) {
		var (
			out *dynamodb.QueryOutput
			err error
		)
		if input.InputCallback != nil {
			if out, err = input.InputCallback(ctx, input.QueryInput); out != nil || err != nil {
				return out, err
			}
		}
		if out, err = db.QueryWithContext(ctx, input.QueryInput); err != nil {
			return out, err
		}
		if input.OutputCallback != nil {
			err = input.OutputCallback(ctx, out)
		}
		return out, err
	}
}

//QueryAllFunc returns an ExecutionFunction that executes a Query api call
// and keeps executing until there are no more LastEvaluatedKey left
func QueryAllFunc(input *QueryInput) ExecutionFunction {
	return func(ctx context.Context, db *dynamodb.DynamoDB) (interface{}, error) {
		var (
			outs []*dynamodb.QueryOutput
			out  *dynamodb.QueryOutput
			err  error
		)
		for {
			if input.InputCallback != nil {
				if out, err = input.InputCallback(ctx, input.QueryInput); out != nil || err != nil {
					if out != nil {
						outs = append(outs, out)
					}
					return outs, err
				}
			}
			if out, err = db.QueryWithContext(ctx, input.QueryInput); err != nil {
				return nil, err
			}
			if input.OutputCallback != nil {
				if err = input.OutputCallback(ctx, out); err != nil {
					return nil, err
				}
			}
			outs = append(outs, out)
			if out.LastEvaluatedKey == nil {
				// no more work
				break
			}
			input.ExclusiveStartKey = out.LastEvaluatedKey
		}
		return outs, err
	}
}

//PutItemFunc returns an ExecutionFunction that executes a PutItem api call
func PutItemFunc(input *PutItemInput) ExecutionFunction {
	return func(ctx context.Context, db *dynamodb.DynamoDB) (interface{}, error) {
		var (
			out *dynamodb.PutItemOutput
			err error
		)
		if input.InputCallback != nil {
			if out, err = input.InputCallback(ctx, input.PutItemInput); out != nil || err != nil {
				return out, err
			}
		}
		if out, err = db.PutItemWithContext(ctx, input.PutItemInput); err != nil {
			return out, err
		}
		if input.OutputCallback != nil {
			err = input.OutputCallback(ctx, out)
		}
		return out, err
	}
}

//GetItemFunc returns an ExecutionFunction that executes a PutItem api call
func GetItemFunc(input *GetItemInput) ExecutionFunction {
	return func(ctx context.Context, db *dynamodb.DynamoDB) (interface{}, error) {
		var (
			out *dynamodb.GetItemOutput
			err error
		)
		if input.InputCallback != nil {
			if out, err = input.InputCallback(ctx, input.GetItemInput); out != nil || err != nil {
				return out, err
			}
		}
		if out, err = db.GetItemWithContext(ctx, input.GetItemInput); err != nil {
			return out, err
		}
		if input.OutputCallback != nil {
			err = input.OutputCallback(ctx, out)
		}
		return out, err
	}
}

//UpdateItemFunc returns an ExecutionFunction that executes an UpdateItem api call
func UpdateItemFunc(input *UpdateItemInput) ExecutionFunction {
	return func(ctx context.Context, db *dynamodb.DynamoDB) (interface{}, error) {
		var (
			out *dynamodb.UpdateItemOutput
			err error
		)
		if input.InputCallback != nil {
			if out, err = input.InputCallback(ctx, input.UpdateItemInput); out != nil || err != nil {
				return out, err
			}
		}
		if out, err = db.UpdateItemWithContext(ctx, input.UpdateItemInput); err != nil {
			return out, err
		}
		if input.OutputCallback != nil {
			err = input.OutputCallback(ctx, out)
		}
		return out, err
	}
}

//DeleteItemFunc returns an ExecutionFunction that executes an DeleteItem api call
func DeleteItemFunc(input *DeleteItemInput) ExecutionFunction {
	return func(ctx context.Context, db *dynamodb.DynamoDB) (interface{}, error) {
		var (
			out *dynamodb.DeleteItemOutput
			err error
		)
		if input.InputCallback != nil {
			if out, err = input.InputCallback(ctx, input.DeleteItemInput); out != nil || err != nil {
				return out, err
			}
		}
		if out, err = db.DeleteItemWithContext(ctx, input.DeleteItemInput); err != nil {
			return out, err
		}
		if input.OutputCallback != nil {
			err = input.OutputCallback(ctx, out)
		}
		return out, err
	}
}

//BatchGetItemFunc returns an ExecutionFunction that executes a BatchGetItem api call
func BatchGetItemFunc(input *BatchGetItemBuilder) ExecutionFunction {
	return func(ctx context.Context, db *dynamodb.DynamoDB) (interface{}, error) {
		var (
			out *dynamodb.BatchGetItemOutput
			err error
		)
		if input.InputCallback != nil {
			if out, err = input.InputCallback(ctx, input.BatchGetItemInput); out != nil || err != nil {
				return out, err
			}
		}

		if out, err = db.BatchGetItemWithContext(ctx, input.BatchGetItemInput); err != nil {
			return out, err
		}
		if input.OutputCallback != nil {
			err = input.OutputCallback(ctx, out)
		}
		return out, err
	}
}

//BatchGetItemAllFunc returns an ExecutionFunction that executes a Query api call
// and keeps executing until there are no more LastEvaluatedKey left
func BatchGetItemAllFunc(input *BatchGetItemBuilder) ExecutionFunction {
	return func(ctx context.Context, db *dynamodb.DynamoDB) (interface{}, error) {
		var (
			outs []*dynamodb.BatchGetItemOutput
			out  *dynamodb.BatchGetItemOutput
			err  error
		)
		for {
			if input.InputCallback != nil {
				if out, err = input.InputCallback(ctx, input.BatchGetItemInput); out != nil || err != nil {
					if out != nil {
						outs = append(outs, out)
					}
					return outs, err
				}
			}

			if out, err = db.BatchGetItemWithContext(ctx, input.BatchGetItemInput); err != nil {
				return nil, err
			}
			if input.OutputCallback != nil {
				if err = input.OutputCallback(ctx, out); err != nil {
					return nil, err
				}
			}
			outs = append(outs, out)
			if out.UnprocessedKeys == nil {
				// no more work
				break
			}
			input.RequestItems = out.UnprocessedKeys
		}
		return outs, err
	}
}

//BatchWriteItemFunc returns an ExecutionFunction that
func BatchWriteItemFunc(input *BatchWriteItemInput) ExecutionFunction {
	return func(ctx context.Context, db *dynamodb.DynamoDB) (interface{}, error) {
		var (
			out *dynamodb.BatchWriteItemOutput
			err error
		)
		if input.InputCallback != nil {
			if out, err = input.InputCallback(ctx, input.BatchWriteItemInput); out != nil || err != nil {
				return out, err
			}
		}
		if out, err = db.BatchWriteItemWithContext(ctx, input.BatchWriteItemInput); err != nil {
			return nil, err
		}
		if input.OutputCallback != nil {
			err = input.OutputCallback(ctx, out)
		}
		return out, err
	}
}

//BatchWriteItemAllFunc returns an ExecutionFunction that runs a BatchWriteItemInput api call
func BatchWriteItemAllFunc(input *BatchWriteItemInput) ExecutionFunction {
	return func(ctx context.Context, db *dynamodb.DynamoDB) (interface{}, error) {
		var (
			outs []*dynamodb.BatchWriteItemOutput
			out  *dynamodb.BatchWriteItemOutput
			err  error
		)
		for {
			if input.InputCallback != nil {
				if out, err = input.InputCallback(ctx, input.BatchWriteItemInput); out != nil || err != nil {
					if out != nil {
						outs = append(outs, out)
					}
					return outs, err
				}
			}
			if out, err = db.BatchWriteItemWithContext(ctx, input.BatchWriteItemInput); err != nil {
				return nil, err
			}

			if input.OutputCallback != nil {
				if err = input.OutputCallback(ctx, out); err != nil {
					return nil, err
				}
			}
			outs = append(outs, out)
			if len(out.UnprocessedItems) < 1 {
				break
			}
			input.RequestItems = out.UnprocessedItems
		}
		return outs, err
	}
}

//CreateTableFunc returns an ExecutionFunction that creates a table
func CreateTableFunc(input *CreateTableBuilder) ExecutionFunction {
	return func(ctx context.Context, db *dynamodb.DynamoDB) (interface{}, error) {
		var (
			out *dynamodb.CreateTableOutput
			err error
		)
		if input.InputCallback != nil {
			if out, err = input.InputCallback(ctx, input.CreateTableInput); out != nil || err != nil {
				return out, err
			}
		}
		if out, err = db.CreateTableWithContext(ctx, input.CreateTableInput); err != nil {
			return nil, err
		}
		if input.OutputCallback != nil {
			err = input.OutputCallback(ctx, nil)
		}
		return out, err
	}
}

//CreateBackupFunc returns an ExecutionFunction that creates a table backup
func CreateBackupFunc(input *CreateBackupInput) ExecutionFunction {
	return func(ctx context.Context, db *dynamodb.DynamoDB) (interface{}, error) {
		var (
			out *dynamodb.CreateBackupOutput
			err error
		)
		if input.InputCallback != nil {
			if out, err = input.InputCallback(ctx, input.CreateBackupInput); out != nil || err != nil {
				return out, err
			}
		}
		if out, err = db.CreateBackupWithContext(ctx, input.CreateBackupInput); err != nil {
			return nil, err
		}
		if input.OutputCallback != nil {
			err = input.OutputCallback(ctx, nil)
		}
		return out, err
	}
}

//DescribeTableFunc returns an ExecutionFunction that runs a DescribeTable api call
func DescribeTableFunc(input *DescribeTableInput) ExecutionFunction {
	return func(ctx context.Context, db *dynamodb.DynamoDB) (interface{}, error) {
		var (
			out *dynamodb.DescribeTableOutput
			err error
		)
		if input.InputCallback != nil {
			if out, err = input.InputCallback(ctx, input.DescribeTableInput); out != nil || err != nil {
				return out, err
			}
		}
		if out, err = db.DescribeTableWithContext(ctx, input.DescribeTableInput); err != nil {
			return nil, err
		}
		if input.OutputCallback != nil {
			err = input.OutputCallback(ctx, nil)
		}
		return out, err
	}
}

//DeleteTableFunc returns an ExecutionFunction that runs a DeleteTableFunc api call
func DeleteTableFunc(input *DeleteTableInput) ExecutionFunction {
	return func(ctx context.Context, db *dynamodb.DynamoDB) (interface{}, error) {
		var (
			out *dynamodb.DeleteTableOutput
			err error
		)
		if input.InputCallback != nil {
			if out, err = input.InputCallback(ctx, input.DeleteTableInput); out != nil || err != nil {
				return out, err
			}
		}
		if out, err = db.DeleteTableWithContext(ctx, input.DeleteTableInput); err != nil {
			return nil, err
		}
		if input.OutputCallback != nil {
			err = input.OutputCallback(ctx, nil)
		}
		return out, err
	}
}

//WaitUntilTableNotExistsFunc returns an ExecutionFunction that waits for a table to no longer exist
func WaitUntilTableNotExistsFunc(input *DescribeTableInput) ExecutionFunction {
	return func(ctx context.Context, db *dynamodb.DynamoDB) (interface{}, error) {
		//var out *dynamodb.DescribeTableOutput
		w := request.Waiter{
			Name:        "WaitUntilTableNotExistsFunc",
			MaxAttempts: 360,
			Delay:       request.ConstantWaiterDelay(time.Second),
			Acceptors: []request.WaiterAcceptor{
				{
					State:    request.SuccessWaiterState,
					Matcher:  request.ErrorWaiterMatch,
					Expected: dynamodb.ErrCodeResourceNotFoundException,
				},
			},
			Logger: db.Config.Logger,
			NewRequest: func(opts []request.Option) (*request.Request, error) {
				var inCpy *dynamodb.DescribeTableInput
				if input != nil {
					tmp := *input.DescribeTableInput
					inCpy = &tmp
				}
				req, _ := db.DescribeTableRequest(inCpy)
				req.SetContext(ctx)
				req.ApplyOptions(opts...)
				return req, nil
			},
		}
		return nil, w.WaitWithContext(ctx)
	}
}

//WaitUntilTableExistsFunc returns an ExecutionFunction that waits for a table to be ready
func WaitUntilTableExistsFunc(input *DescribeTableInput) ExecutionFunction {
	return func(ctx context.Context, db *dynamodb.DynamoDB) (interface{}, error) {
		//var out *dynamodb.DescribeTableOutput
		w := request.Waiter{
			Name:        "WaitUntilTableExistsFunc",
			MaxAttempts: 360,
			Delay:       request.ConstantWaiterDelay(time.Second),
			Acceptors: []request.WaiterAcceptor{
				{
					State:    request.SuccessWaiterState,
					Matcher:  request.PathWaiterMatch,
					Argument: "Table.TableStatus",
					Expected: dynamodb.TableStatusActive,
				},
				{
					State:    request.RetryWaiterState,
					Matcher:  request.ErrorWaiterMatch,
					Expected: dynamodb.ErrCodeResourceNotFoundException,
				},
			},
			Logger: db.Config.Logger,
			NewRequest: func(opts []request.Option) (*request.Request, error) {
				var inCpy *dynamodb.DescribeTableInput
				if input != nil {
					tmp := *input.DescribeTableInput
					inCpy = &tmp
				}
				req, _ := db.DescribeTableRequest(inCpy)
				req.SetContext(ctx)
				req.ApplyOptions(opts...)
				return req, nil
			},
		}
		return nil, w.WaitWithContext(ctx)
	}
}

//WaitUntilBackupExists returns an ExecutionFunction that waits for a backup to be completed
func WaitUntilBackupExists(input *DescribeBackupInput) ExecutionFunction {
	return func(ctx context.Context, db *dynamodb.DynamoDB) (interface{}, error) {
		//var out *dynamodb.DescribeTableOutput
		w := request.Waiter{
			Name:        "WaitUntilTableExistsFunc",
			MaxAttempts: 360,
			Delay:       request.ConstantWaiterDelay(time.Second),
			Acceptors: []request.WaiterAcceptor{
				{
					State:    request.SuccessWaiterState,
					Matcher:  request.PathWaiterMatch,
					Argument: "BackupDescription.BackupStatus",
					Expected: dynamodb.BackupStatusAvailable,
				},
				{
					State:    request.RetryWaiterState,
					Matcher:  request.ErrorWaiterMatch,
					Expected: dynamodb.ErrCodeResourceNotFoundException,
				},
			},
			Logger: db.Config.Logger,
			NewRequest: func(opts []request.Option) (*request.Request, error) {
				var inCpy *dynamodb.DescribeBackupInput
				if input != nil {
					tmp := *input.DescribeBackupInput
					inCpy = &tmp
				}
				req, _ := db.DescribeBackupRequest(inCpy)
				req.SetContext(ctx)
				req.ApplyOptions(opts...)
				return req, nil
			},
		}
		return nil, w.WaitWithContext(ctx)
	}
}
