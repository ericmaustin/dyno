package dyno

import (
	"context"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type BatchGetItemInputCallback interface {
	BatchGetItemInputCallback(context.Context, *dynamodb.BatchGetItemInput) (*dynamodb.BatchGetItemOutput, error)
}

type BatchGetItemInputCallbackFunc func(context.Context, *dynamodb.BatchGetItemInput) (*dynamodb.BatchGetItemOutput, error)

func (cb BatchGetItemInputCallbackFunc) BatchGetItemInputCallback(ctx context.Context, input *dynamodb.BatchGetItemInput) (*dynamodb.BatchGetItemOutput, error) {
	return cb(ctx, input)
}

type BatchGetItemOutputCallback interface {
	BatchGetItemOutputCallback(context.Context, *dynamodb.BatchGetItemOutput) error
}

type BatchGetItemOutputCallbackFunc func(context.Context, *dynamodb.BatchGetItemOutput) error

func (cb BatchGetItemOutputCallbackFunc) BatchGetItemOutputCallback(ctx context.Context, input *dynamodb.BatchGetItemOutput) error {
	return cb(ctx, input)
}

type CreateTableInputCallback interface {
	CreateTableInputCallback(context.Context, *dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error)
}

type CreateTableInputCallbackFunc func(context.Context, *dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error)

func (cb CreateTableInputCallbackFunc) CreateTableInputCallback(ctx context.Context, input *dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error) {
	return cb(ctx, input)
}

type CreateTableOutputCallback interface {
	CreateTableOutputCallback(context.Context, *dynamodb.CreateTableOutput) error
}

type CreateTableOutputCallbackFunc func(context.Context, *dynamodb.CreateTableOutput) error

func (cb CreateTableOutputCallbackFunc) CreateTableOutputCallback(ctx context.Context, input *dynamodb.CreateTableOutput) error {
	return cb(ctx, input)
}

type GetItemInputCallback interface {
	GetItemInputCallback(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error)
}

type GetItemInputCallbackFunc func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error)

func (cb GetItemInputCallbackFunc) GetItemInputCallback(ctx context.Context, input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	return cb(ctx, input)
}

type GetItemOutputCallback interface {
	GetItemOutputCallback(context.Context, *dynamodb.GetItemOutput) error
}

type GetItemOutputCallbackFunc func(context.Context, *dynamodb.GetItemOutput) error

func (cb GetItemOutputCallbackFunc) GetItemOutputCallback(ctx context.Context, input *dynamodb.GetItemOutput) error {
	return cb(ctx, input)
}

type DeleteTableInputCallback interface {
	DeleteTableInputCallback(context.Context, *dynamodb.DeleteTableInput) (*dynamodb.DeleteTableOutput, error)
}

type DeleteTableInputCallbackFunc interface {
	DeleteTableInputCallbackFunc(context.Context, *dynamodb.DeleteTableInput) (*dynamodb.DeleteTableOutput, error)
}

type DeleteTableInputCallbackFuncFunc func(context.Context, *dynamodb.DeleteTableInput) (*dynamodb.DeleteTableOutput, error)

func (cb DeleteTableInputCallbackFuncFunc) DeleteTableInputCallbackFunc(ctx context.Context, input *dynamodb.DeleteTableInput) (*dynamodb.DeleteTableOutput, error) {
	return cb(ctx, input)
}

type DeleteTableOutputCallback interface {
	DeleteTableOutputCallback(context.Context, *dynamodb.DeleteTableOutput) error
}

type DeleteTableOutputCallbackFunc func(context.Context, *dynamodb.DeleteTableOutput) error

func (cb DeleteTableOutputCallbackFunc) DeleteTableOutputCallback(ctx context.Context, input *dynamodb.DeleteTableOutput) error {
	return cb(ctx, input)
}

type GetItemHandleInputCallback interface {
	GetItemHandleInputCallback(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error)
}

type GetItemHandleInputCallbackFunc func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error)

func (cb GetItemHandleInputCallbackFunc) GetItemHandleInputCallback(ctx context.Context, input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	return cb(ctx, input)
}

type GetItemHandleOutputCallback interface {
	GetItemHandleOutputCallback(context.Context, *dynamodb.GetItemOutput) error
}

type GetItemHandleOutputCallbackFunc func(context.Context, *dynamodb.GetItemOutput) error

func (cb GetItemHandleOutputCallbackFunc) GetItemHandleOutputCallback(ctx context.Context, input *dynamodb.GetItemOutput) error {
	return cb(ctx, input)
}

type ScanInputCallback interface {
	ScanInputCallback(context.Context, *dynamodb.ScanInput) (*dynamodb.ScanOutput, error)
}

type ScanInputCallbackFunc func(context.Context, *dynamodb.ScanInput) (*dynamodb.ScanOutput, error)

func (cb ScanInputCallbackFunc) ScanInputCallback(ctx context.Context, input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
	return cb(ctx, input)
}

type ScanOutputCallback interface {
	ScanOutputCallback(context.Context, *dynamodb.ScanOutput) error
}

type ScanOutputCallbackFunc func(context.Context, *dynamodb.ScanOutput) error

func (cb ScanOutputCallbackFunc) ScanOutputCallback(ctx context.Context, input *dynamodb.ScanOutput) error {
	return cb(ctx, input)
}

type QueryInputCallback interface {
	QueryInputCallback(context.Context, *dynamodb.QueryInput) (*dynamodb.QueryOutput, error)
}

type QueryInputCallbackFunc func(context.Context, *dynamodb.QueryInput) (*dynamodb.QueryOutput, error)

func (cb QueryInputCallbackFunc) QueryInputCallback(ctx context.Context, input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
	return cb(ctx, input)
}

type QueryOutputCallback interface {
	QueryOutputCallback(context.Context, *dynamodb.QueryOutput) error
}

type QueryOutputCallbackFunc func(context.Context, *dynamodb.QueryOutput) error

func (cb QueryOutputCallbackFunc) QueryOutputCallback(ctx context.Context, input *dynamodb.QueryOutput) error {
	return cb(ctx, input)
}

type PutItemInputCallback interface {
	PutItemInputCallback(context.Context, *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
}

type PutItemInputCallbackFunc func(context.Context, *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)

func (cb PutItemInputCallbackFunc) PutItemInputCallback(ctx context.Context, input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	return cb(ctx, input)
}

type PutItemOutputCallback interface {
	PutItemOutputCallback(context.Context, *dynamodb.PutItemOutput) error
}

type PutItemOutputCallbackFunc func(context.Context, *dynamodb.PutItemOutput) error

func (cb PutItemOutputCallbackFunc) PutItemOutputCallback(ctx context.Context, input *dynamodb.PutItemOutput) error {
	return cb(ctx, input)
}

type CreateGlobalTableOutputCallback interface {
	CreateGlobalTableOutputCallback(context.Context, *dynamodb.CreateGlobalTableOutput) error
}

type CreateGlobalTableOutputCallbackFunc func(context.Context, *dynamodb.CreateGlobalTableOutput) error

func (cb CreateGlobalTableOutputCallbackFunc) CreateGlobalTableOutputCallback(ctx context.Context, input *dynamodb.CreateGlobalTableOutput) error {
	return cb(ctx, input)
}

type DescribeTableInputCallback interface {
	DescribeTableInputCallback(context.Context, *dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error)
}

type DescribeTableInputCallbackFunc func(context.Context, *dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error)

func (cb DescribeTableInputCallbackFunc) DescribeTableInputCallback(ctx context.Context, input *dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error) {
	return cb(ctx, input)
}

type DescribeTableOutputCallback interface {
	DescribeTableOutputCallback(context.Context, *dynamodb.DescribeTableOutput) error
}

type DescribeTableOutputCallbackFunc func(context.Context, *dynamodb.DescribeTableOutput) error

func (cb DescribeTableOutputCallbackFunc) DescribeTableOutputCallback(ctx context.Context, input *dynamodb.DescribeTableOutput) error {
	return cb(ctx, input)
}

type UpdateItemInputCallback interface {
	UpdateItemInputCallback(context.Context, *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error)
}

type UpdateItemInputCallbackFunc func(context.Context, *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error)

func (cb UpdateItemInputCallbackFunc) UpdateItemInputCallback(ctx context.Context, input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
	return cb(ctx, input)
}

type UpdateItemOutputCallback interface {
	UpdateItemOutputCallback(context.Context, *dynamodb.UpdateItemOutput) error
}

type UpdateItemOutputCallbackFunc func(context.Context, *dynamodb.UpdateItemOutput) error

func (cb UpdateItemOutputCallbackFunc) UpdateItemOutputCallback(ctx context.Context, input *dynamodb.UpdateItemOutput) error {
	return cb(ctx, input)
}

type DeleteItemInputCallback interface {
	DeleteItemInputCallback(context.Context, *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error)
}

type DeleteItemInputCallbackFunc func(context.Context, *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error)

func (cb DeleteItemInputCallbackFunc) DeleteItemInputCallback(ctx context.Context, input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	return cb(ctx, input)
}

type DeleteItemOutputCallback interface {
	DeleteItemOutputCallback(context.Context, *dynamodb.DeleteItemOutput) error
}

type DeleteItemOutputCallbackFunc func(context.Context, *dynamodb.DeleteItemOutput) error

func (cb DeleteItemOutputCallbackFunc) DeleteItemOutputCallback(ctx context.Context, input *dynamodb.DeleteItemOutput) error {
	return cb(ctx, input)
}

type RestoreTableFromBackupInputCallback interface {
	RestoreTableFromBackupInputCallback(context.Context, *dynamodb.RestoreTableFromBackupInput) (*dynamodb.RestoreTableFromBackupOutput, error)
}

type RestoreTableFromBackupInputCallbackFunc func(context.Context, *dynamodb.RestoreTableFromBackupInput) (*dynamodb.RestoreTableFromBackupOutput, error)

func (cb RestoreTableFromBackupInputCallbackFunc) RestoreTableFromBackupInputCallback(ctx context.Context, input *dynamodb.RestoreTableFromBackupInput) (*dynamodb.RestoreTableFromBackupOutput, error) {
	return cb(ctx, input)
}

type RestoreTableFromBackupOutputCallback interface {
	RestoreTableFromBackupOutputCallback(context.Context, *dynamodb.RestoreTableFromBackupOutput) error
}

type RestoreTableFromBackupOutputCallbackFunc func(context.Context, *dynamodb.RestoreTableFromBackupOutput) error

func (cb RestoreTableFromBackupOutputCallbackFunc) RestoreTableFromBackupOutputCallback(ctx context.Context, input *dynamodb.RestoreTableFromBackupOutput) error {
	return cb(ctx, input)
}

type BatchWriteItemInputCallback interface {
	BatchWriteItemInputCallback(context.Context, *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error)
}

type BatchWriteItemInputCallbackFunc func(context.Context, *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error)

func (cb BatchWriteItemInputCallbackFunc) BatchWriteItemInputCallback(ctx context.Context, input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
	return cb(ctx, input)
}

type BatchWriteItemOutputCallback interface {
	BatchWriteItemOutputCallback(context.Context, *dynamodb.BatchWriteItemOutput) error
}

type BatchWriteItemOutputCallbackFunc func(context.Context, *dynamodb.BatchWriteItemOutput) error

func (cb BatchWriteItemOutputCallbackFunc) BatchWriteItemOutputCallback(ctx context.Context, input *dynamodb.BatchWriteItemOutput) error {
	return cb(ctx, input)
}

type CreateBackupInputCallback interface {
	CreateBackupInputCallback(context.Context, *dynamodb.CreateBackupInput) (*dynamodb.CreateBackupOutput, error)
}

type CreateBackupInputCallbackFunc func(context.Context, *dynamodb.CreateBackupInput) (*dynamodb.CreateBackupOutput, error)

func (cb CreateBackupInputCallbackFunc) CreateBackupInputCallback(ctx context.Context, input *dynamodb.CreateBackupInput) (*dynamodb.CreateBackupOutput, error) {
	return cb(ctx, input)
}

type CreateBackupOutputCallback interface {
	CreateBackupOutputCallback(context.Context, *dynamodb.CreateBackupOutput) error
}

type CreateBackupOutputCallbackFunc func(context.Context, *dynamodb.CreateBackupOutput) error

func (cb CreateBackupOutputCallbackFunc) CreateBackupOutputCallback(ctx context.Context, input *dynamodb.CreateBackupOutput) error {
	return cb(ctx, input)
}

type DescribeBackupInputCallback interface {
	DescribeBackupInputCallback(context.Context, *dynamodb.DescribeBackupInput) (*dynamodb.DescribeBackupOutput, error)
}

type DescribeBackupInputCallbackFunc func(context.Context, *dynamodb.DescribeBackupInput) (*dynamodb.DescribeBackupOutput, error)

func (cb DescribeBackupInputCallbackFunc) DescribeBackupInputCallback(ctx context.Context, input *dynamodb.DescribeBackupInput) (*dynamodb.DescribeBackupOutput, error) {
	return cb(ctx, input)
}

type DescribeBackupOutputCallback interface {
	DescribeBackupOutputCallback(context.Context, *dynamodb.DescribeBackupOutput) error
}

type DescribeBackupOutputCallbackFunc func(context.Context, *dynamodb.DescribeBackupOutput) error

func (cb DescribeBackupOutputCallbackFunc) DescribeBackupOutputCallback(ctx context.Context, input *dynamodb.DescribeBackupOutput) error {
	return cb(ctx, input)
}

type ListTablesInputCallback interface {
	ListTablesInputCallback(context.Context, *dynamodb.ListTablesInput) (*dynamodb.ListTablesOutput, error)
}

type ListTablesInputCallbackFunc func(context.Context, *dynamodb.ListTablesInput) (*dynamodb.ListTablesOutput, error)

func (cb ListTablesInputCallbackFunc) ListTablesInputCallback(ctx context.Context, input *dynamodb.ListTablesInput) (*dynamodb.ListTablesOutput, error) {
	return cb(ctx, input)
}

type ListTablesOutputCallback interface {
	ListTablesOutputCallback(context.Context, *dynamodb.ListTablesOutput) error
}

type ListTablesOutputCallbackFunc func(context.Context, *dynamodb.ListTablesOutput) error

func (cb ListTablesOutputCallbackFunc) ListTablesOutputCallback(ctx context.Context, input *dynamodb.ListTablesOutput) error {
	return cb(ctx, input)
}
