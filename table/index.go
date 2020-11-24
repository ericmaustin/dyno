package table

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ericmaustin/dyno"
	"github.com/ericmaustin/dyno/encoding"
)

const (
	// ProjectionTypeInclude is used when only a selected slice of ProjectionColumns should be used with the dynamodb func
	ProjectionTypeInclude = "INCLUDE"
	// ProjectionTypeAll is used when all ProjectionColumns should be used with the dynamodb func
	ProjectionTypeAll = "ALL"
	// ProjectionTypeKeysOnly is used when only the key ProjectionColumns should be used with the dynamodb func
	ProjectionTypeKeysOnly = "KEYS_ONLY"
)

// Index represents an index on a dynamodb table
type Index struct {
	Name string `validate:"required"`
	Key  *Key   `validate:"required"`
	// effectively a RepresentationOfNonKeyAttributes
	ProjectionColumns []string
	//    * KEYS_ONLY - Only the index and primary keys are projected into the index.
	//
	//    * INCLUDE - Only the specified table attributes are projected into the
	//    index. The list of projected attributes are records NonKeyAttributes.
	//
	//    * ALL - All of the table attributes are projected into the index.
	ProjectionType string `validate:"required,oneof=INCLUDE ALL KEYS_ONLY"`
}

// SetSortKey sets the sortKey key for this index
func (idx *Index) SetSortKey(sk *SortKey) {
	if idx.Key == nil {
		idx.Key = &Key{
			sortKey: sk,
		}
	}
	idx.Key.sortKey = sk
}

// SetProjectionType sets the projection type for this index
func (idx *Index) SetProjectionType(projectionType string) {
	idx.ProjectionType = projectionType
}

// AddProjectionColumns adds the given columns to the projection
func (idx *Index) AddProjectionColumns(columns []string) {
	if idx.ProjectionColumns == nil {
		idx.ProjectionColumns = []string{}
	}
	idx.ProjectionColumns = append(idx.ProjectionColumns, columns...)
}

// ExtractKey converts a item's key ProjectionColumns to a map of dynamodb attribute ProjectionColumns for a item
// belonging to this table
func (idx *Index) ExtractKey(input interface{}) map[string]*dynamodb.AttributeValue {
	return idx.Key.extract(encoding.MustMarshalItem(input))
}

// ExtractKeys converts a list of records to a list of dynamodb attribute items
func (idx *Index) ExtractKeys(input interface{}) []map[string]*dynamodb.AttributeValue {
	return idx.Key.extractAll(encoding.MustMarshalItems(input))
}

// Gsi represents a Global Secondary Index
type Gsi struct {
	*Index
	WCUs        int64
	RCUs        int64
	OnDemand    bool
	Description *dynamodb.GlobalSecondaryIndexDescription
}

// NewGsi creae a new Global Secondary Index
func NewGsi(name string, key *Key, WCUs, RCUs int64, onDemand bool) *Gsi {
	gsi := &Gsi{
		Index: &Index{
			Name:              name,
			Key:               key,
			ProjectionType:    ProjectionTypeAll,
			ProjectionColumns: []string{},
		},
		WCUs:     WCUs,
		RCUs:     RCUs,
		OnDemand: onDemand,
	}
	return gsi
}

//Lsi represents a Local Secondary Index
type Lsi struct {
	*Index
	Description *dynamodb.LocalSecondaryIndexDescription
}

func NewLsi(name string, key *Key) *Lsi {
	return &Lsi{
		Index: &Index{
			Name:              name,
			Key:               key,
			ProjectionType:    ProjectionTypeAll,
			ProjectionColumns: []string{},
		},
	}
}

// DynamoLocalSecondaryIndex gets a local secondary index dynamo object representation
func (l *Lsi) DynamoLocalSecondaryIndex() *dynamodb.LocalSecondaryIndex {

	projection := &dynamodb.Projection{}
	projection.ProjectionType = dyno.StringPtr(l.ProjectionType)

	if l.ProjectionType != "ALL" {
		for _, col := range l.ProjectionColumns {
			str, err := dyno.GetStringFromInterface(col)
			if err != nil {
				panic(err)
			}
			projection.NonKeyAttributes = append(projection.NonKeyAttributes, dyno.StringPtr(str))
		}
	}

	return &dynamodb.LocalSecondaryIndex{
		IndexName: &l.Name,
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: dyno.StringPtr(l.Key.partitionKey.name),
				KeyType:       dyno.StringPtr("HASH"),
			},
			{
				AttributeName: dyno.StringPtr(l.Key.sortKey.name),
				KeyType:       dyno.StringPtr("RANGE"),
			},
		},
		Projection: projection,
	}
}

// DynamoGlobalSecondaryIndex returns the dynamodb GlobalSecondaryIndex object for use with dynamo API calls
func (g *Gsi) DynamoGlobalSecondaryIndex() *dynamodb.GlobalSecondaryIndex {
	projection := &dynamodb.Projection{}
	projection.ProjectionType = dyno.StringPtr(g.ProjectionType)

	if g.ProjectionType != "ALL" {
		for _, col := range g.ProjectionColumns {
			str, err := dyno.GetStringFromInterface(col)
			if err != nil {
				panic(err)
			}
			projection.NonKeyAttributes = append(projection.NonKeyAttributes, dyno.StringPtr(str))
		}
	}

	keySchema := []*dynamodb.KeySchemaElement{
		{
			AttributeName: aws.String(g.Key.PartitionName()),
			KeyType:       dyno.StringPtr("HASH"),
		},
	}

	if g.Key.sortKey != nil {
		keySchema = append(keySchema, &dynamodb.KeySchemaElement{
			AttributeName: aws.String(g.Key.SortName()),
			KeyType:       dyno.StringPtr("RANGE"),
		})
	}

	DynamoGsi := &dynamodb.GlobalSecondaryIndex{
		IndexName:  &g.Name,
		KeySchema:  keySchema,
		Projection: projection,
	}

	if !g.OnDemand {
		DynamoGsi.ProvisionedThroughput = &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  &g.RCUs,
			WriteCapacityUnits: &g.WCUs,
		}
	}

	return DynamoGsi
}
