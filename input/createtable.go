package input

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ericmaustin/dyno"
)

// CreateTableBuilder is used to construct a CreateTableInput dynamically
type CreateTableBuilder struct {
	*dynamodb.CreateTableInput
	pendingAttributeDefinitions []*dynamodb.AttributeDefinition
}

// NewCreateTableBuilder creates a new CreateTableBuilder
func NewCreateTableBuilder() *CreateTableBuilder {
	b := &CreateTableBuilder{
		CreateTableInput: &dynamodb.CreateTableInput{
			BillingMode: dyno.StringPtr(dynamodb.BillingModePayPerRequest),
		},
	}
	return b
}

// SetInput sets the CreateTableBuilder's dynamodb.CreateTableInput
func (b *CreateTableBuilder) SetInput(input *dynamodb.CreateTableInput) *CreateTableBuilder {
	b.CreateTableInput = input
	return b
}

// SetProvisionedThroughputCapacityUnits sets the provisioned write and read throughput for this table
func (b *CreateTableBuilder) SetProvisionedThroughputCapacityUnits(rcu, wcu int64) *CreateTableBuilder {
	b.SetProvisionedThroughput(&dynamodb.ProvisionedThroughput{
		ReadCapacityUnits:  &rcu,
		WriteCapacityUnits: &wcu,
	})
	b.SetBillingMode(dynamodb.BillingModeProvisioned)
	return b
}

// AddAttributeDefinition adds an attribute definition to the builder
func (b *CreateTableBuilder) AddAttributeDefinition(attribute *dynamodb.AttributeDefinition) *CreateTableBuilder {
	b.pendingAttributeDefinitions = append(b.pendingAttributeDefinitions, attribute)
	return b
}

// AddGlobalIndex adds one or more local global indexes to the builder
func (b *CreateTableBuilder) AddGlobalIndex(gsi ...*dynamodb.GlobalSecondaryIndex) *CreateTableBuilder {
	b.GlobalSecondaryIndexes = append(b.GlobalSecondaryIndexes, gsi...)
	return b
}

// AddLocalIndex adds one or more local secondary indexes to the builder
func (b *CreateTableBuilder) AddLocalIndex(lsi ...*dynamodb.LocalSecondaryIndex) *CreateTableBuilder {
	b.LocalSecondaryIndexes = append(b.LocalSecondaryIndexes, lsi...)
	return b
}

// AddTag adds a tag to the CreateTableInput using a key value pair of strings
func (b *CreateTableBuilder) AddTag(key, value string) *CreateTableBuilder {
	tag := &dynamodb.Tag{
		Key:   &key,
		Value: &value,
	}
	b.Tags = append(b.Tags, tag)
	return b
}

// AddTags adds tags to the CreateTableInput
func (b *CreateTableBuilder) AddTags(tags ...*dynamodb.Tag) *CreateTableBuilder {
	b.Tags = append(b.Tags, tags...)
	return b
}

// AddTagsFromMap adds tags to the CreateTableInput using a map of strings
func (b *CreateTableBuilder) AddTagsFromMap(tags map[string]string) *CreateTableBuilder {
	for key, value := range tags {
		b.AddTag(key, value)
	}
	return b
}

// Build builds the CreateTableInput
func (b *CreateTableBuilder) Build() (*dynamodb.CreateTableInput, error) {

	if b.pendingAttributeDefinitions != nil {
		for _, ad := range b.pendingAttributeDefinitions {
			for _, attr := range b.AttributeDefinitions {
				// don't add duplicate attribute names
				if *attr.AttributeName == *ad.AttributeName {
					if *attr.AttributeType == *ad.AttributeType {
						continue
					}
					return nil, fmt.Errorf("cannot add duplicate attribute with mismatched type."+
						"attrubuteName = %s, attributeTypes = %s, %s",
						*attr.AttributeName, *attr.AttributeType, *ad.AttributeType)
				}
			}
		}
	}

	if b.GlobalSecondaryIndexes != nil {
		for _, gsi := range b.GlobalSecondaryIndexes {
			if *b.BillingMode == dynamodb.BillingModePayPerRequest {
				gsi.ProvisionedThroughput = nil
			}
		}
	}

	return b.CreateTableInput, nil
}
