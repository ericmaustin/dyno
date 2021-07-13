module github.com/ericmaustin/dyno

require (
	github.com/aws/aws-sdk-go-v2 v1.6.0
	github.com/aws/aws-sdk-go-v2/config v1.3.0
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.1.1
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression v1.1.1
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.3.1
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/google/go-cmp v0.5.5 // indirect
	github.com/kr/pretty v0.2.1 // indirect
	github.com/segmentio/ksuid v1.0.3
	github.com/stretchr/testify v1.7.0
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
	gopkg.in/yaml.v2 v2.3.0
)

retract v1.1.12 // errant release of version 1.x.x
retract v0.2.0 // lock-up errors in some operations
retract v1.1.0 // bad types in some marshalers

go 1.16
