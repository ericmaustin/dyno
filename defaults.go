package dyno

import (
	"time"
)

var (

	// AWSRegion is the AWS region to use
	awsRegion = "us-east-1"
	// AWSProfile is the AWS Profile name to use as set in the credential file, if "" then will
	//  use the first / default set\
	awsProfile string
	// awsCredentialFile is the filename to use for AWS Credentials, if "" then will use
	//  the aws config file set in user's home dir
	awsCredentialFile string

	// awsSessionTTL is the max duration to keep a session alive for, defaults to 1 minute
	awsSessionTTL = time.Minute

	// awsDynamodbMaxTimeout is the maximum time duration that an API call can run before timer out
	awsDynamodbMaxTimeout = time.Minute * 5

	awsDefaultRCUs = 10
	awsDefaultWCUs = 10
)
