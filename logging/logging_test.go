package logging

import (
	"bufio"
	"fmt"
	"git-codecommit.us-east-1.amazonaws.com/v1/repos/dyno.git"
	"github.com/aws/aws-sdk-go/aws"
	awsSession "github.com/aws/aws-sdk-go/aws/session"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestFileWriterHook(t *testing.T) {
	f, err := os.OpenFile("./test.log", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	assert.NoError(t, err)
	fileHook := NewWriter(f, DEBUG, INFO, WARNING)

	log := New(fileHook)

	log.WithFields(logrus.Fields{
		"animal": "walrus",
		"size":   10,
	}).Info("A group of walrus emerges from the ocean")

	err = f.Close()
	assert.NoError(t, err)

	f, _ = os.Open("./test.log")
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		assert.Greater(t, len(scanner.Text()), 0)
		fmt.Println(">", scanner.Text())
	}
	f.Close()
	os.Remove("./test.log")
}

func TestCloudWatchLogging(t *testing.T) {
	// create the session
	awsSess, err := awsSession.NewSessionWithOptions(awsSession.Options{
		Config: aws.Config{
			Region: dyno.StringPtr("us-east-1"),
		},
		Profile:           "mt2_dev",
		SharedConfigState: awsSession.SharedConfigEnable,
	})
	assert.NoError(t, err)

	cwWriter := NewWriter(NewCloudWatchLogWriter(awsSess, "multitouch", "testing"),
		DEBUG, ERROR, INFO, WARNING).SetFormat(&logrus.JSONFormatter{})

	log := New()
	log.AddHook(cwWriter)

	log.WithFields(logrus.Fields{
		"animal": "walrus",
		"size":   10,
	}).Info("A group of walrus emerges from the ocean")
}
