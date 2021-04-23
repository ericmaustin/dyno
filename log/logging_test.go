package log

import (
	"bufio"
	"context"
	"fmt"
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
	_ = f.Close()
	_ = os.Remove("./test.log")
}

func TestCloudWatchLogging(t *testing.T) {
	// create the session
	awsSess, err := awsSession.NewSession()
	assert.NoError(t, err)

	cwWriter := NewWriter(NewCWLogWriter(context.Background(), awsSess, "testgroup", "testing"),
		DEBUG, ERROR, INFO, WARNING).SetFormat(&logrus.JSONFormatter{})

	log := New()
	log.AddHook(cwWriter)

	log.WithFields(logrus.Fields{
		"animal": "walrus",
		"size":   10,
	}).Info("A group of walrus emerges from the ocean")
}
