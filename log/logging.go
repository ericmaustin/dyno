/*
logging is an extension to github.com/sirupsen/logrus that includes a cloudwatch logs logger
 */
package log

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/sirupsen/logrus"
	"io"
	"sync"
	"time"
)

const (
	TRACE   = logrus.TraceLevel
	DEBUG   = logrus.DebugLevel
	INFO    = logrus.InfoLevel
	WARNING = logrus.WarnLevel
	ERROR   = logrus.ErrorLevel
	FATAL   = logrus.FatalLevel
	PANIC   = logrus.PanicLevel
)

// StandardLogger is used in place of a standard logger instance to ensure compatibility with any logger
type StandardLogger interface {
	Debug(...interface{})
	Info(...interface{})
	Warn(...interface{})
	Error(...interface{})
	Fatal(...interface{})
	Panic(...interface{})
	Debugf(string, ...interface{})
	Infof(string, ...interface{})
	Warnf(string, ...interface{})
	Errorf(string, ...interface{})
	Fatalf(string, ...interface{})
	Panicf(string, ...interface{})
}

// Logrus allows passing of a logger or entry
type Logrus interface {
	StandardLogger
	WithFields(fields logrus.Fields) *logrus.Entry
	WithField(key string, value interface{}) *logrus.Entry
}

/*
Writer represents a single Logger handler that implements the logrus hook interface
*/
type Writer struct {
	Writer    io.Writer `validate:"required"`
	LogLevels []logrus.Level
	Format    logrus.Formatter
}

// Levels returns the log levels for this Writer. If none were set, the default levels are passed
// which includes all levels
func (w *Writer) Levels() []logrus.Level {
	if w.LogLevels == nil || len(w.LogLevels) == 0 {
		w.LogLevels = []logrus.Level{TRACE, DEBUG, INFO, WARNING, ERROR, FATAL, PANIC}
	}
	return w.LogLevels
}

// Fire will format the text and then write to the writer
func (w *Writer) Fire(entry *logrus.Entry) error {
	if w.Format == nil {
		w.Format = &logrus.TextFormatter{
			FullTimestamp: true,
		}
	}
	bt, err := w.Format.Format(entry)
	if err != nil {
		return err
	}
	// write the formatted output
	_, err = w.Writer.Write(bt)
	return err
}

// Add Levels adds levels to this handler
func (w *Writer) AddLevels(levels ...logrus.Level) *Writer {
	if w.LogLevels == nil {
		w.LogLevels = levels
	} else {
		w.LogLevels = append(w.LogLevels, levels...)
	}
	return w
}

// NewWriter returns a new handler with the required writer and log level
func NewWriter(writer io.Writer, levels ...logrus.Level) *Writer {
	return &Writer{
		Writer:    writer,
		LogLevels: levels,
	}
}

// SetFormat sets the logging handler's formatter as a string
func (w *Writer) SetFormat(format logrus.Formatter) *Writer {
	w.Format = format
	return w
}

// New returns a new logrus logger and adds all the hooks
func New(hooks ...logrus.Hook) (logger *logrus.Logger) {

	logger = logrus.New()
	logger.SetReportCaller(true)

	// add all the hooks, if any as hooks to this log
	for _, writer := range hooks {
		logger.AddHook(writer)
	}
	return
}

// CloudWatchLogWriter used as a writer for putting events into cloudwatch logs
type CloudWatchLogWriter struct {
	GroupName  *string
	StreamName *string
	KMSKeyId   *string
	GroupTags  map[string]*string
	srv        *cloudwatchlogs.CloudWatchLogs
	seq        *string
	mu         *sync.Mutex
}

// NewCloudWatchLogWriter creates a new CloudWatchLogWriter with given aws session, group name
// and stream name
func NewCloudWatchLogWriter(sess *session.Session, groupName, streamName string) *CloudWatchLogWriter {
	return &CloudWatchLogWriter{
		GroupName:  &groupName,
		StreamName: &streamName,
		srv:        cloudwatchlogs.New(sess),
		mu:         &sync.Mutex{},
	}
}

// SetKMSKeyID sets the KMS key Id for this CloudWatchLogWriter
func (c *CloudWatchLogWriter) SetKMSKeyID(kmsKey string) *CloudWatchLogWriter {
	c.KMSKeyId = &kmsKey
	return c
}

// AddTags adds tags to the cloudwatch group
func (c *CloudWatchLogWriter) AddTags(tags map[string]string) *CloudWatchLogWriter {
	if c.GroupTags == nil {
		c.GroupTags = map[string]*string{}
	}

	for name, tag := range tags {
		c.GroupTags[name] = &tag
	}

	return c
}

func (c *CloudWatchLogWriter) putEvent (event *cloudwatchlogs.InputLogEvent, seq *string) (*cloudwatchlogs.PutLogEventsOutput, error) {
	return c.srv.PutLogEvents(&cloudwatchlogs.PutLogEventsInput{
		LogEvents:     []*cloudwatchlogs.InputLogEvent{event},
		LogGroupName:  c.GroupName,
		LogStreamName: c.StreamName,
		SequenceToken: seq,
	})
}

// Write puts new event into cloudwatch
func (c *CloudWatchLogWriter) Write(p []byte) (n int, err error) {
	n = 0
	c.mu.Lock()

	defer func() {
		c.mu.Unlock()
		if err != nil {
			fmt.Printf("CloudWatchLogWriter got error: %v\n", err)
		}
	}()

	msg := string(p)
	ts := time.Now().UnixNano() / int64(time.Millisecond)
	event := &cloudwatchlogs.InputLogEvent{
		Message:   &msg,
		Timestamp: &ts,
	}
	// getSeqToken gets the sequence token from the log stream
	getSeqToken := func() (*string, error) {
		// describe the event stream
		cwDescribeInput := &cloudwatchlogs.DescribeLogStreamsInput{
			LogGroupName:        c.GroupName,
			LogStreamNamePrefix: c.StreamName,
		}
		streamDesc, err := c.srv.DescribeLogStreams(cwDescribeInput.SetLimit(1))
		if err != nil {
			return nil, err
		}
		if len(streamDesc.LogStreams) < 1 {
			return nil, nil
		}
		return streamDesc.LogStreams[0].UploadSequenceToken, nil
	}
	// check if seq is not set
	if c.seq == nil || len(*c.seq) < 1 {
		// get the sequence token from the stream
		c.seq, err = getSeqToken()
		if err != nil {
			return
		}
	}

	// put the event
	res, err := c.putEvent(event, c.seq)
	if err != nil {
		awsErr, ok := err.(awserr.Error)
		if !ok {
			return
		}
		switch awsErr.Code() {
		// the group or stream was not found
		case cloudwatchlogs.ErrCodeResourceNotFoundException:

			createStreamInput := &cloudwatchlogs.CreateLogStreamInput{
				LogGroupName:  c.GroupName,
				LogStreamName: c.StreamName,
			}

			// attempt to create the missing stream
			_, err = c.srv.CreateLogStream(createStreamInput)

			if err != nil {
				createStreamResponseErr, ok := err.(awserr.Error)
				if !ok {
					return 0, err
				}
				switch createStreamResponseErr.Code() {
				case cloudwatchlogs.ErrCodeResourceNotFoundException:
					// attempt to create the missing cloudwatch log group
					_, err = c.srv.CreateLogGroup(&cloudwatchlogs.CreateLogGroupInput{
						KmsKeyId:     c.KMSKeyId,
						LogGroupName: c.GroupName,
						Tags:         c.GroupTags,
					})
					if err != nil {
						return 0, nil
					}
					// now try adding the stream again
					_, err = c.srv.CreateLogStream(createStreamInput)
					if err != nil {
						return 0, nil
					}
				default:
					err = createStreamResponseErr
					return
				}
			}
		case cloudwatchlogs.ErrCodeInvalidSequenceTokenException:
			// describe
			c.seq, err = getSeqToken()
			if err != nil {
				return
			}
		default:
			err = awsErr
			return
		}

		// attempt to put the log event again
		res, err = c.putEvent(event, c.seq)

		if err != nil {
			return
		}
	}

	c.seq = res.NextSequenceToken

	if res.RejectedLogEventsInfo != nil {
		// try again
		_, err = c.putEvent(event, c.seq)
		if err != nil {
			return
		}
	}

	n = len(p)
	return
}
