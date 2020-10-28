package logging

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	log "github.com/sirupsen/logrus"
	"io"
	"sync"
	"time"
)

const (
	logFormatter = `%{color}%{time:Jan _2 15:04:05.000} %{module}[%{pid}] %{level:.4s} ▶ (%{shortfile} %{longfunc})%{color:reset} %{message}`
	CWFormatter  = `{timestamp:"%{time:Jan _2 15:04:05.000}",level:"%{level:.4s}",pid:%{pid},file:"%{longfile}",mod:"%{module}",func:"%{longfunc}",message:"%{message}"}`
)

const (
	DEBUG   = log.DebugLevel
	INFO    = log.InfoLevel
	WARNING = log.WarnLevel
	ERROR   = log.ErrorLevel
	FATAL   = log.FatalLevel
	PANIC   = log.PanicLevel
)

/*
Writer represents a single Logger handler that implements the logrus hook interface
*/
type Writer struct {
	Writer    io.Writer `validate:"required"`
	LogLevels []log.Level
	Format    log.Formatter
}

func (w *Writer) Levels() []log.Level {
	if w.LogLevels == nil || len(w.LogLevels) == 0 {
		// default to all
		return []log.Level{DEBUG, INFO, WARNING, ERROR, FATAL, PANIC}
	}
	return w.LogLevels
}

// Fire will format the text and then write to the writer
func (w *Writer) Fire(entry *log.Entry) error {
	if w.Format == nil {
		w.Format = &log.TextFormatter{
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
func (w *Writer) AddLevels(levels ...log.Level) *Writer {
	if w.LogLevels == nil {
		w.LogLevels = levels
	} else {
		w.LogLevels = append(w.LogLevels, levels...)
	}
	return w
}

// NewWriter returns a new handler with the required writer and log level
func NewWriter(writer io.Writer, levels ...log.Level) *Writer {
	return &Writer{
		Writer:    writer,
		LogLevels: levels,
	}
}

// SetFormat sets the logging handler's formatter as a string
func (w *Writer) SetFormat(format log.Formatter) *Writer {
	w.Format = format
	return w
}

// New returns a new logrus logger and adds all the hooks
func New(hooks ...log.Hook) (logger *log.Logger) {

	logger = log.New()
	logger.SetReportCaller(true)

	// add all the hooks, if any as hooks to this log
	for _, writer := range hooks {
		logger.AddHook(writer)
	}
	return
}

type CloudWatchLogWriter struct {
	GroupName  *string
	StreamName *string
	KMSKeyId   *string
	GroupTags  map[string]*string
	srv        *cloudwatchlogs.CloudWatchLogs
	seq        *string
	mu         *sync.Mutex
}

func NewCloudWatchLogWriter(sess *session.Session, groupName, streamName string) *CloudWatchLogWriter {
	return &CloudWatchLogWriter{
		GroupName:  &groupName,
		StreamName: &streamName,
		srv:        cloudwatchlogs.New(sess),
		mu:         &sync.Mutex{},
	}
}

func (c *CloudWatchLogWriter) SetKMSKeyID(kmsKey string) *CloudWatchLogWriter {
	c.KMSKeyId = &kmsKey
	return c
}

func (c *CloudWatchLogWriter) AddTags(tags map[string]string) *CloudWatchLogWriter {
	if c.GroupTags == nil {
		c.GroupTags = map[string]*string{}
	}

	for name, tag := range tags {
		c.GroupTags[name] = &tag
	}

	return c
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
	// putEvent puts the cw log event
	putEvent := func(seq *string) (*cloudwatchlogs.PutLogEventsOutput, error) {
		return c.srv.PutLogEvents(&cloudwatchlogs.PutLogEventsInput{
			LogEvents:     []*cloudwatchlogs.InputLogEvent{event},
			LogGroupName:  c.GroupName,
			LogStreamName: c.StreamName,
			SequenceToken: seq,
		})
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
	res, err := putEvent(c.seq)
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
			// try again
			res, err = putEvent(c.seq)
		default:
			err = awsErr
			return
		}

		// attempt to put the log event again
		res, err = putEvent(c.seq)

		if err != nil {
			return
		}
	}

	c.seq = res.NextSequenceToken

	if res.RejectedLogEventsInfo != nil {
		// try again
		res, err = putEvent(c.seq)
		if err != nil {
			return
		}
	}

	n = len(p)
	return
}
