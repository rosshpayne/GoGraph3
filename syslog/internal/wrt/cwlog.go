//go:build cwlog
// +build cwlog

package wrt

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	param "github.com/GoGraph/dygparam"
	"github.com/GoGraph/stats"
	"github.com/GoGraph/syslog/internal/wrt/dbuf"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	cwlogs "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
)

//func (c *Client) PutLogEvents(ctx context.Context, params *PutLogEventsInput, optFns ...func(*Options)) (*PutLogEventsOutput, error)

//type PutLogEventsInput struct {
//		LogEvents []types.InputLogEvent
//		LogGroupName *string
//		LogStreamName *string
//		SequenceToken *string
//}

// type InputLogEvent struct {
// 		Message *string
// 		Timestamp *int64
// }

type cwLog byte

// implement IO.Writer interface
func (b cwLog) Write(p []byte) (i int, err error) {

	s := string(p)
	t := time.Now().UnixMilli()

	logCh <- &types.InputLogEvent{Message: &s, Timestamp: &t}

	return len(s), nil
}

type cwLogChT chan *types.InputLogEvent

func (c cwLogChT) Write(p []byte) (i int, err error) {

	s := string(p)
	t := time.Now().UnixMilli()

	c <- &types.InputLogEvent{Message: &s, Timestamp: &t}

	return len(s), nil
}

var (
	uploadInterval = 2
	lastUpload     time.Time
	logStream      *string
	client         *cwlogs.Client
	//
	newLogStream bool
	logGroup     string
	//
	firstTimeCh  chan bool
	closeFirstCh chan struct{}
	uploadCh     chan struct{}
	errorCh      chan error
	logCh        chan *types.InputLogEvent
	setSeqCh     chan *string
	//
	cancel         context.CancelFunc
	ctx            context.Context
	wgStart, wgEnd sync.WaitGroup
	// fileLogr supports cwlogger errors
	fileLogr *log.Logger
	//
	EOD int64 = -1
)

// func init() {
// 	stats.Register(stats.EvAPI, "upload", "PutLogEvents", "upload()")
//  stats.Register(stats.WaitCh,"uploadCh", uploadCh)
//  stats.Register(stats.Event,"CWLogs")
// }

func New() io.Writer {
	// initialise write-to-buffer index used for doublebuffer. swap() will change w between the two buffers (indexes A and B).
	// upload() will operate on whatever is the alternative buffer to w.
	var w cwLog
	return w
}

func newCWLogClient() *cwlogs.Client {

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		panic("configuration error, " + err.Error())
	}

	return cwlogs.NewFromConfig(cfg)
}

func createLogStream() error {
	// create a Log Stream
	var s strings.Builder
	s.WriteByte('/')
	s.WriteString(param.Environ)
	s.WriteByte('/')
	s.WriteString(param.RunId[:6])
	s.WriteString(".log")

	logStream = aws.String(s.String())

	_, err := client.CreateLogStream(context.Background(), &cwlogs.CreateLogStreamInput{LogGroupName: &logGroup, LogStreamName: logStream})
	return err
}

func Start(flogr *log.Logger) error {

	client = newCWLogClient()

	fileLogr = flogr

	ctx, cancel = context.WithCancel(context.Background())

	// register statistic labels
	dur, err := time.ParseDuration("500ms")
	if err != nil {
		panic(err)
	}
	stats.Register("PutLogEvents", dur)

	wgStart.Add(1)
	wgEnd.Add(1)

	go PowerOn(ctx, &wgStart, &wgEnd)

	wgStart.Wait()

	newLogStream = true

	logGroup = param.AppName + "-" + param.Environ

	return createLogStream()
}

func Stop() {

	// place EOD on logCh - which will empty logCh and then close down.
	logCh <- &types.InputLogEvent{Timestamp: &EOD}

	wgEnd.Wait()
}

// upload to Cloudwatch logs using PutLogEvents(). Communicate NextSequenceToken via channel setSeqCh.
// NB: upload is called only when there is log events to upload
func upload(b []types.InputLogEvent) {

	var seqid *string
	t0 := time.Now()

	// the very first time the upload happens the seqid must be set to nil.
	// Once set the channel is closed (issues zero value, false) thereby sourcing seqid from setSeqCh.
	first := <-firstTimeCh

	if first {
		seqid = nil
	} else {
		// get next log sequence from channel
		seqid = <-setSeqCh
	}

	plei := &cwlogs.PutLogEventsInput{LogEvents: b, LogGroupName: &logGroup, LogStreamName: logStream, SequenceToken: seqid}

	pleo, err := client.PutLogEvents(ctx, plei)
	if err != nil {
		errorCh <- fmt.Errorf("Error in PutLogEvents of CloudwaatchLogs %w", err)
		panic(fmt.Errorf("*** putlogevents errored: %w", err))
		return
	}

	if v := pleo.RejectedLogEventsInfo; v != nil {
		if v.ExpiredLogEventEndIndex != nil {
			fmt.Printf("Rejected: ExpiredLogEventEndIndex %d\n", *v.ExpiredLogEventEndIndex)
		}
		if v.TooNewLogEventStartIndex != nil {
			fmt.Printf("Rejected: TooNewLogEventStartIndex %d\n", *v.TooNewLogEventStartIndex)
		}
		if v.TooOldLogEventEndIndex != nil {
			fmt.Printf("Rejected: TooOldLogEventEndIndex %d\n", *v.TooOldLogEventEndIndex)
		}
	}

	setSeqCh <- pleo.NextSequenceToken
	stats.SaveEventStats(stats.EvAPI, time.Now().Sub(t0), "PutLogEvents")

	if first {
		closeFirstCh <- struct{}{}
	}

	// free any waiting uploads - channel with buffer 1 is used to serialise access to this routine.

	fmt.Println("** upload: send to uploadCh")
	t1 := time.Now()
	uploadCh <- struct{}{}
	fmt.Println("** upload: pass uploadCh")
	stats.SaveEventStats(stats.EvWaitSendOnCh, time.Now().Sub(t1), "uploadCh")

}

func PowerOn(ctx context.Context, wgStart *sync.WaitGroup, wgEnd *sync.WaitGroup) {

	defer wgEnd.Done()

	var (
		timedUpLoad chan struct{}
	)
	// make channels
	logCh = make(chan *types.InputLogEvent, param.LogChBufSize)
	timedUpLoad = make(chan struct{})
	setSeqCh = make(chan *string, 1)
	errorCh = make(chan error)
	closeFirstCh = make(chan struct{}, 1)

	// serialise execution of upload() using uploadCh
	uploadCh = make(chan struct{}, 1)
	// initialise channel
	uploadCh <- struct{}{}

	// start timed upload goroutine
	ctxTim, cancelTimed := context.WithCancel(context.Background())
	var TimedUpldEnd sync.WaitGroup
	var TimedUpldStart sync.WaitGroup
	TimedUpldEnd.Add(1)
	TimedUpldStart.Add(1)

	go func() {
		TimedUpldStart.Done()
		defer TimedUpldEnd.Done()
		// wait for main program loop to start
		wgStart.Wait()
		for {
			select {
			case <-time.After(time.Duration(uploadInterval) * time.Second):
				timedUpLoad <- struct{}{}
			case <-ctxTim.Done():
				return
			}
		}

	}()

	// wait for timed goroutine to start
	TimedUpldStart.Wait()
	lastUpload = time.Now()

	// serialise first time access to upload()
	firstTimeCh = make(chan bool, 1)
	firstTimeCh <- true

	wgStart.Done()

	evBuf := dbuf.New()

	for {

		select {

		case <-closeFirstCh:

			close(firstTimeCh)

		case ie := <-logCh:

			// check for EOD (end-of-data)
			if ie.Timestamp == &EOD {

				if evBuf.WriteBuf() > 0 {
					stats.RecvOnCh(uploadCh, "EOFUpload")
					evBuf.Swap()
					upload(evBuf.Read())
				}
				cancelTimed()
				TimedUpldEnd.Wait()

				return
			}

			if evBuf.Write(ie) == param.CWLogLoadSize {

				// serialise access to upload() via channel uploadCh.
				// receive on channel. For first time initiliased channel will mean there is no wait.
				// subsequent receives (wait) on channel will wait until currently executing upload() finishes when it sends on channel
				// stats is a wrapper that records the duration of the wait.
				stats.RecvOnCh(uploadCh, "FullBufUpload")

				evBuf.Swap()
				lastUpload = time.Now()

				go upload(evBuf.Read())

			}

		case <-timedUpLoad:

			if time.Now().Sub(lastUpload) > 1.8*1000000000 && evBuf.WriteBuf() > 0 {

				stats.RecvOnCh(uploadCh, "TimedUpload")

				evBuf.Swap()
				lastUpload = time.Now()

				go upload(evBuf.Read())

			}

		case e := <-errorCh:

			fileLogr.Print(e)

		case <-ctx.Done():
			cancelTimed()
			TimedUpldEnd.Wait()
			return

		}
	}
}
