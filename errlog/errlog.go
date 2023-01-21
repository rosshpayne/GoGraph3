package errlog

import (
	"context"
	"fmt"
	"strings"
	"sync"
	//"github.com/GoGraph/run" removed as it caused import cycle error
	slog "github.com/GoGraph/syslog"
)

type Errors_ []*payload

type payload struct {
	Id  string
	Err error
}

const (
	logid = "errlog"
)

var (
	addCh        chan *payload
	ListCh       chan error
	ClearCh      chan struct{}
	checkLimit   chan chan bool
	RequestCh    chan Errors_
	PrintCh      chan chan struct{}
	ErrCntByIdCh chan string
	ErrCntRespCh chan int
	ResetCntCh   chan string
	ErrCntCh     chan chan int
)

func CheckLimit(lc chan bool) bool {
	c := <-lc
	return c
}

// Add multiple errors (atleast one err) grouped under a logid to add channel
func Add(logid string, err ...error) {

	if len(err) == 0 {
		panic(fmt.Errorf("elog Add had no second (error) argument"))
	}

	logid = strings.TrimRight(logid, " :")

	for _, e := range err {
		addCh <- &payload{logid, e}
	}
}

func PrintErrors() {
	respCh := make(chan struct{})
	PrintCh <- respCh
	<-respCh
}

func AsyncErrors_() bool {
	respCh := make(chan int)

	ErrCntCh <- respCh
	errs := <-respCh

	if errs > 0 {
		return true
	}
	return false
}

func RunErrored() bool {
	return AsyncErrors_()
}

func Errors() bool {
	return AsyncErrors_()
}

func PowerOn(ctx context.Context, wpStart *sync.WaitGroup, wgEnd *sync.WaitGroup) {

	defer wgEnd.Done()
	var (
		pld      *payload
		errors   Errors_
		errLimit = 5
		lc       chan bool
	)

	errCnt := make(map[string]int) // count errors by Id

	addCh = make(chan *payload)
	PrintCh = make(chan chan struct{})
	//	Add = make(chan error)
	ClearCh = make(chan struct{})
	checkLimit = make(chan chan bool)
	RequestCh = make(chan Errors_)
	ErrCntByIdCh = make(chan string)
	ErrCntRespCh = make(chan int)
	ErrCntCh = make(chan chan int)
	ResetCntCh = make(chan string)

	wpStart.Done()
	slog.LogAlert(logid, "Powering up...")

	for {

		select {

		case pld = <-addCh:

			var errmsg strings.Builder
			errmsg.WriteString(pld.Id)
			errmsg.WriteString(" Error: ")
			errmsg.WriteString(pld.Err.Error())
			// log to log file or CW logs
			slog.LogErr(pld.Id, pld.Err)

			errCnt[pld.Id]++
			errors = append(errors, pld)

			if len(errors) > errLimit {
				for _, e := range errors {
					fmt.Println(e.Err.Error())
				}
				//	run.Panic()
				panic(fmt.Errorf("Number of errors exceeds limit of %d", errLimit))
			}

		case lc = <-checkLimit:

			lc <- len(errors) > errLimit

		case id := <-ErrCntByIdCh:

			if c, ok := errCnt[id]; !ok {
				ErrCntRespCh <- -1
			} else {
				ErrCntRespCh <- c
			}

		case respCh := <-ErrCntCh:

			respCh <- len(errors)

		case id := <-ResetCntCh:

			if _, ok := errCnt[id]; ok {
				errCnt[id] = 0
			}

		case respch := <-PrintCh:

			slog.LogAlert(logid, fmt.Sprintf(" ==================== ERRORS : %d	==============", len(errors)))
			fmt.Printf(" ==================== ERRORS : %d	==============\n", len(errors))
			for _, e := range errors {
				slog.LogAlert(logid, fmt.Sprintf(" %s %s", e.Id, e.Err))
				fmt.Println(e.Id, e.Err)
			}

			respch <- struct{}{}

		case <-ctx.Done():
			slog.LogAlert(logid, "Shutdown.")
			return

		}
	}
}
