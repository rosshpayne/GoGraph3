package errlog

import (
	"context"
	"fmt"
	"strings"
	"sync"

	//"github.com/GoGraph/run" removed as it caused import cycle error
	slog "github.com/GoGraph/syslog"
)

type Errors []*payload

type payload struct {
	Id  string
	Err error
}

const (
	logid = "errlog: "
)

var (
	addCh        chan *payload
	ListCh       chan error
	ClearCh      chan struct{}
	checkLimit   chan chan bool
	RequestCh    chan Errors
	PrintCh      chan chan struct{}
	ErrCntByIdCh chan string
	ErrCntRespCh chan int
	ResetCntCh   chan string
)

func CheckLimit(lc chan bool) bool {
	c := <-lc
	return c
}

func Add(logid string, err error) {

	logid = strings.TrimRight(logid, " :")

	addCh <- &payload{logid, err}
}

func PrintErrors() {
	respCh := make(chan struct{})
	PrintCh <- respCh
	<-respCh
}

func RunErrored() bool {
	respCh := make(chan struct{})
	PrintCh <- respCh
	<-respCh
	errs := <-RequestCh

	if len(errs) > 0 {
		return true
	}
	return false
}

func PowerOn(ctx context.Context, wpStart *sync.WaitGroup, wgEnd *sync.WaitGroup) {

	defer wgEnd.Done()
	var (
		pld      *payload
		errors   Errors
		errLimit = 25
		lc       chan bool
	)

	errCnt := make(map[string]int) // count errors by Id

	addCh = make(chan *payload)
	PrintCh = make(chan chan struct{})
	//	Add = make(chan error)
	ClearCh = make(chan struct{})
	checkLimit = make(chan chan bool)
	RequestCh = make(chan Errors)
	ErrCntByIdCh = make(chan string)
	ErrCntRespCh = make(chan int)
	ResetCntCh = make(chan string)

	wpStart.Done()
	slog.Log(logid, "Powering up...")

	for {

		select {

		case pld = <-addCh:
			fmt.Println("errlog Add ...")
			var errmsg strings.Builder
			errmsg.WriteString(pld.Id)
			errmsg.WriteString(" ")
			errmsg.WriteString(pld.Err.Error())
			// log to log file or CW logs
			slog.LogErr(pld.Id, errmsg.String())

			errCnt[pld.Id] += 1
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

		case id := <-ResetCntCh:

			if _, ok := errCnt[id]; ok {
				errCnt[id] = 0
			}

		case respch := <-PrintCh:

			slog.LogErr(logid, fmt.Sprintf(" ==================== ERRORS : %d	==============", len(errors)))
			fmt.Printf(" ==================== ERRORS : %d	==============\n", len(errors))
			for _, e := range errors {
				slog.LogErr(logid, fmt.Sprintf(" %s %s", e.Id, e.Err))
				fmt.Println(e.Id, e.Err)
			}
			respch <- struct{}{}

		case <-ctx.Done():
			slog.Log(logid, "Shutdown.")
			return

		}
	}
}
