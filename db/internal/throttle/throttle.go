package throttle

import (
	"context"
	"fmt"
	"sync"
	"time"

	param "github.com/GoGraph/dygparam"
	elog "github.com/GoGraph/errlog"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/uuid"
)

const logid = "dbThrottle"

func syslog(s string) {
	slog.Log(logid, s)
}

func alertlog(s string) {
	slog.LogAlert(logid, s)
}

func errlog(s string) {
	slog.LogErr(logid, s)
}

func PowerOn(ctx context.Context, wpStart *sync.WaitGroup, wgEnd *sync.WaitGroup, runId uuid.UID) {

	defer wgEnd.Done()

	DownCh = make(chan db.Throttle)
	UpCh = make(chan db.Throttle)

	//
	go func() {
		wgStart.Done()
		defer wgSnap.Done()
		// wait for grmgr to start for loop
		wpStart.Wait()
		slog.Log(logid, "Report-snapshot Powering up...")
		for {
			select {
			case t := <-time.After(time.Duration(snapInterval) * time.Second):
				snapCh <- t
			case <-ctxSnap.Done():
				slog.Log(logid, "Report-snapshot Shutdown.")
				return
			}
		}

	}()

	slog.Log(logid, "Waiting for gr monitor to start...")
	// wait for snap interrupter to start
	wgStart.Wait()
	slog.Log(logid, "Fully powered up...")
	wpStart.Done()

	for {

		select {

		case appThrottle := <-DownCh:

			t0 := time.Now()

			if t0.Sub(throttleDownActioned) < v.hold {
				alertlog("throttleDown: to soon to throttle down after last throttled action")
			} else {

				if t0.Sub(throttleUpActioned) < v.hold {
					alertlog("throttleDown: to soon to throttle down after last throttled action")
				} else {

					appThrottle.Down()

					throttleDownActioned = t0
				}
			}

		case appThrottle := <-UpCh:

			t0 := time.Now()

			if t0.Sub(throttleDownActioned) < v.hold {
				alertlog("throttleDown: to soon to throttle down after last throttled action")
			} else {

				if t0.Sub(throttleUpActioned) < v.hold {
					alertlog("throttleDown: to soon to throttle down after last throttled action")
				} else {

					appThrottle.Down()

					throttleUpActioned = t0
				}
			}

		case <-ctx.Done():
			// shutdown any goroutines that are started above...
			return

		}
	}
}
