package throttleSrv

import (
	"context"
	"sync"

	"github.com/ros2hp/method-db/log"
	"github.com/ros2hp/method-db/throttle"
)

func alertlog(s string) {
	log.LogAlert("dbThrottle " + s)
}

func Down() {
	DownCh <- struct{}{}
}

func Up() {
	UpCh <- struct{}{}
}

var (
	DownCh chan struct{}
	UpCh   chan struct{}
)

func PowerOn(ctx context.Context, wpStart *sync.WaitGroup, ctxEnd *sync.WaitGroup, appThrottle throttle.Throttler) {

	defer ctxEnd.Done()

	DownCh = make(chan struct{})
	UpCh = make(chan struct{})

	ctxSnap, cancelSnap := context.WithCancel(context.Background())
	var (
		wgSnap  sync.WaitGroup
		wgStart sync.WaitGroup
		//

	)
	wgStart.Add(1)
	wgSnap.Add(1)

	//
	go func() {
		wgStart.Done()
		defer wgSnap.Done()
		// wait for grmgr to start for loop
		wpStart.Wait()
		alertlog("report-snapshot started. (NOT YET CONFIGURED TO DO ANYTING")
		for {
			select {
			// case t := <-time.After(time.Duration(snapInterval) * time.Second):
			// 	snapCh <- t
			case <-ctxSnap.Done():
				alertlog("report-snapshot shutdown.")
				return
			}
		}

	}()

	alertlog("Waiting for gr monitor to start...")
	// wait for snap interrupter to start
	wgStart.Wait()
	alertlog("Fully powered up...")

	wpStart.Done()

	for {

		select {

		case <-DownCh:

			appThrottle.Down()

		case <-UpCh:

			appThrottle.Up()

		case <-ctx.Done():
			alertlog("Received cancel order....shutting down...")
			cancelSnap()
			alertlog("Waiting for gr internal monitoring service to stop...")
			wgSnap.Wait()
			alertlog("gr throttle monitor shutdown")
			// shutdown any goroutines that are started above...
			return

		}
	}
}
