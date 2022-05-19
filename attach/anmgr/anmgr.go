package anmgr

import (
	"context"
	"sync"

	"github.com/GoGraph/attach/ds"
	slog "github.com/GoGraph/syslog"
)

const (
	LogLabel = "anmgr: "
)

type edgeKey struct {
	CuidS string
	PuidS string
	Sortk string
}

type attachRunningMap map[edgeKey]struct{} // set of running attachNodes
var attachRunning attachRunningMap

//type attachDoneMap map[EdgeSn]bool // set of completed attachNodes
var (
	attachDone   int //attachDoneMapA
	attachDoneCh chan *ds.Edge
	AttachNowCh  chan *ds.Edge
)

func AttachDone(e *ds.Edge) { //EdgeSn) {
	attachDoneCh <- e
}

// anmgr providesn synchonisation access to the attachRunning map.
// it offers a service to the attach node program and is used to determine if the selected edge
// can be attached at the current time by checking that the edge nodes are not currently involed in
// a node attachment process.
func PowerOn(ctx context.Context, wp *sync.WaitGroup, wgEnd *sync.WaitGroup) {

	defer wgEnd.Done()
	wp.Done()
	slog.Log(LogLabel, "Powering up...")

	var eKey edgeKey

	attachDoneCh = make(chan *ds.Edge)
	attachRunning = make(attachRunningMap)
	AttachNowCh = make(chan *ds.Edge)

	for {

		select {

		case e := <-attachDoneCh:

			eKey.CuidS, eKey.PuidS, eKey.Sortk = e.Cuid.String(), e.Puid.String(), e.Sortk
			delete(attachRunning, eKey)

		case e := <-AttachNowCh:

			eKey.CuidS, eKey.PuidS, eKey.Sortk = e.Cuid.String(), e.Puid.String(), e.Sortk
			//
			// detect for possible concurrency issues with running attachers - for this to work we need to be aware of when attachers have finished (ie. done)
			attach := true
			for r, _ := range attachRunning {
				// slog.Log(LogLabel, fmt.Sprintf("AttachRunning....%s %s %s", r.CuidS, r.PuidS, r.Sortk))
				// if new edge shares any edges with currently running attach jobs move onto next edge
				if eKey.CuidS == r.CuidS || eKey.PuidS == r.CuidS || eKey.CuidS == r.PuidS || eKey.PuidS == r.PuidS {
					attach = false
					break
				}
			}
			if attach {
				attachRunning[eKey] = struct{}{}
			}

			e.RespCh <- attach

		case <-ctx.Done():
			slog.Log(LogLabel, "Shutdown.")
			return

		}
	}
}
