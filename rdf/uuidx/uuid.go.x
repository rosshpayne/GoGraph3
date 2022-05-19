package uuid

import (
	"context"
	"sync"

	param "github.com/GoGraph/dygparam"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/util"
)

type ndAlias = string // rdf blank-node-id e.g. _:a subject entry in rdf file
type nodeMap map[ndAlias]util.UID

var (
	nodeUID nodeMap
	ReqCh   chan Request
	SaveCh  chan Key
	RespCh  chan util.UID
)

func init() {
	// maps
	nodeUID = make(nodeMap)
	// channels - no buffers as must be synchronised
	ReqCh = make(chan Request)
	SaveCh = make(chan Key)
	RespCh = make(chan util.UID)

}

type Request struct {
	SName        ndAlias
	SuppliedID   string   // (optional) supplied PKEY ID. Not implemented as code presumes PKEY is a UUID.// TODO:  Need more investigation to implement
	SuppliedUUID util.UID // (optional) supplied UUID of node
	RespCh       chan util.UID
}

type Key struct {
	SName ndAlias
	UID   util.UID
}

func PowerOn(ctx context.Context, wp *sync.WaitGroup, wgEnd *sync.WaitGroup) {

	defer wgEnd.Done()
	wp.Done()

	var (
		ok  bool
		req Request
		uid util.UID
	)

	slog.Log(param.Logid, "uuid: Powering up...")

	for {

		select {

		case req = <-ReqCh:

			//slog.Log("rdfuuid: ", fmt.Sprintf("Request received.. %#v", req))
			if uid, ok = nodeUID[req.SName]; !ok {
				// save ID or generate a UUID and save to map
				if req.SName == "__" {
					// for dummy uid-pred entry - when node is first created
					uid = []byte(req.SName)
				} else {
					// if len(req.SuppiedID) > 0 {
					// 	uid = util.UID(req.SuppiedID)
					// } else
					if len(req.SuppliedUUID) > 0 {
						uid = req.SuppliedUUID // as sourced from s-p-o where p="__ID" (converted to UUID from base64 UID string)
					} else {
						// generate a UUID
						uid, _ = util.MakeUID()
					}

				}
				nodeUID[req.SName] = uid
			}

			req.RespCh <- uid

		case <-ctx.Done():

			slog.Log(param.Logid, "uuid: Powering down...")
			return

		}
	}
}
