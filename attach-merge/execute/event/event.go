package event

import (
	"context"

	ev "github.com/GoGraph/event"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/uuid"
)

// Attach Node Event

type AttachNode struct {
	*ev.Event
}

func NewAttachNode(puid uuid.UID, edges int) *AttachNode {

	an := &AttachNode{}              //cuid: cuid, puid: puid, sortk: sortk}
	an.Event = ev.New("AttachEdges") //"EV$event"

	m := mut.NewInsert(tbl.Name("EV$edge")).AddMember("EvID", an.EID).AddMember("Puid", puid).AddMember("Edges", edges)
	an.Add(m) // add mutation to underlying event transaction.

	return an
}

func NewAttachNodeContext(ctx context.Context, puid uuid.UID, edges int) *AttachNode {

	an := &AttachNode{}                          //cuid: cuid, puid: puid, sortk: sortk}
	an.Event = ev.NewContext(ctx, "AttachEdges") //"EV$event"

	m := mut.NewInsert(tbl.Name("EV$edge")).AddMember("EvID", an.EID).AddMember("Puid", puid).AddMember("Edges", edges)
	an.Add(m) // add mutation to underlying event transaction.

	return an
}

type DetachNode struct {
	*ev.Event
	cuid  []byte
	puid  []byte
	sortk string
}

func NewDetachNode(puid, cuid uuid.UID, sortk string) *DetachNode {

	dn := &DetachNode{} //cuid: cuid, puid: puid, sortk: sortk}
	dn.Event = ev.New("DetachEdges")
	dn.Add(dn.NewMutation(tbl.Event, 1).AddMember("cuid", cuid).AddMember("puid", puid).AddMember("sortk", sortk))

	return dn
}
