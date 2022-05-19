package event

import (
	ev "github.com/GoGraph/event"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/util"
)

// Attach Node Event

type AttachNode struct {
	*ev.Event
}

func NewAttachNode(puid, cuid util.UID, sortk string) *AttachNode {

	an := &AttachNode{} //cuid: cuid, puid: puid, sortk: sortk}
	an.Event = ev.New("Attach")

	// add following attributes to base mutation (first entry in active batch of mutations)	.
	// alternative we could create a new mutation, but that increases the number of IOs and therefore CUs required for logging the event.
	m, _ := an.GetMutation(0)
	m.AddMember("cuid", cuid).AddMember("puid", puid).AddMember("sortk", sortk).AddMember("event", "attach")

	return an
}

type DetachNode struct {
	*ev.Event
	cuid  []byte
	puid  []byte
	sortk string
}

func NewDetachNode(puid, cuid util.UID, sortk string) *DetachNode {

	dn := &DetachNode{} //cuid: cuid, puid: puid, sortk: sortk}
	dn.Event = ev.New("Detach")
	dn.Add(dn.NewMutation(tbl.Event, 1).AddMember("cuid", cuid).AddMember("puid", puid).AddMember("sortk", sortk))

	return dn
}
