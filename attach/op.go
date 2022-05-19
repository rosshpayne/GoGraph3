package main

import (
	"time"

	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/types"
	"github.com/GoGraph/util"
)

type AttachOp struct {
	Puid, Cuid util.UID
	Sortk      string
	Bid        int
}

var t0, t1 time.Time

// Status at Start of operation attach - for long running operations this function sets the status flag to "running" typically
// for shot operations thise is not much value.
// TODO: maybe some value in merging functionality of the event and op (operation) pacakages
func (a *AttachOp) Start() []*mut.Mutation {
	return nil
}

// StatusEnd - set the task status to completed or errored.
func (a *AttachOp) End(err error) []*mut.Mutation {

	sk := a.Sortk + "|" + a.Cuid.String()

	if err != nil {

		tblEdgeChild := tbl.EdgeChild + tbl.Name(types.GraphName())
		m := make([]*mut.Mutation, 1, 1)
		e := "E"
		msg := err.Error() // AddMember("Cuid", a.Cuid, mut.IsKey)
		m[0] = mut.NewUpdate(tblEdgeChild).AddMember("Puid", a.Puid, mut.IsKey).AddMember("SortK_Cuid", sk, mut.IsKey).AddMember("Status", e).AddMember("ErrMsg", msg)
		return m

	} else {

		tblEdge := tbl.Edge + tbl.Name(types.GraphName())
		tblEdgeChild := tbl.EdgeChild + tbl.Name(types.GraphName())
		m := make([]*mut.Mutation, 2, 2)
		m[0] = mut.NewUpdate(tbl.Name(tblEdge)).AddMember("Bid", a.Bid, mut.IsKey).AddMember("Puid", a.Puid, mut.IsKey).AddMember("Cnt", 1, mut.Subtract)
		//m[1] = mut.NewMutation(tbl.Name(tblEdgeChild), a.Puid, sk, mut.Update).AddMember("Status", "A")
		// remove status attribute will delete item from index as it is nolonger needed. Alternative to changing Status value.
		m[1] = mut.NewUpdate(tblEdgeChild).AddMember("Puid", a.Puid, mut.IsKey).AddMember("SortK_Cuid", sk, mut.IsKey).AddMember("Status", nil, mut.Remove)
		return m
	}

}
