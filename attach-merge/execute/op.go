package execute

import (
	"time"

	"github.com/GoGraph/dbs"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/uuid"
)

type AttachOp struct {
	Puid, Cuid uuid.UID
	Sortk      string
	Bid        int
}

var t0, t1 time.Time

// Status at Start of operation attach - for long running operations this function sets the status flag to "running" typically
// for shot operations thise is not much value.
// TODO: maybe some value in merging functionality of the event and op (operation) pacakages
func (a *AttachOp) Start() []dbs.Mutation {
	return nil
}

// StatusEnd - set the task status to completed or errored.
//func (a *AttachOp) End(err error) []*mut.Mutation {
func (a *AttachOp) Update(err error) []dbs.Mutation {

	sk := a.Sortk + "|" + string(a.Cuid.EncodeBase64())

	if err != nil {

		tblEdge, tblEdgeChild := tbl.GetEdgeNames()
		m := make([]dbs.Mutation, 2, 2)
		e := "E"
		msg := err.Error() // AddMember("Cuid", a.Cuid, mut.IsKey)
		m[0] = mut.NewMerge(tblEdgeChild).AddMember("Puid", a.Puid, mut.IsKey).AddMember("SortK_Cuid", sk, mut.IsKey).AddMember("Status", e).AddMember("ErrMsg", msg)
		m[1] = mut.NewMerge(tblEdge).AddMember("Bid", a.Bid, mut.IsKey).AddMember("Puid", a.Puid, mut.IsKey).AddMember("Status", e).AddMember("ErrMsg", msg)
		return m

	} else {
		_, tblEdgeChild := tbl.GetEdgeNames()

		//tblEdgeChild := tbl.EdgeChild + tbl.Name(types.GraphName())
		m := make([]dbs.Mutation, 0, 1)
		m = append(m, mut.NewUpdate(tblEdgeChild).AddMember("Puid", a.Puid, mut.IsKey).AddMember("SortK_Cuid", sk, mut.IsKey).AddMember("Status", nil, mut.Remove))
		return m
	}

}

// StatusEnd - set the task status to completed or errored.
//func (a *AttachOp) End(err error) []*mut.Mutation {
func (a *AttachOp) End(err error) []dbs.Mutation {

	sk := a.Sortk + "|" + string(a.Cuid.Base64())

	if err != nil {

		tblEdge, tblEdgeChild := tbl.GetEdgeNames()
		m := make([]dbs.Mutation, 2, 2)
		e := "E"
		msg := err.Error() // AddMember("Cuid", a.Cuid, mut.IsKey)
		m[0] = mut.NewMerge(tblEdgeChild).AddMember("Puid", a.Puid, mut.IsKey).AddMember("SortK_Cuid", sk, mut.IsKey).AddMember("Status", e).AddMember("ErrMsg", msg)
		m[1] = mut.NewMerge(tblEdge).AddMember("Bid", a.Bid, mut.IsKey).AddMember("Puid", a.Puid, mut.IsKey).AddMember("Status", e).AddMember("ErrMsg", msg)
		return m

	} else {

		tblEdge, _ := tbl.GetEdgeNames()

		//tblEdgeChild := tbl.EdgeChild + tbl.Name(types.GraphName())
		m := make([]dbs.Mutation, 1, 1)

		// Insert relies on NoSQL Put that insert or updates based on presense of key. SQL could use merge. Objective to set Cnt to 0
		//m[0] = mut.NewInsert(tblEdge).AddMember("Bid", a.Bid, mut.IsKey).AddMember("Puid", a.Puid, mut.IsKey).AddMember("Cnt", 0)

		// use merge to set Cnt to 0. This represents the commit unit i.e. either all child nodes get attached  or none.
		//m[0] = mut.NewMerge(tblEdge).AddMember("Bid", a.Bid, mut.IsKey).AddMember("Puid", a.Puid, mut.IsKey).AddMember("Cnt", 0)
		m[0] = mut.NewMerge(tblEdge).AddMember("Bid", a.Bid).AddMember("Puid", a.Puid).AddMember("Cnt", 0) // Test - not using IsKey
		return m
	}

}
