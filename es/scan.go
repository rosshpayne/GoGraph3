package main

import (
	"context"

	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx"
	"github.com/GoGraph/uuid"
)

// type rec struct {
// 	PKey  []byte `spanner:"PKey"`
// 	Ty    string `spanner:"Ty"`
// 	Value string `spanner:"S"`
// }

// type batch struct {
// 	Eod       bool // End-of-data
// 	FetchCh   chan *rec
// 	BatchCh   chan batch
// 	LoadAckCh chan struct{}
// }

//table: ftlog (bid, pKey, p) - process bid's worth at a time. No eslog table as ftlog keeps state. Bid is logged to state table
// populate ftlog usig SQL below increment bid after 60 rows . at worst 60 rows need to be reprocessed ie. loaded into ES, which will simply overwrite existing entry
//go db.ScanForESattrs(tysn, sk, FetchCh)

// func NewBatch() batch {
// 	return batch{FetchCh: make(chan *rec), BatchCh: make(chan batch), LoadAckCh: make(chan struct{})}
// }

// Alternate design:

// * scan P_S index with Limit set of 500, starting at  beginning of table or lastkey.
//  keep scanning  into local slice until reaches between 500 and 1000

// * load batch  records found into ES

// * if successfully loaded, record lastkey to state table (in case of restart)

// * go to beginning.

// I like this as the state data is minimial, scan can be parallelised if necessary.
// Need to implement Dynamodb Scan into tx.query.

func Scan(ctx context.Context, batch *int, ty string, sk string, fetchCh chan<- rec, id uuid.UID, restart bool) {

	var (
		err error
		pks []rec
	)

	// State( arg1: id used by tx.Execute to save stateVal to in its State tbl. id is es.runid.
	//        arg2: (optional), restart bool. Restart=True forces Execute to read State val from table. False: no restart, no rrequirement to read from State tbl.
	ptx := tx.NewQuery2(ctx, "label", tbl.TblName, "TyES") //"TyES").Parallel(4)

	//ptx.Select(&pks).Filter("Ty", ty).Filter("SortK", sk).Limit(*batch).Paginate(id, restart)
	ptx.Select(&pks).Key("Ty", ty).Filter("SortK", sk).Limit(*batch).Paginate(id, restart)

	for !ptx.EOD() {

		err = ptx.Execute() // get next page of data

		if err != nil {
			panic(err)
		}

		for _, v := range pks {
			fetchCh <- v
		}
	}
	close(fetchCh)
}
