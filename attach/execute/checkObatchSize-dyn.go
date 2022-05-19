//go:build dynamodb
// +build dynamodb

package execute

import (
	"fmt"
	"time"

	blk "github.com/GoGraph/block"
	"github.com/GoGraph/dbs"
	param "github.com/GoGraph/dygparam"
	"github.com/GoGraph/errlog"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/util"
)

type ASZErr struct {
	key util.UID
	sk  string
	e   error
}

func (a ASZErr) Error() string {
	return fmt.Sprintf("Error in checkOBatchSizeLimitReached for TUID %s, SortK: %s, %s", a.key, a.sk, a.e)
}

func (a ASZErr) Unwrap() error {
	return a.e
}

func checkOBatchSizeLimitReached(cUID util.UID, py *blk.ChPayload) dbs.Mutation {

	// Dynamodb version

	// query to get ASZ from overflow entry
	type Size struct {
		ASZ int64
	}
	// for Dynamodb create a var of struct or []struct.  A slice will perform a Query whereas a struct will perform a GetItem
	// Use reflect to generate Projection from struct fields and UnmarshalMap or UnmarshalListOfMaps based on slice or struct type of input.
	// for SQL, use reflect to generate select list from struct fields and use Input to generate Where clause.
	// Again use the REturn type, slice or struct to determine whether to use single select or multi-row select API.
	var asz Size

	fmt.Println("checkOBatchSizeLimitReached: ", py.TUID, py.Osortk)

	// Filter(name) not shown
	qry, _ := tx.NewQuery(tbl.EOP, "ASZ")
	qry.Select(&asz).Key("PKey", py.TUID).Key("SortK", py.Osortk).Consistent(true)

	err := qry.Execute()
	if err != nil {
		err := ASZErr{py.TUID, py.Osortk, err}
		errlog.Add("checkOBatchSizeLimitReached query", err)
		time.Sleep(2 * time.Second)
		panic(err)
	}
	if param.OvfwBatchSize == asz.ASZ-2 {
		// update XF in UID-Pred (in parent node block)
		xf := py.DI.XF
		xf[py.NdIndex] = blk.OBatchSizeLimit //param.OvfwBatchSize
		return mut.NewUpdate(tbl.EOP).AddMember("PKey", py.DI.Pkey, mut.IsKey).AddMember("SortK", py.DI.GetSortK(), mut.IsKey).AddMember("XF", xf, mut.Set)
	}

	return nil

}
