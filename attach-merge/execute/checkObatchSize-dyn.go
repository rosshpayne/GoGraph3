//go:build dynamodb
// +build dynamodb

package execute

import (
	"fmt"

	blk "github.com/GoGraph/block"
	param "github.com/GoGraph/dygparam"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/uuid"
)

type ASZErr struct {
	key uuid.UID
	sk  string
	e   error
}

func (a ASZErr) Error() string {
	return fmt.Sprintf("Error in checkOBatchSizeLimitReached for TUID %s, SortK: %s, %s", a.key, a.sk, a.e)
}

func (a ASZErr) Unwrap() error {
	return a.e
}

func checkOBatchSizeLimitReached(ctx *tx.Handle, cUID uuid.UID, py *blk.ChPayload) error {

	//fmt.Printf("checkOBatchSizeLimitReached: TUID: %s OsortK: %s\n", py.TUID, py.Osortk)
	// find source mutation
	m := ctx.FindSourceMutation(tbl.EOP, py.TUID, py.Osortk)
	asz := m.GetMemberValue("ASZ").(int)

	if param.OvfwBatchSize == asz-2 {
		// update XF in UID-Pred (in parent node block)
		xf := py.DI.XF
		xf[py.NdIndex] = blk.OBatchSizeLimit
		//mut.NewUpdate(tbl.EOP).AddMember("PKey", py.DI.Pkey, mut.IsKey).AddMember("SortK", py.DI.GetSortK(), mut.IsKey).AddMember("XF", xf, mut.Set)
		ctx.MergeMutation(tbl.EOP, py.DI.Pkey, py.DI.GetSortK(), mut.Update).AddMember("XF", xf, mut.Set)

		//	fmt.Println("checkOBatchSizeLimitReached:  len(xf) %d   asz %d\n", len(xf), asz)
	}

	return nil

}
