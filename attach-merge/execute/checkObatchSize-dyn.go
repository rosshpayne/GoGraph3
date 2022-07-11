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

func checkOBatchSizeLimitReached(txh *tx.Handle, cUID uuid.UID, py *blk.ChPayload) error {

	// find source mutation
	m := txh.FindSourceMutation(tbl.EOP, py.TUID, py.Osortk)
	asz := m.GetMemberValue("ASZ").(int)

	if param.OvfwBatchSize == asz-2 {
		// update XF in UID-Pred (in parent node block)
		xf := py.DI.XF
		xf[py.NdIndex] = blk.OBatchSizeLimit
		//mut.NewUpdate(tbl.EOP).AddMember("PKey", py.DI.Pkey, mut.IsKey).AddMember("SortK", py.DI.GetSortK(), mut.IsKey).AddMember("XF", xf, mut.Set)
		txh.MergeMutation(tbl.EOP, py.DI.Pkey, py.DI.GetSortK(), mut.Update).AddMember("XF", xf, mut.Set)

	}

	return nil

}
