//go:build dynamodb
// +build dynamodb

package execute

import (
	"fmt"

	blk "github.com/GoGraph/block"
	param "github.com/GoGraph/dygparam"
	//slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/tbl"
	"github.com/ros2hp/method-db/tx"
	"github.com/ros2hp/method-db/key"
	"github.com/ros2hp/method-db/mut"
	"github.com/ros2hp/method-db/uuid"
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

	// find source mutation - this is suitable only on initial load as its querying the in memory version of the mutation not the database version.
	//m := txh.FindSourceMutation(tbl.EOP, py.TUID, py.Osortk)
	keys := []key.Key{key.Key{"PKey", py.TUID}, key.Key{"SortK", py.Osortk}}

	m, err := txh.GetMergedMutation(tbl.EOP, keys)
	if err != nil {
		return err
	}

	asz := m.GetMemberValue("ASZ").(int)
	if param.OvfwBatchSize == asz-2 {
		// update XF in UID-Pred (in parent node block) to overflow batch limit reached.
		xf := py.DI.XF
		xf[py.NdIndex] = blk.OBatchSizeLimit
		txh.MergeMutation(tbl.Block, mut.Update, keys).AddMember("XF", xf, mut.Set)
	}

	return nil

}
