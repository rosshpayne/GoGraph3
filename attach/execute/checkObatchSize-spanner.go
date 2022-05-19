// +build spanner

package execute

import (
	"github.com/GoGraph/attach/execute/internal/db"
	"github.com/GoGraph/tx"
)

// Spanner version

func checkOBatchSizeLimitReached(cUID util.UID, py blk.ChPayload) dbs.Mutation {

	return &db.WithOBatchLimit{Ouid: py.TUID, Cuid: cUID, Puid: pUID, DI: py.DI, OSortK: py.Osortk, Index: py.NdIndex}

}
