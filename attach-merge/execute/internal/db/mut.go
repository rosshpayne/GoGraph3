//go:build spanner
// +build spanner

package db

import (
	"github.com/GoGraph/block"
	"github.com/GoGraph/dbs"
	param "github.com/GoGraph/dygparam"
	"github.com/GoGraph/uuid"
)

type WithOBatchLimit struct {
	Ouid   uuid.UID
	Cuid   uuid.UID
	Puid   uuid.UID
	DI     *block.DataItem
	OSortK string // overflow sortk
	Index  int    // UID-PRED Nd index entry
}

func (x *WithOBatchLimit) GetStatements() []dbs.Statement {

	// set OBatchSizeLimit if array size reaches param.OvflBatchSize
	xf := x.DI.XF // or GetXF()
	xf[x.Index] = block.OBatchSizeLimit
	upd := dbs.Statement{
		SQL: "update EOP x set XF=@xf where PKey=@pk and Sortk = @sk and @size = (select ASZ-1 from EOP  where SortK  = @osk and PKey = @opk)",
		Params: map[string]interface{}{
			"pk":   x.DI.Pkey,
			"sk":   x.DI.GetSortK(),
			"xf":   xf,
			"opk":  x.Ouid,
			"osk":  x.OSortK,
			"size": param.OvfwBatchSize,
		},
	}

	return []dbs.Statement{upd}
}
