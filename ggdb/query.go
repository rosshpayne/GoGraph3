package ggdb

import (
	"context"
	"fmt"

	blk "github.com/GoGraph/block"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx"
	"github.com/GoGraph/tx/query"
	"github.com/GoGraph/uuid"
)

var logid string = "ggdb"

// FetchNode performs a Query with KeyBeginsWidth on the SortK value, so all item belonging to the SortK are fetched.
func FetchNode(uid uuid.UID, subKey ...string) (blk.NodeBlock, error) {
	return FetchNodeContext(context.TODO(), uid, subKey...)
}

// FetchNode performs a Query with KeyBeginsWidth on the SortK value, so all item belonging to the SortK are fetched.
func FetchNodeContext(ctx context.Context, uid uuid.UID, subKey ...string) (blk.NodeBlock, error) {

	var (
		err   error
		d     blk.NodeBlock
		sortk string
	)

	sortk = "A#"
	if len(subKey) > 0 {
		sortk = subKey[0]
	}

	stx := tx.NewQueryContext(ctx, "FetchNode", tbl.Block)

	stx.Select(&d).Key("PKey", uid).Key("SortK", sortk, query.BEGINSWITH)
	if stx.Error() != nil {
		return nil, err
	}

	err = stx.Execute()
	if err != nil {
		err = fmt.Errorf("FetchNode: %w", err)
	}

	return d, err
}
