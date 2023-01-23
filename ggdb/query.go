package ggdb

import (
	"context"
	"fmt"

	blk "github.com/GoGraph/block"
	"github.com/GoGraph/tbl"
	"github.com/ros2hp/method-db/tx"
	"github.com/ros2hp/method-db/uuid"
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

	stx.Select(&d).Key("PKey", uid).Key("SortK", sortk, "BEGINSWITH")
	if stx.Error() != nil {
		return nil, err
	}

	err = stx.Execute()
	if err != nil {
		err = fmt.Errorf("FetchNode: %w", err)
	}

	return d, err
}

// FetchNode performs a Query with KeyBeginsWidth on the SortK value, so all item belonging to the SortK are fetched.
func FetchNodeItem(uid uuid.UID, subKey string) (*blk.DataItem, error) {
	return FetchNodeItemContext(context.TODO(), uid, subKey)
}

// FetchNode performs a Query with KeyBeginsWidth on the SortK value, so all item belonging to the SortK are fetched.
func FetchNodeItemContext(ctx context.Context, uid uuid.UID, subKey string) (*blk.DataItem, error) {

	var (
		err   error
		d     blk.DataItem
		sortk string
	)

	sortk = subKey

	stx := tx.NewQueryContext(ctx, "FetchNode", tbl.Block)

	stx.Select(&d).Key("PKey", uid).Key("SortK", sortk)
	if stx.Error() != nil {
		return nil, err
	}

	err = stx.Execute()
	if err != nil {
		err = fmt.Errorf("FetchNodeItem: %w", err)
		return nil, err
	}

	return &d, nil
}
