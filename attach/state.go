package main

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/GoGraph/state"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/tx/query"
)

var bid int
var startingup bool = true

func initState() {
	var err error
	var logid = "initState: "
	//
	// get state
	//
	bid_, err := state.Get("bid#attach")
	if err != nil {
		if errors.Is(err, query.NoDataFoundErr) {
			bid = 1
			slog.Log(logid, fmt.Sprintf("no state data, bid = %d", bid))
			return
		} else {
			panic(err)
		}
	}
	bid, err = strconv.Atoi(bid_)
	if err != nil {
		panic(err)
	}
	slog.Log(logid, fmt.Sprintf("from state table: bid = %d", bid))
}
