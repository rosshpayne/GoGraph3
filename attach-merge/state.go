package main

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/GoGraph/state"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/tx/query"

	_ "github.com/GoGraph/tx/mysql"
)

var bid int
var checkMode bool // set to true if restarting the program
var startingup bool = true

func initState() {
	var err error
	var logid = "initState"
	//
	// get state
	//
	bid_, err := state.Get("bid#attach")
	if err != nil {
		if errors.Is(err, query.NoDataFoundErr) {
			//	if strings.Contains(strings.ToLower(err.Error()), "no rows in result") {
			//if errors.Is(err, query.NoDataFoundErr) {
			bid = 1
			slog.LogAlert(logid, fmt.Sprintf("no state data, bid = %d  checkMode:  %v", bid, checkMode))
			return
		} else {
			panic(err)
		}
	}
	bid, err = strconv.Atoi(bid_)
	if err != nil {
		panic(err)
	}
	// must be a restart set checkMode
	checkMode = true

	slog.LogAlert(logid, fmt.Sprintf("from state table: bid = %d   checkMode: %v", bid, checkMode))
}
