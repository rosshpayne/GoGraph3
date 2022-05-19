package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/GoGraph/attach/anmgr"
	"github.com/GoGraph/attach/ds"
	"github.com/GoGraph/attach/ebuf"
	"github.com/GoGraph/attach/execute"
	dbadmin "github.com/GoGraph/db/admin"
	param "github.com/GoGraph/dygparam"
	"github.com/GoGraph/errlog"
	elog "github.com/GoGraph/errlog"
	"github.com/GoGraph/grmgr"
	"github.com/GoGraph/monitor"
	"github.com/GoGraph/run"
	"github.com/GoGraph/state"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx"
	"github.com/GoGraph/tx/query"
	"github.com/GoGraph/types"
	"github.com/GoGraph/util"
)

type scanStatus int

const (
	logid = param.Logid

	eod       scanStatus = 1
	more                 = 2
	scanError            = 3
)

var (
	ctx    context.Context
	wpEnd  sync.WaitGroup
	cancel context.CancelFunc
)

func syslog(s string) {
	slog.Log(logid, s)
}

//var attachers = flag.Int("a", 1, "Attachers: ")

var (
	stats     = flag.Int("stats", 0, `Show system stats [1: enable 0: disable (default)]`)
	environ   = flag.String("env", "dev", "Environment [ dev: Development, prd: production]")
	table     = flag.String("tbl", string(tbl.TblName), "Graph Table from which other table names are derived")
	debug     = flag.String("debug", "", `Enable logging by component "c1,c2,c3" or switch on complete logging "all"`)
	attachers = flag.Int("c", 6, "# parallel goroutines")
	graph     = flag.String("g", "", "Graph: ")
	showsql   = flag.Int("sql", 0, "Show generated SQL [1: enable 0: disable (default)]")
	reduceLog = flag.Int("rlog", 1, "Reduced Logging [1: enable (default)  0: disable]")

	runId run.Runid

	tblEdge      tbl.Name
	tblEdgeChild tbl.Name
)

func GetRunId() run.Runid {
	return runId
}

func main() {

	flag.Parse()

	fmt.Printf("Argument: stats: %d\n", *stats)
	fmt.Printf("Argument: table: %s\n", *table)
	fmt.Printf("Argument: env: %s\n", *environ)
	fmt.Printf("Argument: debug: %v\n", *debug)
	fmt.Printf("Argument: concurrent: %d\n", *attachers)
	fmt.Printf("Argument: showsql: %v\n", *showsql)
	fmt.Printf("Argument: graph: %s\n", *graph)
	fmt.Printf("Argument: reduced logging: %v\n", *reduceLog)
	var (
		wpEnd, wpStart sync.WaitGroup
		runNow         bool // whether to run attachNode on current edge
		err            error
		runid          run.Runid
	)

	// allocate a run id
	param.ReducedLog = false
	if *reduceLog == 1 {
		param.ReducedLog = true
	}
	if *showsql == 1 {
		param.ShowSQL = true
	}
	if *stats == 1 {
		param.StatsSystem = true
	}
	if tbl.Name(*table) != tbl.TblName {
		tbl.Set(*table)
	}

	if len(*debug) > 0 {
		if strings.ToUpper(*debug) == "ALL" {
			param.DebugOn = true
		} else {
			for _, v := range strings.Split(*debug, ",") {
				param.LogServices = append(param.LogServices, v)
			}
		}
	}

	// set environment
	*environ = strings.ToLower(*environ)
	if *environ != "prd" && *environ != "dev" {
		fmt.Printf("\nEnvironment must be either %q or %q. Default: %[2]q\n", "prd", "dev")
		return
	}
	param.Environ = *environ

	// set graph to use
	if len(*graph) == 0 {
		fmt.Printf("Must supply a graph name\n")
		flag.PrintDefaults()
		return
	}
	runid, err = run.New(logid, "attacher")
	defer run.Finish(err)
	if err != nil {
		fmt.Println(fmt.Sprintf("Error in  MakeRunId() : %s", err))
		return
	}

	// start any syslog services - dependency on runid
	err = slog.Start()
	if err != nil {
		fmt.Println("Error: ", err)
		panic(err)
	} else {
		fmt.Println("\n no error ")
	}

	ctx, cancel := context.WithCancel(context.Background())
	// edgeCh = make(chan *ds.Edge, 20)
	// moreEdgesCh = make(chan struct{})
	//	scanCh = make(chan scanStatus)

	// set Graph and load types into memory - dependency on syslog
	err = types.SetGraph(*graph)
	if err != nil {
		syslog(fmt.Sprintf("Error in SetGraph: %s ", err.Error()))
		fmt.Printf("Error in SetGraph: %s\n", err)
		return
	}

	syslog(fmt.Sprintf("Argument: table: %s", *table))
	syslog(fmt.Sprintf("Argument: stats: %v", *stats))
	syslog(fmt.Sprintf("Argument: env: %s", *environ))
	syslog(fmt.Sprintf("Argument: concurrency: %d", *attachers))
	syslog(fmt.Sprintf("Argument: showsql: %v", *showsql))
	syslog(fmt.Sprintf("Argument: debug: %v", *debug))
	syslog(fmt.Sprintf("Argument: graph: %s", *graph))
	syslog(fmt.Sprintf("Argument: reduced logging: %v", *reduceLog))

	initState()
	//
	// Regstier tables/indexes
	//
	tblEdge = tbl.Edge + tbl.Name(types.GraphName())
	tbl.Register(tblEdge, "Bid", "Puid")
	tblEdge = tbl.Edge + tbl.Name(types.GraphName())
	tbl.RegisterIndex(tbl.IdxName("bid_cnt"), tbl.Name(tblEdge), "Bid", "Cnt")
	tblEdgeChild = tbl.EdgeChild + tbl.Name(types.GraphName())
	tbl.Register(tblEdgeChild, "Puid", "SortK_Cuid")
	tbl.RegisterIndex(tbl.IdxName("status_idx"), tbl.Name(tblEdgeChild), "Puid", "Status")
	//
	// start services
	//
	wpEnd.Add(4)
	wpStart.Add(4)
	go grmgr.PowerOn(ctx, &wpStart, &wpEnd, runid) // concurrent goroutine manager service
	go errlog.PowerOn(ctx, &wpStart, &wpEnd)       // error logging service
	go anmgr.PowerOn(ctx, &wpStart, &wpEnd)        // attach node service
	go monitor.PowerOn(ctx, &wpStart, &wpEnd)      // repository of system statistics service
	wpStart.Wait()
	//
	// setup db related services (e.g. stats snapshot save)
	dbadmin.Setup()
	//
	// main
	//
	syslog("All services started. Proceed with attach processing")
	t0 := time.Now()

	limiterAttach := grmgr.New("nodeAttach", *attachers)
	respch := make(chan bool)
	//	errRespCh := make(chan bool)

	var (
		edges = 0
		wg    sync.WaitGroup
		// accumulate unprocessed edges (not attached due to concurrency issues)
		// into batches, one to accumulate, one to process
	)
	// unprocessed edge batches (double buffer implementation)
	ubat := ebuf.New()

	slog.Log(logid, "Main: Started. Waiting on EdgeCh...")

	// TODO consdier pattern that creates ch, reads from channel, receieve close channel. Maybe more elegant than below

	for {

		i := 0
		for e := range processBatch(ubat.Read()) {

			i++
			e.RespCh = respch
			anmgr.AttachNowCh <- e
			runNow = <-e.RespCh

			if runNow {
				op := AttachOp{Puid: e.Puid, Cuid: e.Cuid, Sortk: e.Sortk, Bid: e.Bid}

				limiterAttach.Ask()
				<-limiterAttach.RespCh()
				edges++
				wg.Add(1)

				go execute.AttachNode(util.UID(e.Cuid), util.UID(e.Puid), e.Sortk, e, &wg, limiterAttach, &op)

			} else {
				// accumulate unprocessed edges in batch which will be processed in next processBatch()
				ubat.Write(e)
			}

		}
		// swap buffers
		ubat.Swap()
		// check for eod
		if i == 0 {
			break
		}
		wg.Wait()

	}
	t1 := time.Now()
	limiterAttach.Unregister()
	// print monitor report
	monitor.Report()

	printErrors()
	// send cancel to all registered goroutines
	cancel()
	wpEnd.Wait()

	// stop system logger services (if any)
	slog.Stop()

	// save db stats
	dbadmin.Persist()
	//
	syslog(fmt.Sprintf("Attach operation finished. See log file for any errors. RunId: %q, Edges: %d  Duration: %s ", runid.String(), edges, t1.Sub(t0)))
}

// processBatch returns a channel into which either new edges are sent or any unprocessed edges
// from previous processBatch run which have been accumulated in the bat argument.
func processBatch(bat ebuf.EdgeBuf) <-chan *ds.Edge {

	fmt.Println("=========================. processBatch ========================== ", len(bat))
	edgeCh := make(chan *ds.Edge, 20)

	if len(bat) > 0 {

		go unprocessedEdges(bat, edgeCh)

	} else {

		go scanForEdges(edgeCh)
	}

	return edgeCh
}

func unprocessedEdges(edges ebuf.EdgeBuf, edgeCh chan<- *ds.Edge) {
	for _, v := range edges {
		edgeCh <- v
	}
	close(edgeCh)
}

// ChildEdge Service...

func scanForEdges(edgeCh chan<- *ds.Edge) { //, abortCh chan<- struct){}) {

	// until EOD for Edge_
	if pnodes := fetchParentNode(); len(pnodes) > 0 {

		for _, n := range pnodes { // in batches of 150

			edge, err := fetchChildEdge(n.Puid)
			if err != nil {
				if errors.Is(err, query.NoDataFoundErr) {
					// this should not happen. A row should always exist however
					// due to dynamodb reasons it can return no row due to either
					// a limit (value 1) or a synchronisation issue
					slog.Log(logid, "Warning: fetchChildEdge returned no item. Logically this should not happen .")
					continue
				}
				elog.Add(logid, fmt.Errorf("fetchChildEdge: %w", err))
				break
			}
			edge.Bid = n.Bid

			edgeCh <- edge
		}
	}
	close(edgeCh)
}

type pNodeBid struct {
	Bid  int
	Puid util.UID
	Cnt  int64
}

func fetchParentNode() []pNodeBid {

	const logid = "ChildEdge: "

	// type PuidS []util.UID

	// var result PuidS

	var result []pNodeBid

	slog.Log(logid, fmt.Sprintf("fetchParentNode invoked for bid %d", bid))

	for _, lc := range []int{1, 2} {

		// scan index by CNT descending.
		qtx, err := tx.NewQuery(tblEdge, "Edge_", "bid_cnt")
		if err != nil {
			panic(err)
		}
		// keep reading a batch until all items have Cnt==0, and only then move to next batch
		// read all items in a batch (150) - require 2 RCUs each time
		qtx.Select(&result).Key("Bid", bid).Key("Cnt", 0, query.GT).Sort(query.DESC) //.Consistent(false)
		err = qtx.Execute()
		if err != nil {
			if !errors.Is(err, query.NoDataFoundErr) {
				elog.Add(logid, fmt.Errorf("Error while fetching in ScanForNodes: %w", err))
				panic(err)
			}
		}
		// for i, v := range result {
		// 	fmt.Println("result desc order: index, Cnt, Puid ", i, v.Cnt, v.Puid)
		// }
		// result batch size is upto 150.
		if len(result) == 0 && lc == 1 {

			// batch has been consumed, proceed to next batch
			bid++
			state.Set("bid#attach", bid)
			slog.Log(logid, "bid++")

		} else {

			return result
		}
	}

	return nil
}

// FetchEdge fetches a parent-child edge given a parent with status unprocessed.
func fetchChildEdge(puid util.UID) (*ds.Edge, error) {

	const logid = "ChildEdge: "

	var err error
	//
	// query operation - limited to 1 item
	//
	result := []ds.EdgeChild{}

	qtx, _ := tx.NewQuery(tblEdgeChild, "EdgeChild", "status_idx")
	if err != nil {
		panic(err)
	}
	qtx.Select(&result).Key("Puid", puid).Key("Status", "X").Limit(1)
	err = qtx.Execute()
	if err != nil {
		if errors.Is(err, query.NoDataFoundErr) {
			panic(err)
		}
		return nil, err
	}
	sc := strings.SplitN(result[0].SortK_Cuid, "|", 3)

	d := ds.Edge{Puid: result[0].Puid, Cuid: util.FromString(sc[2]), Sortk: sc[0] + "|" + sc[1]}

	return &d, nil
}

func printErrors() {

	errlog.ReqErrCh <- struct{}{}
	errs := <-errlog.RequestCh
	syslog(fmt.Sprintf(" ==================== ERRORS : %d	==============", len(errs)))
	fmt.Printf(" ==================== ERRORS : %d	==============\n", len(errs))
	if len(errs) > 0 {
		for _, e := range errs {
			syslog(fmt.Sprintf(" %s:  %s", e.Id, e.Err))
			fmt.Println(e.Id, e.Err)
		}
	}
}
