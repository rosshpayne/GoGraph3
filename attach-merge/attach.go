package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/GoGraph/attach-merge/ds"
	"github.com/GoGraph/attach-merge/execute"
	"github.com/GoGraph/cache"
	dyn "github.com/GoGraph/db"
	dbadmin "github.com/GoGraph/db/admin"
	param "github.com/GoGraph/dygparam"
	elog "github.com/GoGraph/errlog"
	"github.com/GoGraph/grmgr"
	"github.com/GoGraph/monitor"
	"github.com/GoGraph/mysql"
	"github.com/GoGraph/run"
	"github.com/GoGraph/state"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx"
	"github.com/GoGraph/tx/db"
	"github.com/GoGraph/tx/query"
	"github.com/GoGraph/types"
	"github.com/GoGraph/uuid"
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
	stats        = flag.Int("stats", 0, `Show system stats [1: enable 0: disable (default)]`)
	environ      = flag.String("env", "dev", "Environment [ dev: Development] prd: production]")
	table        = flag.String("tbl", string(tbl.TblName), "Graph Table from which other table names are derived")
	debug        = flag.String("debug", "", `Enable logging by component "c1,c2,c3" or switch on complete logging "all"`)
	attachers    = flag.Int("c", 6, "# parallel goroutines")
	graph        = flag.String("g", "", "Graph: ")
	showsql      = flag.Int("sql", 0, "Show generated SQL [1: enable 0: disable (default)]")
	reduceLog    = flag.Int("rlog", 1, "Reduced Logging [1: enable (default)  0: disable]")
	runid        run.Runid
	tblEdge      tbl.Name
	tblEdgeChild tbl.Name
)

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
		tstart, tend   time.Time
		err            error
	)

	// start any syslog services - dependency on runid
	err = slog.Start()
	if err != nil {
		fmt.Println("Error: ", err)
		panic(err)
	} else {
		fmt.Println("\n no error ")
	}

	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// setup concurrent routine to capture OS signals.
	appSignal := make(chan os.Signal, 3)
	var (
		terminate os.Signal = syscall.SIGTERM // os kill
		interrupt os.Signal = syscall.SIGINT  // ctrl-C
	)
	signal.Notify(appSignal, terminate, interrupt) // TODO: add ctrl-C signal

	// concurrent process to capture os process termination signals and call context cancel to release db resources.
	go func() {
		select {
		case <-appSignal:
			// broadcast kill switch to all context aware goroutines including mysql
			cancel()
			wpEnd.Wait()

			tend := time.Now()
			syslog(fmt.Sprintf("Terminated.....Duration: %s", tstart.Sub(tend).String()))
			os.Exit(2)
		}
	}()

	// register default database client
	//db.Init(ctx)
	// db.Init(ctx, &wpEnd, []db.Option{db.Option{Name: "throttler", Val: grmgr.Control}, db.Option{Name: "Region", Val: "us-east-1"}}...)
	// mysql.Init(ctx)
	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "throttler", Val: grmgr.Control}, db.Option{Name: "Region", Val: "us-east-1"}}...)
	mysql.Register(ctx, "mysql-GoGraph", "admin:gjIe8Hl9SFD1g3ahyu6F@tcp(mysql8.cjegagpjwjyi.us-east-1.rds.amazonaws.com:3306)/GoGraph")

	tstart = time.Now()

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
				v = strings.TrimRight(v, " ")
				v = strings.TrimLeft(v, " ")
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

	// allocate a run id
	runid, err = run.New(logid, "attacher")
	defer run.Finish(err)
	if err != nil {
		fmt.Println(fmt.Sprintf("Error in  MakeRunId() : %s", err))
		return
	}

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
	syslog(fmt.Sprintf("RunId :  %s  %s", runid, runid.EncodeBase64()))

	initState()

	tblEdge, tblEdgeChild = tbl.SetEdgeNames(*graph)

	syslog(fmt.Sprintf("Edge tables: %s   %s", tblEdge, tblEdgeChild))
	//
	// start services
	//
	wpEnd.Add(3)
	wpStart.Add(3)
	go grmgr.PowerOn(ctx, &wpStart, &wpEnd, runid) // concurrent goroutine manager service
	go elog.PowerOn(ctx, &wpStart, &wpEnd)         // error logging service
	go monitor.PowerOn(ctx, &wpStart, &wpEnd)      // repository of system statistics service
	//	go cache.PowerOn(ctx, &wpStart, &wpEnd)        //in development
	wpStart.Wait()
	//
	// setup db related services (e.g. stats snapshot save)
	dbadmin.Setup()
	//
	// main
	//
	syslog("All services started. Proceed with attach processing")

	limiterAttach := grmgr.New("nodeAttach", *attachers)

	var (
		edges int64
		wg    sync.WaitGroup
		// accumulate unprocessed edges (not attached due to concurrency issues)
		// into batches, one to accumulate, one to process
		once sync.Once
	)

	// register default database client
	// db.Init(ctx)

	slog.Log(logid, "Main: Started. Waiting on EdgeCh...")

	nextBatchCh := make(chan struct{}, 1)

	// allocate cache
	cache.NewCache()

	qetx := tx.NewQueryContext(ctx, "EdgeChild", tblEdgeChild).DB("mysql-GoGraph").Prepare()
	defer qetx.Close()
	qptx := tx.NewQueryContext(ctx, "EdgeParent", tblEdge).DB("mysql-GoGraph").Prepare()
	defer qptx.Close()

	for {

		i := 0
		for e := range nodeEdges(qptx, qetx) { // loop for all puids (containing all edges) associated with a single #bid.

			i++

			limiterAttach.Ask()
			<-limiterAttach.RespCh()
			wg.Add(1)

			edges += int64(len(e))

			go execute.AttachNodeEdges(ctx, e, &wg, limiterAttach, checkMode, nextBatchCh, &once)

		}
		// check for eod
		if i == 0 {
			break
		}

		wg.Wait()

		select {
		case <-nextBatchCh:
			//
			fmt.Println("============ nextBatchCh received. ================")
		default:
		}

	}
	tend = time.Now()
	limiterAttach.Unregister()
	// print monitor report
	monitor.Report()

	elog.PrintErrors()
	// send cancel to all registered goroutines
	cancel()
	wpEnd.Wait()

	// stop system logger services (if any)
	slog.Stop()

	// save db stats
	dbadmin.Persist()

	//
	syslog(fmt.Sprintf("Attach operation finished. See log file for any errors. RunId: %q, Edges: %d  Duration: %s ", runid.Base64(), edges, tend.Sub(tstart)))
}

// processBatch returns a channel into which either new edges are sent or any unprocessed edges
// from previous processBatch run which have been accumulated in the bat argument.
func nodeEdges(qetx, qptx *tx.QHandle) <-chan []*ds.Edge {

	//fmt.Println("=========================. processBatch ========================== ")
	edgeCh := make(chan []*ds.Edge) //, 10)

	go scanForEdges(qetx, qptx, edgeCh)

	return edgeCh
}

// ChildEdge Service...

func scanForEdges(qptx, qetx *tx.QHandle, edgeCh chan<- []*ds.Edge) { //, abortCh chan<- struct){}) {

	const log_id = "scanForEdges"
	// read a batch worth of Puids. Initial value of Batch, bid, is source from state table.
	// Fetch Cuid for each Puid and send all in channel
	// when batch finished close channel - which will initiate a New scanForEdges
	if pnodes := fetchParentNode(qptx); len(pnodes) > 0 {

		for _, n := range pnodes { // in batches of 150
			slog.Log("scanForEdges", fmt.Sprintf("Puid: %s", n.Puid.Base64()))
			edges, err := fetchChildEdges(qetx, n.Puid) // returns all child nodes for parent puid
			if err != nil {
				if errors.Is(err, query.NoDataFoundErr) {
					// this should not happen. A row should always exist however
					// due to dynamodb reasons it can return no row due to either
					// a limit (value 1) or a synchronisation issue
					slog.Log(logid, "Warning: fetchChildEdge returned no item. Logically this should not happen .")
					panic(fmt.Errorf("Child edges returns 0 rows - should be > 0"))
				}
				slog.Log(logid, fmt.Sprintf("fetchChildEdge errored %s", err))
				elog.Add(logid, fmt.Errorf("fetch: %w", err))
				break
			}

			for _, edge := range edges {
				edge.Bid = n.Bid
			}

			edgeCh <- edges // all edges for a puid (upto #attacher routines + 1 loaded in memory)
		}
	}
	// close at end of batch
	close(edgeCh)
}

type pNodeBid struct {
	Bid  int
	Puid uuid.UID
	Cnt  int64
}

// fetchParentNode reads a bid's worth of parent node UID's in edge Cnt descending order.
func fetchParentNode(qtx *tx.QHandle) []pNodeBid {

	const logid = "fetchParentNode"

	// type PuidS []uuid.UID

	// var result PuidS

	var (
		err    error
		result []pNodeBid
	)

	slog.Log(logid, fmt.Sprintf("fetchParentNode invoked for bid %d", bid))
	result = []pNodeBid{}
	for _, lc := range []int{1, 2} {

		// scan index by CNT descending.
		//qtx := tx.NewQuery2(ctx, "EdgeParent", tblEdge).DB("mysql-GoGraph").Prepare()
		// keep reading a batch until all items have Cnt==0, and only then move to next batch
		// read all items in a batch (150) - require 2 RCUs each time
		// TODO : put bid in its own package

		// note: Sort for Dynamodb is based on SortKey .. sort(sortOrder)
		//       Sort for SQL DB is based on attributes in Select.. sortk(sortOrder, col ...cols)
		// TODO: should each db haave its own tx parser - to cope with what will be a difference Sort()
		qtx.Select(&result).Key("Bid", bid).Filter("Cnt", 0, "GT").OrderBy("Cnt", query.Desc) // Sort(query.DESC) //.Consistent(false)

		err = qtx.Execute()
		if err != nil && !errors.Is(err, query.NoDataFoundErr) {
			elog.Add(logid, fmt.Errorf("Error while fetching in ScanForNodes: %w", err))
			panic(err)
		}
		// result batch size is upto 150.
		if len(result) == 0 && lc == 1 {

			// batch has been consumed, proceed to next batch
			bid++
			err = state.Set("bid#attach", bid)
			if err != nil {
				panic(err)
			}
			if checkMode {
				// checkMode only needs to be set for the bid defined at the restart.
				// as this bid is now completed we can turn off checkMode
				checkMode = false
			}

		} else {

			//fmt.Printf("fetchParentNode cnt: %d  for bid: %d \n", len(result), bid)
			return result // contains upto 150 puids
		}
	}

	return nil
}

// FetchEdge fetches a parent-child edge given a parent with status unprocessed.
func fetchChildEdges(qtx *tx.QHandle, puid uuid.UID) ([]*ds.Edge, error) {

	const logid = "ChildEdges"
	var err error
	//
	// query operation - limited to 1 item
	//
	result := []ds.EdgeChild{}

	qtx.Select(&result).Key("Puid", puid).Filter("Status", "X")

	err = qtx.Execute()
	if err != nil {
		slog.Log(logid, fmt.Sprintf("Errored..Query returns : %d, Error: %s", len(result), err))
		if errors.Is(err, query.NoDataFoundErr) {
			panic(err)
		}
		return nil, err
	}

	slog.Log(logid, fmt.Sprintf("Query returns : %d  for Puid: %s", len(result), puid.Base64()))

	// if len(result) == 0 {

	// }
	edges := make([]*ds.Edge, len(result), len(result))

	for i, _ := range result {
		sc := strings.SplitN(result[i].SortK_Cuid, "|", 3)

		edges[i] = &ds.Edge{Puid: result[i].Puid, Cuid: uuid.FromString(sc[2]), Sortk: sc[0] + "|" + sc[1]}

	}
	return edges, nil
}
