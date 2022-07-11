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

	//"github.com/GoGraph/attach/anmgr"
	"github.com/GoGraph/cache"
	"github.com/GoGraph/db"
	dbadmin "github.com/GoGraph/db/admin"
	param "github.com/GoGraph/dygparam"
	"github.com/GoGraph/errlog"
	elog "github.com/GoGraph/errlog"
	"github.com/GoGraph/grmgr"
	"github.com/GoGraph/monitor"
	"github.com/GoGraph/run"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/tx/query"
	"github.com/GoGraph/types"
	"github.com/GoGraph/uuid"

	"github.com/GoGraph/mysql"
)

const (
	logid  = param.Logid
	loadId = "DP Load"
)

var (
	stats     = flag.Int("stats", 0, `Show system stats [1: enable 0: disable (default)]`)
	environ   = flag.String("env", "dev", "Environment [ dev: Development] prd: production]")
	table     = flag.String("tbl", string(tbl.TblName), "Graph Table")
	debug     = flag.String("debug", "", `Enable logging by component "c1,c2,c3" or switch on complete logging "all"`)
	parallel  = flag.Int("c", 6, "# parallel operations")
	graph     = flag.String("g", "", "Graph: ")
	showsql   = flag.Int("sql", 0, "Show generated SQL [1: enable 0: disable]")
	reduceLog = flag.Int("rlog", 1, "Reduced Logging [1: enable 0: disable]")
	batchSize = flag.Int("bs", 20, "Scan batch size [defaut: 20]")
)

var (
	runId int64
)

func syslog(s string) {
	slog.Log(logid, s)
}

func alertlog(s string) {
	slog.LogAlert(logid, s)
}

// func errlog(s string) {
// 	slog.LogErr(logid, s)
// }

func main() {
	// determine types which reference types that have a cardinality of 1:1
	flag.Parse()

	fmt.Printf("Argument: table: %s\n", *table)
	fmt.Printf("Argument: batch size: %d\n", *batchSize)
	fmt.Printf("Argument: stats: %d\n", *stats)
	fmt.Printf("Argument: env: %s\n", *environ)
	fmt.Printf("Argument: debug: %v\n", *debug)
	fmt.Printf("Argument: concurrent: %d\n", *parallel)
	fmt.Printf("Argument: showsql: %v\n", *showsql)
	fmt.Printf("Argument: graph: %s\n", *graph)
	fmt.Printf("Argument: reduced logging: %v\n", *reduceLog)
	var (
		wpEnd, wpStart sync.WaitGroup
		err            error
		tstart         time.Time
		runid          uuid.UID

		stateId uuid.UID
		restart bool
		ls      string
		status  string
	)

	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())

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
			err = setLoadStatus(nil, loadId, "S", nil)
			if err != nil {
				fmt.Printf("Error setting load status: %s\n", err)
			}
			wpEnd.Wait()

			tend := time.Now()
			syslog(fmt.Sprintf("Terminated.....Duration: %s", tstart.Sub(tend).String()))
			os.Exit(2)
		}
	}()

	// register default database client
	db.Init(ctx, &wpEnd, []db.Option{db.Option{Name: "throttler", Val: grmgr.Control}, db.Option{Name: "Region", Val: "us-east-1"}}...)
	mysql.Init(ctx)

	tbl.Register("pgState", "Id", "Name")
	tbl.Register("State$", "Graph", "Name")

	param.ReducedLog = false
	if *reduceLog == 1 {
		param.ReducedLog = true
	}
	if *showsql == 1 {
		param.ShowSQL = true
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

	// set table
	if tbl.Name(*table) != tbl.TblName {
		tbl.Set(*table)
	}

	// set graph to use
	if len(*graph) == 0 {
		fmt.Printf("Must supply a graph name\n")
		flag.PrintDefaults()
		return
	}

	// create run identifier
	runid, err = run.New(logid, "dp")
	if err != nil {
		fmt.Println(fmt.Sprintf("Error in  MakeRunId() : %s", err))
		return
	}

	//start syslog services (if any)
	err = slog.Start()
	if err != nil {
		panic(fmt.Errorf("Error starting syslog services: %w", err))
	}

	// set graph and type data
	err = types.SetGraph(*graph)
	if err != nil {
		syslog(fmt.Sprintf("Error in SetGraph: %s ", err.Error()))
		fmt.Printf("Error in SetGraph: %s\n", err)
		return
	}
	fmt.Println("GraphShortName: ", types.GraphSN())
	// check state of processing, restart?
	ls, stateId, err = getLoadStatus(ctx, loadId)
	if err != nil {

		if errors.Is(err, query.NoDataFoundErr) {

			// first time
			err = setLoadStatus(ctx, loadId, "R", nil, runid)
			if err != nil {
				fmt.Printf("Error setting load status: %s\n", err)
				return
			}
			stateId = runid

		} else {
			fmt.Printf("Error in determining load status: %s\n", err)
			return
		}

	} else {

		switch ls {
		case "C":
			fmt.Println("Load is already completed. Abort this run.")
			return
		case "R":
			fmt.Println("Currently loading..aborting this run")
			return
		case "S", "E":
			fmt.Println("Previous load errored or was terminated, will now rerun")
			restart = true
			err = setLoadStatus(ctx, loadId, "R", nil)
			if err != nil {
				fmt.Printf("Error setting load status: %s\n", err)
				return
			}
		}
	}

	syslog(fmt.Sprintf("runid: %s", runid.Base64()))

	// batch size
	if batchSize != nil {
		if *batchSize > 500 {
			*batchSize = 500
		}
		param.DPbatch = *batchSize
	}

	syslog(fmt.Sprintf("Argument: table: %s", *table))
	syslog(fmt.Sprintf("Argument: batch size: %d", *batchSize))
	syslog(fmt.Sprintf("Argument: stats: %v", *stats))
	syslog(fmt.Sprintf("Argument: env: %s", *environ))
	syslog(fmt.Sprintf("Argument: concurrency: %d", *parallel))
	syslog(fmt.Sprintf("Argument: showsql: %v", *showsql))
	syslog(fmt.Sprintf("Argument: debug: %v", *debug))
	syslog(fmt.Sprintf("Argument: graph: %s", *graph))
	syslog(fmt.Sprintf("Argument: reduced logging: %v", *reduceLog))

	syslog(fmt.Sprintf("runid: %v", runid.Base64()))

	if param.DB == param.Dynamodb {
		// Regstier index
		tbl.RegisterIndex(tbl.IdxName("TyIX"), tbl.Name("GoGraph"), "Ty", "IX") // Ty prepended with GraphSN()
	}
	//
	// start services
	//
	wpEnd.Add(3)
	wpStart.Add(3)
	go grmgr.PowerOn(ctx, &wpStart, &wpEnd, runid) // concurrent goroutine manager service
	go errlog.PowerOn(ctx, &wpStart, &wpEnd)       // error logging service
	//go anmgr.PowerOn(ctx, &wpStart, &wpEnd)        // attach node service
	go monitor.PowerOn(ctx, &wpStart, &wpEnd) // repository of system statistics service
	wpStart.Wait()

	// setup db related services (e.g. stats snapshot save)
	dbadmin.Setup()

	syslog("All services started. Proceed with attach processing")

	has11 := make(map[string]struct{})
	dpTy := make(map[string]struct{}) // TODO: why not a []string??

	for k, v := range types.TypeC.TyC {
		for _, vv := range v {
			if vv.Ty == "" {
				continue
			}
			if _, ok := has11[k]; ok {
				break
			}
			if vv.Card == "1:1" {
				has11[k] = struct{}{}
			}
		}
	}
	for k, v := range types.TypeC.TyC {
		for _, vv := range v {
			if _, ok := has11[vv.Ty]; ok {
				if sn, ok := types.GetTyShortNm(k); ok {
					dpTy[sn] = struct{}{}
				}
			}
		}
	}

	var wgc sync.WaitGroup
	limiterDP := grmgr.New("dp", *parallel)

	// channel used to sync DP processing with a new db Fetch. Replaces sleep() wait
	DPbatchCompleteCh := make(chan struct{})

	for k, _ := range dpTy {
		syslog(fmt.Sprintf(" Type containing 1:1 type: %s", k))
	}
	if len(dpTy) == 0 {
		syslog(fmt.Sprintf(" No 1:1 Types found"))
	}
	syslog(fmt.Sprintf("Start double propagation processing...%#v", dpTy))

	// allocate cache for node data
	cache.NewCache()

	tstart = time.Now()
	for ty, _ := range dpTy {

		ty := ty
		b := 0
		// loop until channel closed, proceed to next type
		for n := range FetchNodeCh(ctx, ty, stateId, restart, DPbatchCompleteCh) {
			b++
			wgc.Add(1)
			n := n
			limiterDP.Ask()
			<-limiterDP.RespCh()

			go Propagate(ctx, limiterDP, &wgc, n, ty, has11)

			if b == param.DPbatch {
				// finished batch - wait for remaining DPs to finish and alert scanner to fetch next batch
				wgc.Wait()
				DPbatchCompleteCh <- struct{}{}
				b = 0
			}
		}
		// wait got last DP process to complete i.e. run post delete, so it will not be fetched again in FetchNode query.
		wgc.Wait()
	}

	limiterDP.Unregister()
	monitor.Report()
	elog.PrintErrors()

	if elog.RunErrored() {
		status = "E"
	} else {
		status = "C"
	}
	err = setLoadStatus(ctx, loadId, status, err)
	if err != nil {
		elog.Add("SetLoadStatus", fmt.Errorf("Error setting load status to %s: %w", status, err))
	}
	cancel()
	wpEnd.Wait()

	run.Finish(err)
	tend := time.Now()

	syslog(fmt.Sprintf("double propagate finished....Runid:  %q   Duration: %s", runid.Base64(), tend.Sub(tstart)))
	time.Sleep(1 * time.Second)

	// stop system logger services (if any)
	slog.Stop()
	// stop db admin services and save to db.
	dbadmin.Persist()

}

//type PKey []byte

func getLoadStatus(ctx context.Context, id string) (string, uuid.UID, error) {

	var err error

	type Status struct {
		Value string
		RunId []byte
	}
	var status Status
	opt := db.Option{Name: "singlerow", Val: true}
	// check if ES load completed
	ftx := tx.NewQueryContext(ctx, "GetLoadStatus", "State$").DB("mysql-goGraph", []db.Option{opt}...)
	ftx.Select(&status).Key("Graph", types.GraphSN()).Key("Name", id) // other values: "E","R"

	err = ftx.Execute()
	if err != nil {
		return "", nil, err
	}
	alertlog(fmt.Sprintln("getLoadStatus: STATE$ - Name: [%s] status: %s   runid: %s", id, status.Value, uuid.UID(status.RunId).Base64()))
	return status.Value, status.RunId, nil
}

func setLoadStatus(ctx context.Context, id string, status string, err_ error, runid ...uuid.UID) error {

	var err error

	if strings.IndexAny(status, "ERSC") == -1 {
		panic(fmt.Errorf("setLoadStatus : value is empty"))
	}
	// runid supplied if its the first time - ie. perform an insert
	switch len(runid) > 0 {

	case true: // first run
		ftx := tx.New("setLoadStatus").DB("mysql-goGraph")
		m := ftx.NewInsert("State$").AddMember("Graph", types.GraphSN(), mut.IsKey).AddMember("Name", id, mut.IsKey).AddMember("Value", status).AddMember("RunId", runid[0])
		m.AddMember("Created", "$CURRENT_TIMESTAMP$")
		err = ftx.Execute()
		if err != nil {
			return err
		}

	case false: // restart
		// merge, preserving original runid which also happens to be the stateId used by the tx package for paginated queries.
		ftx := tx.New("setLoadStatus").DB("mysql-goGraph")
		ftx.NewMerge("State$").AddMember("Graph", types.GraphSN(), mut.IsKey).AddMember("Name", id, mut.IsKey).AddMember("Value", status).AddMember("LastUpdated", "$CURRENT_TIMESTAMP$")
		err = ftx.Execute()
		if err != nil {
			return err
		}
	}

	return nil

}

func FetchNodeCh(ctx context.Context, ty string, stateId uuid.UID, restart bool, DPbatchCompleteCh <-chan struct{}) <-chan uuid.UID {

	dpCh := make(chan uuid.UID)

	go ScanForDPitems(ty, dpCh, DPbatchCompleteCh)

	return dpCh

}

type Unprocessed struct {
	PKey uuid.UID
}
type PKey []byte

// ScanForDPitems fetches candiate items to which DP will be applied. Items fetched in batches and sent on channel to be picked up by main process DP loop.
func ScanForDPitems(ty string, dpCh chan<- uuid.UID, DPbatchCompleteCh <-chan struct{}) {

	// Option 1: uses an index query. For restartability purposes we mark each processed item, in this case by deleting its IX attribute, which has the
	//           affect of removing the item from the index. The index therefore will contain only unprocessed items.
	//
	// 1. Fetch a batch (200) of items from TyIX (based on Ty & IX)
	// 2. for each item
	//   2.1   DP it.
	//   2.2   Post DP: for restartability and identify those items in the tbase that have already been processed,
	//         remove IX attribute from assoicated item (using PKey, Sortk). This has the affect of removing entry from TyIX index.
	// 3. Go To 1.
	//
	// the cose of restartability can be measured in step 2.2 in terms of WCUs for each processed item.
	//
	// Option 2: replace index query with a paginated scan.
	//
	// 1. Paginated scan of index. Use page size of 200 items (say)
	// 2. DP it.
	// 3. Go To 1.
	//
	// The great advantage of the paginated scan is it eliminates the expensive writes of step 2.2 in option 1. Its costs is in more RCUs - so its a matter of determining
	// whether more RCUs is worth it. This will depend on the number of items fetched to items scanned ratio. If it is less than 25% the paginated scan is more efficient.
	// However the granularity of restartability is now much larger, at the size of a page of items rather than the individual items in option 1.
	// This will force some items to be reprocessed in the case of restarting after a failed run. Is this an issue?
	// As DP uses a PUT (merge mutation will force all mutations into original put mutation) it will not matter, as put will overwrite existing item(s) - equiv to SQL's merge.
	// The cost is now in the processing of items that have already been processed after a failed run.
	// For the case of no failure the cost is zero, whereas in option 1, the cost remains the same for a failured run as a non-failed run (step 2.2)
	// So paginated scan is much more efficient for zero failure runs, which will be the case for the majority of executions.

	var (
		logid = "ScanForDPitems"
		stx   *tx.QHandle
		err   error
		b     int
	)

	defer close(dpCh)

	for {

		slog.Log(logid, fmt.Sprintf("ScanForDPitems for type %q started. Batch %d", ty, b))

		rec := []Unprocessed{}

		stx = tx.NewQuery2("dpScan", tbl.Block, "TyIX")
		if err != nil {
			close(dpCh)
			elog.Add(logid, err)
			return
		}
		stx.Select(&rec).Key("Ty", types.GraphSN()+"|"+ty).Key("IX", "X").Limit(param.DPbatch).Consistent(false)

		err = stx.Execute()
		if err != nil {
			elog.Add(logid, err)
			break
		}

		for _, v := range rec {
			dpCh <- v.PKey
		}

		// exit loop when #fetched items less than limit
		if len(rec) < param.DPbatch {
			slog.LogAlert(logid, fmt.Sprintf("#items %q exiting fetch loop", ty))
			break
		}
		b++

		slog.LogAlert(logid, "Waiting on last DP to finish..")
		<-DPbatchCompleteCh
		slog.LogAlert(logid, "Waiting on last DP to finish..Done")

	}
	slog.LogAlert(logid, fmt.Sprintf("About to close dpCh "))

}
