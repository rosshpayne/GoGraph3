package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	//"github.com/GoGraph/attach/anmgr"
	//"github.com/GoGraph/block"
	"github.com/GoGraph/cache"
	//	dyn "github.com/GoGraph/db"
	// dbadmin "github.com/GoGraph/db/admin"
	param "github.com/GoGraph/dygparam"
	"github.com/GoGraph/errlog"
	elog "github.com/GoGraph/errlog"
	"github.com/GoGraph/grmgr"
	"github.com/GoGraph/monitor"
	"github.com/GoGraph/run"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/tbl"

	"github.com/GoGraph/tx"
	"github.com/GoGraph/tx/db"
	dyn "github.com/GoGraph/tx/dynamodb"
	dbadmin "github.com/GoGraph/tx/dynamodb/admin"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/tx/mysql"
	"github.com/GoGraph/tx/query"
	"github.com/GoGraph/tx/uuid"

	//"github.com/GoGraph/mysql"
	"github.com/GoGraph/types"
)

const (
	logid = param.Logid
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

type UnprocRec struct {
	PKey uuid.UID
	Ty   string
}

var (
	runId int64
)

func syslog(s string) {
	slog.Log(logid, s)
}

func alertlog(s string) {
	slog.LogAlert(logid, s)
}

func logerr(id string, e error) {
	elog.Add(logid, e)
}

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

		tyState string
	)

	//start syslog services (if any)
	err = slog.Start()
	syslog(fmt.Sprintf("Argument: table: %s", *table))
	if err != nil {
		panic(fmt.Errorf("Error starting syslog services: %w", err))
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

			syslog(`Terminated....set run status to "S"`)
			err = setRunStatus(nil, "S", nil)
			if err != nil {
				fmt.Printf("Error setting load status: %s\n", err)
			}

			tend := time.Now()
			syslog(fmt.Sprintf("Terminated.....Duration: %s", tend.Sub(tstart).String()))
			cancel()
			wpEnd.Wait()
			os.Exit(2)
		}
	}()

	// register  databases
	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "scan", Val: db.Enabled}, db.Option{Name: "throttler", Val: grmgr.Control}, db.Option{Name: "Region", Val: "us-east-1"}}...)
	mysql.Register(ctx, "mysql-GoGraph", os.Getenv("MYSQL")+"/GoGraph")

	logrmDB := slog.NewLogr("mdb")
	logrmGr := slog.NewLogr("grmgr")

	tx.SetLogger(logrmDB) //, tx.Alert)
	grmgr.SetLogger(logrmGr)
	//tx.SetLogger(logrmDB, tx.NoLog)

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

	// each run has its own run identifer which allows each run can to have its own runstats (runid is the key).
	// The very first run stores the runid as it a stateId, to which all state data is associated for this and all subsequent runs,
	// until the run is marked as coomplete.
	runid, err = run.New(logid, "dp")
	if err != nil {
		fmt.Println(fmt.Sprintf("Error in  MakeRunId() : %s", err))
		return
	}
	//
	// start services
	//
	wpEnd.Add(3)
	wpStart.Add(3)
	//grCfg := grmgr.Config{"dbname": "default", "table": "runstats", "runid": runid}
	grCfg := grmgr.Config{"runid": runid}
	go grmgr.PowerOn(ctx, &wpStart, &wpEnd, grCfg) // concurrent goroutine manager service
	go errlog.PowerOn(ctx, &wpStart, &wpEnd)       // error logging service
	//go anmgr.PowerOn(ctx, &wpStart, &wpEnd)        // attach node service
	go monitor.PowerOn(ctx, &wpStart, &wpEnd) // repository of system statistics service
	wpStart.Wait()

	logerr := func(id string, e error) {
		elog.Add("mdb", e)
	}
	tx.SetErrLogger(logerr)
	grmgr.SetErrLogger(logerr)

	// set graph and type data
	err = types.SetGraph(*graph)
	if err != nil {
		syslog(fmt.Sprintf("Error in SetGraph: %s ", err.Error()))
		fmt.Printf("Error in SetGraph: %s\n", err)
		return
	}

	// check state of processing, restart?
	ls, stateId, err = getRunStatus(ctx)
	if err != nil {

		if errors.Is(err, query.NoDataFoundErr) {

			// first run...
			err = setRunStatus(ctx, "R", nil, runid)
			if err != nil {
				alertlog(fmt.Sprintf("Error setting load status: %s\n", err))
				fmt.Printf("Error setting load status: %s\n", err)
				return
			}
			stateId = runid

		} else {
			alertlog(fmt.Sprintf("Error in determining load status: %s\n", err))
			fmt.Printf("Error in determining load status: %s\n", err)
			return
		}

	} else {

		switch ls {
		case "C":
			alertlog("Load is already completed. Abort this run.")
			fmt.Println("Load is already completed. Abort this run.")
			return
		case "R":
			alertlog("Currently loading..aborting this run")
			fmt.Println("Currently loading..aborting this run")
			return
		case "S", "E":
			alertlog("Previous load errored or was terminated, will now rerun")
			fmt.Println("Previous load errored or was terminated, will now rerun")
			restart = true
			err = setRunStatus(ctx, "R", nil)
			if err != nil {
				fmt.Printf("Error setting load status: %s\n", err)
				return
			}
		}
		err = addRun(ctx, stateId, runid)
		if err != nil {
			elog.Add("addRun()", err)
			return
		}
	}

	// batch size
	if batchSize != nil {
		if *batchSize > 400 {
			*batchSize = 400
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

	// if param.DB == param.Dynamodb {
	// 	// Regstier index
	// 	tbl.RegisterIndex(tbl.IdxName("TyIX"), tbl.Name("GoGraph"), "Ty", "IX") // Ty prepended with GraphSN()
	// }

	// setup db related services (e.g. stats snapshot save)
	dbadmin.Setup(runid)

	syslog("All services started. Proceed with attach processing")

	has11 := make(map[string]struct{})
	var dpTy sort.StringSlice

	// k: type long name, v: block.TyAttrD{}
	for k, v := range types.TypeC.TyC {
		for _, vv := range v {
			// consider uid-pred attributes only
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
					dpTy = append(dpTy, sn)
				}
			}
		}
	}

	// sort types containing 1:1 attributes and dp process in sort order.
	dpTy.Sort()

	for k, s := range dpTy {
		syslog(fmt.Sprintf(" Type containing 1:1 type: %d, %s", k, s))
	}
	if len(dpTy) == 0 {
		syslog(fmt.Sprintf(" No 1:1 Types found"))
		return
	}
	syslog(fmt.Sprintf("Start double propagation processing...%#v", dpTy))

	if restart {
		tyState, err = getState(ctx, stateId)
		if err != nil {
			alertlog(fmt.Sprintf("Error in getState(): %s", err))
			return
		}
	}

	// allocate cache for node data
	cache.NewCache()

	tstart = time.Now()

	for _, ty := range dpTy {

		if restart && ty < tyState {
			continue
		}
		if restart && ty > tyState {
			setState(ctx, stateId, ty)
		} else if !restart {
			setState(ctx, stateId, ty)
		}

		// blocking call to DP
		err = DP(ctx, ty, stateId, restart, has11)
		if err != nil {
			elog.Add(logid, err)
			break
		}
		restart = false
	}

	//LoadFromStage(ctx, stateId, false)

	//limiterDP.Unregister()
	monitor.Report()
	elog.PrintErrors()
	if elog.RunErrored() {
		status = "E"
	} else {
		status = "C"
	}
	syslog(fmt.Sprintf("setLoadstatus..to %s", status))
	err = setRunStatus(ctx, status, err)

	if err != nil {
		elog.Add("SetLoadStatus", fmt.Errorf("Error setting load status to %s: %w", status, err))
	}
	syslog("cancel() initiated. Waiting for DP services to shutdown...")
	cancel()
	wpEnd.Wait()
	syslog("All DP services shutdown.")

	run.Finish(err)
	tend := time.Now()

	syslog(fmt.Sprintf("double propagate finished....Runid:  %q   Duration: %s", runid.Base64(), tend.Sub(tstart)))
	time.Sleep(1 * time.Second)

	// stop system logger services (if any)
	slog.Stop()
	// stop db admin services and save to db.
	dbadmin.Persist(runid)

}

//type PKey []byte

func getRunStatus(ctx context.Context) (string, uuid.UID, error) {

	var err error

	type Status struct {
		Status string
		RunId  []byte
	}
	var status Status
	opt := db.Option{Name: "singlerow", Val: true}
	// check if ES load completed
	ftx := tx.NewQueryContext(ctx, "GetRunStatus", "Run$Operation").DB("mysql-goGraph", []db.Option{opt}...)
	ftx.Select(&status).Key("Graph", types.GraphSN()).Key("TableName", *table).Key("Operation", "DP") // other values: "E","R"

	err = ftx.Execute()
	if err != nil {
		return "", nil, err
	}
	alertlog(fmt.Sprintf("getRunStatus: run$state - Operation: DP  status: %s   runid: %s", status.Status, uuid.UID(status.RunId).Base64()))
	return status.Status, status.RunId, nil
}

func setRunStatus(ctx context.Context, status string, err_ error, runid ...uuid.UID) error {

	var err error

	if strings.IndexAny(status, "ERSC") == -1 {
		panic(fmt.Errorf("setRunStatus : value is empty"))
	}
	// runid supplied if it is the first time - ie. perform an insert
	switch len(runid) > 0 {

	case true: // first run
		ftx := tx.New("setRunStatus").DB("mysql-goGraph")
		m := ftx.NewInsert("Run$Operation").AddMember("Graph", types.GraphSN(), mut.IsKey).AddMember("TableName", *table).AddMember("Operation", "DP", mut.IsKey).AddMember("Status", status).AddMember("RunId", runid[0])
		m.AddMember("Created", "$CURRENT_TIMESTAMP$")
		err = ftx.Execute()
		if err != nil {
			return err
		}

	case false: // restart
		// merge, preserving original runid which also happens to be the stateId used by the tx package for paginated queries.
		ftx := tx.New("setRunStatus").DB("mysql-goGraph")
		ftx.NewMerge("Run$Operation").AddMember("Graph", types.GraphSN(), mut.IsKey).AddMember("TableName", *table).AddMember("Operation", "DP", mut.IsKey).AddMember("Status", status).AddMember("LastUpdated", "$CURRENT_TIMESTAMP$")
		err = ftx.Execute()
		if err != nil {
			return err
		}
	}

	return nil

}

func getState(ctx context.Context, runid uuid.UID) (string, error) {

	var err error

	type Status struct {
		Value string
	}
	var status Status
	opt := db.Option{Name: "singlerow", Val: true}
	// check if ES load completed
	ftx := tx.NewQueryContext(ctx, "getState", "Run$State").DB("mysql-goGraph", []db.Option{opt}...)
	ftx.Select(&status).Key("RunId", runid).Key("Name", "Type")

	err = ftx.Execute()
	if err != nil {
		alertlog(fmt.Sprintf("getState: RunId: %s ", runid.Base64()))
		return "", err
	}
	alertlog(fmt.Sprintf("getState: RunId: %s  TypeName:  %q", runid, status.Value))
	return status.Value, nil
}

func setState(ctx context.Context, stateId uuid.UID, ty string) error {

	var err error
	alertlog(fmt.Sprintf("setState: RunId: %s  TypeName:  %q", stateId, ty))
	ftx := tx.New("setState").DB("mysql-goGraph")
	ftx.NewMerge("Run$State").AddMember("RunId", stateId, mut.IsKey).AddMember("Name", "Type").AddMember("Value", ty).AddMember("LastUpdated", "$CURRENT_TIMESTAMP$")
	err = ftx.Execute()
	if err != nil {
		return err
	}
	return nil
}

func addRun(ctx context.Context, stateid, runid uuid.UID) error {

	var err error
	alertlog(fmt.Sprintf("addRun: StateId: %s  RunId:  %q", stateid.Base64(), runid.Base64()))
	ftx := tx.New("addRun").DB("mysql-goGraph")
	ftx.NewInsert("Run$Run").AddMember("RunId", stateid, mut.IsKey).AddMember("Associated_RunId", runid, mut.IsKey).AddMember("Created", "$CURRENT_TIMESTAMP$")
	err = ftx.Execute()
	if err != nil {
		return err
	}
	return nil
}

// DP passes a propagate function to index query using key with node type
func DP(ctx context.Context, ty string, id uuid.UID, restart bool, has11 map[string]struct{}) error {

	var buf1, buf2 []UnprocRec

	slog.LogAlert(logid, fmt.Sprintf("started. paginate id: %s. restart: %v, grmgr-parallel: %d ", id.Base64(), restart, *parallel))

	ptx := tx.NewQueryContext(ctx, "dpQuery", tbl.Block, "TyIX")
	ptx.Select(&buf1, &buf2).Key("Ty", types.GraphSN()+"|"+ty)
	ptx.Limit(param.DPbatch).Paginate(id, restart) // not a scan so no worker possible

	limiterDP := grmgr.New("dp", *parallel)

	// blocking call..
	err := ptx.ExecuteByFunc(func(ch_ interface{}) error {

		ch := ch_.(chan []UnprocRec)
		var dpWg sync.WaitGroup

		for qs := range ch {
			// page (aka buffer) of UnprocRec{}

			for _, u := range qs {

				ty := u.Ty[strings.Index(u.Ty, "|")+1:]
				dpWg.Add(1)

				limiterDP.Ask()
				<-limiterDP.RespCh()

				go Propagate(ctx, limiterDP, &dpWg, u.PKey, ty, has11)

				if elog.Errors() {
					panic(fmt.Errorf("Error in an asynchronous routine - see system log"))
				}
			}
			// wait for dp ascyn routines to finish
			// alternate solution, extend to three bind variables. Requires MethodDB change.
			dpWg.Wait()
		}
		return nil
	})

	limiterDP.Unregister()

	return err

}
