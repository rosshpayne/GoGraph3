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
	"github.com/GoGraph/cache"
	dyn "github.com/GoGraph/db"
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
	"github.com/GoGraph/tx/db"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/tx/query"
	"github.com/GoGraph/types"
	"github.com/GoGraph/uuid"

	"github.com/GoGraph/mysql"
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

type Unprocessed struct {
	PKey uuid.UID
	Ty   string // query.NullString
}
type PKey []byte

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

		ls     string
		status string
	)

	//start syslog services (if any)
	err = slog.Start()
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
			cancel()
			syslog(`Terminated....set run status to "S"`)
			err = setRunStatus(nil, "S", nil)
			if err != nil {
				fmt.Printf("Error setting load status: %s\n", err)
			}
			wpEnd.Wait()

			tend := time.Now()
			syslog(fmt.Sprintf("Terminated.....Duration: %s", tend.Sub(tstart).String()))
			os.Exit(2)
		}
	}()

	// TODO: how to register default database from app rather than inside Init

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "scan", Val: db.Enabled}, db.Option{Name: "throttler", Val: grmgr.Control}, db.Option{Name: "Region", Val: "us-east-1"}}...)
	mysql.Register(ctx, "mysql-GoGraph", "admin:gjIe8Hl9SFD1g3ahyu6F@tcp(mysql8.cjegagpjwjyi.us-east-1.rds.amazonaws.com:3306)/GoGraph")

	//	tbl.Register("pgState", "Id", "Name")
	// following tables are in MySQL - should not need to be registered as its a dynamodb requirement.
	// TODO: use introspection in dynmaodb so tables do not need to be registered
	// tbl.Register("Run$Operation", "Graph")
	// tbl.Register("Run$Run", "RunId")
	// tbl.Register("Run$State", "RunId")

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
			//restart = true
			err = setRunStatus(ctx, "R", nil)
			if err != nil {
				fmt.Printf("Error setting load status: %s\n", err)
				return
			}
		}
		err = addRun(ctx, stateId, runid)
		if err != nil {
			elog.Add(logid, fmt.Errorf("Error in addRun(): %w", err))
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
	//
	// start services
	//
	wpEnd.Add(3)
	wpStart.Add(3)
	go grmgr.PowerOn(ctx, &wpStart, &wpEnd, runid) // concurrent goroutine manager service
	go errlog.PowerOn(ctx, &wpStart, &wpEnd)       // error logging service
	go monitor.PowerOn(ctx, &wpStart, &wpEnd)      // repository of system statistics service
	wpStart.Wait()

	// setup db related services (e.g. stats snapshot save)
	dbadmin.Setup()

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
	// not strictly necessary for this version of main but used to keep in sync
	// with other versions (dp1 etc)
	//dpTy.Sort()

	var wgc sync.WaitGroup
	limiterDP := grmgr.New("dp", *parallel)

	for k, s := range dpTy {
		syslog(fmt.Sprintf(" Type containing 1:1 type: %d, %s", k, s))
	}
	if len(dpTy) == 0 {
		syslog(fmt.Sprintf(" No 1:1 Types found"))
		return
	}
	syslog(fmt.Sprintf("Start double propagation processing...%#v", dpTy))

	// allocate cache for node data
	cache.NewCache()

	tstart = time.Now()
	//
	// traditional client-server pattern for database access
	//  forever {
	//   get data from db
	//   if no-data-found {
	// 	     break
	//   }
	//   process data
	//  }
	for _, ty := range dpTy {

		ty := ty
		var (
			ty_ string
			uid uuid.UID
		)
		for {

			unproc, err := queryForDP(ctx, ty)
			if err != nil {
				break
			}

			for _, u := range unproc {

				wgc.Add(1)
				uid = u.PKey
				// if u.Ty.Valid {
				// 	// ty_ = u.Ty[strings.Index(u.Ty, "|")+1:]
				// 	ty_ = u.Ty.String[strings.Index(u.Ty.String, "|")+1:]
				// }
				ty_ = u.Ty[strings.Index(u.Ty, "|")+1:]
				limiterDP.Ask()
				<-limiterDP.RespCh()

				go Propagate(ctx, limiterDP, &wgc, uid, ty_, has11)

				// check for errors based on logid
			}
			wgc.Wait()
			time.Sleep(500 * time.Millisecond) // wait for all dynamodb distributed writes to complete. Required for index scan (no fullconsistency)
		}
		if err != nil {
			if !errors.Is(query.NoDataFoundErr, err) {
				elog.Add(logid, err)
			}
			wgc.Wait()
			break
		}
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
	err = setRunStatus(ctx, status, err)
	syslog(fmt.Sprintf("setLoadstatus...."))
	if err != nil {
		elog.Add("SetLoadStatus", fmt.Errorf("Error setting load status to %s: %w", status, err))
	}
	syslog("Cancel initiated. Waiting for DP services to shutdown...")
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
	dbadmin.Persist()

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

// queryForDP fetches candiate items to which DP will be applied. Items fetched in batches and sent on channel to be picked up by main process DP loop.
func queryForDP(ctx context.Context, ty string) ([]Unprocessed, error) {

	var (
		logid = "queryForDP"
		stx   *tx.QHandle
		err   error
	)

	slog.LogAlert(logid, fmt.Sprintf("queryForDP for type %q started.", ty))

	rec := []Unprocessed{}

	stx = tx.NewQueryContext(ctx, "dpScan", tbl.Block, "TyIX")
	if err != nil {
		return nil, err
	}
	stx.Select(&rec).Key("Ty", types.GraphSN()+"|"+ty).Key("IX", "X").Limit(param.DPbatch)

	err = stx.Execute()
	if err != nil {
		if errors.Is(query.NoDataFoundErr, err) {
			slog.LogAlert(logid, fmt.Sprintf("No data found for type %q.", ty))
		}
		return nil, err
	}

	return rec, nil

}
