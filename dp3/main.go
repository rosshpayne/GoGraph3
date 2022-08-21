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
	//"github.com/GoGraph/block"
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
	tx "github.com/GoGraph/tx"
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

type unprocBuf struct {
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

		stateId  uuid.UID
		restart  bool
		opStatus string

		tyState string
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
			err = setOpStatus(nil, "S", nil)
			if err != nil {
				fmt.Printf("Error setting DP status: %s\n", err)
			}
			wpEnd.Wait()

			tend := time.Now()
			syslog(fmt.Sprintf("Terminated.....Duration: %s", tend.Sub(tstart).String()))
			os.Exit(2)
		}
	}()

	// register default database client
	db.Init(ctx, &wpEnd, []db.Option{db.Option{Name: "throttler", Val: grmgr.Control}, db.Option{Name: "Region", Val: "us-east-1"}}...)
	mysql.Init(ctx)

	//	tbl.Register("pgState", "Id", "Name")
	// following tables are in MySQL - should not need to be registered as its a dynamodb requirement.
	// TODO: use introspection in dynmaodb so tables do not need to be registered
	// tbl.Register("run$Op", "Graph")
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
	opStatus, stateId, err = getOpStatus(ctx)
	if err != nil {

		if errors.Is(err, query.NoDataFoundErr) {

			// first run...
			err = setOpStatus(ctx, "R", nil, runid)
			if err != nil {
				alertlog(fmt.Sprintf("Error setting DP status: %s\n", err))
				fmt.Printf("Error setting DP status: %s\n", err)
				return
			}
			stateId = runid

		} else {
			alertlog(fmt.Sprintf("Error in determining DP status: %s\n", err))
			return
		}

	} else {

		switch opStatus {
		// case "C":
		// 	alertlog("Load is already completed. Abort this run.")
		// 	fmt.Println("Load is already completed. Abort this run.")
		// 	return
		case "C":
			alertlog("DP can be run as minimum interval has elapsedd. ")
			fmt.Println("DP can be run as minimum interval has elapsedd. ")
			err = setOpStatus(ctx, "R", nil, runid)
			stateId = runid
		case "R":
			alertlog("Currently loading..aborting this run")
			fmt.Println("Currently loading..aborting this run")
			return
		case "S", "E":
			alertlog("Previous DP errored or was terminated, will now rerun")
			fmt.Println("Previous DP errored or was terminated, will now rerun")
			restart = true
			err = setOpStatus(ctx, "R", nil)
		}
		if err != nil {
			fmt.Printf("Error setting DP status: %s\n", err)
			return
		}
		err = addRun(ctx, stateId, runid)
		if err != nil {
			elog.Add(fmt.Sprintf("Error in addRun(): %s", err))
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
	//go anmgr.PowerOn(ctx, &wpStart, &wpEnd)        // attach node service
	go monitor.PowerOn(ctx, &wpStart, &wpEnd) // repository of system statistics service
	wpStart.Wait()

	// setup db related services (e.g. stats snapshot save)
	dbadmin.Setup()

	syslog("All services started. Proceed with attach processing")

	has11 := make(map[string]struct{})
	var dpTy []string //sort.StringSlice

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
	//dpTy.Sort()

	//var wgc sync.WaitGroup
	//limiterDP := grmgr.New("dp", *parallel)

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
		fmt.Println("ty: ", tyState)
	}

	// allocate cache for node data
	cache.NewCache()
	var wg sync.WaitGroup

	tstart = time.Now()

	chs, err := UnprocessedCh(ctx, dpTy, stateId, restart) // []chan []unprocBuf

	if err != nil {

		elog.Add("UnprocessedCh", err)

	} else {

		// start read channel services
		for _, ch := range chs {

			wg.Add(1)
			go propagateGR(ctx, &wg, has11, ch)
		}

	}
	wg.Wait()

	monitor.Report()
	elog.PrintErrors()
	if elog.RunErrored() {
		opStatus = "E"
	} else {
		opStatus = "C"
	}
	err = setOpStatus(ctx, opStatus, err)

	if err != nil {
		elog.Add("SetLoadStatus", fmt.Errorf("Error setting operation status to %s: %w", opStatus, err))
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

func propagateGR(ctx context.Context, wg *sync.WaitGroup, has11 map[string]struct{}, ch <-chan []unprocBuf) {

	defer wg.Done()

	for un := range ch {

		for _, u := range un {

			pkey := u.PKey
			ty := u.Ty[strings.Index(u.Ty, "|")+1:]

			Propagate(ctx, pkey, ty, has11)
		}
	}
}

//type PKey []byte

func getOpStatus(ctx context.Context) (string, uuid.UID, error) {

	var err error

	type Oper struct {
		Id      string
		Name    string
		Enabled string
	}

	opt := db.Option{Name: "singlerow", Val: true}

	var oper Oper
	// check if operation exists and is enabled.
	ftx := tx.NewQueryContext(ctx, "OpEnabled", "mtn$Op").DB("mysql-goGraph", []db.Option{opt}...)
	ftx.Select(&oper).Key("Id", "DP")

	err = ftx.Execute()

	if err != nil {
		if errors.Is(err, query.NoDataFoundErr) {
			return "", nil, fmt.Errorf("Operation identifer %q not found", oper.Id)
		}
		fmt.Println("err: ", err)
		return "", nil, err
	}

	if strings.ToUpper(oper.Enabled) != "Y" {
		return "", nil, fmt.Errorf("Operation %q is disabled", oper.Name)
	}
	alertlog(fmt.Sprintf("getOpStatus: Oper query Enabled: %s", oper.Enabled))

	type Status struct {
		Created string
		Status  string
		RunId   []byte
	}
	var status Status

	//  view run$Op_Last_v also does the following = just to show you can specify in MethodsDB
	etx := tx.NewQueryContext(ctx, "GetRunStatus", "run$Op_Status_v").DB("mysql-goGraph", []db.Option{opt}...)
	etx.Select(&status).Key("GraphId", types.GraphSN()).Key("OpId", oper.Id).OrderBy("Created", query.Desc).Limit(1)

	err = etx.Execute()

	fmt.Println("Last run: ", status.Created, uuid.UID(status.RunId).Base64()) // 2022-08-20 01:08:45

	if err != nil {
		if errors.Is(err, query.NoDataFoundErr) {
			return "", nil, fmt.Errorf("A %q run has yet to have executed. %w", oper.Id, err)
		}
		return "", nil, err
	}

	if status.Status == "R" {
		return "", nil, fmt.Errorf("Operation is currently running")
	}

	statusCreated = status.Created
	fmt.Println("status.Created ", status.Created)

	runStart, err = time.Parse("2006-01-02 15:04:05", status.Created) //2022-08-20 01:08:45

	if err != nil {
		panic(err)
	}
	fmt.Println("runStart: ", runStart.Format("2006-01-02 15:04:05"))

	// type Pass struct {
	// 	OpId        string
	// 	LastUpdated string
	// }
	// var pass Pass

	// stx := tx.NewQueryContext(ctx, "GetRunStatus", "run$Op_PastInterval_v").DB("mysql-goGraph", []db.Option{opt}...)
	// stx.Select(&pass).Key("GraphId", types.GraphSN()).Key("OpId", oper.Id).Key("Created", status.Created) // runStart.Format("2006-01-02 15:04:05")) // status.Created)

	// err = stx.Execute()

	// if err != nil {
	// 	if errors.Is(err, query.NoDataFoundErr) {
	// 		return "", nil, fmt.Errorf("Cannot run operation as required interval after last run has not passed.")
	// 	}
	// 	return "", nil, err
	// }

	// if strings.ToUpper(oper.Enabled) != "Y" {
	// 	return "", nil, fmt.Errorf("Operation %q is disabled", pass.OpId)
	// }
	// alertlog(fmt.Sprintf("getOpStatus: run$state - Operation: DP  status: %s   runid: %s", status.Status, uuid.UID(status.RunId).Base64()))

	//
	// equivalent to last query but now using MethodDB rather than PastInterval view to do the required logic check.
	//
	type Pass2 struct {
		OpId        string
		C           string `literal:"DATE_SUB(NOW(), Interval MinIntervalDaySecond DAY_SECOND)"`
		LastUpdated string
	}
	var pass2 Pass2
	atx := tx.NewQueryContext(ctx, "GetRunStatus", "run$Op_Status_v").DB("mysql-goGraph", []db.Option{opt}...)
	atx.Select(&pass2).Key("GraphId", types.GraphSN()).Key("OpId", oper.Id).Key("Created", status.Created).Filter("C", "LastUpdated", "GT")

	err = atx.Execute()

	if err != nil {
		if errors.Is(err, query.NoDataFoundErr) {
			return "", nil, fmt.Errorf("Cannot run operation as required interval after last run has not passed. ")
		}
		return "", nil, err
	}

	alertlog(fmt.Sprintf("getOpStatus:  2 run$state - Operation: DP  status: %s   runid: %s", status.Status, uuid.UID(status.RunId).Base64()))

	return status.Status, status.RunId, nil
}

var runStart time.Time
var statusCreated string

func setOpStatus(ctx context.Context, status string, err_ error, runid ...uuid.UID) error {

	var err error

	if strings.IndexAny(status, "ERSC") == -1 {
		panic(fmt.Errorf("setOpStatus : value is empty"))
	}

	// runid supplied if it is the first time - ie. perform an insert
	switch len(runid) > 0 {

	case true: // first run

		runStart = time.Now()
		fmt.Println("runStart: ", runStart.Format("2006-01-02 15:04:05")) //  runStart.Format("2006-01-02 15:04:05"))
		ftx := tx.New("setOpStatus").DB("mysql-goGraph")
		m := ftx.NewInsert("run$Op").AddMember("GraphId", types.GraphSN(), mut.IsKey).AddMember("OpId", "DP", mut.IsKey).AddMember("Status", status).AddMember("RunId", runid[0])
		m.AddMember("Created", runStart.Format("2006-01-02 15:04:05"))
		err = ftx.Execute()
		if err != nil {
			return err
		}

	case false:
		// merge, preserving original runid which also happens to be the stateId used by the tx package for paginated queries.
		fmt.Println("Set Op Status................................................")
		ftx := tx.New("setOpStatus").DB("mysql-goGraph")
		ftx.NewMerge("run$Op").AddMember("GraphId", types.GraphSN(), mut.IsKey).AddMember("OpId", "DP", mut.IsKey).AddMember("Created", runStart.Format("2006-01-02 15:04:05")).AddMember("Status", status).AddMember("LastUpdated", "$CURRENT_TIMESTAMP$")
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
	// check if ES DP completed
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

// func UnprocessedCh(ctx context.Context, dpty []string, stateId uuid.UID, restart bool) <-chan []unprocBuf {

// 	dpCh := make(chan []unprocBuf, 1) // NB: only use 0 or 1 for buffer size

// 	go ScanForDPitems(ctx, dpty, dpCh, stateId, restart)

// 	return dpCh

// }

// ScanForDPitems scans index for all interested ty and returns items in channel. Executed once.
func UnprocessedCh(ctx context.Context, dpTy []string, id uuid.UID, restart bool) ([]chan []unprocBuf, error) {

	var (
		buf1, buf2 []unprocBuf

		logid = "ScanForDPitems"
	)

	slog.Log(logid, fmt.Sprintf("started. paginate id: %s. restart: %v", id.Base64(), restart))

	bufs := [2][]unprocBuf{buf1, buf2}

	ptx := tx.NewQueryContext(ctx, "dpScan", tbl.Block, "TyIX") // tbl.Block, "TyIX")
	ptx.Select(&bufs[0], &bufs[1])

	// Note: for scan operations do not inlude Key, use Filter only. Also perform one scan not multiple if possible
	// which requires all filter data to be nclude in the one step hence "OrFilter"
	for i, k := range dpTy {
		if i == 0 {
			ptx.Filter("Ty", types.GraphSN()+"|"+k)
		} else {
			ptx.OrFilter("Ty", types.GraphSN()+"|"+k)
		}
	}
	// use limit to batch the fetch for restarting purposes. Remember the PGstate data is stored back to dynamodb after limit is reached-i.e. last Operation of Execute()
	// if no paginate is used then granularity of restart is the whole table/index - not good.
	ptx.Limit(param.DPbatch).Paginate(id, restart).Parallel(*parallel)

	chs, err := ptx.ExecuteByChannel()

	if err != nil {
		return nil, err
	}

	chs_, ok := chs.([]chan []unprocBuf)
	if !ok {
		return nil, fmt.Errorf(`Error in type assertion of channel returned by ExecuteByChannel. Expected "[]chan []unprocBuf, got %T`, chs)
	}

	return chs_, nil

}
