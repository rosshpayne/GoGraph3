package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"sync"
	"time"

	//"github.com/GoGraph/attach/anmgr"
	"github.com/GoGraph/cache"
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
	"github.com/GoGraph/types"
	"github.com/GoGraph/uuid"
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

var runId int64

func syslog(s string) {
	slog.Log(logid, s)
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
		runid          uuid.UID
	)

	// allocate a run id
	// allocate a run id

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
	//
	// set graph to use
	//
	if len(*graph) == 0 {
		fmt.Printf("Must supply a graph name\n")
		flag.PrintDefaults()
		return
	}

	// create a runid in preparation for starting syslog service
	runid, err = run.New(logid, "dp")
	if err != nil {
		fmt.Println(fmt.Sprintf("Error in  MakeRunId() : %s", err))
		return
	}
	defer run.Finish(err)

	// batch size
	if batchSize != nil {
		if *batchSize > 200 {
			*batchSize = 200
		}
		param.DPbatch = *batchSize
	}
	//start syslog services (if any)
	err = slog.Start()
	if err != nil {
		panic(fmt.Errorf("Error starting syslog services: %w", err))
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

	syslog(fmt.Sprintf("runid: %v", runid))

	ctx, cancel := context.WithCancel(context.Background())

	if param.DB == param.Dynamodb {
		//
		// Regstier index
		//
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

	err = types.SetGraph(*graph)
	if err != nil {
		syslog(fmt.Sprintf("Error in SetGraph: %s ", err.Error()))
		fmt.Printf("Error in SetGraph: %s\n", err)
		return
	}

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

	for k, _ := range dpTy {
		syslog(fmt.Sprintf(" Type containing 1:1 type: %s", k))
	}
	if len(dpTy) == 0 {
		syslog(fmt.Sprintf(" No 1:1 Types found"))
	}
	syslog(fmt.Sprintf("Start double propagation processing...%#v", dpTy))

	// allocate cache for node data
	cache.NewCache()

	t0 := time.Now()
	for ty, _ := range dpTy {

		ty := ty

		// loop until channel closed, proceed to next type
		for n := range FetchNodeCh(ty) {

			wgc.Add(1)
			n := n
			limiterDP.Ask()
			<-limiterDP.RespCh()

			go Propagate(limiterDP, &wgc, n, ty, has11)
		}
		fmt.Println("about to Wait()")
		wgc.Wait()
		fmt.Println("about to Wait() - Pass")

	}

	t1 := time.Now()
	limiterDP.Unregister()
	monitor.Report()
	printErrors()
	cancel()
	wpEnd.Wait()

	// stop db admin services and save to db.
	dbadmin.Persist()

	syslog(fmt.Sprintf("double propagate processing finished. Duration: %s", t1.Sub(t0)))

}

func FetchNodeCh(ty string) <-chan uuid.UID {

	dpCh := make(chan uuid.UID) // no buffer to reduce likelihood of doube dp processing.

	go ScanForDPitems(ty, dpCh)

	return dpCh

}

type Unprocessed struct {
	PKey uuid.UID
}
type PKey []byte

func ScanForDPitems(ty string, dpCh chan<- uuid.UID) {

	// load all type ty data into all slice.
	var (
		stx *tx.QHandle
		err error
		b   int
	)

	// a SCAN operation was considered but would be potentially quite inefficient as the candidate data may represent only a small part
	// of the overal table data. In this respect an Index scan is more efficient as only the candidate items are read.
	// TODO: implement SCAN code in db package.

	for {

		slog.Log("ScanForDPitems:", fmt.Sprintf("ScanForDPitems for type %q started. Batch %d", ty, b))
		rec := []Unprocessed{}
		stx = tx.NewQuery(tbl.Block, "dpScan", "TyIX")
		if err != nil {
			close(dpCh)
			elog.Add("ScanForDPitems", err)
			return
		}
		stx.Select(&rec).Key("Ty", types.GraphSN()+"|"+ty).Key("IX", "X").Limit(param.DPbatch).Consistent(false) // GSI (not LSI) cannot have consistent reads. TODO: need something to detect GSI.

		err = stx.Execute()
		if err != nil {
			panic(err)
		}

		for _, v := range rec {
			// channel has no buffer so for loop is synchronised with DP processing, ie. loop exits on last DP operation which means
			// the subsequent query  will only be executed after all DP operations have finished - except for very last few perhaps.
			// These still have a chance of being reprocessed, but as DP is idempotent it is data safe. Sleep below is designed to minimise repeat DPs.
			// Keeping param.DPbatch relatively large means the proportion of repeat DPs is kept very small.
			// param.DPbatch of 50 (with a  dpCh buffer of 10) showed between 16% and 20% of data was reprocessed.
			// param.DPbatch of 500 with for dpCh buffer of 0 should reduce this to near zero.
			dpCh <- v.PKey
		}

		if len(rec) < param.DPbatch {
			slog.Log("ScanForDPitems:", fmt.Sprintf("ScanForDPitems for %q exiting", ty))
			break
		}
		b++
		// wait for last dp's to synchronise their updates
		// since dp is an idempotent operation (ie. uses put's only) it doesn't matter if a dp operation is performed twice.
		// overhead of sleep is minimised by using a large param.DPbatch value (500 or more)
		time.Sleep(500 * time.Millisecond)
	}
	// delay close of channel giving time for dp goroutines to start and wgc counter to increment.
	// without this delay the wgc.Wait can be indefinite.
	slog.Log("ScanForDPitems:", fmt.Sprintf("About to close dpCh - after 500ms wait"))
	time.Sleep(500 * time.Millisecond)

	close(dpCh)

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
