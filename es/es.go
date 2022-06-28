package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/GoGraph/db"
	param "github.com/GoGraph/dygparam"
	elog "github.com/GoGraph/errlog"
	"github.com/GoGraph/grmgr"
	"github.com/GoGraph/run"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/tx/query"
	"github.com/GoGraph/types"
	"github.com/GoGraph/uuid"

	esv7 "github.com/elastic/go-elasticsearch/v7"
	esapi "github.com/elastic/go-elasticsearch/v7/esapi"
	//	esv8 "github.com/elastic/go-elasticsearch/v8"

	"github.com/GoGraph/mysql"
)

const (
	logid  = "ElasticSearch: "
	loadId = "ES Load"
)

type logEntry struct {
	d    *Doc // node containing es index type
	pkey []byte
	err  error
	s    chan struct{}
}

type Doc struct {
	Attr  string // <GraphShortName>|<AttrName> e.g. m|title
	Value string
	PKey  string
	SortK string // A#A#:N
	Type  string //
}

type ftrec struct {
	Attr  string
	Ty    string
	Sortk string
}

var debug = flag.Int("debug", 0, "Enable full logging [ 1: enable] 0: disable")
var parallel = flag.Int("c", 3, "# ES loaders")
var graph = flag.String("g", "", "Graph: ")
var showsql = flag.Int("sql", 0, "Show generated SQL [1: enable 0: disable]")
var reduceLog = flag.Int("rlog", 1, "Reduced Logging [1: enable 0: disable]")
var table = flag.String("tbl", string(tbl.TblName), "Graph Table from which other table names are derived")
var esidx = flag.String("idx", "", "ES index name [no default]")
var batch = flag.Int("batch", 500, "Batch size used for scan of items to load into ES. [Default 500]")

func logerr(e error, panic_ ...bool) {

	if len(panic_) > 0 && panic_[0] {
		slog.Log(logid, e.Error(), true)
		panic(e)
	}
	slog.Log(logid, e.Error())
}

func syslog(s string) {
	slog.Log(logid, s)
}

type rec struct {
	PKey  uuid.UID
	SortK string
	S     string
	Ty    string
	//LastEvaluatedKey map[string]dynamodb.AtributeValue
}

func main() {
	// determine types which reference types that have a cardinality of 1:1
	flag.Parse()
	param.DebugOn = true

	fmt.Printf("Argument: concurrent: %d\n", *parallel)
	fmt.Printf("Argument: table: %s\n", *table)
	fmt.Printf("Argument: ES index: %s\n", *esidx)
	fmt.Printf("Argument: showsql: %v\n", *showsql)
	fmt.Printf("Argument: debug: %v\n", *debug)
	fmt.Printf("Argument: graph: %s\n", *graph)
	fmt.Printf("Argument: reduced logging: %v\n", *reduceLog)
	fmt.Printf("Argument: batch size: %d\n", *batch)
	var (
		wpEnd, wpStart sync.WaitGroup
		err            error

		stateId uuid.UID
		tstart  time.Time
		restart bool

		ecnt, ecnta int
		eStatus     string
		ls          string

		esloadCnt    int
		ty, sk, attr string
	)

	if len(*esidx) == 0 {
		fmt.Println("Must supply name of ES index")
		flag.PrintDefaults()
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// setup concurrent routine to capture OS signals.
	appSignal := make(chan os.Signal, 3)
	var (
		terminate os.Signal = syscall.SIGTERM // os kill
		interrupt os.Signal = syscall.SIGINT  // ctrl-C
	)
	signal.Notify(appSignal, terminate, interrupt) // TODO: add ctrl-C signal

	go func() {
		select {
		case <-appSignal:
			cancel()
			err = setLoadStatus(nil, loadId, "S", nil)
			if err != nil {
				fmt.Printf("Error setting load status: %s\n", err)
			}
			err = setAttrLoadStatus(nil, ty, sk, "S", attr, esloadCnt)
			if err != nil {
				fmt.Printf("Error setting attribute status: %s\n", err)
			}
			time.Sleep(1 * time.Second)
			wpEnd.Wait()
			tend := time.Now()
			syslog(fmt.Sprintf("Terminated.....Duration: %s", tend.Sub(tstart).String()))
			os.Exit(2)
		}
	}()

	// register default database client
	db.Init(ctx, &wpEnd, []db.Option{db.Option{Name: "throttler", Val: grmgr.Control}, db.Option{Name: "Region", Val: "us-east-1"}}...)
	mysql.Init(ctx)

	// process input arguments
	param.ReducedLog = false
	if *reduceLog == 1 {
		param.ReducedLog = true
	}
	if *showsql == 1 {
		param.ShowSQL = true
	}
	if *debug == 1 {
		param.DebugOn = true
	}
	if tbl.Name(*table) != tbl.TblName {
		tbl.Set(*table)
	}
	tbl.Register("pgState", "Id", "Name")
	tbl.Register("State$", "Graph", "Name")
	tbl.Register("State$ES", "Graph", "Ty", "Sortk")
	tbl.RegisterIndex("TyES", tbl.TblName, "Ty", "E")
	tbl.RegisterIndex("P_S", tbl.TblName, "P", "S")
	tbl.RegisterIndex("Ty-E-index", tbl.TblName, "Ty", "E")

	// set graph (validates it exists)
	if len(*graph) == 0 {
		fmt.Printf("Must supply a graph name\n")
		flag.PrintDefaults()
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

	err = types.SetGraph(*graph)
	if err != nil {
		syslog(fmt.Sprintf("Error in SetGraph: %s ", err.Error()))
		fmt.Printf("Error in SetGraph: %s\n", err)
		return
	}

	// create run identifier
	runid, err := run.New(logid, "es")
	if err != nil {
		fmt.Println(fmt.Sprintf("Error in  MakeRunId() : %s", err))
		return
	}

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
			LoadFTattrs(runid)

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

	syslog(fmt.Sprintf("runid: %s", runid))
	defer run.Finish(err)
	tstart = time.Now()

	//
	// establish connection to elasticsearch
	//
	err = connect()
	if err != nil {
		fmt.Println("Cannot establish connection to elasticsearch: ", err)
		return
	}

	//batch := NewBatch()
	syslog(fmt.Sprintf("Argument: table: %s", *table))
	syslog(fmt.Sprintf("Argument: ES index: %s", *esidx))
	syslog(fmt.Sprintf("Argument: concurrency: %d", *parallel))
	syslog(fmt.Sprintf("Argument: showsql: %v", *showsql))
	syslog(fmt.Sprintf("Argument: debug: %v", *debug))
	syslog(fmt.Sprintf("Argument: graph: %s", *graph))
	syslog(fmt.Sprintf("Argument: reduced logging: %v", *reduceLog))
	syslog(fmt.Sprintf("Argument: batch: %d", *batch))

	// start load services
	wpEnd.Add(2)
	wpStart.Add(2)

	// log es loads to a log - to enable restartability
	//go logit(ctx, &wpStart, &wpEnd, logCh, saveCh, saveAckCh)
	// services
	//go stop.PowerOn(ctx, &wpStart, &wpEnd)    // detect a kill action (?) to terminate program alt just kill-9 it.
	go grmgr.PowerOn(ctx, &wpStart, &wpEnd, runid) // concurrent goroutine manager service
	go elog.PowerOn(ctx, &wpStart, &wpEnd)         // error logging service
	// Dynamodb only: go monitor.PowerOn(ctx, &wpStart, &wpEnd)      // repository of system statistics service
	syslog("Waiting on services to start...")
	wpStart.Wait()

	syslog("All services started. Start load process into ES")

	type sortk = string
	type tySn = string

	lmtrES := grmgr.New("es", *parallel)
	var loadwg sync.WaitGroup

	//	for _, v := range ftAttrs { // drive off state table for states running, todo
	ftAttrs, err := fetchUnprocessedAttrs(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Printf("ftAttrs %#v\n", ftAttrs)
	for _, v := range ftAttrs {

		ty, sk, attr = v.Ty, v.Sortk, v.Attr // populate package vars for terminate purposes
		// mark state Ty,Sortk - running

		fetchCh := make(chan rec, 20)
		esloadCnt = 0
		eStatus = "R"
		err = setAttrLoadStatus(ctx, v.Ty, v.Sortk, eStatus, v.Attr, esloadCnt)

		elog.ErrCntByIdCh <- logid
		ecnt = <-elog.ErrCntRespCh

		go Scan(ctx, batch, v.Ty, v.Sortk, fetchCh, stateId, restart)
		//go ScanParallel(ctx, *parallel, v.Ty, v.Sortk, fetchCh, stateId, restart)

		for r := range fetchCh {

			doc := &Doc{Attr: v.Attr, Value: r.S, PKey: r.PKey.String(), SortK: r.SortK, Type: r.Ty}

			loadwg.Add(1)
			esloadCnt++

			lmtrES.Ask()
			<-lmtrES.RespCh()

			go esLoad(doc, r.PKey, &loadwg, lmtrES)
		}
		syslog(fmt.Sprintf("es: 	loadwg.Wait()"))
		loadwg.Wait()

		// record status of es load for Ty, Sortk combination
		elog.ErrCntByIdCh <- logid
		ecnta = <-elog.ErrCntRespCh

		eStatus = "C"
		if ecnta != ecnt {
			eStatus = "E"
		}
		err = setAttrLoadStatus(ctx, v.Ty, v.Sortk, eStatus, v.Attr, esloadCnt)
		if eStatus != "C" {
			break
		}

		elog.ResetCntCh <- logid
		restart = false

	}
	loadwg.Wait()

	err = setLoadStatus(ctx, loadId, eStatus, err)
	if err != nil {
		elog.Add("SetLoadStatus", err)
	}

	elog.PrintErrors()

	// shutdown support services
	cancel()
	time.Sleep(1 * time.Second)
	wpEnd.Wait()
	tend := time.Now()
	syslog(fmt.Sprintf("Exit.....Duration: %s", tend.Sub(tstart).String()))
	return
}

var (
	cfg esv7.Config
	es  *esv7.Client
	err error
)

func connect() error {

	var logid = "ESconnect"
	syslog(fmt.Sprintf("Establish ES client..."))
	fmt.Println("Establish ES client...")
	cfg = esv7.Config{
		Addresses: []string{
			//"http://ec2-54-234-180-49.compute-1.amazonaws.com:9200",
			//	"http://ip-172-31-14-66.ec2.internal:9200",
			"http://ip-172-31-14-66.ec2.internal:9200", // public interface works where private did not.
		},
		// ...
	}
	es, err = esv7.NewClient(cfg)
	if err != nil {
		elog.Add(logid, fmt.Errorf("ES Error creating the client: %s", err))
		return err
	}
	//
	// 1. Get cluster info
	//
	res, err := es.Info()
	if err != nil {
		elog.Add(logid, fmt.Errorf("ES Error getting Info response: %s", err))
		return err
	}
	defer res.Body.Close()
	// Check response status
	if res.IsError() {
		elog.Add(logid, fmt.Errorf("ES Error: %s", res.String()))
		return err
	}
	// Deserialize the response into a map.
	var r map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		elog.Add(logid, fmt.Errorf("ES Error parsing the response body: %s", err))
		return err
	}
	// Print client and server version numbers.
	syslog(fmt.Sprintf("Client: %s", esv7.Version))
	fmt.Printf("Client: %s\n", esv7.Version)
	syslog(fmt.Sprintf("Server: %s", r["version"].(map[string]interface{})["number"]))

	return nil
}

func esLoad(d *Doc, pkey []byte, wp *sync.WaitGroup, lmtr *grmgr.Limiter) {

	defer lmtr.EndR()
	defer wp.Done()

	// Initialize a client with the default settings.
	//
	//	es, err := esv7.NewClient(cfg)
	// if err != nil {
	// 	syslog(fmt.Sprintf("Error creating the client: %s", err))
	// }
	//
	// 2. Index document
	//
	// Build the request body.

	var b, doc strings.Builder
	b.WriteString(`{"graph" : "`)
	b.WriteString(types.GraphSN())
	b.WriteString(`","attr" : "`)
	b.WriteString(d.Attr)
	b.WriteString(`","value" : "`)
	b.WriteString(d.Value)
	b.WriteString(`","sortk" : "`)
	b.WriteString(d.SortK)
	b.WriteString(`","type" : "`)
	b.WriteString(d.Type)
	b.WriteString(`"}`)
	//
	doc.WriteString(d.PKey)
	doc.WriteByte('|')
	doc.WriteString(d.Attr)
	//
	// syslog(fmt.Sprintf("Body: %s   Doc: %s", b.String(), doc.String()))
	// fmt.Println("Body: ", b.String())
	// time.Sleep(50 * time.Millisecond)
	//Set up the request object.
	t0 := time.Now()
	req := esapi.IndexRequest{
		Index:      *esidx, // must be in lowercase otherwise generates a 400 error
		DocumentID: doc.String(),
		Body:       strings.NewReader(b.String()),
		Refresh:    "true",
	}

	// Perform the request with the client.
	res, err := req.Do(context.Background(), es)
	t1 := time.Now()
	if err != nil {
		elog.Add(logid, fmt.Errorf("Error getting response: %s", err))
	}
	defer res.Body.Close()

	if res.IsError() {
		elog.Add(logid, fmt.Errorf("Error indexing document ID=%s. Status: %v ", d.PKey, res.Status()))
	} else {
		// Deserialize the response into a map.
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			elog.Add(logid, fmt.Errorf("Error parsing the response body: %s", err))
		} else {
			// Print the response status and indexed document version.
			syslog(fmt.Sprintf("[%s] %s; version=%d   API Duration: %s", res.Status(), r["result"].(string), int(r["_version"].(float64)), t1.Sub(t0)))
		}
	}
}

func getLoadStatus(ctx context.Context, id string) (string, uuid.UID, error) {
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
	if err != nil { //query.NoDataFoundErr {
		fmt.Println("*** NO DATA FOUND in STATE$")
		return "", nil, err
	}
	fmt.Println("DATA FOUND in STATE$  - ", status.Value, status.RunId)
	return status.Value, status.RunId, nil
}

func setLoadStatus(ctx context.Context, id string, s string, err_ error, runid ...uuid.UID) error {

	var err error
	if len(s) == 0 {
		panic(fmt.Errorf("setLoadStatus : value is empty"))
	}
	// runid supplied if its the first time i.e no other runs occured
	switch len(runid) > 0 {

	case true: // first run
		ftx := tx.New("setLoadStatus").DB("mysql-goGraph")
		m := ftx.NewInsert("State$").AddMember("Graph", types.GraphSN(), mut.IsKey).AddMember("Name", id, mut.IsKey).AddMember("Value", s).AddMember("RunId", runid[0])
		m.AddMember("Created", "$CURRENT_TIMESTAMP$")
		err = ftx.Execute()
		if err != nil {
			return err
		}

	case false: // restart
		// get original runid which also happens to be the stateId used by the tx package.
		// set status to Running
		ftx := tx.New("setLoadStatus").DB("mysql-goGraph")
		ftx.NewMerge("State$").AddMember("Graph", types.GraphSN(), mut.IsKey).AddMember("Name", id, mut.IsKey).AddMember("Value", s).AddMember("LastUpdated", "$CURRENT_TIMESTAMP$")
		err = ftx.Execute()
		if err != nil {
			return err
		}
	}

	return nil

}

func LoadFTattrs(runid uuid.UID) error {

	var (
		ftAttrs []ftrec
	)

	// collect all FullText Searchable attrbutes - as defined in the type
	for k, v := range types.TypeC.TyC {
		for _, vv := range v {
			switch vv.Ix {
			case "FT", "ft", "FTg", "ftg":

				var sortk strings.Builder
				sortk.WriteString(types.GraphSN())
				sortk.WriteByte('|')
				sortk.WriteString("A#")
				sortk.WriteString(vv.P)
				sortk.WriteString("#:")
				sortk.WriteString(vv.C)
				ty := types.GraphSN() + "|"
				tysn, _ := types.GetTyShortNm(k)
				ty += tysn

				ftAttrs = append(ftAttrs, ftrec{Attr: vv.Name, Ty: ty, Sortk: sortk.String()})
				//esAttr[ty] = sortk.String()
				// save ftAttrs to state table
			}
		}
	}

	stx := tx.NewTx("LoadFTattrs").DB("mysql-goGraph")
	for _, v := range ftAttrs {
		m := stx.NewInsert("State$ES").AddMember("Graph", types.GraphSN()).AddMember("Attr", v.Attr).AddMember("Ty", v.Ty).AddMember("Sortk", v.Sortk).AddMember("Status", "U")
		m.AddMember("RunId", runid).AddMember("Loaded", 0)
	}
	stx.NewInsert("State$").AddMember("Graph", types.GraphSN()).AddMember("Name", "FT attr loaded").AddMember("Value", "C").AddMember("RunId", runid).AddMember("Created", "$CURRENT_TIMESTAMP$")

	return stx.Execute()
}

// func setStatus(ctx context.Context, ty, sortk, s string, attr string) error {
// 	stx := tx.NewC(ctx, "setStatus").DB("mysql-goGraph")
// 	stx.NewMerge("State$ES").AddMember("Graph", types.GraphSN()).AddMember("Ty", ty, mut.IsKey).AddMember("Sortk", sortk, mut.IsKey).AddMember("Status", s)
// 	return stx.Execute()
// }

func setAttrLoadStatus(ctx context.Context, ty, sortk, s string, attr string, loadCnt int) error {
	var ftx *tx.TxHandle

	if ctx != nil {
		ftx = tx.NewContext(ctx, "fetchFTattrs").DB("mysql-goGraph")
	} else {
		ftx = tx.New("fetchFTattrs").DB("mysql-goGraph")
	}
	m := ftx.NewMerge("State$ES").AddMember("Graph", types.GraphSN(), mut.IsKey).AddMember("Ty", ty, mut.IsKey).AddMember("Sortk", sortk, mut.IsKey).AddMember("Attr", attr, mut.IsKey).AddMember("Status", s)
	m.AddMember("Loaded", loadCnt, mut.Add)
	err := ftx.Execute()
	if err != nil {
		return err
	}
	return nil
}

func fetchUnprocessedAttrs(ctx context.Context) ([]ftrec, error) {

	var ftAttrs []ftrec

	ftx := tx.NewQueryContext(ctx, "fetchFTattrs", "State$ES").DB("mysql-goGraph")
	ftx.Select(&ftAttrs).Filter("Graph", types.GraphSN()).Filter("Status", "C", query.NE)
	err := ftx.Execute()
	if err != nil {
		return nil, err
	}
	return ftAttrs, nil
}
