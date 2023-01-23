package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	blk "github.com/GoGraph/block"
	//dyn "github.com/GoGraph/db"
	//dbadmin "github.com/GoGraph/db/admin"
	"github.com/GoGraph/stats/admin"
	dyn "github.com/ros2hp/method-db/dynamodb"
	dbadmin "github.com/ros2hp/method-db/dynamodb/admin"
	//"github.com/GoGraph/client"
	//"github.com/GoGraph/db"
	param "github.com/GoGraph/dygparam"
	"github.com/GoGraph/monitor"
	"github.com/GoGraph/run"
	"github.com/GoGraph/tbl"
	//"github.com/GoGraph/rdf/anmgr"
	//"github.com/GoGraph/rdf/dp"
	elog "github.com/GoGraph/errlog"
	"github.com/GoGraph/grmgr"
	"github.com/GoGraph/rdf/ds"
	"github.com/GoGraph/rdf/edge"
	"github.com/GoGraph/rdf/reader"
	"github.com/GoGraph/rdf/save"
	"github.com/ros2hp/method-db/mysql"
	//"github.com/GoGraph/rdf/uuid"
	slog "github.com/GoGraph/syslog"
	"github.com/ros2hp/method-db/tx"
	"github.com/ros2hp/method-db/db"
	//"github.com/ros2hp/method-db/mut"
	"github.com/ros2hp/method-db/uuid"
	"github.com/GoGraph/types"
)

const (
	// number of nodes in rdf to load in single read: TODO should be number of tuples not nodes as a node may contain
	// millions of rows - not a scalable and work load balanced design when based on nodes

	logid = param.Logid
)
const (
	I  = "I"
	F  = "F"
	S  = "S"
	Nd = "Nd"
	// SS  = ""SS
	// SI  = "SI"
	// SF  = "SF"
	LS  = "LS"
	LI  = "LI"
	LF  = "LF"
	LBl = "LbL"
	//SBl = "SBl"
)

type savePayload struct {
	sname        string   // node ID aka ShortName or blank-node-id
	suppliedUUID uuid.UID // user supplied UUID
	attributes   []ds.NV
}

// channels
var verifyCh chan verifyNd
var saveCh chan savePayload //[]ds.NV // TODO: consider using a struct {SName, UUID, []ds.NV}

var errNodes ds.ErrNodes

type verifyNd struct {
	n     int
	nodes []*ds.Node
}

func syslog(s string) {
	slog.Log(logid, s)
}

func init() {
	errNodes = make(ds.ErrNodes)
}

var (
	tblgraph   = flag.String("tbl", "GoGraph.Dev", `Table name containing graph data [default GoGraph.Dev]`)
	stats      = flag.Int("stats", 0, `Show system stats [1: enable 0: disable (default)`)
	environ    = flag.String("env", "dev", "Environment [ dev: Development] prd: production")
	inputFile  = flag.String("f", "rdf_test.rdf", "RDF Filename: ")
	debug      = flag.String("debug", "", `Enable logging by component "c1,c2,c3" or switch on complete logging "all"`)
	concurrent = flag.Int("c", 6, "# parallel goroutines")
	graph      = flag.String("g", "", "Graph: ")
	showsql    = flag.Int("sql", 0, "Show generated SQL [1: enable 0: disable]")
	reduceLog  = flag.Int("rlog", 1, "Reduced Logging [1: enable 0: disable]")
	bypassLoad = flag.Int("noLoad", 0, "Bypass db load operation:  [1: yes 0: no]")
)

func main() { //(f io.Reader) error { // S P O
	//
	flag.Parse()
	//
	fmt.Printf("Argument: table: %s\n", *tblgraph)
	fmt.Printf("Argument: env: %s\n", *environ)
	fmt.Printf("Argument: stats: %d\n", *stats)
	fmt.Printf("Argument: inputfile: %s\n", *inputFile)
	fmt.Printf("Argument: concurrent: %d\n", *concurrent)
	fmt.Printf("Argument: showsql: %v\n", *showsql)
	fmt.Printf("Argument: debug: %v\n", *debug)
	fmt.Printf("Argument: graph: %s\n", *graph)
	fmt.Printf("Argument: reduced logging: %v\n", *reduceLog)
	fmt.Printf("Argument: bypass db load: %d\n", *bypassLoad)
	//

	// initialise channels with buffers
	verifyCh = make(chan verifyNd, 3)
	saveCh = make(chan savePayload, 12)

	// start any syslog services
	err := slog.Start()
	if err != nil {
		fmt.Println("Error: ", err)
		panic(err)
	} else {
		fmt.Println("\n no error ")
	}

	//
	f, err := os.Open(*inputFile)
	if err != nil {
		fmt.Printf("Error opening file %q, %s", *inputFile, err)
		return
	}

	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// setup concurrent routine to capture OS signals.
	appSignal := make(chan os.Signal, 3)
	var (
		wpStart, wpEnd sync.WaitGroup
		ctxEnd         sync.WaitGroup
		n              int // for loop counter
		eof            bool
		tstart         time.Time

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
			syslog(fmt.Sprintf("Terminated.....Duration: %s", tend.Sub(tstart).String()))
			os.Exit(2)
		}
	}()
	// register default database client
	// TODO: Init should support of aws.Options e.g. WithRegion, etc
	// db.Init(ctx, &ctxEnd, []db.Option{db.Option{Name: "throttler", Val: grmgr.Control}, db.Option{Name: "Region", Val: "us-east-1"}}...)
	// mysql.Init(ctx)
	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "throttler", Val: grmgr.Control}, db.Option{Name: "Region", Val: "us-east-1"}}...)
	mysql.Register(ctx, "mysql-GoGraph", "admin:gjIe8Hl9SFD1g3ahyu6F@tcp(mysql8.cjegagpjwjyi.us-east-1.rds.amazonaws.com:3306)/GoGraph")

	logrmDB := slog.NewLogr("mdb")
	logrmGr := slog.NewLogr("grmgr")

	tx.SetLogger(logrmDB) //, tx.Alert)
	grmgr.SetLogger(logrmGr)

	param.ReducedLog = false
	if *reduceLog == 1 {
		param.ReducedLog = true
	}
	if *showsql == 1 {
		param.ShowSQL = true
	}

	if tblgraph != nil {
		tbl.Set(*tblgraph)
	}

	if *stats == 1 {
		param.StatsSystem = true
	}

	readBatchSize := *concurrent //  keep as same size as concurrent argument

	// allocate a run id
	runid, err := run.New(logid, "rdfLoader")
	if err != nil {
		fmt.Println(fmt.Sprintf("Error in  run.New() : %s", err))
		return
	}
	//defer run.Finish(err)
	tstart = time.Now()

	// set graph to use
	if len(*graph) == 0 {
		flag.PrintDefaults()
		return
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

	// dump program parameters to syslog
	syslog(fmt.Sprintf("Argument: table: %s", *tblgraph))
	syslog(fmt.Sprintf("Argument: env: %s\n", *environ))
	syslog(fmt.Sprintf("Argument: stats: %d", *stats))
	syslog(fmt.Sprintf("Argument: inputfile: %s\n", *inputFile))
	syslog(fmt.Sprintf("Argument: concurrency: %d", *concurrent))
	syslog(fmt.Sprintf("Argument: showsql: %v", *showsql))
	syslog(fmt.Sprintf("Argument: debug: %v", *debug))
	syslog(fmt.Sprintf("Argument: graph: %s", *graph))
	syslog(fmt.Sprintf("Argument: reduced logging: %v", *reduceLog))
	syslog(fmt.Sprintf("Argument: bypass db load: %v", *bypassLoad))

	// purge graph table if running in dev
	// tblEdge, tblEdgeChild := tbl.SetEdgeNames(types.GraphName())
	// etx := tx.NewTx("truncate").DB("mysql-GoGraph") // TODO: what about tx.New()
	// etx.NewTruncate([]tbl.Name{tbl.Name(tblEdgeChild), tbl.Name(tblEdge)})
	// err = etx.Execute()
	// if err != nil {
	// 	panic(err)
	// }
	//
	wpStart.Add(7)
	// check verify and saveNode have finished. Each goroutine is responsible for closing and waiting for all routines they spawn.
	wpEnd.Add(2)
	//
	// start pipeline goroutines
	go verify(ctx, &wpStart, &wpEnd)
	go saveNode(&wpStart, &wpEnd)
	//
	// start supporting services
	//
	// services
	ctxEnd.Add(5)
	go uuid.PowerOn(ctx, &wpStart, &ctxEnd) // generate and store UUIDs service
	grCfg := grmgr.Config{"runid": runid}
	go grmgr.PowerOn(ctx, &wpStart, &wpEnd, grCfg)
	//go grmgr.PowerOn(ctx, &wpStart, &ctxEnd, runid) // concurrent goroutine manager service
	go elog.PowerOn(ctx, &wpStart, &ctxEnd)    // error logging service
	go monitor.PowerOn(ctx, &wpStart, &ctxEnd) // repository of system statistics service
	go edge.PowerOn(ctx, &wpStart, &ctxEnd)

	// wait for services to start
	syslog(fmt.Sprintf("waiting on services to start...."))
	wpStart.Wait()
	syslog(fmt.Sprintf("all load services started "))

	logerr := func(id string, e error) {
		elog.Add("mdb", e)
	}
	tx.SetErrLogger(logerr)
	grmgr.SetErrLogger(logerr)

	// set Graph and load types into memory - dependency on syslog
	err = types.SetGraph(*graph)
	if err != nil {
		fmt.Println("Error in determining graph, may not exist: Error: %s", err)
		return
	}

	// purge state tables
	tblEdge, tblEdgeChild := tbl.SetEdgeNames(types.GraphName())
	etx := tx.NewTx("truncate").DB("mysql-GoGraph") // TODO: what about tx.New()
	etx.NewTruncate([]tbl.Name{tbl.Name(tblEdgeChild), tbl.Name(tblEdge)})
	err = etx.Execute()
	if err != nil {
		panic(err)
	}

	// setup db related services (e.g. stats snapshot save)
	dbadmin.Setup(runid)

	// create rdf reader
	rdr, _ := reader.New(f)

	for {
		//
		// make nodes
		//
		nodes := make([]*ds.Node, readBatchSize, readBatchSize)
		// assign pointers
		for i := range nodes {
			nodes[i] = new(ds.Node)
		}
		//
		// read rdf file []nodes at a time
		//
		n, eof, err = rdr.Read(nodes)
		if err != nil {
			// log error and continue to read until eof reached
			elog.Add(logid, fmt.Errorf("Read error: %s", err.Error()))
		}
		//
		// send []nodes on pipeline to be unmarshalled and saved to db
		//
		v := verifyNd{n: n, nodes: nodes}

		verifyCh <- v
		//
		// exit when
		if n < len(nodes) || eof {
			break
		}
	}

	// shutdown pipeline goroutines - verify and save goroutines...
	close(verifyCh)
	tclose := time.Now()
	// and wait for them to exit
	syslog("Waiting on wpEnd")
	wpEnd.Wait()
	syslog("Pass wpEnd Wait")

	elog.PrintErrors()
	// Save edge data to db.
	edge.Persist(string(tblEdge))

	// output monitor report
	monitor.Report()

	// shutdown all support services...
	cancel()
	// and wait for them to exit
	ctxEnd.Wait()

	// persist database api stats
	dbadmin.Persist(runid)

	// Complete run state for the program
	run.Finish(err)
	tend := time.Now()

	syslog(fmt.Sprintf("Completed....Runid:  %q   Duration: Load: %s  PostLoad: %s", runid.Base64(), tclose.Sub(tstart), tend.Sub(tstart)))
	fmt.Printf("Completed.....Duration: ToClose: %s  ToEnd: %s", tclose.Sub(tstart), tend.Sub(tstart))
	// wait for last syslog message to be processed
	time.Sleep(1 * time.Second)

	// stop system logger services (if any)
	slog.Stop()
	// persist wait stats
	admin.Persist()

	return
}

// verify is a goroutine (aka service_ started when program is instantiated.
// It persists for duration of load into database after which the verify channel from which it reads is closed.
// It forms part of a pipeline wiht the reader and main routine.
func verify(ctx context.Context, wpStart *sync.WaitGroup, wpEnd *sync.WaitGroup) { //, wg *sync.WaitGroup) {

	defer wpEnd.Done()
	defer close(saveCh)
	// sync verify's internal goroutines
	syslog("verify started....")
	wpStart.Done()

	// waitgroups
	var wg sync.WaitGroup
	//
	// concurrent settings for goroutines
	//
	//	unmarshalTrg := grmgr.Trigger{R: routine, C: 5, Ch: make(chan struct{})}

	//limitUnmarshaler := grmgr.New("unmarshaler", *concurrent*2)
	//limitUnmarshaler := grmgr.NewConfig("unmarshaler", *concurrent*2, min,upInc,downInc,changeInterval)
	limitUnmarshaler, err := grmgr.NewConfig("unmarshaler", *concurrent*2, 2, 1, 3, "1m")
	if err != nil {
		panic(err)
	}

	// the loop will terminate on close of channel when rdf file is fully read.
	for nodes_ := range verifyCh {

		nodes := nodes_.nodes

		// unmarshal (& validate) each node in its own goroutine
		for i := 0; i < nodes_.n; i++ {

			if len(nodes[i].Lines) == 0 { // a line is a s-p-o tuple.//TODO: should i be ii
				break
			}
			ii := i
			ty, err := getType(nodes[ii])
			if err != nil {
				elog.Add(logid, err)
			}
			// first pipeline func. Passes NV data to saveCh and then to database.
			//	slog.Log("verify: ", fmt.Sprintf("Pass to unmarshal... %d %#v", i, nodes[ii]))
			limitUnmarshaler.Ask()
			<-limitUnmarshaler.RespCh()

			wg.Add(1)
			go unmarshalRDF(ctx, nodes[ii], ty, &wg, limitUnmarshaler)

		}
	}
	syslog("verify waiting on unmarshalRDFs to finish...")
	wg.Wait()
	syslog("verify pass Wait ...")
	limitUnmarshaler.Unregister()
}

// unmarshalRDF merges the rdf lines for an individual node (identical subject value) to create NV entries
// for the type of the node.
func unmarshalRDF(ctx context.Context, node *ds.Node, ty blk.TyAttrBlock, wg *sync.WaitGroup, lmtr *grmgr.Limiter) {
	defer wg.Done()

	genSortK := func(ty blk.TyAttrD) string {
		var s strings.Builder

		s.WriteString("A#") // leading sortk

		if ty.DT == "Nd" {
			// all uid-preds are listed under G partition
			s.WriteString("G#:")
			s.WriteString(ty.C)
		} else {
			s.WriteString(ty.P)
			s.WriteString("#:")
			s.WriteString(ty.C)
		}
		return s.String()
	}
	//
	defer lmtr.EndR()

	// accumulate predicate (spo) n.Object values in the following map
	type mergedRDF struct {
		value interface{}
		name  string // not populated below. TODO: why use it then.??
		dt    string
		sortk string
		c     string // type attribute short name
		ix    string // index type + support Has()
		null  bool   // true: nullable
	}
	// map[predicate]
	var attr map[string]*mergedRDF
	attr = make(map[string]*mergedRDF)
	//
	var nv []ds.NV // Node's AttributName-Value

	// find predicate in s-p-o lines matching pred  name in ty name
	// create attr entry indexed by pred.
	// may need to merge multiple s-p-o lines with the same pred into one attr entry e.g. list or set types
	// attr (predicate) will then be used to create NV entries, where the name (pred) gets associated with value (ob)
	var found bool

	for _, v := range ty {
		found = false

		for _, n := range node.Lines {

			// match the rdf node pred value to the nodes type attribute
			if !strings.EqualFold(v.Name, n.Pred) {
				continue
			}
			found = true

			switch v.DT {

			// As unmarshalRDF is part of the RDF load into DB process it is not necessary
			// to convert numbers (int, float) from string to their respective type as they
			// will only be converted back to string before laoding into Dynamodb.
			// However in the case of Spanner they should be converted.

			case I:
				// check n.Object can be coverted to int

				//i, err := strconv.Atoi(n.Obj)
				i, err := strconv.ParseInt(n.Obj, 10, 64)
				if err != nil {
					err := fmt.Errorf("expected Integer got %s ", n.Obj)
					node.Err = append(node.Err, err)
					continue
				}
				attr[v.Name] = &mergedRDF{value: i, dt: v.DT, ix: v.Ix, null: v.N, c: v.C}

			case F:
				// check n.Object can be converted to float
				f, err := strconv.ParseFloat(n.Obj, 64)
				if err != nil {
					err := fmt.Errorf("error in ParseFloat64 on %s ", n.Obj)
					node.Err = append(node.Err, err)
					continue
				}
				attr[v.Name] = &mergedRDF{value: f, dt: v.DT, ix: v.Ix}

			case S:
				// check n.Object can be converted to float

				//attr[v.Name] = n.Obj
				attr[v.Name] = &mergedRDF{value: n.Obj, dt: v.DT, ix: v.Ix, null: v.N, c: v.C}

			//case DT: // TODO- implement
			// check n.Object can be converted to float

			//attr[v.Name] = n.Obj
			//	attr[v.Name] = &mergedRDF{value: n.Obj, dt: v.DT, ix: v.Ix, null: v.N, c: v.C}

			// NOTE: SS is deprecated. Replaced with LS
			// case SS:

			// 	if a, ok := attr[v.Name]; !ok {
			// 		ss := make([]string, 1)
			// 		ss[0] = n.Obj
			// 		attr[v.Name] = &mergedRDF{value: ss, dt: v.DT, c: v.C, null: v.N}
			// 	} else {
			// 		if ss, ok := a.value.([]string); !ok {
			// 			err := fmt.Errorf("Conflict with SS type at line %d", n.N)
			// 			node.Err = append(node.Err, err)
			// 		} else {
			// 			// merge (append) obj value with existing attr (pred) value
			// 			ss = append(ss, n.Obj)
			// 			attr[v.Name].value = ss
			// 		}
			// 	}

			// NOTE: SI is deprecated. Replaced with LI
			// case SI:

			// 	if a, ok := attr[v.Name]; !ok {

			// 		si := make([]int, 1)
			// 		i, err := strconv.Atoi(n.Obj)
			// 		if err != nil {
			// 			err := fmt.Errorf("expected Integer got %s", n.Obj)
			// 			node.Err = append(node.Err, err)
			// 			continue
			// 		}
			// 		si[0] = i
			// 		syslog(fmt.Sprintf("Add to SI . [%d]", i))
			// 		attr[v.Name] = &mergedRDF{value: si, dt: v.DT, c: v.C, null: v.N}

			// 	} else {

			// 		if si, ok := a.value.([]int); !ok {
			// 			err := fmt.Errorf("Conflict with SS type at line %d", n.N)
			// 			node.Err = append(node.Err, err)
			// 		} else {
			// 			i, err := strconv.Atoi(n.Obj)
			// 			if err != nil {
			// 				err := fmt.Errorf("expected Integer got %s", n.Obj)
			// 				node.Err = append(node.Err, err)
			// 				continue
			// 			}
			// 			// merge (append) obj value with existing attr (pred) value
			// 			syslog(fmt.Sprintf("Add to SI . [%d]", i))
			// 			si = append(si, i)
			// 			attr[v.Name].value = si
			// 		}
			// 	}

			// case SBl:
			// case SB:
			// case LBl:
			// case LB:

			case LS:
				if a, ok := attr[v.Name]; !ok {
					ls := make([]string, 1)
					ls[0] = n.Obj
					attr[v.Name] = &mergedRDF{value: ls, dt: v.DT, c: v.C, null: v.N}
					//	attr[v.Name] = ls
				} else {
					if ls, ok := a.value.([]string); !ok {
						err := fmt.Errorf("Conflict with SS type at line %d", n.N)
						node.Err = append(node.Err, err)
					} else {
						ls = append(ls, n.Obj)
						attr[v.Name].value = ls
					}
				}

			case LI:
				if a, ok := attr[v.Name]; !ok {
					li := make([]int64, 1)
					//i, err := strconv.Atoi(n.Obj)
					i, err := strconv.ParseInt(n.Obj, 10, 64)
					if err != nil {
						err := fmt.Errorf("expected Integer got %s", n.Obj)
						node.Err = append(node.Err, err)
						continue
					}
					li[0] = i // n.Obj  int
					//attr[v.Name] = li
					attr[v.Name] = &mergedRDF{value: li, dt: v.DT, null: v.N, c: v.C}
				} else {
					if li, ok := a.value.([]int64); !ok {
						err := fmt.Errorf("Conflict with LI type at line %d", n.N)
						node.Err = append(node.Err, err)
					} else {
						//i, err := strconv.Atoi(n.Obj)
						i, err := strconv.ParseInt(n.Obj, 10, 64)
						if err != nil {
							err := fmt.Errorf("expected Integer got  %s", n.Obj)
							node.Err = append(node.Err, err)
							continue
						}
						li = append(li, i)
						attr[v.Name].value = li
					}
				}

			case Nd:
				// _:d Friends _:abc .
				// _:d Friends _:b .
				// _:d Friends _:c .
				// need to convert n.Obj value of SName to UID
				if a, ok := attr[v.Name]; !ok {
					ss := make([]string, 1)
					ss[0] = n.Obj // child node
					attr[v.Name] = &mergedRDF{value: ss, dt: v.DT, c: v.C}
				} else {
					// attach child (obj) short name to value slice (reperesenting list of child nodes to be attached)
					if nd, ok := a.value.([]string); !ok {
						err := fmt.Errorf("Conflict with Nd type at line %d", n.N)
						node.Err = append(node.Err, err)
					} else {
						nd = append(nd, n.Obj)
						attr[v.Name].value = nd // child nodes: _:abc,_:b,_:c
					}
				}
				//	addEdgesCh<-
			default:
				panic(fmt.Errorf("unmarshalRDF: type %q not in supported", v.DT))
			}
			//
			// generate sortk key
			//
			at := attr[v.Name]
			at.sortk = genSortK(v)
		}
		//
		//
		//
		if !found {
			if !v.N && v.DT != "Nd" {
				err := fmt.Errorf("Not null type attribute %q must be specified in node %s", v.Name, node.ID)
				node.Err = append(node.Err, err)
			}
		}
		if len(node.Err) > 0 {
			slog.Log("unmarshalRDF: ", fmt.Sprintf("return with %d errors. First error:  %s", len(node.Err), node.Err[0].Error()))
			for _, e := range node.Err {
				elog.Add("unmarshall:", e)
			}
			//elog.AddBatch <- node.Err
			return
		}

	}
	//
	// unmarshal attr into NV -except Nd types, handle in next for
	//
	// add type of node to NV - note a A#A# means it is associated with the scalar attributes. If the node has no scalars it will always
	// have a A#A#T so the type of the node can be determined if only the scalar data is fetched.
	//
	e := ds.NV{Sortk: "A#A#T", SName: node.ID, Value: node.TyName, DT: "ty"}
	nv = append(nv, e)
	//
	// add scalar predicates
	//
	for k, v := range attr {
		//
		if v.dt == Nd {
			continue
		}
		//
		// for nullable attributes only, populate Ty (which should be anyway) plus Ix (with "x") so a GSI entry is created in Ty_Ix to support Has(<predicate>) func.
		//
		e := ds.NV{Sortk: v.sortk, Name: k, SName: node.ID, Value: v.value, DT: v.dt, C: v.c, Ty: node.TyName, Ix: v.ix}
		nv = append(nv, e)
	}
	//
	// check all uid-predicate types (DT="Nd") have an NV entry - as this simplies later processing if one is guaranteed to exist even if not originally defined in RDF file
	//
	for _, v := range ty {
		if v.DT == Nd {
			// create empty item
			value := []string{"__"}
			e := ds.NV{Sortk: genSortK(v), Name: v.Name, SName: "__", Value: value, DT: Nd, Ty: node.TyName} // TODO: added Ty so A#T item can be removed (at some point)
			nv = append(nv, e)
		}
	}

	var (
		psn   uuid.UID
		edges int
		err   error
	)

	lch := make(chan uuid.UID)

	// get or generate node (parent) UID
	uuid.ReqCh <- uuid.Request{SName: node.ID, RespCh: lch}
	psn = <-lch
	//tblEdgeChild := string(tbl.EdgeChild) + types.GraphName()
	etx := tx.NewTxContext(ctx, "edgeChild").DB("mysql-GoGraph", db.Option{"prepare", false}, db.Option{"prepare", true}).Prepare()

	_, tblEdgeChild := tbl.SetEdgeNames(types.GraphName())
	for _, v := range attr {

		if v.dt == "Nd" {
			// in the case of nodes wihtout scalars we need to add a type item
			childNodes := v.value.([]string) // child nodes
			// for the node create a edge entry to each child node (for the Nd pred) in the anmgr service
			// These entries will be used later to attach the actual nodes together (propagate child data etc)
			// conn:=db.NewConnection("MySQL")
			// tx:=conn.NewTx()

			// etx:=tx.DB("MySQL").NewBatch()

			// db.Set("MySQL")
			// no concept of batch in (database/sql) mysql - prepare is closest, ie. create stmt and issue many times
			//etx := tx.NewBatch("edgeChild").DB("mysql").Prepare() // returns "prepared"

			// perform following inserts as as signel transaction use: tx.NewTx
			// tx := db.BeginTx(ctx,)   DB will vaidate tx method is appropriate for the database selected.
			//etx := tx.NewTx("edgeChild")
			// increment node edge counter
			edges += len(childNodes)

			// for each child node
			for _, s := range childNodes {

				uuid.ReqCh <- uuid.Request{SName: s, RespCh: lch}
				csn := <-lch

				// bulk insert should be in dedicated transaction or transaction Batch - see MakeBatch() above
				var sk strings.Builder
				sk.WriteString(types.GraphSN())
				sk.WriteByte('|')
				sk.WriteString(v.sortk)
				// append csn to sortk as we need to support a two key only design in Dynamodb (lowest common denominator in this case) for a table that had three
				sk.WriteByte('|')
				sk.WriteString(string(csn.Base64()))

				//etx.Add(mut.NewInsert(tbl.Name(tblEdgeChild)).AddMember("Puid", psn).AddMember("SortK_Cuid", sk.String()).AddMember("Status", "X"))

				// For SQL dbs:  MergeInsert() same as NewInsert() - all the magic happens at Execute time.
				// For NewSQL & NoSQL: Merge will append to arrays types in source mutation and incr/decr number values.
				//	etx.MergeInsert(tbl.Name(tblEdgeChild)).AddMember("Puid", psn).AddMember("SortK_Cuid", sk.String()).AddMember("Status", "X")
				etx.NewInsert(tbl.Name(tblEdgeChild)).AddMember("Puid", psn).AddMember("SortK_Cuid", sk.String()).AddMember("Status", "X")
			}
		}
	}

	err = etx.Execute()
	if err != nil {
		elog.Add("ChildEdge", err)
	}
	if edges > 0 {
		// for post edge processing...
		edge.LoadCh <- edge.NEdges{Puid: psn, Edges: edges}
	}

	if *bypassLoad > 0 {
		return
	}

	// pass NV onto save-to-database channel if no errors detected
	if len(node.Err) == 0 {

		if len(nv) == 0 {
			panic(fmt.Errorf("unmarshalRDF: nv is nil "))
		}
		payload := savePayload{sname: node.ID, suppliedUUID: node.UUID, attributes: nv}

		// save nodes NV content to database
		saveCh <- payload

	} else {

		node.Lines = nil
		errNodes[node.ID] = node

	}
	//
}

func saveNode(wpStart *sync.WaitGroup, wpEnd *sync.WaitGroup) {

	defer wpEnd.Done()
	wpStart.Done()

	syslog("saveNode started......")
	var wg sync.WaitGroup
	//
	// define goroutine limiters
	//
	//limiterSave := grmgr.New("saveNode", *concurrent)
	limiterSave, err := grmgr.NewConfig("saveNode", *concurrent, 4, 1, 4, "1m")
	if err != nil {
		panic(err)
	}

	var c int

	// read from save channel. Payload represens an individual nodes NV data.
	for py := range saveCh {
		c++

		limiterSave.Ask()
		<-limiterSave.RespCh()

		wg.Add(1)
		go save.SaveRDFNode(py.sname, py.suppliedUUID, py.attributes, &wg, limiterSave)

	}
	syslog(fmt.Sprintf("waiting for SaveRDFNodes...loop count. %d", c))
	wg.Wait()
	limiterSave.Unregister()
	syslog("saveNode finished waiting.")
	//
	// limiterAttach := grmgr.New("nodeAttach", *attachers)
	//
	// fetch edge node ids from attach-node-manager routine. This will send each edge node pair via its AttachNodeCh.
	//
	//anmgr.JoinNodes <- struct{}{}
	return
}

// }
// 	c = 0

// 	//AttachNodeCh is populated by service anmgr (AttachNodeManaGeR)

// 	for e := range anmgr.AttachNodeCh {
// 		c++
// 		//	e := <-anmgr.AttachNodeCh
// 		if string(e.Cuid) == "eod" {
// 			break
// 		}
// 		limiterAttach.Ask()
// 		<-limiterAttach.RespCh()

// 		wg.Add(1)

// 		go client.AttachNode(uuid.UID(e.Cuid), uuid.UID(e.Puid), e.Sortk, e, &wg, limiterAttach)
// 		//client.AttachNode(uuid.UID(e.Cuid), uuid.UID(e.Puid), e.Sortk, e, &wg, limiterAttach)

// 	}
// 	wg.Wait()
// 	limiterAttach.Finish()
// 	syslog("attach nodes finished waiting...")
// 	nb, err := db.Fetch("EOPCount")
// 	if err != nil {
// 		syslog(fmt.Sprintf("db.Fetch EOPCount: %s", err))
// 	}
// 	syslog(fmt.Sprintf("EOP Count: %d", nb[0].GetI()))
// 	// determine types which reference types that have a cardinality of 1:1

// 	has11 := make(map[string]struct{})
// 	dpTy := make(map[string]struct{})

// 	for k, v := range types.TypeC.TyC {
// 		for _, vv := range v {
// 			if vv.Ty == "" {
// 				continue
// 			}
// 			if _, ok := has11[k]; ok {
// 				break
// 			}
// if vv.Card == "1:1" {
//     has11[k] = struct{}{}
// }
// 		}
// 	}
// 	for k, v := range types.TypeC.TyC {
// 		for _, vv := range v {
// 			if _, ok := has11[vv.Ty]; ok {
// 				if sn, ok := types.GetTyShortNm(k); ok {
// 					dpTy[sn] = struct{}{}
// 				}
// 			}
// 		}
// 	}
// 	var wgc sync.WaitGroup
// 	limiterDP := grmgr.New("dp", *attachers)
// 	for k, _ := range dpTy {
// 		syslog(fmt.Sprintf(" Type containing 1:1 type: %s", k))
// 	}
// 	syslog("Start double propagation processing...")
// 	t0 := time.Now()
// 	for ty, _ := range dpTy {

// 		ty := ty
// 		for n := range dp.FetchNodeCh(ty) {

// 			n := n
// 			limiterDP.Ask()
// 			<-limiterDP.RespCh()
// 			wgc.Add(1)

// 			go dp.Process(limiterDP, &wgc, n, ty, has11)

// 		}
// 		wgc.Wait()
// 	}
// 	t1 := time.Now()
// 	limiterDP.Finish()
// 	syslog(fmt.Sprintf("double propagate processing finished. Duration: %s", t1.Sub(t0)))

// } 				has11[k] = struct{}{}

func getType(node *ds.Node) (blk.TyAttrBlock, error) {

	// type loc struct {
	// 	sync.Mutex
	// }
	//	var ll loc

	// is there a type defined
	if len(node.TyName) == 0 {
		node.Err = append(node.Err, fmt.Errorf("No type defined for %s", node.ID))
	}
	//syslog(fmt.Sprintf("node.TyName : [%s]", node.TyName))
	//ll.Lock() - all types loaded at startup time - no locks required
	//ty, err := cache.FetchType(node.TyName)
	ty, err := types.FetchType(node.TyName)
	//ll.Unlock()
	if err != nil {
		return nil, err
	}
	return ty, nil
}
