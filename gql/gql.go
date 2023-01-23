package gql

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	param "github.com/GoGraph/dygparam"
	"github.com/GoGraph/gql/ast"
	"github.com/GoGraph/gql/parser"
	stat "github.com/GoGraph/monitor"
	"github.com/GoGraph/run"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/tbl"

	"github.com/ros2hp/grmgr"

	"github.com/ros2hp/method-db/mut"
	_ "github.com/ros2hp/method-db/mysql"
	"github.com/ros2hp/method-db/tx"
)

var (
	ctx    context.Context
	cancel context.CancelFunc
	ctxEnd sync.WaitGroup
	//
	replyCh        chan interface{}
	statTouchNodes stat.Request
	statTouchLvl   stat.Request
	statDbFetches  stat.Request
	//
	expectedJSON       string
	expectedTouchLvl   []int
	expectedTouchNodes int
	//
	t0, t1, t2 time.Time
)

const logid = param.Logid

func syslog(s string) {
	slog.Log(logid, s)
}

func init() {

	// start any syslog services - dependency on runid
	err := slog.Start()
	if err != nil {
		fmt.Println("Error: ", err)
		panic(err)
	} else {
		fmt.Println("\n no error ")
	}

	replyCh = make(chan interface{})
	statTouchNodes = stat.Request{Id: stat.TouchNodeFiltered, ReplyCh: replyCh}
	statTouchLvl = stat.Request{Id: stat.TouchLvlFiltered, ReplyCh: replyCh}
	statDbFetches = stat.Request{Id: stat.NodeFetch, ReplyCh: replyCh}

	Startup()

	fmt.Println("====================== STARTUP =====================")
}

func Execute(graph string, query string, tbname *string, concurrent *int) *ast.RootStmt {

	if len(*tbname) > 0 {
		tbl.Set(*tbname)
		//clear monitor stats
		stat.ClearCh <- struct{}{}
	}
	tbl.RegisterIndex("P_S", tbl.Graph, "P", "S")
	tbl.RegisterIndex("P_N", tbl.Graph, "P", "N")
	tbl.RegisterIndex("P_B", tbl.Graph, "P", "B")

	t0 = time.Now()
	p := parser.New(graph, query)
	stmt, errs := p.ParseInput()
	if len(errs) > 0 {
		for _, v := range errs {
			fmt.Println("error: ", v)
		}
		panic(errs[0])
	}
	//
	param.DebugOn = true
	//
	t1 = time.Now()
	stmt.Execute(concurrent)
	t2 = time.Now()

	fmt.Printf("Duration:  Parse  %s  Execute: %s    \n", t1.Sub(t0), t2.Sub(t1))
	syslog(fmt.Sprintf("Duration: Parse  %s  Execute: %s ", t1.Sub(t0), t2.Sub(t1)))
	time.Sleep(2 * time.Second) // give time for stat to empty its channel queues

	stat.PrintCh <- struct{}{}
	//Shutdown()

	return stmt

}

func Startup() {

	var (
		wpStart, wpEnd sync.WaitGroup
	)
	syslog("Startup...")

	wpStart.Add(2)
	// check verify and saveNode have finished. Each goroutine is responsible for closing and waiting for all routines they spawn.
	ctxEnd.Add(2)

	// allocate a run id
	runid := run.New2(logid)

	ctx, cancel = context.WithCancel(context.Background())

	go stat.PowerOn(ctx, &wpStart, &ctxEnd)
	go grmgr.PowerOn(ctx, &wpStart, &wpEnd, runid) // concurrent goroutine manager service

	wpStart.Wait()

	syslog(fmt.Sprintf("services started "))

}

func Shutdown() {

	syslog("Shutdown commenced...")
	cancel()

	ctxEnd.Wait()
	syslog("Shutdown Completed")
}

func compareStat(result interface{}, expected interface{}) bool {
	//
	// return true when args are different
	//
	if result == nil && expected != nil {
		switch x := expected.(type) {
		case int:
			if x != 0 {
				return true
			}
		case []int:
			if x[0] != 0 {
				return true
			}
		}
		return false
	}

	switch x := result.(type) {

	case int:
		return expected.(int) != x

	case []int:
		if result == nil && len(x) == 1 && x[0] == 0 {
			return false
		}
		fmt.Println("in comparStat ", x)
		if exp, ok := expected.([]int); !ok {
			panic(fmt.Errorf("Expected should be []int"))
		} else {

			for i, v := range x {
				if i == len(exp) {
					return false
				}
				if v != exp[i] {
					return true
				}
			}
			// 			if len(x) > len(exp) {
			// 				for i := len(exp); i < len(x); i++ {
			// 					if x[i] != 0 {
			// 						return true
			// 					}
			// 				}
			// 			}
			return false
		}
	}
	return true
}

func compareJSON(doc, expected string) bool {

	return trimWS(doc) != trimWS(expected)

}

// trimWS trims whitespaces from input string. Objective is to compare strings real content - not influenced by whitespaces
func trimWS(input string) string {

	var out strings.Builder
	for _, v := range input {
		if !(v == '\u0009' || v == '\u0020' || v == '\u000A' || v == '\u000D' || v == ',') {
			out.WriteRune(v)
		}
	}
	return out.String()

}

// checkErrors compares actual errors from test against slice of expected errors
func checkErrors(errs []error, expectedErr []string, t *testing.T) {

	for _, ex := range expectedErr {
		if len(ex) == 0 {
			break
		}
		found := false
		for _, err := range errs {
			if trimWS(err.Error()) == trimWS(ex) {
				found = true
			}
		}
		if !found {
			t.Errorf(`Expected Error = [%q]`, ex)
		}
	}
	for _, got := range errs {
		found := false
		for _, exp := range expectedErr {
			if trimWS(got.Error()) == trimWS(exp) {
				found = true
			}
		}
		if !found {
			t.Errorf(`Unexpected Error = [%q]`, got.Error())
		}
	}
}
func validate(t *testing.T, result string, abort ...bool) {

	var msg string

	t.Log(result)

	stat.GetCh <- statTouchNodes
	nodes := <-replyCh

	stat.GetCh <- statTouchLvl
	levels := <-replyCh

	stat.GetCh <- statDbFetches
	fetches := <-replyCh

	status := "P" // Passed
	if compareStat(nodes, expectedTouchNodes) {
		status = "F" // Failed
		msg = fmt.Sprintf("Error: in nodes touched. Expected %d got %d", expectedTouchNodes, nodes)
		t.Error(msg)
	}
	if compareStat(levels, expectedTouchLvl) {
		status = "F" // Failed
		msg += fmt.Sprintf(" | Error: in nodes touched at levels. Expected %v got %v", expectedTouchLvl, levels)
		t.Error(msg)
	}

	if len(expectedJSON) > 0 && compareJSON(result, expectedJSON) {
		t.Error("JSON is not as expected: ")
	}
	//
	// must check if stats have been populated which will not be the case when all nodes have failed to pass the filter.
	// note: this code presumes expected variables always have values even when nothing is expected (in which case they will be populated with zero values)
	var (
		fetches_, nodes_ int
		levels_          []int
		abort_           bool
	)
	if len(abort) > 0 {
		abort_ = abort[0]
	} else {
		abort_ = false
	}
	if levels != nil {
		levels_ = levels.([]int)
	}
	if fetches != nil {
		fetches_ = fetches.(int)
	}
	if nodes != nil {
		nodes_ = nodes.(int)
	}
	SaveTestResult(t.Name(), status, nodes_, levels_, t1.Sub(t0), t2.Sub(t1), msg, result, fetches_, abort_)
	//
	// clear
	//
	expectedJSON = ``
	expectedTouchNodes = -1
	expectedTouchLvl = []int{}
}

func SaveTestResult(test string, status string, nodes int, levels []int, parseET time.Duration, execET time.Duration, msg string, json string, fetches int, abort bool) {

	if abort {
		return
	}

	stx := tx.New("SaveTestResult").DB("mysql-GoGraph")
	smut := mut.NewInsert("Log$GoTest").AddMember("LogDT", "$CURRENT_TIMESTAMP$").AddMember("Test", test).AddMember("Status", status).AddMember("Nodes", nodes)
	smut.AddMember("Levels", fmt.Sprintf("%v", levels)).AddMember("ParseET", parseET.Seconds()*1000).AddMember("ExecET", execET.Seconds()*1000)
	smut.AddMember("JSON", json).AddMember("DBread", fetches).AddMember("Msg", msg)
	stx.Add(smut)

	err := stx.Execute()
	if err != nil {
		fmt.Println(err)
	}

}
