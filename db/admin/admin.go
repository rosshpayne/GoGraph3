//go:build dynamodb
// +build dynamodb

package admin

//
// admin exists because of package "cycle" issues when some of its functions (print, save2DB) where part of the db/stats package.
// The particular cycle in question was:
//    stats.Save -> tx -> db -> stats   (cycle completes)
// So admin was created that provides the save and print functions
//    admin.Save -> tx -> db -> stats   (no cycle)
//
import (
	"context"
	"fmt"
	"sync"
	"time"

	slog "github.com/GoGraph/syslog"

	"github.com/GoGraph/db/stats"
	param "github.com/GoGraph/dygparam"
	"github.com/GoGraph/run"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx"
)

const logid = "dbadmin"

func alertlog(s string) {
	slog.LogAlert(logid, s)
}

// type statPK struct {
// 	pk []byte
// 	sk string
// }

// var keys []statPK

// func saveKey(pk []byte, sk string) {
// 	keys := append(keys, statPK{pk, sk})
// }

var (
	ctxCancel context.CancelFunc
	wgSnap    sync.WaitGroup
)

func Setup() {

	ctxSnap, c := context.WithCancel(context.Background())
	ctxCancel = c
	wgSnap.Add(1)

	snapStats(ctxSnap, 15)

	return
}

func Cancel() {
	ctxCancel()
}

func Persist() {
	alertlog("About to persist dynamodb statistics")
	// stop snapshot save
	ctxCancel()
	// wait for snap save goroutine to respond to cancel
	wgSnap.Wait()
	// print and save
	PrintStats()
	saveStats()
	alertlog("dynamodb statistics saved")
}

func snapStats(ctxSnap context.Context, snapInterval int) {

	go func() {
		defer wgSnap.Done()

		alertlog("snapStats service started.")
		for {
			select {

			case <-time.After(time.Duration(snapInterval) * time.Second):
				saveStats(false)

			case <-ctxSnap.Done():
				alertlog("snapStats service shutdown.")
				return
			}
		}
	}()
}

func saveStats(final_ ...bool) {

	// operation statistics
	// Table: run_stats
	// run,    sortk,           <statistics>
	// Label: db#name,          execz,mean,stddev, p50, p80, min,max, sum
	//        cap#db#name       capacity
	//        cap#db#tbl#name   capacity
	//        cap#db#gsi#name   capacity
	//        cap#db#lsi#name   capacity
	//        cap#tbl#name      capacity
	//        cap#gsi#name      capacity
	//        cap#lsi#name      capacity
	var (
		stx   *tx.Handle
		final bool
	)

	stats.Save.Lock()
	defer stats.Save.Unlock()

	// default is zero argument for last save (at end of program).
	if len(final_) == 0 || (len(final_) > 0 && final_[0]) {
		// only perform aggregation on final save, typically issued at end-of-execution
		final = true
		stats.AggregateDurationStats()
	}

	ctx := context.Background()

	runid := run.GetRunId()
	tblRunStat := tbl.RunStat

	stx = tx.NewBatchContext(ctx, param.StatsSaveTag)
	//stx = tx.NewTxContext(ctx, param.StatsSaveTag) //.DB("mysql-GoGraph")

	for k, v := range stats.DurTx {
		var mm *stats.I64mmx
		sk := "db#tx#" + k + "#time"
		mmx := v.MMX()
		m := stx.NewInsert(tblRunStat).AddMember("run", runid).AddMember("sortk", sk)
		m.AddMember("execs", mmx.Cnt).AddMember("min", mmx.Min).AddMember("max", mmx.Max).AddMember("sum", mmx.Sum).AddMember("mean", float64(mmx.Sum/mmx.Cnt)/1000000.0)
		if final {

			mut := v.GetNumMutations()
			if mut != nil {
				mm = mut.MMX()
			}
			m.AddMember("SampleMean", v.Mean()).AddMember("SD", v.SD()).AddMember("SampleSize", v.SampleSize())
			m.AddMember("p50", v.P50()).AddMember("p80", v.P80())
			m.AddMember("min_mutations_per_exec", mm.Min).AddMember("max_mutations_per_exec", mm.Max).AddMember("mutations_total", mm.Sum)
		}
	}
	for k, v := range stats.DurTbl {
		sk := "db#tbl#" + k + "#time"
		mmx := v.MMX()
		m := stx.NewInsert(tblRunStat).AddMember("run", runid).AddMember("sortk", sk)
		m.AddMember("execs", mmx.Cnt).AddMember("min", mmx.Min).AddMember("max", mmx.Max).AddMember("sum", mmx.Sum).AddMember("mean", float64(mmx.Sum/mmx.Cnt)/1000000.0)
		if final {
			m.AddMember("SampleMean", v.Mean()).AddMember("SD", v.SD()).AddMember("p50", v.P50()).AddMember("p80", v.P80())
			m.AddMember("min", mmx.Min).AddMember("max", mmx.Max).AddMember("sum", mmx.Sum).AddMember("SampleSize", v.SampleSize())
		}
	}
	//
	for k, v := range stats.ApiCnt {
		sk := "db#tbl#" + k + "#api"
		m := stx.NewInsert(tblRunStat).AddMember("run", runid).AddMember("sortk", sk)
		m.AddMember("Query", v[stats.Query]).AddMember("GetItem", v[stats.GetItem]).AddMember("Scan", v[stats.Scan])
		m.AddMember("BatchInsert", v[stats.BatchInsert]).AddMember("BatchDelete", v[stats.BatchDelete]).AddMember("Transaction", v[stats.Transaction])
		m.AddMember(stats.PutItem.String(), v[stats.PutItem]).AddMember(stats.UpdateItem.String(), v[stats.UpdateItem])
		m.AddMember(stats.UpdateItemCF.String(), v[stats.UpdateItemCF]).AddMember(stats.PutItemCF.String(), v[stats.PutItemCF]).AddMember(stats.DeleteItemCF.String(), v[stats.DeleteItemCF])
	}
	for k, v := range stats.ApiMuts {
		sk := "db#tbl#" + k + "#muts"
		m := stx.NewInsert(tblRunStat).AddMember("run", runid).AddMember("sortk", sk)
		m.AddMember("Query", v[stats.Query]).AddMember("GetItem", v[stats.GetItem]).AddMember("Scan", v[stats.Scan])
		m.AddMember("BatchInsert", v[stats.BatchInsert]).AddMember("BatchDelete", v[stats.BatchDelete]).AddMember("Transaction", v[stats.Transaction])
		m.AddMember("PutItem", v[stats.PutItem]).AddMember("UpdateItem", v[stats.UpdateItem])
		m.AddMember(stats.UpdateItemCF.String(), v[stats.UpdateItemCF]).AddMember(stats.PutItemCF.String(), v[stats.PutItemCF]).AddMember(stats.DeleteItemCF.String(), v[stats.DeleteItemCF])
	}
	//
	for k, v := range stats.CapByTx {
		sk := "db#tx#" + k

		if v.CapacityUnits != nil {
			sk := sk + "#CapacityUnits"
			mmx := v.CapacityUnits.MMX()
			m := stx.NewInsert(tblRunStat).AddMember("run", runid)
			m.AddMember("sortk", sk).AddMember("execs", mmx.Cnt).AddMember("min", mmx.Min).AddMember("max", mmx.Max).AddMember("sum", mmx.Sum)
		}
		if v.ReadCapacityUnits != nil {
			sk := sk + "#ReadCapacityUnits"
			mmx := v.ReadCapacityUnits.MMX()
			m := stx.NewInsert(tblRunStat).AddMember("run", runid)
			m.AddMember("sortk", sk).AddMember("execs", mmx.Cnt).AddMember("min", mmx.Min).AddMember("max", mmx.Max).AddMember("sum", mmx.Sum)
		}
		if v.WriteCapacityUnits != nil {
			sk := sk + "#WriteCapacityUnits"
			mmx := v.WriteCapacityUnits.MMX()
			m := stx.NewInsert(tblRunStat).AddMember("run", runid)
			m.AddMember("sortk", sk).AddMember("execs", mmx.Cnt).AddMember("min", mmx.Min).AddMember("max", mmx.Max).AddMember("sum", mmx.Sum)
		}
		for k, v := range v.Table {
			{
				if v.CapacityUnits != nil {
					sk := sk + "#tbl#" + k + "#CapacityUnits"
					mmx := v.CapacityUnits.MMX()
					m := stx.NewInsert(tblRunStat).AddMember("run", runid)
					m.AddMember("sortk", sk).AddMember("max", mmx.Max).AddMember("min", mmx.Min).AddMember("execs", mmx.Cnt).AddMember("sum", mmx.Sum)
				}
			}
			{
				if v.ReadCapacityUnits != nil {
					sk := sk + "#tbl#" + k + "#ReadCapacityUnits"
					mmx := v.ReadCapacityUnits.MMX()
					m := stx.NewInsert(tblRunStat).AddMember("run", runid)
					m.AddMember("sortk", sk).AddMember("max", mmx.Max).AddMember("min", mmx.Min).AddMember("execs", mmx.Cnt).AddMember("sum", mmx.Sum)
				}
			}
			{
				if v.WriteCapacityUnits != nil {
					sk := sk + "#tbl#" + k + "#WriteCapacityUnits"
					mmx := v.WriteCapacityUnits.MMX()
					m := stx.NewInsert(tblRunStat).AddMember("run", runid)
					m.AddMember("sortk", sk).AddMember("max", mmx.Max).AddMember("min", mmx.Min).AddMember("execs", mmx.Cnt).AddMember("sum", mmx.Sum)
				}
			}
		}
		for k, v := range v.Retrieved {
			m := stx.NewInsert(tblRunStat).AddMember("run", runid)
			sk := sk + "#tbl#" + k + "#Retrieved"
			mmx := v.MMX()
			m.AddMember("sortk", sk).AddMember("max", mmx.Max).AddMember("min", mmx.Min).AddMember("execs", mmx.Cnt).AddMember("sum", mmx.Sum)
		}
		for k, v := range v.Scanned {
			m := stx.NewInsert(tblRunStat).AddMember("run", runid)
			sk := sk + "#tbl#" + k + "#Scanned"
			mmx := v.MMX()
			m.AddMember("sortk", sk).AddMember("max", mmx.Max).AddMember("min", mmx.Min).AddMember("execs", mmx.Cnt).AddMember("sum", mmx.Sum)
		}
		for k, v := range v.GlobalSecondaryIndexes {
			sk := sk + "#gsi#" + k
			if v.CapacityUnits != nil {
				sk := sk + "#CapacityUnits"
				mmx := v.CapacityUnits.MMX()
				m := stx.NewInsert(tblRunStat).AddMember("run", runid)
				m.AddMember("sortk", sk).AddMember("execs", mmx.Cnt).AddMember("min", mmx.Min).AddMember("max", mmx.Max).AddMember("sum", mmx.Sum)
			}
			if v.ReadCapacityUnits != nil {
				sk := sk + "#ReadCapacityUnits"
				mmx := v.ReadCapacityUnits.MMX()
				m := stx.NewInsert(tblRunStat).AddMember("run", runid)
				m.AddMember("sortk", sk).AddMember("execs", mmx.Cnt).AddMember("min", mmx.Min).AddMember("max", mmx.Max).AddMember("sum", mmx.Sum)
			}
			if v.WriteCapacityUnits != nil {
				sk := sk + "#WriteCapacityUnits"
				mmx := v.WriteCapacityUnits.MMX()
				m := stx.NewInsert(tblRunStat).AddMember("run", runid)
				m.AddMember("sortk", sk).AddMember("execs", mmx.Cnt).AddMember("min", mmx.Min).AddMember("max", mmx.Max).AddMember("sum", mmx.Sum)
			}
		}
		for k, v := range v.LocalSecondaryIndexes {
			sk := sk + "#lsi#" + k
			if v.CapacityUnits != nil {
				sk := sk + "#CapacityUnits"
				mmx := v.CapacityUnits.MMX()
				m := stx.NewInsert(tblRunStat).AddMember("run", runid)
				m.AddMember("sortk", sk).AddMember("execs", mmx.Cnt).AddMember("min", mmx.Min).AddMember("max", mmx.Max).AddMember("sum", mmx.Sum)
			}
			if v.ReadCapacityUnits != nil {
				sk := sk + "#ReadCapacityUnits"
				mmx := v.ReadCapacityUnits.MMX()
				m := stx.NewInsert(tblRunStat).AddMember("run", runid)
				m.AddMember("sortk", sk).AddMember("execs", mmx.Cnt).AddMember("min", mmx.Min).AddMember("max", mmx.Max).AddMember("sum", mmx.Sum)
			}
			if v.WriteCapacityUnits != nil {
				sk := sk + "#WriteCapacityUnits"
				mmx := v.WriteCapacityUnits.MMX()
				m := stx.NewInsert(tblRunStat).AddMember("run", runid)
				m.AddMember("sortk", sk).AddMember("execs", mmx.Cnt).AddMember("min", mmx.Min).AddMember("max", mmx.Max).AddMember("sum", mmx.Sum)
			}
		}
	}
	//
	for k, v := range stats.CapByGSI {
		sk := "db#gsi#" + k
		if v.CapacityUnits != nil {
			sk := sk + "#CapacityUnits"
			mmx := v.CapacityUnits.MMX()
			m := stx.NewInsert(tblRunStat).AddMember("run", runid)
			m.AddMember("sortk", sk).AddMember("execs", mmx.Cnt).AddMember("min", mmx.Min).AddMember("max", mmx.Max).AddMember("sum", mmx.Sum)
		}
		if v.ReadCapacityUnits != nil {
			sk := sk + "#ReadCapacityUnits"
			mmx := v.ReadCapacityUnits.MMX()
			m := stx.NewInsert(tblRunStat).AddMember("run", runid)
			m.AddMember("sortk", sk).AddMember("execs", mmx.Cnt).AddMember("min", mmx.Min).AddMember("max", mmx.Max).AddMember("sum", mmx.Sum)
		}
		if v.WriteCapacityUnits != nil {
			sk := sk + "#WriteCapacityUnits"
			mmx := v.WriteCapacityUnits.MMX()
			m := stx.NewInsert(tblRunStat).AddMember("run", runid)
			m.AddMember("sortk", sk).AddMember("execs", mmx.Cnt).AddMember("min", mmx.Min).AddMember("max", mmx.Max).AddMember("sum", mmx.Sum)
		}
	}
	//
	for k, v := range stats.CapByLSI {
		sk := "db#lsi#" + k
		if v.CapacityUnits != nil {
			sk := sk + "#CapacityUnits"
			mmx := v.CapacityUnits.MMX()
			m := stx.NewInsert(tblRunStat).AddMember("run", runid)
			m.AddMember("sortk", sk).AddMember("execs", mmx.Cnt).AddMember("min", mmx.Min).AddMember("max", mmx.Max).AddMember("sum", mmx.Sum)
		}
		if v.ReadCapacityUnits != nil {
			sk := sk + "#ReadCapacityUnits"
			mmx := v.ReadCapacityUnits.MMX()
			m := stx.NewInsert(tblRunStat).AddMember("run", runid)
			m.AddMember("sortk", sk).AddMember("execs", mmx.Cnt).AddMember("min", mmx.Min).AddMember("max", mmx.Max).AddMember("sum", mmx.Sum)
		}
		if v.WriteCapacityUnits != nil {
			sk := sk + "#WriteCapacityUnits"
			mmx := v.WriteCapacityUnits.MMX()
			m := stx.NewInsert(tblRunStat).AddMember("run", runid)
			m.AddMember("sortk", sk).AddMember("execs", mmx.Cnt).AddMember("min", mmx.Min).AddMember("max", mmx.Max).AddMember("sum", mmx.Sum)
		}
	}
	//
	for k, v := range stats.CapByTable {
		sk := "db#tbl#" + k
		if v.CapacityUnits != nil {
			sk := sk + "#CapacityUnits"
			mmx := v.CapacityUnits.MMX()
			m := stx.NewInsert(tblRunStat).AddMember("run", runid)
			m.AddMember("sortk", sk).AddMember("execs", mmx.Cnt).AddMember("min", mmx.Min).AddMember("max", mmx.Max).AddMember("sum", mmx.Sum)
		}
		if v.ReadCapacityUnits != nil {
			sk := sk + "#ReadCapacityUnits"
			mmx := v.ReadCapacityUnits.MMX()
			m := stx.NewInsert(tblRunStat).AddMember("run", runid)
			m.AddMember("sortk", sk).AddMember("execs", mmx.Cnt).AddMember("min", mmx.Min).AddMember("max", mmx.Max).AddMember("sum", mmx.Sum)
		}
		if v.WriteCapacityUnits != nil {
			sk := sk + "#WriteCapacityUnits"
			mmx := v.WriteCapacityUnits.MMX()
			m := stx.NewInsert(tblRunStat).AddMember("run", runid)
			m.AddMember("sortk", sk).AddMember("execs", mmx.Cnt).AddMember("min", mmx.Min).AddMember("max", mmx.Max).AddMember("sum", mmx.Sum)
		}
	}
	sk := "db#lastUpdate"
	stx.NewInsert(tblRunStat).AddMember("run", runid).AddMember("sortk", sk).AddMember("t", param.CURRENT_TIMESTAMP)

	err := stx.Execute()
	if err != nil {
		panic(err)
	}

}

func PrintStats() {

	// calculate percentiles

	// for k, v := range stats.AcTx {
	// 	switch v {
	// 	case query.GetItem:
	// 		fmt.Printf("Access Type: %s %s\n", k, "GetItem")
	// 	case query.Scan:
	// 		fmt.Printf("Access Type: %s %s\n", k, "Scan")
	// 	case query.Query:
	// 		fmt.Printf("Access Type: %s %s\n", k, "Query")
	// 	}
	// }

	for k, v := range stats.CapByTx {
		fmt.Println("============= Operation Stats ================")
		fmt.Printf("capByTX     %s:  \n %s\n", k, v.String())
		fmt.Println("Table: ", v.TableName)
		fmt.Println("Items: ", v.Retrieved)
		fmt.Println("Scanned: ", v.Scanned)
	}
	for k, v := range stats.CapByTable {
		fmt.Println("============= Table Stats ================")
		fmt.Printf("CapByTable      %s:  \n %s\n", k, v.String())
	}
	for k, v := range stats.CapByGSI {
		fmt.Println("=============  GSI Stats ================")
		fmt.Printf("CapByGSI      %s:  \n %s\n", k, v.String())
	}

	for k, v := range stats.CapByLSI {
		fmt.Println("============= LSI Stats ================")
		fmt.Printf("CapByLSI      %s:  \n %s\n", k, v.String())
	}

}
