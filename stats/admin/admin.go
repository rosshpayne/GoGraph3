package admin

//
// admin exists because of package cycle issues when its functions (print, save2DB) where enclosed in the stats package.
// The particular cycle in question was:
//    stats.Save -> tx -> db -> stats   (cycle completes)
// So admin was created that provides the save and print functions
//    admin.Save -> tx -> db -> stats   (no cycle)
//

import (
	"fmt"
	"strings"
	"time"

	param "github.com/GoGraph/dygparam"
	"github.com/GoGraph/run"
	"github.com/GoGraph/stats"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx"
)

var execCnt int

// same methods used by db/admin. Maybe turned into a interface for some sort of admininstration system.
func Setup() {}

func Cancel() {}

func Persist() {
	persistOnExit()
}

func persistOnExit() {

	execCnt++

	if execCnt > 1 {
		panic(fmt.Errorf("Persist for statistics is designed to run only once at end of the program. Persist should not be run while statistics are being collected."))
	}
	stats.AggregateDurationStats()

	runid := run.GetRunId()

	stx := tx.NewBatch(param.StatsSystemTag)

	tz, _ := time.LoadLocation(param.TZ)

	for k, vv := range stats.GetLabellMap() {
		for _, v := range *vv {
			if v == nil {
				continue
			}
			var sk strings.Builder
			mmx := v.GetMMS()
			sk.WriteString("wait#")
			sk.WriteString(v.Event())
			sk.WriteString("#tag#")
			sk.WriteString(string(k))

			m := stx.NewInsert(tbl.RunStat).AddMember("run", runid).AddMember("sortk", sk.String())
			m.AddMember("execs", mmx.Cnt).AddMember("SampleMean", v.Mean()).AddMember("SD", v.SD()).AddMember("Sum", mmx.Sum).AddMember("MaxValue", mmx.Max).AddMember("MinValue", mmx.Min)
			m.AddMember("p50", v.P50()).AddMember("p80", v.P80()).AddMember("SampleSize", v.SampleSize()).AddMember("LastSampled", v.LastSample().In(tz).String())
			m.AddMember("Mean", float64(mmx.Sum/mmx.Cnt)/1000000.0)
		}
	}

	err := stx.Execute()
	if err != nil {
		panic(err)
	}
}
