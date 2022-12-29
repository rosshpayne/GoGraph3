package run

import (
	"context"
	"fmt"
	"time"

	param "github.com/GoGraph/dygparam"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/uuid"
)

const logid = "run"

type Runid = uuid.UID

var (
	runid  uuid.UID
	tstart time.Time
)

func New(logid string, program string) (uuid.UID, error) {

	var err error
	runid, _ = uuid.MakeUID()
	status := "R"
	param.RunId = runid.String()
	rtx := tx.New(param.StatsSystemTag)
	m := rtx.NewInsert(tbl.Monrun).AddMember("run", runid).AddMember("sortk", "AA").AddMember("start", "$CURRENT_TIMESTAMP$").AddMember("program", program).AddMember("status", status)
	m.AddMember("logfile", param.LogFile)
	rtx.Add(m)
	err = rtx.Execute()
	if err != nil {
		// TODO: check for duplicate error
		return nil, err
	}
	tstart = time.Now()

	return runid, nil
}

func New2(logid string) (runid uuid.UID) {

	runid, _ = uuid.MakeUID()

	return

}

func Finish(err error) {

	ctx := context.Background()

	// use Context version as cancel() may have been called which makes dbHdl context ineffective.
	rtx := tx.NewSingleContext(ctx, param.StatsSystemTag)
	status := "C"
	if err != nil {
		status = "E"
	}
	tfinish := time.Now()

	m := rtx.NewUpdate(tbl.Monrun).AddMember("run", runid, mut.IsKey).AddMember("sortk", "AA", mut.IsKey).AddMember("finish", "$CURRENT_TIMESTAMP$")
	rtx.Add(m.AddMember("status", status).AddMember("Elapsed", tfinish.Sub(tstart).String()))

	err = rtx.Execute()
	if err != nil {
		fmt.Printf("Error in FinishRun(): %s\n", err)
	}
}

func Panic() {

	rtx := tx.New(param.StatsSystemTag)

	status := "P"

	fmt.Println("in run panic....")

	m := rtx.NewUpdate(tbl.Monrun).AddMember("run", runid, mut.IsKey).AddMember("sortk", "AA", mut.IsKey)
	rtx.Add(m.AddMember("Status", status).AddMember("finish", "$CURRENT_TIMESTAMP$"))

	err := rtx.Execute()
	if err != nil {
		fmt.Printf("Error in FinishRun(): %s\n", err)
	}
}

func GetRunId() uuid.UID {
	return runid
}
