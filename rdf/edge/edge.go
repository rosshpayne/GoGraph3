package edge

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	param "github.com/GoGraph/dygparam"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx"
	"github.com/GoGraph/uuid"
)

const logid = "EdgeLoad"

// reverse sort type. Can use to "convert" existing []int types.
type IntSliceR []int

func (x IntSliceR) Len() int           { return len(x) }
func (x IntSliceR) Less(i, j int) bool { return x[j] < x[i] }
func (x IntSliceR) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

// Sort is a convenience method: x.Sort() calls Sort(x).
func (x IntSliceR) Sort() { sort.Sort(x) }

type NEdges struct {
	Puid  uuid.UID
	Edges int
}

type dumpPy struct {
	respCh chan struct{}
	tbl    string
}

var (
	LoadCh    chan NEdges
	Dump2DBCh chan dumpPy
)

func Persist(tbl string) {
	d := dumpPy{respCh: make(chan struct{}), tbl: tbl}
	Dump2DBCh <- d
	<-d.respCh

}

func PowerOn(ctx context.Context, wp *sync.WaitGroup, wgEnd *sync.WaitGroup) {

	defer wgEnd.Done()
	wp.Done()

	var (
		maxEdges int
		edges    map[int][]uuid.UID
	)
	LoadCh = make(chan NEdges, 55)
	Dump2DBCh = make(chan dumpPy)

	edges = make(map[int][]uuid.UID)

	slog.Log(param.Logid, "Powering up...")

	for {

		select {

		case ec := <-LoadCh:

			if ec.Edges > maxEdges {
				maxEdges = ec.Edges
			}
			if n, ok := edges[ec.Edges]; !ok {
				edges[ec.Edges] = []uuid.UID{ec.Puid}
			} else {
				n = append(n, ec.Puid)
				edges[ec.Edges] = n
			}

		case py := <-Dump2DBCh:

			fmt.Println("edge dump.....", len(edges))
			var (
				err    error
				bc, bi int     // batch item counter, bulkinsert item counter
				bid    int = 1 // batch id
			)

			cnt := make([]int, len(edges), len(edges))
			i := 0
			for k := range edges {
				cnt[i] = k
				i++
			}
			IntSliceR(cnt).Sort()

			slog.Log(logid, "Start Edge save...")
			t0 := time.Now()
			//etx := tx.NewTx("edge")
			//etx := tx.New("edge").DB("mysql-GoGraph").Prepare()
			etx := tx.NewTxContext(ctx, "edge").DB("mysql-GoGraph").Prepare()
			//etx := tx.NewTxContext(ctx, "edge").DB("mysql-GoGraph")

			for _, v := range cnt {

				for _, n := range edges[v] {

					etx.NewInsert(tbl.Name(py.tbl)).AddMember("Bid", bid).AddMember("Puid", n).AddMember("Cnt", v)
					bi++
					bc++
					//
					if bi == param.DBbulkInsert {
						// execute active batch of mutations now rather then keep adding to batch-of-batches to reduce memory requirements.
						err = etx.Execute()
						if err != nil {
							break
						}
						//etx = tx.NewTx("edge")
						//tx = tx.New("edge").DB("mysql-GoGraph").Prepare()
						etx = tx.NewTxContext(ctx, "edge").DB("mysql-GoGraph").Prepare()
						//etx = tx.NewTxContext(ctx, "edge").DB("mysql-GoGraph")
						bi = 0
					}
					// increment batch id when number of items in current batch exceeds 150
					if bc == 150 {
						bc = 0
						bid++
					}
				}
				if err != nil {
					break
				}
			}

			if err != nil {
				for _, e := range etx.GetErrors() {
					slog.Log(logid, e.Error())
				}
				slog.Log(logid, fmt.Sprintf("End Edge Save, Errors: %d  Duration: %s", len(etx.GetErrors()), time.Now().Sub(t0)))
			} else {

				err = etx.Execute()
				if err != nil {
					for _, e := range etx.GetErrors() {
						slog.Log(logid, e.Error())
					}
					slog.Log(logid, fmt.Sprintf("End Edge Save, Errors: %d  Duration: %s", len(etx.GetErrors()), time.Now().Sub(t0)))
				} else {
					slog.Log(logid, fmt.Sprintf("End Edge Save, Errors: 0 Duration: %s", time.Now().Sub(t0)))
				}

			}

			py.respCh <- struct{}{}

		case <-ctx.Done():
			slog.Log(logid, "Shutdown.")
			return

		}
	}
}
