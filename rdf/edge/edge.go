package edge

import (
	"context"
	"fmt"
	"sort"
	"sync"

	param "github.com/GoGraph/dygparam"
	elog "github.com/GoGraph/errlog"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx"
	"github.com/GoGraph/types"
	"github.com/GoGraph/uuid"
)

const LogId = "Edge: "

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

var (
	LoadCh    chan NEdges
	Dump2DBCh chan struct{}
)

func Persist() {
	Dump2DBCh <- struct{}{}
}

func PowerOn(ctx context.Context, wp *sync.WaitGroup, wgEnd *sync.WaitGroup) {

	defer wgEnd.Done()
	wp.Done()

	var (
		maxEdges int
		edges    map[int][]uuid.UID
	)
	LoadCh = make(chan NEdges, 55)
	Dump2DBCh = make(chan struct{})

	edges = make(map[int][]uuid.UID)

	slog.Log(param.Logid, "Powering up...")

	//tblEdge := string(tbl.Edge) + types.GraphName()

	tblEdge, _ := tbl.SetEdgeNames(types.GraphName())

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

		case <-Dump2DBCh:

			var (
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

			etx := tx.New("edge").DB("mysql-GoGraph")
			//etx := tx.NewTx("edge")

			for _, v := range cnt {

				for _, n := range edges[v] {

					etx.NewInsert(tbl.Name(tblEdge)).AddMember("Bid", bid).AddMember("Puid", n).AddMember("Cnt", v)
					bc++
					bi++
					//
					if bi == param.DBbulkInsert {
						// execute active batch of mutations now rather then keep adding to batch-of-batches to reduce memory requirements.
						err := etx.Execute()
						if err != nil {
							elog.Add("Edge: ", fmt.Errorf("Error in bulk insert %w", err))
							panic(err)
						}
						etx = tx.New("edge").DB("mysql-GoGraph")
						//etx = tx.NewTx("edge")
						bi = 0
					}
					// increment batch id when number of items in current batch exceeds 150
					if bc == 150 {
						bc = 0
						bid++
					}
				}

			}
			//
			err := etx.Execute()
			if err != nil {
				slog.Log(param.Logid, err.Error())
			}

		case <-ctx.Done():
			slog.Log(LogId, "Shutdown.")
			return

		}
	}
}
