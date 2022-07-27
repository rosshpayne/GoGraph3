package monitor

import (
	"context"
	"fmt"
	"sync"
	"time"

	slog "github.com/GoGraph/syslog"
)

const (
	// graph query execution statistics - used to validate correctness of each test case
	Candidate = iota
	PassRootFilter
	TouchNode
	TouchLvl
	TouchNodeFiltered
	TouchLvlFiltered
	NodeFetch
	// database statistics
	DBFetch // total db Fetch API calls
	//
	// Operation statistics
	AttachNode
	DetachNode
	LIMIT
	// max sample sizes
	maxDurSample = 1000
)

type Stat struct {
	Id    int
	Lvl   int
	Value interface{}
}

type Fetch struct {
	Fetches       int64
	CapacityUnits float64
	Items         int
	Duration      time.Duration
}

type Request struct {
	Id      int
	ReplyCh chan<- interface{}
}

var (
	StatCh  chan Stat
	GetCh   chan Request
	ClearCh chan struct{}
	PrintCh chan struct{}
	stats   []interface{}
)

func Report() {
	PrintCh <- struct{}{}
}

func PowerOn(ctx context.Context, wps *sync.WaitGroup, wgEnd *sync.WaitGroup) {

	defer wgEnd.Done()

	stats := make([]interface{}, LIMIT, LIMIT)

	StatCh = make(chan Stat)
	ClearCh = make(chan struct{})
	GetCh = make(chan Request)
	PrintCh = make(chan struct{})

	slog.LogAlert("monitor", "Started")
	wps.Done()

	var (
		n   int
		s   Stat
		val int
		ok  bool
	)
	//
	// Note: Select on channel can be a performance killer if not implemented correctly
	//       Better to keep ctx.Done() in main select.
	//       Probing both ctx.Done() and StatCH channel eats CPU and increases Dynamodb API response times by x10.
	//       Test cases go from 50ms to 250ms
	//
	for {

		select {

		case s = <-StatCh:

			switch x := s.Id; x {

			case TouchNode:
				// increment total counter and Level counter

				if stats[x] == nil {
					stats[x] = 1
					a := make([]int, 1, 1)
					stats[TouchLvl] = a
					// build slice to hold level counters
					for len(a) < s.Lvl {
						a = append(a, 0)
						stats[TouchLvl] = a
					}
					a[s.Lvl] += 1
				} else {
					n, _ = stats[x].(int)
					stats[x] = n + 1
					a := stats[TouchLvl].([]int)
					// extend slice to hold level counters (if necessary)
					for len(a)-1 < s.Lvl {
						a = append(a, 0)
						stats[TouchLvl] = a
					}
					a[s.Lvl] += 1
				}

			case TouchNodeFiltered:
				// increment total counter and Level counter

				if stats[x] == nil {
					stats[x] = 1
					a := make([]int, 1, 1)
					stats[TouchLvlFiltered] = a
					// build slice to hold level counters
					for len(a) < s.Lvl {
						a = append(a, 0)
						stats[TouchLvlFiltered] = a
					}
					a[s.Lvl] += 1
				} else {
					n, _ = stats[x].(int)
					stats[x] = n + 1
					a := stats[TouchLvlFiltered].([]int)
					// extend slice to hold level counters (if necessary)
					for len(a)-1 < s.Lvl {
						a = append(a, 0)
						stats[TouchLvlFiltered] = a
					}
					a[s.Lvl] += 1
				}

			case DBFetch:

				var v *Fetch

				if f, ok := s.Value.(*Fetch); !ok {
					panic(fmt.Errorf("Monitor Error: DBFetch has wrong payload type. Should be type monitor.Fetch"))
				} else {
					if stats[s.Id] == nil {
						v = &Fetch{}
						stats[s.Id] = v
					} else {
						v = stats[s.Id].(*Fetch)
					}
					v.Fetches += 1
					v.CapacityUnits += f.CapacityUnits
					v.Items += f.Items
					v.Duration += f.Duration
				}

			default: // increment ... must be int
				if s.Value == nil {
					val = 1
				} else {
					if val, ok = s.Value.(int); !ok {
						val = 1
					}
				}
				if stats[x] == nil {
					stats[x] = val
				} else {
					switch n := stats[s.Id].(type) {
					case int64:
						stats[s.Id] = n + int64(val)
					case int:
						stats[s.Id] = n + val
					}
				}
			}

		// case s := <-AddCh:

		// 	s.Add()

		case <-ClearCh:

			for i, v := range stats {
				switch v.(type) {
				case int:
					stats[i] = 0
				case float64:
					stats[i] = 0.0
				case []int:
					stats[i] = []int{}
				case []float64:
					stats[i] = []float64{}
				}
			}

		case x := <-GetCh:

			x.ReplyCh <- stats[x.Id]

		case <-PrintCh:

			if len(stats) > DBFetch {
				if st := stats[DBFetch]; st != nil {
					slog.LogAlert("monitor", fmt.Sprintf("monitor: %#v %#v\n", stats, *(st.(*Fetch))))
				}
			}

		case <-ctx.Done():

			slog.LogAlert("monitor", "Shutdown.")
			return

		}
	}
}
