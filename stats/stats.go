package stats

import (
	"fmt"
	"sync"
	"time"

	param "github.com/GoGraph/dygparam"
	hdr "github.com/HdrHistogram/hdrhistogram-go"
)

type Event int8
type Label string

const (
	EvWaitOnCh Event = iota + 1
	EvWaitSendOnCh
	EvWaitRecvOnCh
	EvWaitOnMx
	EvWaitOnRWMx
	EvAPI
	_limit_
)

func (e Event) String() string {
	switch e {
	case 0:
		return "NA"
	case EvWaitOnCh:
		return "OnCh"
	case EvWaitSendOnCh:
		return "SendOnCh"
	case EvWaitRecvOnCh:
		return "RecvOnCh"
	case EvWaitOnMx:
		return "OnMutex"
	case EvWaitOnRWMx:
		return "OnRWMx"
	case EvAPI:
		return "API"
	}
	return "NoString"
}

func init() {
	//
	lblMap = make(evLabelMap)
	regMap = make(map[Label]*limits)

	// set system sample duration used when user defined statistic lable limits are not set.
	var err error
	SampleDuration, err = time.ParseDuration(param.SampleDurWaits)
	if err != nil {
		panic(err)
	}

}

// any duration based stats e.g. cloud api elapsed time, go channel wait, mutex wait
// type durEvent struct {
// 	label    Label // wait on mutex, wait on channel, api elapsed time
// 	evDur    Event //
// 	duration int64
// }

type durStats struct {
	event  Event
	last   time.Time
	d      []int64 // duration values
	mean   float64
	stddev float64
	p50    float64 // milliseconds
	p80    float64 // milliseconds
	m      *i64mmx // min,max,sum,x
}

func (d *durStats) Mean() float64 {
	return d.mean
}

func (d *durStats) SD() float64 {
	return d.stddev
}
func (d *durStats) P50() float64 {
	return d.p50
}
func (d *durStats) P80() float64 {
	return d.p80
}

func (s *durStats) GetDurs() []int64 {
	return s.d
}

func (s *durStats) GetMMS() *I64mmx {
	if s != nil {
		return s.m.MMX()
	}
	return nil
}

func (s *durStats) SampleSize() int {
	return len(s.d)
}

func (s *durStats) LastSample() time.Time {
	return s.last
}

func (s *durStats) Keep(l Label) (time.Time, bool) {
	var (
		saveit    bool
		sampledur time.Duration
	)
	t := time.Now()
	if r, ok := regMap[l]; !ok {
		sampledur = SampleDuration
	} else {
		sampledur = r.sampleDur
	}

	if t.Sub(s.last).Milliseconds() > sampledur.Milliseconds() {
		saveit = true
	}
	return t, saveit

}

func (s *durStats) Event() string {
	return s.event.String()
}

//type evLabelMap map[Label]*durStats

type eventS []*durStats
type evLabelMap map[Label]*eventS

var (
	lblMap evLabelMap
	//	evDur  eventS
	save sync.RWMutex
	//statistics thresholds
	SampleDuration time.Duration // duration between samples for stats gathering when not stipulated.
	//
)

type I64mmx struct {
	Min int64
	Max int64
	Cnt int64
	Sum int64
}

type i64mmx struct {
	min int64
	max int64
	cnt int64
	sum int64
}

func newi64mmx(v int64) *i64mmx {
	return &i64mmx{min: v, max: v, sum: v, cnt: 1}
}

func (c *i64mmx) update(v int64) {

	if c.min > v {
		c.min = v
	}
	if c.max < v {
		c.max = v
	}
	c.sum += v
	c.cnt++
}

func (c *i64mmx) String() string {
	if c != nil {
		return fmt.Sprintf("Min: %d, Max: %d  Cnt: %d  Sum: %d  Avg: %g\n", c.min, c.max, c.cnt, c.sum, float64(c.sum)/float64(c.cnt))
	}
	return ""
}

func (i *i64mmx) MMX() *I64mmx {
	return &I64mmx{Min: i.min, Max: i.max, Cnt: i.cnt, Sum: i.sum}
}

type limits struct {
	sampleDur time.Duration // upto sampleDur per second
	maxSample int           // max number of samples
}

var regMap map[Label]*limits
var reg sync.Mutex

func Register(lbl Label, sampledur time.Duration, maxSam ...int) {

	save.Lock()

	if l, ok := regMap[lbl]; !ok {
		if len(maxSam) > 0 {
			regMap[lbl] = &limits{sampleDur: sampledur, maxSample: maxSam[0]}
		} else {
			regMap[lbl] = &limits{sampleDur: sampledur}
		}

	} else {

		l.sampleDur = sampledur
		if len(maxSam) > 0 {
			l.maxSample = maxSam[0]
		}
	}

	save.Unlock()
}

func AggregateDurationStats() {

	// chose not to aggregate by event - as the resulting data is pretty meaningless in terms of identifying any problem area

	// for _, v := range evDur {
	// 	hist := hdr.New(-900000000, 900000000, 5)
	// 	if v == nil {
	// 		continue
	// 	}
	// 	for _, val := range v.d {
	// 		val := val / 1000 // microseconds
	// 		if err := hist.RecordValue(val); err != nil {
	// 			panic(err)
	// 		}
	// 	}
	// 	// output in millsecond (1000 microseconds)
	// 	v.mean = float64(hist.Mean()) / 1000
	// 	v.stddev = float64(hist.StdDev()) / 1000
	// 	v.p50 = float64(hist.ValueAtQuantile(50)) / 1000
	// 	v.p80 = float64(hist.ValueAtQuantile(80)) / 1000
	// 	//
	// 	//fmt.Printf("TX: k %s \np50: %g\np80: %g\n mean: %g, stddev: %g \n min:  %d max: %d cnt: %d \n", k, v.p50, v.p80, v.mean, v.stddev, v.m.min, v.m.max, v.m.cnt)

	// }

	// aggregate by label
	for k, v := range lblMap {
		var first int
		fmt.Println("stats output: label: ", k)
		if v == nil {
			fmt.Println("stats output: content: is null")
		}
		for _, e := range *v {
			hist := hdr.New(-900000000, 900000000, 5)
			if e == nil {
				continue
			}
			// ignore first value in sample set if more than one in sample
			if len(e.d) == 1 {
				first = 0
			} else {
				first = 1
			}
			for _, val := range e.d[first:] {
				val := val / 1000 // microseconds
				if err := hist.RecordValue(val); err != nil {
					panic(err)
				}
			}
			// output in millsecond (1000 microseconds)
			e.mean = float64(hist.Mean()) / 1000
			e.stddev = float64(hist.StdDev()) / 1000
			e.p50 = float64(hist.ValueAtQuantile(50)) / 1000
			e.p80 = float64(hist.ValueAtQuantile(80)) / 1000
			//
			//	fmt.Printf("TX: k %s \np50: %g\np80: %g\n mean: %g, stddev: %g \n min:  %d max: %d cnt: %d \n", k, v.p50, v.p80, v.mean, v.stddev, v.m.min, v.m.max, v.m.cnt)
		}
	}
}

func RecvOnCh(ch chan struct{}, label Label) {
	t0 := time.Now()

	<-ch

	// currently aggregate into WaitOnCh rather than WaitRecvOnCh
	saveEventStats(EvWaitRecvOnCh, time.Now().Sub(t0), label)
}

func SendOnCh(ch chan struct{}, label Label) {
	t0 := time.Now()

	ch <- struct{}{}

	saveEventStats(EvWaitSendOnCh, time.Now().Sub(t0), label)
}

func Run(f func(), label Label) {

	t0 := time.Now()
	f()
	saveEventStats(EvAPI, time.Now().Sub(t0), label)
}

func SaveEventStats(ev Event, dur time.Duration, label Label) {

	saveEventStats(ev, dur, label)

}

// saveEventStats stores the stats data into slice and map structures (if label is provided). It is concurrency safe.
// The stats are stored ina couple of memory structures which start out as a fixed size and will grow only when all attempts to
// use the fixed size has been explored. This is made difficult as the program has no idea how long the program will run.
// If it is short then all the stats should be saved. If it is long then only a small or representative subset of the data will be saved
// How is this done:
// 1. at the source - for multiple threaded applications

func saveEventStats(ev Event, dur time.Duration, label Label) {

	save.Lock()

	if l, ok := lblMap[label]; !ok {

		evs := make(eventS, _limit_, _limit_)
		evs[ev] = &durStats{event: ev, d: []int64{int64(dur)}, m: newi64mmx(int64(dur)), last: time.Now()}
		lblMap[label] = &evs

	} else {

		// determine whether the data should be saved based on label limit values (see Register())
		dr := (*l)[ev]
		if t, sv := dr.Keep(label); sv {
			if len(dr.d) < param.MaxSampleSet {
				dr.d = append(dr.d, int64(dur))
				dr.last = t
			}
		}
		// keep track of min,max,execs,sum for all save requests
		dr.m.update(int64(dur))
	}
	save.Unlock()
}

// persistEventStats is used to perform a once off save to the database of all the event statistics.
// It is not concurrency safe as it is not intended to be run while the program is executing, only when the program exits.

func GetLabellMap() evLabelMap {
	return lblMap
}

// store stats in limited
