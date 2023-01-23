//go:build dynamodb
// +build dynamodb

package stats

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ros2hp/method-db/dynamodb/internal/param"
	"github.com/ros2hp/method-db/log"
	"github.com/ros2hp/method-db/query"

	hdr "github.com/HdrHistogram/hdrhistogram-go"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	//"github.com/aws/aws-sdk-go/service/dynamodb"
)

// type StatId byte
type Source int

const (
	// Oper StatId = iota
	// Table
	// GSI
	// LSI

	// Query
	GetItem Source = iota + 1
	Query
	Scan
	ScanParallel
	// Batch
	BatchInsert
	BatchDelete
	// note - Dynamodb has no BatchUpdate
	// Transaction
	Transaction
	// Single
	PutItem
	UpdateItem
	DeleteItem
	// single resulting in Condition Fail
	PutItemCF
	UpdateItemCF
	DeleteItemCF
	// // Remove attribute (deprecated, now part of UpdateItem)
	// Remove
	//
	_apiLimit_
)

func (a Source) String() string {
	switch a {
	case GetItem:
		return "GetItem"
	case Query:
		return "Query"
	case Scan:
		return "Scan"
	case ScanParallel:
		return "Parallel Scan"
	case BatchInsert:
		return "BatchInsert"
	case BatchDelete:
		return "BatchDelete"
	case Transaction:
		return "Transaction"
	case PutItem:
		return "PutItem"
	case UpdateItem:
		return "UpdateItem"
	case DeleteItem:
		return "DeleteItem"
	case PutItemCF:
		return "PutItem-CF"
	case UpdateItemCF:
		return "UpdateItem-CF"
	case DeleteItemCF:
		return "DeleteItem-CF"
		// case Remove:
		// 	return "RemoveAttribute"
	}
	return "NA"
}

type Label = string // opTx/tx label, table name, gsi name, lsi name

type opItem struct {
	Tag       Label
	Src       Source // batchDelete, batchInsert, Transaction, SingleInsert, SingleUpdate, SingleDelete, Query, GetItem.
	Acs       query.AccessTy
	Tx        *types.ConsumedCapacity
	tblName   string
	Retrieved int64
	Scanned   int64
	Duration  int64
	//TableName string
	Mutations int
}

// type StatTy struct {
// 	Id    StatId
// 	Label Label
// }

// type TxCapacity map[StatTy]*Capacity

type Capacity struct {
	CapacityUnits      float64
	ReadCapacityUnits  float64
	WriteCapacityUnits float64
}

func syslog(s string) {
	var out strings.Builder
	out.WriteString("stats ")
	out.WriteString(s)
	log.LogAlert(out.String())
}

// query statistics, GetItem, Query, Scan
func SaveQueryStat(src Source, tag string, cc *types.ConsumedCapacity, cnt int32, scnt int32, dur time.Duration) {
	if !param.StatsSystem && tag == param.StatsSystemTag {
		return
	}
	if tag == param.StatsSaveTag {
		return
	}

	//fmt.Printf("\nSaveStat:%s  %#v %s\n", src, *cc, dur.String())
	s := NewopItem(src, tag, cc, cnt, scnt, dur, 1)
	s.Add()
}

// all transaction mutations
func SaveTransactStat(tag string, cc []types.ConsumedCapacity, dur time.Duration, muts ...int) {
	if !param.StatsSystem && tag == param.StatsSystemTag {
		return
	}
	if tag == param.StatsSaveTag {
		return
	}
	var mutations int
	if len(muts) > 0 {
		mutations = muts[0]
	}
	for i := 0; i < len(cc); i++ {
		NewopItem(Transaction, tag, &cc[i], 0, 0, dur, mutations).Add()
	}
}

// Batched insert, delete
func SaveBatchStat(src Source, tag string, cc []types.ConsumedCapacity, dur time.Duration, muts int) {
	if !param.StatsSystem && tag == param.StatsSystemTag {
		return
	}
	if tag == param.StatsSaveTag {
		return
	}

	for i := 0; i < len(cc); i++ {
		s := NewopItem(src, tag, &cc[i], 0, 0, dur, muts)
		s.Add()
	}
}

func SaveSingleStat(src Source, tag string, cc *types.ConsumedCapacity, dur time.Duration) {
	SaveStdStat(src, tag, cc, dur)
}

// Single item insert, update, delete
func SaveStdStat(src Source, tag string, cc *types.ConsumedCapacity, dur time.Duration) {
	if !param.StatsSystem && tag == param.StatsSystemTag {
		return
	}
	if tag == param.StatsSaveTag {
		return
	}
	//	fmt.Printf("\nSaveStdStat: %#v %s, %d\n", *cc, dur.String(), 1)
	s := NewopItem(src, tag, cc, 0, 0, dur, 1)
	s.Add()
}

// NewopItem reformats the dynamodb.ConsumedCapacity based on indexes used.
func NewopItem(source Source, tag Label, cc *types.ConsumedCapacity, cnt int32, scnt int32, dur time.Duration, m int) *opItem {
	return &opItem{Tag: tag, Src: source, Tx: cc, Retrieved: int64(cnt), Scanned: int64(scnt), Duration: dur.Nanoseconds(), Mutations: m} //TableName: *cc.TableName, }
}

func NewopItem2(source Source, tag Label, cc *types.ConsumedCapacity, cnt int32, scnt int32, dur time.Duration, m int) *opItem {
	return &opItem{Tag: tag, Src: source, Tx: cc, Retrieved: int64(cnt), Scanned: int64(scnt), Duration: dur.Nanoseconds(), Mutations: m} //TableName: *cc.TableName, }
}

type opTxCapacity struct {
	CapacityUnits      *mmx
	ReadCapacityUnits  *mmx
	WriteCapacityUnits *mmx
}

func (o *opTxCapacity) String() string {
	var w strings.Builder

	if o == nil {
		return "empty.."
	}

	if o.CapacityUnits != nil {
		w.WriteString("      Capacity: ")
		w.WriteString(o.CapacityUnits.String())
	}
	if o.ReadCapacityUnits != nil {
		w.WriteString(" Read Capacity: ")
		w.WriteString(o.ReadCapacityUnits.String())
	}
	if o.WriteCapacityUnits != nil {
		w.WriteString("Write Capacity: ")
		w.WriteString(o.WriteCapacityUnits.String())
	}
	return w.String()
}

func (o *opTxCapacity) update(c types.Capacity) {

	if o == nil { //|| c == nil {
		return
	}
	if c.CapacityUnits != nil {
		o.CapacityUnits.update(c.CapacityUnits)
	}
	if c.ReadCapacityUnits != nil {
		o.ReadCapacityUnits.update(c.ReadCapacityUnits)
	}
	if c.WriteCapacityUnits != nil {
		o.WriteCapacityUnits.update(c.WriteCapacityUnits)
	}

}

func (o *opTxCapacity) update2(c *float64, r *float64, w *float64) {

	if o == nil {
		return
	}
	o.CapacityUnits.update(c)
	o.ReadCapacityUnits.update(r)
	o.WriteCapacityUnits.update(w)

}

func (o *opTxCapacity) initialise(c types.Capacity) *opTxCapacity {

	initialise := func(v *float64) *mmx {
		if v == nil {
			return nil
		}
		return &mmx{min: *v, max: *v, sum: *v, cnt: 1}
	}

	if o == nil {
		return o
	}
	// if c == nil {
	// 	return o
	// }
	o.CapacityUnits = initialise(c.CapacityUnits)
	o.ReadCapacityUnits = initialise(c.ReadCapacityUnits)
	o.WriteCapacityUnits = initialise(c.WriteCapacityUnits)

	return o
}

func (o *opTxCapacity) initialise2(c *float64, r *float64, w *float64) *opTxCapacity {

	initialise := func(v *float64) *mmx {
		if v == nil {
			return nil
		}
		return &mmx{min: *v, max: *v, sum: *v, cnt: 1}
	}

	if o == nil {
		return o
	}
	if c == nil {
		return o
	}
	o.CapacityUnits = initialise(c)
	o.ReadCapacityUnits = initialise(r)
	o.WriteCapacityUnits = initialise(w)

	return o
}

// consumedCapacity (CC) applies to an operation. An dynamodb operation applies to a single table, however an application operation can involve multiple tables
// hence CC must accomoated multiple tables
type consumedCapacity struct {
	CapacityUnits      *mmx
	ReadCapacityUnits  *mmx
	WriteCapacityUnits *mmx

	GlobalSecondaryIndexes map[string]*opTxCapacity
	LocalSecondaryIndexes  map[string]*opTxCapacity

	Table map[string]*opTxCapacity

	TableName []string

	Retrieved map[string]*i64mmx
	Scanned   map[string]*i64mmx
	//duration  *i64mmx
}

func (o *consumedCapacity) String() string {
	var w strings.Builder
	w.WriteString("Consumed Capacity: \n")
	w.WriteString(fmt.Sprintf("Table: %s\n", o.TableName))
	w.WriteString(fmt.Sprintf("CapacityUnits: %s\n", o.CapacityUnits.String()))
	w.WriteString(fmt.Sprintf("ReadCapacityUnits: %s\n", o.ReadCapacityUnits.String()))
	w.WriteString(fmt.Sprintf("WriteCapacityUnits: %s\n", o.WriteCapacityUnits.String()))
	w.WriteString("Table:\n")
	for k, v := range o.Table {
		w.WriteString(fmt.Sprintf("       %s\n", k))
		w.WriteString(fmt.Sprintf("     %s \n", v.String()))
	}
	for k, v := range o.GlobalSecondaryIndexes {
		w.WriteString(fmt.Sprintf("GSI: %s\n", k))
		w.WriteString(fmt.Sprintf("     %s \n", v.String()))
	}
	for k, v := range o.LocalSecondaryIndexes {
		w.WriteString(fmt.Sprintf("LSI:  %s\n", k))
		w.WriteString(fmt.Sprintf("      %s\n", v.String()))
	}
	w.WriteString(" ")
	for k, v := range o.Retrieved {
		w.WriteString(fmt.Sprintf(" Retrieved      %s\n", k))
		w.WriteString(fmt.Sprintf("     %s \n", v.String()))
	}
	for k, v := range o.Scanned {
		w.WriteString(fmt.Sprintf(" Scanned      %s\n", k))
		w.WriteString(fmt.Sprintf("     %s \n", v.String()))
	}

	//	w.WriteString(fmt.Sprintf("Elapsed: %v\n", o.duration))
	return w.String()
}
func (o *consumedCapacity) update(c *types.ConsumedCapacity, r int64, s int64) {

	o.CapacityUnits.update(c.CapacityUnits)
	o.ReadCapacityUnits.update(c.ReadCapacityUnits)
	o.WriteCapacityUnits.update(c.WriteCapacityUnits)

	for k, v := range c.GlobalSecondaryIndexes {
		if o.GlobalSecondaryIndexes == nil {
			o.GlobalSecondaryIndexes = make(map[string]*opTxCapacity)
		}
		if gsi, ok := o.GlobalSecondaryIndexes[k]; !ok {
			opTxc := &opTxCapacity{}
			o.GlobalSecondaryIndexes[k] = opTxc.initialise(v) // &opTxCapacity{CapacityUnits: c, ReadCapacityUnits: r, WriteCapacityUnits: w}

		} else {

			gsi.update(v)
		}
	}
	for k, v := range c.LocalSecondaryIndexes {
		if o.LocalSecondaryIndexes == nil {
			o.LocalSecondaryIndexes = make(map[string]*opTxCapacity)
		}
		if gsi, ok := o.LocalSecondaryIndexes[k]; !ok {
			opTxc := &opTxCapacity{}
			o.LocalSecondaryIndexes[k] = opTxc.initialise(v) // &opTxCapacity{CapacityUnits: c, ReadCapacityUnits: r, WriteCapacityUnits: w}

		} else {

			gsi.update(v)
		}
	}
	if len(*c.TableName) > 0 {

		if o.Table == nil {
			o.Table = make(map[string]*opTxCapacity)
		}
		if tbl, ok := o.Table[*c.TableName]; !ok {
			opTxc := &opTxCapacity{}
			if c.Table != nil {
				o.Table[*c.TableName] = opTxc.initialise(*c.Table)
			} else {
				o.Table[*c.TableName] = opTxc.initialise2(c.CapacityUnits, c.ReadCapacityUnits, c.WriteCapacityUnits)
			}
		} else {
			if c.Table != nil {
				tbl.update(*c.Table)
			} else {
				tbl.update2(c.CapacityUnits, c.ReadCapacityUnits, c.WriteCapacityUnits)
			}
		}

		if r > 0 {
			if o.Retrieved == nil {
				o.Retrieved = make(map[string]*i64mmx)
			}
			if tbl, ok := o.Retrieved[*c.TableName]; !ok {
				o.Retrieved[*c.TableName] = newI64mmx(r) // &opTxCapacity{CapacityUnits: c, ReadCapacityUnits: r, WriteCapacityUnits: w}
			} else {
				tbl.update(r)
			}
		}
		if s > 0 {
			if o.Scanned == nil {
				o.Scanned = make(map[string]*i64mmx)
			}
			if tbl, ok := o.Scanned[*c.TableName]; !ok {
				o.Scanned[*c.TableName] = newI64mmx(s) // &opTxCapacity{CapacityUnits: c, ReadCapacityUnits: r, WriteCapacityUnits: w}
			} else {
				tbl.update(s)
			}
		}
	}

	//	o.duration.update(dur)
}

type capacity struct {
	cus      *mmx // capacity units
	readcus  *mmx // read capacity units
	writecus *mmx // write capacity units
}

func (c capacity) String() string {
	var w strings.Builder

	if c.cus != nil {
		w.WriteString("\n      Capacity: ")
		w.WriteString(c.cus.String())
	}
	if c.readcus != nil {
		w.WriteString("\n Read Capacity: ")
		w.WriteString(c.readcus.String())
	}
	if c.writecus != nil {
		w.WriteString("\nWrite Capacity: ")
		w.WriteString(c.writecus.String())
	}
	w.WriteByte('\n')
	return w.String()
}

type mmx struct {
	min float64
	max float64
	cnt int64
	sum float64
}

type MMX struct {
	Min float64
	Max float64
	Cnt int64
	Sum float64
}

func (m *mmx) MMX() *MMX {
	return &MMX{Min: m.min, Max: m.max, Cnt: m.cnt, Sum: m.sum}
}

type i64mmx struct {
	min int64
	max int64
	cnt int64
	sum int64
}

func (i *i64mmx) MMX() *I64mmx {
	return &I64mmx{Min: i.min, Max: i.max, Cnt: i.cnt, Sum: i.sum}
}

type I64mmx struct {
	Min int64
	Max int64
	Cnt int64
	Sum int64
}

func (c *mmx) update(v *float64) {
	if v == nil {
		return
	}
	if c == nil {
		return
	}
	if c.min > *v {
		c.min = *v
	}
	if c.max < *v {
		c.max = *v
	}
	c.sum += *v
	c.cnt++
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

func newI64mmx(v int64) *i64mmx {

	return &i64mmx{min: v, max: v, sum: v, cnt: 1}

}

func newI32mmx(v int64) *i64mmx {

	return &i64mmx{min: v, max: v, sum: v, cnt: 1}

}

func newF64mmx(v float64) *mmx {

	return &mmx{min: v, max: v, sum: v, cnt: 1}

}

func (c *i64mmx) String() string {
	if c != nil {
		return fmt.Sprintf("Min: %d, Max: %d  Cnt: %d  Sum: %d  Avg: %g\n", c.min, c.max, c.cnt, c.sum, float64(c.sum)/float64(c.cnt))
	}
	return ""
}

func (c *mmx) String() string {
	if c != nil {
		return fmt.Sprintf("Min: %g, Max: %g  Cnt: %d  Sum: %g  Avg: %g\n", c.min, c.max, c.cnt, c.sum, c.sum/float64(c.cnt))
	}
	return ""
}

type durStats struct {
	last   time.Time
	d      []int64
	mean   float64
	stddev float64
	p50    float64 // milliseconds
	p80    float64 // milliseconds
	m      *i64mmx
	muts   *i64mmx
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
func (d *durStats) MMX() *I64mmx {
	return &I64mmx{Min: d.m.min, Max: d.m.max, Cnt: d.m.cnt, Sum: d.m.sum}
}
func (s *durStats) GetDurs() []int64 {
	return s.d
}
func (s *durStats) GetNumMutations() *i64mmx {
	return s.muts
}
func (s *durStats) AddDur(dur int64) {
	if len(s.d) < param.MaxSampleSet {
		s.d = append(s.d, dur)
	}
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

type limits struct {
	sampleDur time.Duration // upto sampleDur per second
	maxSample int           // max number of samples
}

var regMap map[Label]*limits
var reg sync.Mutex

func Register(lbl Label, sampledur time.Duration, maxSam ...int) {

	Save.Lock()

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

	Save.Unlock()
}

// func (s *durStats) Keep(l Label) (time.Time, bool) {
// 	// currently don't register labels so a system wide default of 2 per second is used for the frequency threshold
// 	var saveit bool
// 	t := time.Now()
// 	if t.Sub(s.last).Seconds() > 0.5 {
// 		saveit = true
// 	}
// 	return t, saveit

// }

type apiStats [_apiLimit_]int64

// populate duration metadata based on results of raw duration times
func AggregateDurationStats() {

	for _, v := range DurTx {
		var first int
		hist := hdr.New(-900000000, 900000000, 5)
		if len(v.d) == 1 {
			first = 0
		} else {
			first = 1
		}
		for _, val := range v.d[first:] {
			val := val / 1000 // microseconds
			if err := hist.RecordValue(val); err != nil {
				panic(err)
			}
		}
		// output in millsecond (1000 microseconds)
		v.mean = float64(hist.Mean()) / 1000
		v.stddev = float64(hist.StdDev()) / 1000
		v.p50 = float64(hist.ValueAtQuantile(50)) / 1000
		v.p80 = float64(hist.ValueAtQuantile(80)) / 1000
		//
		//fmt.Printf("TX: k %s \np50: %g\np80: %g\n mean: %g, stddev: %g \n min:  %d max: %d cnt: %d \n", k, v.p50, v.p80, v.mean, v.stddev, v.m.min, v.m.max, v.m.cnt)
	}
	for _, v := range DurTbl {
		var first int
		hist := hdr.New(-900000000, 900000000, 5)
		if len(v.d) == 1 {
			first = 0
		} else {
			first = 1
		}
		// ignore first value in sample set if more than one in sample
		for _, val := range v.d[first:] {
			val := val / 1000 // microseconds
			if err := hist.RecordValue(val); err != nil {
				panic(err)
			}
		}
		// output in millsecond (1000 microseconds)
		v.mean = float64(hist.Mean()) / 1000
		v.stddev = float64(hist.StdDev()) / 1000
		v.p50 = float64(hist.ValueAtQuantile(50)) / 1000
		v.p80 = float64(hist.ValueAtQuantile(80)) / 1000
		//
		//	fmt.Printf("TX: k %s \np50: %g\np80: %g\n mean: %g, stddev: %g \n min:  %d max: %d cnt: %d \n", k, v.p50, v.p80, v.mean, v.stddev, v.m.min, v.m.max, v.m.cnt)
	}
}

func (d durStats) String() string { return string("") }

type DurMap map[Label]*durStats // nanoseconds. From durations calculate the min, max and precentiles p50, p90. When? Post run task and saved to database using runid.

type ApiMap map[Label]*apiStats

//type accessTyMap map[Label]query.AccessTy

type capacityMap map[Label]*opTxCapacity
type capacityTxMap map[Label]*consumedCapacity

// package var that need to be serially accessed via monitor gatekeeper
var (
	Save sync.Mutex
	//	AcTx       accessTyMap
	DurTx      DurMap
	DurTbl     DurMap
	ApiCnt     ApiMap
	ApiMuts    ApiMap
	CapByTx    capacityTxMap
	CapByTable capacityMap
	CapByGSI   capacityMap
	CapByLSI   capacityMap

	//statistics thresholds
	SampleDuration time.Duration // duration between samples for stats gathering when not stipulated.
)

func init() {
	//	AcTx = make(accessTyMap)
	// stats for a TX
	DurTx = make(DurMap)
	DurTbl = make(DurMap)
	ApiCnt = make(ApiMap)
	ApiMuts = make(ApiMap)
	CapByTx = make(capacityTxMap)
	// stats aggregated across all TXs
	CapByTable = make(capacityMap)
	CapByGSI = make(capacityMap)
	CapByLSI = make(capacityMap)
	//
	var err error
	SampleDuration, err = time.ParseDuration(param.SampleDurDB)
	if err != nil {
		panic(err)
	}
}

func (s *opItem) Add() {
	Save.Lock()
	add(s)
	Save.Unlock()
}

func add(s *opItem) {

	// save access type, getItem, query etc..
	//	AcTx[s.Tag] = s.Acs

	// accumulate duration values
	// if x, ok := DurTx[s.Tag]; !ok {
	// 	DurTx[s.Tag] = &durStats{d: []int64{s.Duration}, m: newI64mmx(s.Duration), muts: newI64mmx(int64(s.Mutations))}
	// } else {
	// 	if len(x.d) < MaxDurSample {
	// 		x.d = append(x.d, s.Duration)
	// 	}
	// 	x.m.update(s.Duration)
	// 	x.muts.update(int64(s.Mutations))
	// }
	// accumulate duration values by Table
	if s.Tx.TableName != nil {
		key := s.Tag + "#tbl#" + *s.Tx.TableName
		if x, ok := DurTx[key]; !ok {
			DurTx[key] = &durStats{d: []int64{s.Duration}, m: newI64mmx(s.Duration), muts: newI64mmx(int64(s.Mutations))}
		} else {
			if t, sv := x.Keep(s.Tag); sv {
				x.AddDur(s.Duration)
				x.last = t
			}
			x.m.update(s.Duration)
			x.muts.update(int64(s.Mutations))
		}
	}

	if s.Tx.TableName != nil {
		if x, ok := DurTbl[*s.Tx.TableName]; !ok {
			DurTbl[*s.Tx.TableName] = &durStats{d: []int64{s.Duration}, m: newI64mmx(s.Duration), muts: newI64mmx(int64(s.Mutations))}
		} else {
			if t, sv := x.Keep(s.Tag); sv {
				x.AddDur(s.Duration)
				x.last = t
			}
			x.m.update(s.Duration)
			x.muts.update(int64(s.Mutations))
		}
	}
	if s.Tx.TableName != nil {
		tbl := *s.Tx.TableName
		if x, ok := ApiCnt[tbl]; !ok {
			a := apiStats{}
			a[s.Src]++
			ApiCnt[tbl] = &a
		} else {
			x[s.Src]++
		}
	}

	if s.Tx.TableName != nil {
		tbl := *s.Tx.TableName
		if x, ok := ApiMuts[tbl]; !ok {
			a := apiStats{}
			a[s.Src] = int64(s.Mutations)
			ApiMuts[tbl] = &a
		} else {
			x[s.Src] += int64(s.Mutations)
		}
	}
	//
	// TX populate - aggregate for same TX
	if opTx, ok := CapByTx[s.Tag]; !ok {

		// first time for this TX - create only (no aggregation)
		opTx = &consumedCapacity{}
		CapByTx[s.Tag] = opTx

		//opTx.initialise(s.Op)
		// consumedcapacity output from  Query or GetItem
		cc := s.Tx
		var cu, ru, wu *mmx

		if cc.CapacityUnits != nil {
			cu = &mmx{}
			cu.min = *cc.CapacityUnits
			cu.max = *cc.CapacityUnits
			cu.sum = *cc.CapacityUnits
			cu.cnt = 1
		}
		opTx.CapacityUnits = cu

		if cc.ReadCapacityUnits != nil {
			ru := &mmx{}
			ru.min = *cc.ReadCapacityUnits
			ru.max = *cc.ReadCapacityUnits
			ru.sum = *cc.ReadCapacityUnits
			ru.cnt = 1
		}
		opTx.ReadCapacityUnits = ru

		if cc.WriteCapacityUnits != nil {
			wu = &mmx{}
			wu.min = *cc.WriteCapacityUnits
			wu.max = *cc.WriteCapacityUnits
			wu.sum = *cc.WriteCapacityUnits
			wu.cnt = 1
		}
		opTx.WriteCapacityUnits = wu
		//
		for k, v := range cc.GlobalSecondaryIndexes {
			if opTx.GlobalSecondaryIndexes == nil {
				opTx.GlobalSecondaryIndexes = make(map[string]*opTxCapacity)
			}
			o := &opTxCapacity{}
			opTx.GlobalSecondaryIndexes[k] = o.initialise(v) // &opTxCapacity{CapacityUnits: c, ReadCapacityUnits: r, WriteCapacityUnits: w}
		}
		//
		for k, v := range cc.LocalSecondaryIndexes {
			if opTx.LocalSecondaryIndexes == nil {
				opTx.LocalSecondaryIndexes = make(map[string]*opTxCapacity)
			}
			o := &opTxCapacity{}
			opTx.LocalSecondaryIndexes[k] = o.initialise(v) // &opTxCapacity{CapacityUnits: c, ReadCapacityUnits: r, WriteCapacityUnits: w}
		}
		//
		if len(*cc.TableName) > 0 {
			//
			opTx.TableName = append(opTx.TableName, *cc.TableName)
			//
			if opTx.Table == nil {
				opTx.Table = make(map[string]*opTxCapacity)
			}
			o := &opTxCapacity{}
			if cc.Table != nil {
				opTx.Table[*cc.TableName] = o.initialise(*cc.Table) // &opTxCapacity{CapacityUnits: c, ReadCapacityUnits: r, WriteCapacityUnits: w}
			} else {
				opTx.Table[*cc.TableName] = o.initialise2(cc.CapacityUnits, cc.ReadCapacityUnits, cc.WriteCapacityUnits)
			}
			//	opTx.Table[*cc.TableName] = o.initialise(cc.Table) // &opTxCapacity{CapacityUnits: c, ReadCapacityUnits: r, WriteCapacityUnits: w}

			if s.Retrieved > 0 {
				if opTx.Retrieved == nil {
					opTx.Retrieved = make((map[string]*i64mmx))
				}
				opTx.Retrieved[*cc.TableName] = newI64mmx(s.Retrieved)
			}
			if s.Scanned > 0 {
				if opTx.Scanned == nil {
					opTx.Scanned = make((map[string]*i64mmx))
				}
				opTx.Scanned[*cc.TableName] = newI64mmx(s.Retrieved)
			}
		}

	} else {

		//  aggregate for same TX (when execute more than once - e.g. TX in a loop)
		opTx.update(s.Tx, s.Retrieved, s.Scanned)

	}

	// now using the TX stats aggregate into top level GSI, LSI, Table statistics
	for k, v := range s.Tx.GlobalSecondaryIndexes {
		if gsi, ok := CapByGSI[k]; !ok {
			opTxc := &opTxCapacity{}
			CapByGSI[k] = opTxc.initialise(v) // &opTxCapacity{CapacityUnits: c, ReadCapacityUnits: r, WriteCapacityUnits: w}

		} else {
			gsi.update(v)
		}
	}
	for k, v := range s.Tx.LocalSecondaryIndexes {
		if lsi, ok := CapByLSI[k]; !ok {
			opTxc := &opTxCapacity{}
			CapByLSI[k] = opTxc.initialise(v) // &opTxCapacity{CapacityUnits: c, ReadCapacityUnits: r, WriteCapacityUnits: w}

		} else {
			lsi.update(v)
		}
	}
	// if s.Tx.Table != nil {
	// 	if x, ok := CapByTable[s.TableName]; !ok {
	// 		opTxc := &opTxCapacity{}
	// 		CapByTable[s.TableName] = opTxc.initialise(s.Tx.Table) // &opTxCapacity{CapacityUnits: c, ReadCapacityUnits: r, WriteCapacityUnits: w}

	// 	} else {
	// 		x.update(s.Tx.Table)
	// 	}
	// }

	if len(*s.Tx.TableName) > 0 {
		tblName := *s.Tx.TableName
		if tbl, ok := CapByTable[tblName]; !ok {
			opTxc := &opTxCapacity{}
			if s.Tx.Table != nil {
				CapByTable[tblName] = opTxc.initialise(*s.Tx.Table)
			} else {
				CapByTable[tblName] = opTxc.initialise2(s.Tx.CapacityUnits, s.Tx.ReadCapacityUnits, s.Tx.WriteCapacityUnits)
			}
		} else {
			if s.Tx.Table != nil {
				tbl.update(*s.Tx.Table)
			} else {
				tbl.update2(s.Tx.CapacityUnits, s.Tx.ReadCapacityUnits, s.Tx.WriteCapacityUnits)
			}
		}
	}

}
