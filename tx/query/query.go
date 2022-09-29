package query

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	elog "github.com/GoGraph/errlog"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/uuid"
)

type ScanOrder int8
type Orderby int8
type AccessTy byte
type BoolCd byte
type Mode byte

const (
	NA string = "NA"
	//
	ASC     ScanOrder = 0 // default
	Forward ScanOrder = 0
	DESC    ScanOrder = 1
	Reverse ScanOrder = 1
	//
	Asc  Orderby = 0 // default
	Desc Orderby = 1
	//
	GetItem AccessTy = iota
	Query
	Scan
	Transact
	Batch
	Null
	//
	NIL BoolCd = iota
	AND
	OR
	//
	CHANNEL Mode = iota
	FUNC
	//
)

type Attrty byte

type Label string

//type OpTag Label

const (
	IsKey Attrty = iota + 1
	IsFilter
	IsFetch // projection fields
)

func syslog(s string) {
	slog.Log("query", s)
}
func syslogAlert(s string) {
	slog.LogAlert("query", s)
}

type Attr struct {
	name    string
	param   string
	value   interface{}
	literal string // alternative to value - replace attribute name with literal in query stmt.
	aty     Attrty // attribute type, e.g. Key, Filter, Fetch
	eqy     string
	boolCd  BoolCd // And, Or - appropriate for Filter only.
}

func (a *Attr) GetOprStr() string {
	return strings.ToUpper(a.eqy)
}

func (a *Attr) AttrType() Attrty {
	return a.aty
}

func (a *Attr) Name() string {
	return a.name
}

func (a *Attr) Literal() string {
	return a.literal
}

func (a *Attr) BoolCd() BoolCd {
	return a.boolCd
}

func (a *Attr) Value() interface{} {
	return a.value
}

func (a *Attr) IsKey() bool {
	return a.aty == IsKey
}

func (a *Attr) IsFetch() bool {
	return a.aty == IsFetch
}

func (a *Attr) Filter() bool {
	return a.aty == IsFilter
}

type orderby struct {
	attr string
	sort Orderby
}
type QueryHandle struct {
	Tag string
	//
	err error
	//ctx context.Context
	//stateId util.UID - id for maintaining state
	attr     []*Attr // all attributes sourced from  Key , Select, filter , addrVal clauses as determined by attr atyifier (aty)
	tbl      tbl.Name
	idx      tbl.Name
	limit    int
	parallel int
	css      bool // read consistent mode
	//
	queryMode Mode
	//
	channel interface{}
	f       func() error
	// prepare mutation in db's that support separate preparation phase
	prepare  bool
	prepStmt interface{}
	//
	//first bool ??
	//
	eodlc int           // eod loop counter - used to determine first EOD execution which drives switch logic
	abuf  int           // active buffer
	bufs  []interface{} // buffers
	//
	scan bool // NewScan specified
	//
	// pk     string
	// sk     string
	//	orderby  string
	so       ScanOrder
	orderBy  orderby
	accessTy AccessTy // TODO: remove
	// select() handlers
	fetch   interface{} //  Select()
	select_ bool        // indicates Select() has been executed. Catches cases when Select() specified more than once.
	// is query restarted. Paginated queries only.
	restart bool
	// pagination state
	pgStateId   uuid.UID
	pgStateValI interface{}
	pgStateValS string
	// other runtime state data
	eod bool
	// varM map[string]interface{}
	// varS []interface{}
	worker int
}

func New(tbl tbl.Name, label string, idx ...tbl.Name) *QueryHandle {
	if len(idx) > 0 {
		return &QueryHandle{Tag: label, tbl: tbl, accessTy: Null, idx: idx[0], css: true}
	}
	return &QueryHandle{Tag: label, tbl: tbl, accessTy: Null, css: true}
}

func New2(label string, tbl tbl.Name, idx ...tbl.Name) *QueryHandle {
	if len(idx) > 0 {
		return &QueryHandle{Tag: label, tbl: tbl, accessTy: Null, idx: idx[0], css: true}
	}
	return &QueryHandle{Tag: label, tbl: tbl, accessTy: Null, css: true}
}

// func NewContext(ctx context.Context, tbl tbl.Name, label string, idx ...tbl.Name) *QueryHandle {
// 	if len(idx) > 0 {
// 		return &QueryHandle{Tag: label, ctx: ctx, tbl: tbl, accessTy: Null, idx: idx[0], css: true}
// 	}
// 	return &QueryHandle{Tag: label, ctx: ctx, tbl: tbl, accessTy: Null, css: true}
// }

// func (q *QueryHandle) SetDB(d driver.Handle) {
// 	q.dbh = d
// }

func (q *QueryHandle) Clone() *QueryHandle {
	d := QueryHandle{}
	d.Tag = q.Tag
	//stateId uuid.UID - id for maintaining state
	d.attr = q.attr // all attributes sourced from  Key , Select, filter , addrVal clauses as determined by attr atyifier (aty)
	d.tbl = q.tbl
	d.idx = q.idx
	d.limit = q.limit
	d.parallel = q.parallel
	d.css = q.css
	//
	//d.first = q.first
	//
	d.scan = q.scan
	//
	// d.pk = q.pk
	// d.sk = q.sk
	//	orderby  string
	d.so = q.so
	d.accessTy = q.accessTy
	d.orderBy = q.orderBy
	// select() handlers
	// is query restarted. Paginated queries only.
	d.restart = q.restart
	// pagination state
	d.pgStateId = q.pgStateId
	d.pgStateValI = q.pgStateValI
	d.pgStateValS = q.pgStateValS
	d.eod = q.eod

	return &d

}

func (q *QueryHandle) qh() {}

// Reset nullifies certain data after a prepared stmt execution, for later reprocessing
func (q *QueryHandle) Reset() {
	q.attr = nil
	//	q.pk, q.sk = "", ""
}

func (q *QueryHandle) Channel() Mode {
	return CHANNEL
}

func (q *QueryHandle) Func() Mode {
	return FUNC
}

func (q *QueryHandle) SetFunc(f func() error) {
	q.queryMode = FUNC
	q.f = f
}

func (q *QueryHandle) GetFunc() func() error {
	return q.f
}

func (q *QueryHandle) Error() error {
	return q.err
}

func (q *QueryHandle) SetWorkerId(i int) {
	q.worker = i
}

func (q *QueryHandle) Worker() int {
	return q.worker
}

func (q *QueryHandle) SetPrepare() {
	q.prepare = true
}

func (q *QueryHandle) SetQueryMode(m Mode) {
	q.queryMode = m
}

func (q *QueryHandle) QueryMode() Mode {
	return q.queryMode
}

func (q *QueryHandle) Prepare() bool {
	return q.prepare
}

func (q *QueryHandle) PrepStmt() interface{} {
	return q.prepStmt
}

func (q *QueryHandle) SetPrepStmt(p interface{}) {
	q.prepStmt = p
}

// func (q *QueryHandle) Ctx() context.Context {
// 	return q.ctx
// }

// func (q *QueryHandle) Channel() interface{} {
// 	return q.channel
// }

func (q *QueryHandle) SetChannel(c interface{}) {
	q.channel = c
}

func (q *QueryHandle) GetChannel() interface{} {
	return q.channel
}

func (q *QueryHandle) GetTag() string {
	return q.Tag
}

func (q *QueryHandle) GetTable() string {
	return string(q.tbl)
}

func (q *QueryHandle) GetTableName() string {
	return string(q.tbl)
}

func (q *QueryHandle) IndexSpecified() bool {
	return len(q.idx) > 0
}

func (q *QueryHandle) SetScan() {
	q.scan = true
}

func (q *QueryHandle) SetWorker(i int) {
	q.worker = i
}

func (q *QueryHandle) SetTag(s string) {
	q.Tag = s
}

func (q *QueryHandle) Parallel(n int) {
	q.parallel = n
}

func (q *QueryHandle) GetParallel() int {
	return q.parallel
}

// func (q *QueryHandle) SKset() bool {
// 	return len(q.sk) > 0
// }
func (q *QueryHandle) GetIndex() string {
	return string(q.idx)
}

func (q *QueryHandle) GetIndexName() string {
	return string(q.idx)
}

func (q *QueryHandle) IsScanASCSet() bool {
	return q.so == Forward
}

func (q *QueryHandle) IsScanForwardSet() bool {
	return q.so == Forward
}

func (q *QueryHandle) GetAttr() []*Attr {
	return q.attr
}

func (q *QueryHandle) ConsistentMode() bool {
	if len(q.idx) == 0 {
		return q.css
	} else {
		return false
	}
}

func (q *QueryHandle) GetLimit() int {
	return q.limit
}

func (q *QueryHandle) Access() AccessTy {
	return q.accessTy
}

func (q *QueryHandle) GetFetch() interface{} {
	return q.fetch
}

func (q *QueryHandle) Fetch() interface{} {
	return q.fetch
}

func (q *QueryHandle) SetFetch(v interface{}) {
	q.fetch = v
}

// SetFetch uses reflect to set the internal value of a value.
func (q *QueryHandle) SetFetchValue(v reflect.Value) {
	reflect.ValueOf(q.fetch).Elem().Set(v)
}

// func (q *QueryHandle) GetPkSk() (string, string) {
// 	return q.pk, q.sk
// }

// func (q *QueryHandle) GetPK() string {
// 	return q.pk
// }

// func (q *QueryHandle) GetSK() string {
// 	return q.sk
// }

func (q *QueryHandle) GetKeyValue(n string) interface{} {
	return q.getValue(IsKey, n)
}

func (q *QueryHandle) GetFilterValue(n string) interface{} {
	return q.getValue(IsFilter, n)
}

func (q *QueryHandle) getValue(t Attrty, n string) interface{} {
	for _, v := range q.attr {
		if v.name == n && v.aty == t {
			return v.value
		}
	}
	return nil
}

func (q *QueryHandle) GetKeys() []string {
	var keys []string
	for _, v := range q.attr {
		if v.aty == IsKey {
			keys = append(keys, v.name)
		}
	}
	return keys
}

func (q *QueryHandle) SetEOD() {
	q.eod = true
}

func (q *QueryHandle) EOD() bool {

	if len(q.pgStateId) == 0 {
		// query has not configured paginate
		q.err = fmt.Errorf("Query [tag: %s] has not configured paginate. EOD is therefore not available", q.Tag)
		elog.Add(q.Tag, q.err)
		return false
	}
	// check if multiple select vars used and switch appropriate
	if len(q.bufs) > 0 {
		if q.eodlc > 0 {
			// ignore first execution of EOD to switch buffer
			q.switchBuf()
		}
		q.eodlc++
	}
	return q.eod
}

func (q *QueryHandle) FilterSpecified() bool {
	for _, v := range q.attr {
		if v.aty == IsFilter {
			return true
		}
	}
	return false
}

func (q *QueryHandle) Restart() bool {
	return q.restart
}

// func (q *QueryHandle) State(id uuid.UID, restart bool) {
// 	q.pgStateId = id
// 	q.pgStateVal = ""
// }

func (q *QueryHandle) PgStateId() uuid.UID {
	return q.pgStateId
}

func (q *QueryHandle) SetPgStateValS(val string) {
	q.pgStateValS = val
}

func (q *QueryHandle) PgStateValS() string {
	return q.pgStateValS
}

func (q *QueryHandle) SetPgStateValI(val interface{}) {
	q.pgStateValI = val
}

func (q *QueryHandle) PgStateValI() interface{} {
	return q.pgStateValI
}

func (q *QueryHandle) IsRestart() bool {
	return q.restart
}

func (q *QueryHandle) SetRestart(b bool) {
	q.restart = b
}

// func (q *QueryHandle) LastKey(key, d) string {
// 	return q.varM[key]
// }

// func (q *QueryHandle) SetVarM(key string, d interface{}) {
// 	if q.varM == nil {
// 		q.varM = make(map[string]interface{})
// 	}
// 	q.varM[key] = d
// }

// func (q *QueryHandle) GetVarM(key string) interface{} {
// 	if q.varM == nil {
// 		return nil
// 	}
// 	return q.varM[key]
// }

// func (q *QueryHandle) GetComparitor(n string) string {
// 	for _, v := range q.attr {
// 		if v.name == n && v.aty == IsFilter {
// 			return v.eqy
// 		}
// 	}
// 	return NA
// }

func (q *QueryHandle) GetFilter() []string {
	var flt []string
	for _, v := range q.attr {
		if v.aty == IsFilter {
			flt = append(flt, v.name)
		}
	}
	return flt
}

func (q *QueryHandle) GetFilterAttr() []*Attr {
	var flt []*Attr
	for _, v := range q.attr {
		if v.aty == IsFilter {
			flt = append(flt, v)
		}
	}
	return flt
}

// GetWhereAttrs - for SQL only
func (q *QueryHandle) GetWhereAttrs() []*Attr {
	var flt []*Attr
	for _, v := range q.attr {
		switch v.aty {
		case IsFilter:
			flt = append(flt, v)
		case IsKey:
			flt = append(flt, v)
		}
	}
	return flt
}

// func (a Attr) Fetch() bool {
// 	return a.aty == IsFilter
// }
// for Query only. In Dynamodb Attribute must be indexed (LSI or GSI).
// func (q *QueryHandle) OrderBy(a string, ob ...SortOrder) *QueryHandle {

// 	q.orderby = a
// 	q.order = ASC
// 	return q
// }

func (q *QueryHandle) ReadConsistency(b bool) *QueryHandle {
	q.css = b
	return q
}

func (q *QueryHandle) Limit(l int) *QueryHandle {
	q.limit = l
	return q
}

func (q *QueryHandle) Key(a string, v interface{}, e ...string) *QueryHandle {
	// Input fields are fields used as key for query and filter attribute (when non-key)
	// all var fields are addrVal fields, ie. return values from db.

	// delay all validation checks till execute() - as query has no access to db (tried but always run into import cycles which it must based on current design)
	eqy := "EQ"
	if len(e) > 0 {
		eqy = e[0]
	}

	at := &Attr{name: a, value: v, aty: IsKey, eqy: eqy}
	q.attr = append(q.attr, at)

	return q
}

func (q *QueryHandle) PkeyAssigned() bool {
	for _, v := range q.attr {
		if v.aty == IsKey {
			return true
		}
	}
	return false
}

func (q *QueryHandle) GetKeyComparOpr(sk string) string {
	return q.getComparOpr(IsKey, sk)
}

func (q *QueryHandle) GetFilterComparOpr(sk string) string {
	return q.getComparOpr(IsFilter, sk)
}

func (q *QueryHandle) getComparOpr(t Attrty, s string) string {
	for _, a := range q.attr {
		if a.name == s && a.aty == t {
			return a.eqy
		}
	}
	return NA
}

func (q *QueryHandle) Paginate(id uuid.UID, restart bool) *QueryHandle {

	q.restart = restart
	q.pgStateId = id
	return q
}

func (q *QueryHandle) appendFilter(a string, v interface{}, bcd BoolCd, e ...string) {

	eq := "EQ"
	if len(e) > 0 {
		eq = e[0]
	}

	at := &Attr{name: a, value: v, aty: IsFilter, eqy: eq, boolCd: bcd}
	q.attr = append(q.attr, at)

}
func (q *QueryHandle) Filter(a string, v interface{}, e ...string) *QueryHandle {

	var found bool

	for _, a := range q.attr {
		if a.aty == IsFilter {
			found = true
			break
		}
	}
	if found {
		err := errors.New("Filter condition already specified. Use either AndFilter or OrFilter")
		elog.Add("parseQuery", err)
		q.err = err
		return q
		//
	}

	if q.err != nil {
		return q
	}

	q.appendFilter(a, v, NIL, e...)

	return q
}

func (q *QueryHandle) appendBoolFilter(a string, v interface{}, bcd BoolCd, e ...string) *QueryHandle {

	var found bool

	for _, a := range q.attr {
		if a.aty == IsFilter && a.boolCd == NIL {
			found = true
			break
		}
	}

	if !found {
		err := errors.New(fmt.Sprintf(`Query Tag: %s, no "Filter" condition specified`, q.Tag))
		elog.Add("query", err)
		q.err = err
		return q
		//
	}

	q.appendFilter(a, v, bcd, e...)

	return q
}

func (q *QueryHandle) AndFilter(a string, v interface{}, e ...string) *QueryHandle {
	return q.appendBoolFilter(a, v, AND, e...)

}

func (q *QueryHandle) OrFilter(a string, v interface{}, e ...string) *QueryHandle {
	return q.appendBoolFilter(a, v, OR, e...)
}

func (q *QueryHandle) ScanOrder(so ScanOrder) *QueryHandle {
	// if !q.SKset() {
	// 	panic(fmt.Errorf("When using Sort() a sort key must be specified using Key()"))
	// }
	q.so = so
	return q
}

func (q *QueryHandle) Sort(so ScanOrder) *QueryHandle {
	return q.ScanOrder(so)
}

func (q *QueryHandle) OrderBy(ob string, s Orderby) *QueryHandle {
	q.orderBy = orderby{ob, s}
	return q
}

func (q *QueryHandle) HasOrderBy() bool {
	return len(q.orderBy.attr) > 0
}

func (q *QueryHandle) OrderByString() string {
	s := " order by " + q.orderBy.attr
	if q.orderBy.sort == Asc {
		return s + " asc"
	}
	return s + " desc"

}

func (q *QueryHandle) OutBuf() int {
	return q.Result()
}

func (q *QueryHandle) Result() int {
	abuf := q.abuf
	syslog(fmt.Sprintf("Result Buffer id: %d", abuf))
	return abuf
}

func (q *QueryHandle) switchBuf() {

	q.abuf++
	if q.abuf > len(q.bufs)-1 {
		q.abuf = 0
	}
	q.fetch = q.bufs[q.abuf]
	syslogAlert(fmt.Sprintf("switch Buffer now : %d of %d", q.abuf, len(q.bufs))) //reflect.ValueOf(q.abuf).Elem().Len()))

}

func (q *QueryHandle) Bufs() []interface{} {
	return q.bufs
}

// Select specified the destination variable for the query data. Can be specified multiple times for a query.
func (q *QueryHandle) Select(a_ ...interface{}) *QueryHandle {

	if q.err != nil {
		return q
	}

	a := a_[0]

	q.abuf = 0
	q.bufs = a_ // []interface{} []*[]unprocBuf
	// if q.select_ && !q.prepare {
	// 	panic(fmt.Errorf("Select already specified. Only one Select permitted."))
	// }

	// q.select_ = true

	f := reflect.TypeOf(a) // *[]struct
	if f.Kind() != reflect.Ptr {
		panic(fmt.Errorf("Fetch argument: expected a pointer, got a %s", f.Kind()))
	}
	//save addressable component of interface argument

	if q.fetch != nil {
		q.fetch = a
		return q
	}
	q.fetch = a

	s := f.Elem()

	switch s.Kind() {
	case reflect.Struct:
		// used in GetItem (single row select)
		var name, lit string
		for i := 0; i < s.NumField(); i++ {
			v := s.Field(i)
			if name = v.Tag.Get("dynamodbav"); len(name) == 0 {
				name = v.Name
				lit = v.Tag.Get("literal")
				fmt.Println("name, lit: ", name, lit)
			}
			at := &Attr{name: name, aty: IsFetch, literal: lit}
			q.attr = append(q.attr, at)
		}
	case reflect.Slice:

		st := s.Elem() // used in Query (multi row select)

		if st.Kind() == reflect.Slice {
			st = st.Elem() // [][]rec - used for parallel scan
		}
		if st.Kind() == reflect.Pointer {
			st = st.Elem()
		}
		if st.Kind() == reflect.Struct {
			var name, lit string
			for i := 0; i < st.NumField(); i++ {
				v := st.Field(i)
				if name = v.Tag.Get("dynamodbav"); len(name) == 0 {
					name = v.Name
					lit = v.Tag.Get("literal")
					fmt.Println("name, lit: ", name, lit)
				}
				at := &Attr{name: name, aty: IsFetch, literal: lit}

				q.attr = append(q.attr, at)
			}
		} else {
			panic(fmt.Errorf("QueryHandle Select(): expected a struct got %s", st.Kind()))
		}
	}

	return q
}

func (q *QueryHandle) HasInstring() bool {
	for _, v := range q.attr {
		if v.aty == IsKey {
			if strings.ToUpper(v.eqy) != "EQ" {
				return false
			}
		}
	}
	return true
}

// func (q *QueryHandle) MakeResultSlice(size int) reflect.Value {

// 	return reflect.MakeSlice(q.addrValRVal, size, size)

// }

// Split creates the bind variables used in db.Exec().
// TODO: Used by MySQL only (so far), consequently Split should be in mysql/query (maybe)
// The source of the bind variables is the interface{} argument passed in the Select method,
// which is either a struct or a slice of structs.
// TODO: investigate non-struct types
func (q *QueryHandle) Split() []interface{} { // )

	//
	var bind []interface{}

	v := reflect.ValueOf(q.fetch).Elem()

	switch v.Kind() {
	case reflect.Struct:

		for i := 0; i < v.NumField(); i++ {

			p := v.Field(i).Addr().Interface()

			bind = append(bind, p)
		}

	case reflect.Slice:

		// use New to allocate memory for a new struct that will be appended to the slice pointed to by q.fetch pointer.
		n := reflect.New(v.Type().Elem()).Elem()
		// append new struct to slice and assign result of append back to q.addrVal.
		// following Set is equiv to x:=append(x,a) to allow for allocation of a new underlying x array when current array size will be exceeded.
		v.Set(reflect.Append(v, n))
		// the bind variables are taken from each field of the struct just appended to the slice
		// mysql will populate them with data from the db, in the Exec() function.
		ival := v.Index(v.Len() - 1)

		for i := 0; i < ival.NumField(); i++ {

			p := ival.Field(i).Addr().Interface()

			bind = append(bind, p)
		}
	}
	for i, v := range bind {
		slog.Log("Split: ", fmt.Sprintf("bind: %d   %#v\n", i, v))
	}
	return bind
}

// func (q *QueryHandle) ClearState() error {
// 	txs := New("label", tbl.State)

// 	txs.NewDelete().AddMember(q.ID, mut.IsKey)

// 	err := txs.Execute()

// 	return err

// }

// func (q *QueryHandle) UpdateState(st) error {
// 	txs := New("label", tbl.State)

// 	txs.NewMerge().AddMember(q.ID, mut.IsKey).AddMember("state")

// 	err := txs.Execute()

// 	return err

// }
