package query

import (
	"fmt"
	"reflect"

	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/uuid"
)

type Equality = ComparOpr

type ComparOpr byte
type ScanOrder int8
type Orderby int8
type AccessTy byte

const (
	EQ ComparOpr = iota + 1
	NE
	GT
	GE
	LE
	LT
	BEGINSWITH
	NOT
	NA
	//
	ASC     ScanOrder = 0 // default
	Forward ScanOrder = 0
	DESC    ScanOrder = 1
	Reverse ScanOrder = 1
	//
	Asc  Orderby = 0 // default
	Desc Orderby = 1
	//
	GetItem AccessTy = iota + 1
	Query
	Scan
	Transact
	Batch
	Null
	//
)

func (c ComparOpr) String() string {
	switch c {
	case EQ:
		return " = "
	case NE:
		return " != "
	case GT:
		return " > "
	case GE:
		return " >= "
	case LE:
		return " <= "
	case LT:
		return " < "
	case BEGINSWITH:
		return "beginsWith("
	case NOT:
		return " !"
	}
	return "NA"
}

type Attrty byte

type Label string

//type OpTag Label

const (
	IsKey Attrty = iota + 1
	IsFilter
	IsFetch // projection fields
)

func syslog(s string) {
	slog.Log("query: ", s)
}

type Attr struct {
	name  string
	param string
	value interface{}
	aty   Attrty
	eqy   ComparOpr
}

type orderby struct {
	attr string
	sort Orderby
}
type QueryHandle struct {
	Tag string
	//ctx context.Context
	//stateId util.UID - id for maintaining state
	attr     []Attr // all attributes sourced from  Key , Select, filter , addrVal clauses as determined by attr atyifier (aty)
	tbl      tbl.Name
	idx      tbl.Name
	limit    int
	parallel int
	css      bool // read consistent mode
	// prepare mutation in db's that support separate preparation phase
	prepare  bool
	prepStmt interface{}
	//
	first bool
	//
	scan bool // NewScan specified
	//
	keycnt int
	pk     string
	sk     string
	//	orderby  string
	so       ScanOrder
	orderBy  orderby
	accessTy AccessTy
	err      error
	// select() handlers
	fetch   interface{} // used in Dynamodb Select()
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

func (q *QueryHandle) Duplicate() *QueryHandle {
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
	d.first = q.first
	//
	d.scan = q.scan
	//
	d.keycnt = q.keycnt
	d.pk = q.pk
	d.sk = q.sk
	//	orderby  string
	d.so = q.so
	d.accessTy = q.accessTy
	// select() handlers
	d.fetch = q.fetch
	d.select_ = q.select_ // indicates Select() has been executed. Catches cases when Select() specified more than once.
	// is query restarted. Paginated queries only.
	d.restart = q.restart
	// pagination state
	d.pgStateId = q.pgStateId
	d.pgStateValI = q.pgStateValI
	d.pgStateValS = q.pgStateValS
	// other runtime state data
	return &d

}

func (q *QueryHandle) qh() {}

// Reset nullifies certain data after a prepared stmt execution, for later reprocessing
func (q *QueryHandle) Reset() {
	q.attr = nil
	q.pk, q.sk = "", ""
}

func (q *QueryHandle) SetWorderId(i int) {
	q.worker = i
}

func (q *QueryHandle) Worker() int {
	return (q.worker)
}

func (q *QueryHandle) SetPrepare() {
	q.prepare = true
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

func (q *QueryHandle) GetTag() string {
	return q.Tag
}

func (q *QueryHandle) GetTable() tbl.Name {
	return q.tbl
}

func (q *QueryHandle) GetTableName() tbl.Name {
	return q.tbl
}

func (q *QueryHandle) IndexSpecified() bool {
	return len(q.idx) > 0
}

func (q *QueryHandle) SetScan() {
	q.scan = true
}

func (q *QueryHandle) Parallel(n int) {
	q.parallel = n
}

func (q *QueryHandle) GetParallel() int {
	return q.parallel
}

func (q *QueryHandle) SKset() bool {
	return len(q.sk) > 0
}
func (q *QueryHandle) GetIndex() tbl.Name {
	return q.idx
}

func (q *QueryHandle) GetIndexName() tbl.Name {
	return q.idx
}

func (q *QueryHandle) IsScanASCSet() bool {
	return q.so == Forward
}

func (q *QueryHandle) IsScanForwardSet() bool {
	return q.so == Forward
}

func (q *QueryHandle) GetAttr() []Attr {
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

func (q *QueryHandle) KeyCnt() int {
	return q.keycnt
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

func (q *QueryHandle) GetError() error {
	return q.err
}

func (q *QueryHandle) GetPkSk() (string, string) {
	return q.pk, q.sk
}

func (q *QueryHandle) GetPK() string {
	return q.pk
}

func (q *QueryHandle) GetSK() string {
	return q.sk
}

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

func (q *QueryHandle) SetEOD() {
	q.eod = true
}

func (q *QueryHandle) EOD() bool {
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

// func (q *QueryHandle) GetComparitor(n string) ComparOpr {
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

func (q *QueryHandle) GetWhereAttrs() []Attr {
	var flt []Attr
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

func (a Attr) GetOprStr() string {
	return a.eqy.String()
}

func (a Attr) ComparOpr() ComparOpr {
	return a.eqy
}

func (a Attr) AttrType() Attrty {
	return a.aty
}

func (a Attr) Name() string {
	return a.name
}

func (a Attr) Value() interface{} {
	return a.value
}

func (a Attr) IsKey() bool {
	return a.aty == IsKey
}

func (a Attr) IsFetch() bool {
	return a.aty == IsFetch
}

func (a Attr) Filter() bool {
	return a.aty == IsFilter
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

func (q *QueryHandle) Consistent(b bool) *QueryHandle {
	q.css = b
	return q
}

func (q *QueryHandle) Limit(l int) *QueryHandle {
	q.limit = l
	return q
}

func (q *QueryHandle) Key(a string, v interface{}, e ...ComparOpr) *QueryHandle {
	// Input fields are fields used as key for query and filter attribute (when non-key)
	// all var fields are addrVal fields, ie. return values from db.
	var pk, sk string

	obj := q.tbl
	if len(q.idx) > 0 {
		obj = q.idx
	}

	pk, sk, _ = tbl.GetKeys(obj)
	//
	if a != pk && a != sk {
		q.err = fmt.Errorf("%q Not a key of %s ", a, obj)
		return q
	}
	q.keycnt++
	// set query condition
	eqy := EQ
	if len(e) > 0 && e[0] != EQ {
		eqy = e[0]

	}
	if a == pk {
		q.pk = a
	} else {
		if a == sk {
			q.sk = a
		}
	}
	// switch a {
	// case pk:
	// 	q.pk = a
	// 	switch eqy {

	// 	case EQ:
	// 		if len(sk) == 0 {
	// 			q.accessTy = GetItem
	// 		} else {
	// 			if q.GetComparOpr(sk) == EQ {
	// 				q.accessTy = GetItem
	// 			} else {
	// 				q.accessTy = Query
	// 			}
	// 		}

	// 	}
	// 	if eqy == EQ && len(sk) == 0 {
	// 		q.accessTy = GetItem
	// 	} else {
	// 		if eqy == EQ && len(sk) > 0 {
	// 			if q.GetComparOpr(sk) == EQ {
	// 				q.accessTy = GetItem
	// 			} else {
	// 				q.accessTy = Query
	// 			}
	// 		}
	// 	}

	// case sk:

	// 	q.sk = a
	// 	if eqy == EQ && len(pk) > 0 {
	// 		q.accessTy = Scan

	// 	}
	// }
	//
	at := Attr{name: a, value: v, aty: IsKey, eqy: eqy}
	q.attr = append(q.attr, at)

	return q
}

func (q *QueryHandle) GetKeyComparOpr(sk string) ComparOpr {
	return q.getComparOpr(IsKey, sk)
}

func (q *QueryHandle) GetFilterComparOpr(sk string) ComparOpr {
	return q.getComparOpr(IsFilter, sk)
}

func (q *QueryHandle) getComparOpr(t Attrty, s string) ComparOpr {
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

func (q *QueryHandle) Filter(a string, v interface{}, e ...ComparOpr) *QueryHandle {

	if q.err != nil {
		return q
	}

	eq := EQ
	if len(e) > 0 {
		eq = e[0]
	}

	at := Attr{name: a, value: v, aty: IsFilter, eqy: eq}
	q.attr = append(q.attr, at)

	return q
}

func (q *QueryHandle) ScanOrder(so ScanOrder) *QueryHandle {
	if !q.SKset() {
		panic(fmt.Errorf("When using Sort() a sort key must be specified using Key()"))
	}
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

func (q *QueryHandle) Select(a interface{}) *QueryHandle {

	if q.err != nil {
		return q
	}
	if q.select_ && !q.prepare {
		panic(fmt.Errorf("Select already specified. Only one Select permitted."))
	}
	q.select_ = true

	f := reflect.TypeOf(a)
	if f.Kind() != reflect.Ptr {
		panic(fmt.Errorf("Fetch argument: not a pointer"))
	}
	//save addressable component of interface argument
	q.fetch = a

	s := f.Elem()

	switch s.Kind() {
	case reflect.Struct:
		// used in GetItem (single row select)

		for i := 0; i < s.NumField(); i++ {
			v := s.Field(i)
			at := Attr{name: v.Name, aty: IsFetch}
			q.attr = append(q.attr, at)
		}
	case reflect.Slice:

		st := s.Elem() // used in Query (multi row select)
		fmt.Println("st ", st.Kind())
		if st.Kind() == reflect.Slice {
			st = st.Elem() // [][]rec - used for parallel scan
			fmt.Println("st ", st.Kind())
		}
		if st.Kind() == reflect.Pointer {
			st = st.Elem() // [][]rec - used for parallel scan
			fmt.Println("st ", st.Kind())
		}
		if st.Kind() == reflect.Struct {
			var name string
			for i := 0; i < st.NumField(); i++ {
				v := st.Field(i)
				if name = v.Tag.Get("dynamodbav"); len(name) == 0 {
					name = v.Name
				}
				at := Attr{name: name, aty: IsFetch}
				q.attr = append(q.attr, at)
			}
		} else {
			panic(fmt.Errorf("QueryHandle Select(): expected a struct got %s", st.Kind()))
		}
	}

	return q
}

func (q *QueryHandle) HasInComparOpr() bool {
	for _, v := range q.attr {
		if v.aty == IsKey {
			if v.eqy != EQ {
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
