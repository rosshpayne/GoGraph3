package tx

import (
	"context"
	"fmt"
	"strings"
	"time"

	//"github.com/GoGraph/db"
	"github.com/GoGraph/dbs"
	param "github.com/GoGraph/dygparam"
	//elog "github.com/GoGraph/errlog"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/tx/db"
	"github.com/GoGraph/tx/key"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/tx/query"
	"github.com/GoGraph/tx/tbl"
	"github.com/GoGraph/uuid"
)

const logid = "Tx"

func syslog(s string) {
	slog.Log(logid, s)
}

func logAlert(s string) {
	slog.LogAlert(logid, s)
}

func logErr(s string) {
	slog.LogErr(logid, s)
}

// Handle represents a transaction composed of 1 to many mutations.
type Handle = TxHandle

type Label string

// API determines db API to use
// for Spaneers SQL db, there is no distinction between single (UpdateItem,putItem,DeleteItem), batch (put,delete ops) and transaction.
// for no-SQL there is a distinct API for each api type.

type TxRequest = TxHandle

//type TxTag Label

type TxHandle struct {
	Tag string
	api db.API
	ctx context.Context
	//
	dbHdl   db.Handle // database handle. mysql sql.DB, [default: either, spanner spanner.NewClient, dynamodb dynamodb.New]
	maxMuts int       // depending on database driver. -1 or param.MaxMutations
	options []db.Option
	//
	prepare bool
	//
	i        int            // batch counter ???
	mergeMut bool           // MergeMutation  mutation
	am       *mut.Mutation  // active-mutation (i.e. current) generated by MergeMutation
	amTy     mut.StdMut     // update,insert,delete,merge
	sm       *mut.Mutation  // source-mutation used during MergeMutation
	m        *mut.Mutations // ptr to array of mutations in active batch.
	b        int            //batch id
	//	l      sync.Mutex
	batch []*mut.Mutations // batch containing array of array of mutations. TODO: consider adding API to a batch, so one batch can use the transaction API, another a single API etc.
	// all executed under one txHandle rather than multiple. Advantages???
	done  bool // has been executed.
	admin bool // tx associated with some sort of administration operation - not to be monitored.
	// TODO: should we have a logger??
	TransactionStart time.Time
	TransactionEnd   time.Time
	//
	err []error
}

func (h *TxHandle) String() string {

	var (
		l int
		s strings.Builder
		m map[string]int
	)
	m = make(map[string]int)
	s.WriteString(fmt.Sprintf("\ntag: %s\n", h.Tag))
	s.WriteString(fmt.Sprintf("mutations in active batch: %d \n", len(*h.m)))
	s.WriteString(fmt.Sprintf("#non-active batches:  %d\n", len(h.batch)))

	for _, v := range h.batch {
		l += len(*v)
		for _, vv := range *v {
			m[vv.(*mut.Mutation).GetOpr().String()]++
		}
	}

	for _, v := range *h.m {
		m[v.(*mut.Mutation).GetOpr().String()]++
	}
	s.WriteString(fmt.Sprintf("Mutations in non-active batches: %d\n", l))
	s.WriteString(fmt.Sprintf("b:    %d\n", h.b))
	s.WriteString(fmt.Sprintf("Puts:    %d\n", m[mut.Insert.String()]))
	s.WriteString(fmt.Sprintf("Updates: %d\n", m[mut.Update.String()]))
	s.WriteString(fmt.Sprintf("Merge:   %d\n", m[mut.Merge.String()]))
	s.WriteString(fmt.Sprintf("Deletes: %d\n", m[mut.Delete.String()]))
	return s.String()

}

// NewTx - new transactionm, treat all associated mutations as part of a single transaction.
// Can be disabled by setting parameter: "transactions=disabled", which effectively halves the
// write capacity units compared to its transaction equivalent.
func NewTx(tag string, m ...*mut.Mutation) *TxHandle {

	tx := &TxHandle{Tag: tag, m: new(mut.Mutations), ctx: context.TODO(), api: db.TransactionAPI, maxMuts: param.MaxMutations, dbHdl: db.GetDefaultDBHdl()}

	if tag == param.StatsSystemTag {
		tx.admin = true
	}
	if len(m) > 0 {
		for _, v := range m {
			tx.Add(v)
		}
	}

	return tx

}

// ****************************** Choose API ************************************
//
// NewCTx transacational API with context
func NewTxContext(ctx context.Context, tag string, m ...*mut.Mutation) *TxHandle {

	if ctx == nil {
		syslog("NewTxContext() Error : ctx argument is nil")
	}
	tx := &TxHandle{Tag: tag, ctx: ctx, m: new(mut.Mutations), api: db.TransactionAPI, maxMuts: param.MaxMutations, dbHdl: db.GetDefaultDBHdl()}

	if tag == param.StatsSystemTag {
		tx.admin = true
	}
	if len(m) > 0 {
		for _, v := range m {
			tx.Add(v)
		}
	}

	return tx

}

// NewSingle - single api not involved as a transaction. Allows puItem, deleteItem, updateItem operations without the overhead
// of being in a transaction. This essentially represents the behaviour of GoGraph prior to the inclusion of transactions.
func NewSingle(tag string) *TxHandle {

	return New(tag)

}

func NewSingleContext(ctx context.Context, tag string) *TxHandle {

	return NewContext(ctx, tag)

}

// New - default mutation represents standard api
func New(tag string, m ...*mut.Mutation) *TxHandle {

	tx := &TxHandle{Tag: tag, api: db.StdAPI, ctx: context.TODO(), m: new(mut.Mutations), maxMuts: param.MaxMutations, dbHdl: db.GetDefaultDBHdl()}

	if tag == param.StatsSystemTag {
		tx.admin = true
	}
	if len(m) > 0 {
		for _, v := range m {
			tx.Add(v)
		}
	}

	return tx

}

// NewC - default represents standard api with context
func NewContext(ctx context.Context, tag string, m ...*mut.Mutation) *TxHandle {

	if ctx == nil {
		syslog("NewContext() Error : ctx argument is nil")
	}

	tx := &TxHandle{Tag: tag, ctx: ctx, api: db.StdAPI, m: new(mut.Mutations), maxMuts: param.MaxMutations, dbHdl: db.GetDefaultDBHdl()}

	if tag == param.StatsSystemTag {
		tx.admin = true
	}
	if len(m) > 0 {
		for _, v := range m {
			tx.Add(v)
		}
	}

	return tx

}

// NewBatch -  bundle put/deletes as a batch. Provides no transaction consistency but is cheaper to run. No conditions allowed??
func NewBatch(tag string, m ...*mut.Mutation) *TxHandle {
	return NewBatchContext(context.TODO(), tag, m...)
}

// NewBatchContext -  bundle put/deletes as a batch. Provides no transaction consistency but is cheaper to run. No conditions allowed??
func NewBatchContext(ctx context.Context, tag string, m ...*mut.Mutation) *TxHandle {

	if ctx == nil {
		syslog("NewBatchContext() Error : ctx argument is nil")
	}

	tx := &TxHandle{Tag: tag, ctx: ctx, api: db.BatchAPI, m: new(mut.Mutations), maxMuts: param.MaxMutations, dbHdl: db.GetDefaultDBHdl()}

	if tag == param.StatsSystemTag {
		tx.admin = true
	}
	if len(m) > 0 {
		for _, v := range m {
			tx.Add(v)
		}
	}

	return tx

}

// NewOptim ???
func NewOptim(tag string, m ...*mut.Mutation) *TxHandle {

	tx := &TxHandle{Tag: tag, api: db.OptimAPI, ctx: context.TODO(), m: new(mut.Mutations), maxMuts: param.MaxMutations, dbHdl: db.GetDefaultDBHdl()}

	return tx.new(m...)
}

func (h *TxHandle) new(m ...*mut.Mutation) *TxHandle {

	if h.Tag == param.StatsSystemTag {
		h.admin = true
	}
	if len(m) > 0 {
		for _, v := range m {
			h.Add(v)
		}
	}

	return h
}

func (q *TxHandle) GetErrors() []error {
	var es []error

	for _, m := range *q.m {
		if m, ok := m.(*mut.Mutation); ok {
			es = append(es, m.GetError()...)
		}
	}
	for _, b := range q.batch {
		for _, m := range *b {

			if m, ok := m.(*mut.Mutation); ok {
				es = append(es, m.GetError()...)
			}
		}
	}

	es = append(es, q.err...)

	return es
}

func (q *TxHandle) Prepare() *TxHandle {
	q.prepare = true
	return q
}

// Add exists to support legacy tx. It has been replaced by New* implemenations below
// which have built in add functionality. There will always be atleast one argument (mutation) supplied
func (h *TxHandle) Add(m ...dbs.Mutation) {

	for _, v := range m {
		h.add(v)
	}
}

// add a new mutation to active batch. Note the client will/can modify the mutation (i.e. AddMember) after it is added.
// if number of mutations in active batch exceeds permitted maximum it will be added it to batch-of-batches and
// a new active batch created
func (h *TxHandle) add(m dbs.Mutation) {
	if m == nil {
		return
	}

	// check if m already added
	for _, v := range *h.m {
		if v == m {
			return
		}
	}

	// check if active batch contains more than permitted mutations
	if len(*h.m) == h.maxMuts {
		h.MakeBatch()
	}
	// add mutation to active batch
	*h.m = append(*h.m, m)
}

// add active batch to batch-of-batches and create new active batch.
func (h *TxHandle) MakeBatch() error {
	// serialise access to batch if Executes are run as goroutines.
	// h.l.Lock()
	// defer h.l.Unlock()
	if len(*h.m) == 0 {
		// no active mutations batch - ignore MakeBatch request
		return nil
	}
	h.b++
	// add active mutation batch to array of batches
	h.batch = append(h.batch, h.m)
	// create new active batch
	h.m = new(mut.Mutations)

	return nil
}

//
// ****************************** New Mutation ************************************
//
// func (h *TxHandle) NewMutation(table tbl.Name, pk uuid.UID, sk string, opr mut.StdMut) *mut.Mutation {
// 	m := mut.NewMutation(table, pk, sk, opr)
// 	h.add(m)
// 	return m
// }

func (h *TxHandle) NewMutation2(table tbl.Name, pk uuid.UID, sk string, opr mut.StdMut) *mut.Mutation {
	keys := []key.Key{key.Key{"PKey", pk}, key.Key{"SortK", sk}}
	return mut.NewMutation(table, opr, keys)
}

func (h *TxHandle) NewMutation(table tbl.Name, opr mut.StdMut, keys []key.Key) *mut.Mutation {
	m := mut.NewMutation(table, opr, keys)
	h.add(m)
	return m
}

func (h *TxHandle) NewInsert(table tbl.Name) *mut.Mutation {
	m := mut.NewInsert(table)
	h.add(m)
	return m
}

func (h *TxHandle) NewUpdate(table tbl.Name) *mut.Mutation {
	if h.api == db.BatchAPI {
		panic(fmt.Errorf("Cannot have an Update operation included in a batch"))
	}

	// validate merge keys with actual table keys
	tableKeys, err := h.dbHdl.GetTableKeys(h.ctx, string(table))
	if err != nil {
		h.addErr(fmt.Errorf("Error in finding table keys for table %q: %w", table, err))
		return nil
	}

	m := mut.NewUpdate(table)
	m.AddTableKeys(tableKeys)
	h.add(m)
	return m
}

func (h *TxHandle) NewDelete(table tbl.Name) *mut.Mutation {
	if h.api == db.BatchAPI {
		panic(fmt.Errorf("Cannot have an Update operation included in a batch"))
	}

	// validate merge keys with actual table keys
	tableKeys, err := h.dbHdl.GetTableKeys(h.ctx, string(table))
	if err != nil {
		h.addErr(fmt.Errorf("Error in finding table keys for table %q: %w", table, err))
		return nil
	}

	m := mut.NewDelete(table)
	m.AddTableKeys(tableKeys)
	h.add(m)
	return m
}

func (h *TxHandle) NewMerge(table tbl.Name) *mut.Mutation {
	if h.api == db.BatchAPI {
		panic(fmt.Errorf("Cannot have a Merge operation included in a batch"))
	}
	// validate merge keys with actual table keys
	tableKeys, err := h.dbHdl.GetTableKeys(h.ctx, string(table))
	if err != nil {
		h.addErr(fmt.Errorf("Error in finding table keys for table %q: %w", table, err))
		return nil
	}

	m := mut.NewMerge(table)
	m.AddTableKeys(tableKeys)
	h.add(m)
	return m
}

// Truncate table
func (h *TxHandle) NewTruncate(tbs []tbl.Name) {

	for _, table := range tbs {
		m := mut.Truncate(table)
		h.add(m)
	}

}

func (h *TxHandle) addErr(err error) {
	h.err = append(h.err, err)
}

func (h *TxHandle) SetContext(ctx context.Context) *TxHandle {
	h.ctx = ctx
	return h
}

// func (h *TxHandle) NewBulkInsert(table tbl.Name) *mut.Mutation {
// 	if tx.api != batch {

// 	}
// 	m := mut.NewInsert(table)
// 	h.add(m)
// 	return m
// }

// func (h *TxHandle) NewBulkMerge(table tbl.Name) *mut.Mutation {
// 	m := mut.NewBulkMerge(table)
// 	h.add(m)
// 	return m
// }

func (h *TxHandle) MaxMutations() bool {
	return h.m.NumMutations()+1 == param.MaxMutations
}

// GetMutation retrieves the i'th mutation. Given mutations are batched into 25 mutations slices need to determine what slice and then index into it.
func (h *TxHandle) GetMutation(i int) (*mut.Mutation, error) {

	var r, b int

	if len(h.batch) == 0 {
		if i < len(*h.m) {
			return (*h.m)[i].(*mut.Mutation), nil
		}
		return nil, fmt.Errorf("GetMutation error. Requested mutation index %d is greater than number of mutations", i)
	}
	if i != 0 {
		b = int(i/param.MaxMutations) - 1
		r = i - b*param.MaxMutations
	}

	if b < len(h.batch) {
		if r < len(*h.m) {
			return (*h.batch[b])[r].(*mut.Mutation), nil
		}
	}
	return nil, fmt.Errorf("GetMutation error. Requested mutation index %d is greater than number of mutations", i)

}

func (h *TxHandle) HasMutations() bool {

	if h.b > 0 || len(*h.m) > 0 {
		return true
	}
	return false
}

func (h *TxHandle) AddErr(e error) {
	logErr(e.Error())
	h.err = append(h.err, e)
}

func (h *TxHandle) DB(s string, opt ...db.Option) *TxHandle {
	var err error
	h.dbHdl, err = db.GetDBHdl(s)

	if err != nil {
		h.AddErr(err)
		return h
	}
	h.options = opt
	return h
}

func (h *TxHandle) Close() {

	if h.prepare {
		h.dbHdl.CloseTx(h.batch)
	}

}

func (h *TxHandle) Persist() error {
	return h.Execute()
}

func (h *TxHandle) Commit() error {
	return h.Execute()
}

func (h *TxHandle) Execute(m ...*mut.Mutation) error {

	var err error

	if errs := h.GetErrors(); len(errs) > 0 {

		if len(errs) > 1 {
			return fmt.Errorf("Errors in Tx setup [%d errors] (see system log for complete list): %w", len(errs), errs[0])
		}
		return fmt.Errorf("Error in Tx setup (see system log for complete list): %w", errs[0])
	}

	if h.done {
		syslog(fmt.Sprintf("Error: transaction %q has already been executed", h.Tag))
		return fmt.Errorf("Transaction has already been executed.")
	}
	// append passed in mutation(s) to transaction
	if len(m) > 0 {
		for _, v := range m {
			h.Add(v)
		}
	}
	// are there active mutations?
	if h.b == 0 && len(*h.m) == 0 {
		syslog(fmt.Sprintf("No mutations in transaction %q to execute", h.Tag))
		return nil
	}

	// add current active batch  to array-of-batches
	if len(*h.m) != 0 {
		// assign to a batch under either of the following conditions
		if h.b == 0 || h.batch[len(h.batch)-1] != h.m {
			if err := h.MakeBatch(); err != nil {
				return err
			}
		}
	}

	h.TransactionStart = time.Now()

	// TODO: make h.prepare a db.Option
	// context.Context at two levels: 1) at tx 2) at dhHdl (based on db.Init(ctx)
	// tx overrides dhHdl context.

	err = h.dbHdl.Execute(h.ctx, h.batch, h.Tag, h.api, h.prepare, h.options...)

	h.TransactionEnd = time.Now()

	if err != nil {
		return err
	}
	// nullify pointers so transaction cannot be repeated.
	h.m = nil
	h.batch = nil
	h.done = true

	return nil

}

////////////////////  MergeMutation. /////////////////////////

// MergeMutation - deprecate this func. Refers to keys by name and type so is not generic. Use MergeMutation2.
// func (h *TxHandle) MergeMutation(table tbl.Name, pk uuid.UID, sk string, opr mut.StdMut) *TxHandle {

// 	h.mergeMut = true
// 	// null source mutation from previous MergeMutation
// 	h.sm = nil
// 	h.am = nil

// 	// assign active mutation with new mutation and add to active batch
// 	if opr == mut.Merge {
// 		panic(fmt.Errorf("Merge not supported in a MergeMutation operation. Use Insert, Update or Delete"))
// 	}
// 	h.am = mut.NewMutation(table, pk, sk, opr)

// 	h.sm = h.findSourceMutation(table, pk, sk)
// 	if h.sm == nil {
// 		h.add(h.am)
// 	}

// 	return h
// }

// MergeMutation2 will merge the current mutation with a previously defined source mutation based on the table and key(s) values.
// Array data types will be appended too, and numbers will be driven by mut.Modifier. Other types will overwrite exising values.
// Supplied key values will be validated against table definitions.
func (h *TxHandle) MergeMutation(table tbl.Name, opr mut.StdMut, keys []key.Key) *TxHandle {

	h.mergeMut = true
	// null source mutation from previous MergeMutation
	h.sm = nil
	h.am = nil
	// opr cannot be a Merge. Only Insert/Update
	if opr == mut.Merge {
		h.addErr(fmt.Errorf("Error in Tx tag %q. Merge not supported in a MergeMutation operation. Use Insert, Update or Delete", h.Tag))
		return h
	}

	// validate merge keys with actual table keys
	tableKeys, err := h.dbHdl.GetTableKeys(h.ctx, string(table))
	if err != nil {
		h.addErr(fmt.Errorf("Error in finding table keys for table %q: %w", table, err))
		return h
	}

	// check tableKeys match merge keys - we need to match all keys to find source mutation
	if len(keys) != len(tableKeys) {
		h.addErr(fmt.Errorf("Error in Tx tag %q. Number of keys supplied (%d) do not match number of keys on table (%d)", h.Tag, len(keys), len(tableKeys)))
		return h
	}
	// generate ordered list of merge keys based on table key order and argument keys.
	mergeKeys := make([]key.MergeKey, len(keys), len(keys))

	// keys are not necessarily correctly ordered as per table definition
	for _, k := range keys {
		var found, found2 bool
		var mean string

		// tableKeys are correctly ordered based on table def
		for ii, kk := range tableKeys {
			if kk.Name == k.Name {
				found = true
				mergeKeys[ii].Name = k.Name
				mergeKeys[ii].Value = k.Value
				mergeKeys[ii].DBtype = kk.DBtype
			}
			if strings.ToUpper(kk.Name) == strings.ToUpper(k.Name) {
				found2 = true
				mean = kk.Name
			}
			if strings.ToUpper(kk.Name[:len(kk.Name)-1]) == strings.ToUpper(k.Name) {
				found2 = true
				mean = kk.Name
			}
		}
		if !found {
			if found2 {
				h.addErr(fmt.Errorf("Error in Tx tag %q. Specified merge key %q, is not a key for table %q. Do you mean %q", h.Tag, k.Name, table, mean))
			} else {
				h.addErr(fmt.Errorf("Error in Tx tag %q. Specified merge key %q, is not a key for table %q", h.Tag, k.Name, table))
			}
			return h
		}
	}

	h.am = mut.NewMutation(table, opr, keys)

	h.sm = h.findSourceMutation2(table, mergeKeys)
	if h.sm == nil {
		h.add(h.am)
	}

	return h
}

func (h *TxHandle) findSourceMutation(table tbl.Name, pk uuid.UID, sk string) *mut.Mutation {

	// cycle through batch of mutation batches .
	for _, bm := range h.batch {

		sm := bm.FindMutation(table, pk, sk)
		if sm != nil {
			return sm
		}
	}
	// search active batch if source mutation not found.
	return h.m.FindMutation(table, pk, sk)
}

func (h *TxHandle) findSourceMutation2(table tbl.Name, keys []key.MergeKey) *mut.Mutation {

	// cycle through batch of mutation batches .
	for _, bm := range h.batch {

		sm, err := bm.FindMutation2(table, keys)
		if err != nil {
			h.addErr(err)
		}
		if sm != nil {
			return sm
		}
	}
	// search active batch if source mutation not found.
	s, err := h.m.FindMutation2(table, keys)
	if err != nil {
		h.addErr(err)
	}
	return s
}

// GetMergedMutation finds source mutation based on key data values which will be validated against and merged with actual table key metadata
// based on logic i MergeMutation2()
func (h *TxHandle) GetMergedMutation(table tbl.Name, keys []key.Key) (*mut.Mutation, error) {

	// validate merge keys with actual table keys
	tableKeys, err := h.dbHdl.GetTableKeys(h.ctx, string(table))
	if err != nil {
		return nil, fmt.Errorf("Error in finding table keys for table %q: %w", table, err)
	}
	syslog(fmt.Sprintf("tableKeys: %v", tableKeys))
	// check tableKeys match merge keys - we need to match all keys to find source mutation
	if len(keys) != len(tableKeys) {
		return nil, fmt.Errorf("Error in Tx tag %q. Number of keys supplied (%d) do not match number of keys on table (%d)", h.Tag, len(keys), len(tableKeys))
	}
	// generate ordered list of keys based on table key order and argument keys.
	mergeKeys := make([]key.MergeKey, len(keys), len(keys))

	// keys are not necessarily correctly ordered as per table definition
	for _, k := range keys {
		var found, found2 bool
		var mean string

		// tableKeys are correctly ordered based on table def
		for ii, kk := range tableKeys {
			if kk.Name == k.Name {
				found = true
				mergeKeys[ii].Name = k.Name
				mergeKeys[ii].Value = k.Value
				mergeKeys[ii].DBtype = kk.DBtype
			}
			if strings.ToUpper(kk.Name) == strings.ToUpper(k.Name) {
				found2 = true
				mean = kk.Name
			}
			if strings.ToUpper(kk.Name[:len(kk.Name)-1]) == strings.ToUpper(k.Name) {
				found2 = true
				mean = kk.Name
			}
		}
		if !found {
			if found2 {
				err := fmt.Errorf("Error in Tx tag %q. Specified merge key %q, is not a key for table %q. Do you mean %q", h.Tag, k.Name, table, mean)
				h.addErr(err)
				return nil, err
			} else {
				err := fmt.Errorf("Error in Tx tag %q. Specified merge key %q, is not a key for table %q", h.Tag, k.Name, table)
				h.addErr(err)
				return nil, err
			}
		}
	}
	// cycle through mutation batches.
	for _, bm := range h.batch {
		sm, err := bm.FindMutation2(table, mergeKeys)
		if err != nil {
			h.addErr(err)
			return nil, err
		}
		if sm != nil {
			return sm, nil
		}
	}
	// search active batch if source mutation not found.
	s, err := h.m.FindMutation2(table, mergeKeys)
	if err != nil {
		h.addErr(err)
		return nil, err
	}
	return s, nil
}

// func (h *TxHandle) AddCondition(cond mut.Cond, attr string, value ...interface{}) *TxHandle {
// 	if h.sm == nil {
// 		h.am.AddCondition(cond, attr, value)
// 	} else {
// 		h.sm.AddCondition(cond, attr, value)
// 	}
// 	return h
// }

// AddMember specified with MergeMutation mutation. Member of TxHandle.
// Alternate AddMember associated with NewInsert, NewUpdate, NewMerge, member of Mutation (not TxHandel)
func (h *TxHandle) AddMember(attr string, value interface{}, mod_ ...mut.Modifier) *TxHandle {

	var opr mut.Modifier
	// am - active mutation
	// bm - batch of batch mutations
	// sm - source mutation

	// abort if tx errors exist
	if len(h.GetErrors()) > 0 {
		return h
	}
	if !h.mergeMut {
		panic(fmt.Errorf("tx.AddMember method can only be part of a MergeMutation operation. "))
	}

	// no source mutation exists ie. no mergeMutation specified
	if h.sm == nil {
		if h.am == nil {
			h.addErr(fmt.Errorf("AddMember error: h.am is nil"))
			return h
		}
		h.am.AddMember(attr, value, mod_...)

	} else {

		// *** merge current mutation with source mutation ***

		//h.am.GetOpr() == mut.Update { // && mut.IsArray(value) { // MergeMutation built for DP and DP always uses arrays.

		//fmt.Println("AddMember - update h.sm = ", h.sm)

		// fmt.Printf("Found source mutation %#v\n", *h.sm)
		// fmt.Printf(" now add mutation data  %#v\n", value)
		// remove newly created mutation and merge its data with source mutation
		// precedence dot notation, slice operator, pointer deref
		//*h.m = (*h.m)[:len(*h.m)-1]
		// if len(*h.m) == 0 {
		// 	fmt.Println("Batch size = ", len(h.batch))
		// 	if len(h.batch) > 0 {
		// 		l := len(*h.batch[len(h.batch)-1])
		// 		*h.batch[len(h.batch)-1] = (*h.batch[len(h.batch)-1])[:l-1]
		// 	}
		// } else {
		// 	*h.m = (*h.m)[:len(*h.m)-1]
		// }
		var found bool
		sa := h.sm.GetMembers()
		for i, v := range sa { // source mutation attributes

			// all members of active mutation match previous mutation - found source/original mutation
			if v.Name != attr {
				continue
			}
			// found attribute belonging to source mutation
			found = true
			opr = v.Mod
			if len(mod_) > 0 {
				opr = mod_[0] // override source attributes modifer with current mutation attribute modifier
			}
			if opr == mut.Remove {
				// set source member operator to remove
				v.Mod = mut.Remove // TODO: v.SetRemove()??, only occassion where Mod is assigned
				return h
			}

			switch x := v.Value.(type) {

			case int64:

				if n, ok := value.(int64); !ok {
					panic(fmt.Errorf("AddMember2: Expected int64 got passed in value of %T", value))
				} else {
					switch opr {
					case mut.Add:
						x += n
						sa[i].Value = x
					case mut.Subtract:
						x -= n
						sa[i].Value = x
					case mut.Set:
						sa[i].Value = x
					default:
						panic(fmt.Errorf("Expected member operator of Set, Add, Subtract got %q", opr))
					}
				}

			case int:

				if n, ok := value.(int); !ok {
					panic(fmt.Errorf("AddMember2: Expected int got passed in value of %T", value))
				} else {
					switch opr {
					case mut.Add:
						x += n
						sa[i].Value = x
					case mut.Subtract:
						x -= n
						sa[i].Value = x
					case mut.Set:
						sa[i].Value = x
					default:
						panic(fmt.Errorf("Expected member operator of Set, Add, Subtract got %q", opr))
					}
				}

			case float64:

				if n, ok := value.(float64); !ok {
					panic(fmt.Errorf("AddMember2: Expected float64 got passed in value of %T", value))
				} else {
					switch opr {
					case mut.Add:
						x += n
						sa[i].Value = x
					case mut.Subtract:
						x -= n
						sa[i].Value = x
					case mut.Set:
						sa[i].Value = x
					default:
						panic(fmt.Errorf("Expected member operator of Set, Add, Subtract got %q", opr))
					}
				}

			case string:

				if _, ok := value.(string); !ok {
					panic(fmt.Errorf("AddMember2: Expected string got passed in value of %T", value))
				}
				sa[i].Value = value

			case bool:

				if _, ok := value.(bool); !ok {
					panic(fmt.Errorf("AddMember2: Expected bool got passed in value of %T", value))
				}
				sa[i].Value = value

			case [][]byte:

				if _, ok := value.([][]byte); !ok {
					panic(fmt.Errorf("Expected member type [][]byte, passed in value of type %T for attribute %s ", value, v.Name))
				}
				switch opr {
				case mut.Set:
					sa[i].Value = x
				default: // Append
					if s, ok := value.([][]byte); !ok {
						panic(fmt.Errorf("AddMember2: Expected []int64 got %T", x))
					} else {
						x = append(x, s...)
						sa[i].Value = x
						//	fmt.Printf("====. merged [][]byte  %s %d\n", sa[i].Name, len(sa[i].Value.([][]byte)))
					}
				}

			case []uuid.UID:

				if _, ok := value.([]uuid.UID); !ok {
					if _, ok := value.([][]byte); !ok {
						panic(fmt.Errorf("Expected member type []uuid.UID, passed in value of type %T for attribute %s ", value, v.Name))
					}
				}
				switch opr {
				case mut.Set:
					// set should override append in put
					sa[i].Value = x
				default: //
					// Append appropriate for updateItem
					if s, ok := value.([]uuid.UID); !ok {
						panic(fmt.Errorf("AddMember2: Expected []int64 got %T", x))
					} else {
						x = append(x, s...)
						sa[i].Value = x
						//	fmt.Printf("====. merged []uuid.UID %s %d\n", sa[i].Name, len(sa[i].Value.([][]uuid.UID)))
					}
				}

			case []string:
				if _, ok := value.([]string); !ok {
					panic(fmt.Errorf("Expected member type []string, passed in value of type %T for attribute %s ", value, v.Name))
				}
				switch opr {
				case mut.Set:
					sa[i].Value = x
				default: // Append
					if s, ok := value.([]string); !ok {
						panic(fmt.Errorf("AddMember2: Expected []string got %T", x))
					} else {
						x = append(x, s...)
						sa[i].Value = x
						//	fmt.Printf("====. merged %s %d\n", sa[i].Name, len(sa[i].Value.([]int)))
					}
				}

			case []int64:
				if _, ok := value.([]int64); !ok {
					panic(fmt.Errorf("Expected member type []int64, passed in value of type %T for attribute %s ", value, v.Name))
				}
				switch opr {
				case mut.Set:
					sa[i].Value = x
				default: // Append
					if s, ok := value.([]int64); !ok {
						panic(fmt.Errorf("AddMember2: Expected []int64 got %T", x))
					} else {
						x = append(x, s...)
						sa[i].Value = x
						//	fmt.Printf("====. merged %s %d\n", sa[i].Name, len(sa[i].Value.([]int)))
					}
				}

			case []int:
				if _, ok := value.([]int); !ok {
					panic(fmt.Errorf("Expected member type []int, passed in value of type %T for attribute %s ", value, v.Name))
				}
				switch opr {
				case mut.Set:
					sa[i].Value = x
				default: // Append
					if s, ok := value.([]int); !ok {
						panic(fmt.Errorf("AddMember2: Expected []int64 got %T", x))
					} else {
						x = append(x, s...)
						sa[i].Value = x
						//	fmt.Printf(" =. merged %s %d\n", sa[i].Name, len(sa[i].Value.([]int)))
					}
				}

			case []float64:
				if _, ok := value.([]float64); !ok {
					panic(fmt.Errorf("Expected member type []float64, passed in value of type %T for attribute %s ", value, v.Name))
				}
				switch opr {
				case mut.Set:
					sa[i].Value = x
				default: // Append
					if s, ok := value.([]float64); !ok {
						panic(fmt.Errorf("AddMember2: Expected []float64 got %T", x))
					} else {
						x = append(x, s...)
						sa[i].Value = x
						//	fmt.Printf("====. merged %s %d\n", sa[i].Name, len(sa[i].Value.([]int)))
					}
				}

			case []bool:
				if _, ok := value.([]bool); !ok {
					panic(fmt.Errorf("Expected member type []bool, passed in value of type %T for attribute %s ", value, v.Name))
				}
				switch opr {
				case mut.Set:
					sa[i].Value = x
				default: // Append
					if s, ok := value.([]bool); !ok {
						panic(fmt.Errorf("AddMember2: Expected []int64 got %T", x))
					} else {
						x = append(x, s...)
						sa[i].Value = x
						//	fmt.Printf("====. merged %s %d\n", sa[i].Name, len(sa[i].Value.([]int)))
					}
				}
			}
		}

		if !found {
			// no attr exists in the source mutation - lets create one
			syslog(fmt.Sprintf("create member %s as it does not exist in source mutation.", attr))
			h.sm.AddMember(attr, value)
		}
	}
	return h
}

// ****************************** New Query API ************************************

type QHandle struct {
	dbHdl              db.Handle       // mysql handle, [default: either, spanner spanner.NewClient, dynamodb dynamodb.New]
	options            []db.Option     // []db.Option
	ctx                context.Context // moved from QueryHandle.ctx. As set in tx.NewQuery*
	*query.QueryHandle                 // GoGraph query handle - single thread
	//	workers            []*query.QueryHandle // GoGraph query handle's for worker scans - one per parallel thread
}

// func NewQuery(tbln tbl.Name, label string, idx ...tbl.Name) *QHandle {
// 	// if err := tbl.IsRegistered(tbln); err != nil {
// 	// 	panic(err)
// 	// }

// 	if len(idx) > 0 {
// 		// if err := tbl.IsRegistered(idx[0]); err != nil {
// 		// 	return nil
// 		// }
// 		return &QHandle{QueryHandle: query.New(tbln, label, idx[0]), dbHdl: db.GetDefaultDBHdl()}
// 	}
// 	return &QHandle{QueryHandle: query.New(tbln, label), dbHdl: db.GetDefaultDBHdl()}
// }

func NewQuery2(label string, tbln tbl.Name, idx ...tbl.Name) *QHandle {

	return NewQueryContext(context.Background(), label, tbln, idx...)
}
func NewQuery(label string, tbln tbl.Name, idx ...tbl.Name) *QHandle {

	return NewQueryContext(context.Background(), label, tbln, idx...)
}

func NewQueryContext(ctx context.Context, label string, tbln tbl.Name, idx ...tbl.Name) *QHandle {
	// if err := tbl.IsRegistered(tbln); err != nil {
	// 	panic(err)
	// }

	if len(idx) > 0 {
		// if err := tbl.IsRegistered(idx[0]); err != nil {
		// 	panic(err)
		// }
		return &QHandle{ctx: ctx, QueryHandle: query.New(tbln, label, idx[0]), dbHdl: db.GetDefaultDBHdl()}
	}
	return &QHandle{ctx: ctx, QueryHandle: query.New(tbln, label), dbHdl: db.GetDefaultDBHdl()}
}

func (h *QHandle) DB(s string, opt ...db.Option) *QHandle {
	var err error
	h.dbHdl, err = db.GetDBHdl(s)
	if err != nil {
		panic(err)
	}
	h.options = opt

	return h
}

// func (h *QHandle) RetryOp(e error) bool {

// 	return db.RetryOp(e)
// }

func (h *QHandle) Prepare() *QHandle {

	h.SetPrepare()
	return h
}

func (h *QHandle) Close() error {

	return h.dbHdl.Close(h.QueryHandle)
}

//func (h *QHandle) ChanOutput(s interface{}) *QHandle {} // s is a chan ?. Can ChanOutput direct data onto s?

// Workers called from client code. Creates worker number of QueryHandles with associated Fetch (bind variable)
//
// func (h *QHandle) Workers() []*query.QueryHandle {

// 	//  n workers requires n QueryHandle's

// 	h.workers = make([]*query.QueryHandle, h.NumWorkers())

// 	r := reflect.ValueOf(h.Fetch()).Elem() // TODO: consider making slice parallel in size when client does

// 	for i := 0; i < h.NumWorkers(); i++ {

// 		d := h.QueryHandle.Clone()
// 		d.SetWorkerId(i)

// 		// ptx [][]rec
// 		// operation we want to emulate in reflect is select(&[]rec) which will be assigned to q.fetch inteface{}
// 		d.SetFetch(r.Index(i).Addr().Interface())

// 		h.workers[i] = d

// 	}

// 	return h.workers

// }

func (h *QHandle) ExecuteByChannel() (interface{}, error) {

	h.SetExecMode(h.Channel())

	err := h.Execute()

	if err != nil {
		return nil, err
	}

	return h.GetChannel(), nil
}

func (h *QHandle) ExecuteByFunc(f query.Cfunc) error {

	h.SetExecMode(h.Func())

	h.SetFunc(f)

	if h.NumWorkers() == 0 {
		// no Workers() specififed set to 1 so it can work with channels.
		// which will be setup internally
		h.SetWorkers(1)
	}

	return h.Execute()

}

// Execute - single threaded operations only. ExecuteByFunc or ExecuteByChannel for multi-threaded
func (h *QHandle) Execute(w ...int) error {

	if h.Error() != nil {
		return h.Error()
	}
	// NewQueryContext takes precedence over dbHdl.ctx
	ctx := h.ctx
	if ctx == nil {
		ctx = h.dbHdl.Ctx()
	}

	// switch len(w) {
	// case 0:
	// 	return h.dbHdl.ExecuteQuery(ctx, h.QueryHandle, h.options...)
	// default:
	// 	return h.dbHdl.ExecuteQuery(ctx, h.workers[w[0]], h.options...)
	// }

	return h.dbHdl.ExecuteQuery(ctx, h.QueryHandle, h.options...)

}

// func (h *QHandle) Execute() error {
// 	// NewQueryContext takes precedence over dbHdl.ctx
// 	ctx := h.ctx
// 	if ctx == nil {
// 		ctx = h.dbHdl.Ctx()
// 	}
// 	return h.dbHdl.ExecuteQuery(ctx, h.QueryHandle, h.options...)
// }
