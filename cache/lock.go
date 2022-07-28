package cache

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	blk "github.com/GoGraph/block"
	elog "github.com/GoGraph/errlog"
	"github.com/GoGraph/ggdb"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/types"
	"github.com/GoGraph/uuid"
)

const (
	single byte = iota
	multi
)

// FetchOvflBatch sends the cUIDs within an overflow block batch to the channel passed in.
// Does not read from cache or into cache, but uses cache as the lock entity.
// ouid - overflow block UID (parent node for overflow batches in that block)
// idx - overflow block index 1..n. 0 where n is the number of overflow blocks
// maxbid - max overflow batch (5 batches say) within a overflow block
func (g *GraphCache) FetchOvflBatch(ouid uuid.UID, idx int, maxbid int64, wg *sync.WaitGroup, sortk string, bCh chan<- BatchPy) {

	defer wg.Done()

	g.Lock()
	ouidb64 := ouid.EncodeBase64()
	e := g.cache[ouidb64]
	slog.LogAlert("FetchOvflBatch", fmt.Sprintf("idx %d maxbid %d ouid %s  sortk: %s", idx, maxbid, ouid.EncodeBase64(), sortk))
	// e is nill when UID not in cache (map), e.NodeCache is nill when node cache is cleared.
	if e == nil {
		e = &entry{ready: make(chan struct{})}
		g.cache[ouidb64] = e
		e.NodeCache = &NodeCache{m: make(map[SortKey]*blk.DataItem), gc: g}
		g.Unlock()
		close(e.ready)
	} else {
		g.Unlock()
		<-e.ready
	}
	e.Lock()
	var (
		sk string
	)
	for b := 1; b <= int(maxbid); b++ {
		// each overflow batch id has its own sortk value: <sortk>%<bid>
		sk = sortk + "%" + strconv.Itoa(b)
		// read directly from database - single item read
		nb, err := ggdb.FetchNode(ouid, sk)
		if err != nil {
			elog.Add("FetchOvflBatch: ", err)
			break
		}

		e.lastAccess = time.Now()
		// an overflow batch consists of 1 item with an attribute array containing cuids.
		bCh <- BatchPy{Bid: idx, Batch: b, Puid: ouid, DI: nb[0]}
	}
	close(bCh)
	e.Unlock()

}

// FetchUOB is redundant (most likely)
// concurrently fetches complete Overflow Block for each UID-PRED OUID entry.
// Called from cache.UnmarshalNodeCache() for Nd attribute only.
// TODO: need to decide when to release lock.
func (g *GraphCache) FetchUOB(ouid uuid.UID, wg *sync.WaitGroup, ncCh chan<- *NodeCache) {
	var sortk_ string

	defer wg.Done()

	sortk_ = "A#G#" // complete block - header items +  propagated scalar data belonging to Overflow block
	uid := ouid.EncodeBase64()

	g.Lock()
	e := g.cache[uid] // e will be nil if uidb64 not in map

	if e == nil || e.NodeCache == nil || e.NodeCache.m == nil {
		if e == nil {
			e = &entry{ready: make(chan struct{})}
			g.cache[uid] = e
		}
		if e.NodeCache == nil {
			e.NodeCache = &NodeCache{m: make(map[SortKey]*blk.DataItem), Uid: ouid, gc: g}
		}
		if e.NodeCache.m == nil {
			e.NodeCache.m = make(map[SortKey]*blk.DataItem)
		}
		g.Unlock()

		nb, err := ggdb.FetchNode(ouid, sortk_)
		if err != nil {
			return
		}
		en := e.NodeCache
		for _, v := range nb {
			en.m[v.Sortk] = v
		}
		close(e.ready)
	} else {
		g.Unlock()
		<-e.ready
	}
	//
	// lock node cache.
	//
	e.Lock()
	defer e.Unlock()
	// check if cache has been cleared while waiting to acquire lock, so try again
	if e.NodeCache == nil {
		g.FetchUOB(ouid, wg, ncCh)
	}
	var cached bool
	// check sortk is cached
	for k := range e.m {
		if strings.HasPrefix(k, sortk_) {
			cached = true
			break
		}
	}
	if !cached {
		e.dbFetchSortK(sortk_)
		ncCh <- e.NodeCache
		return
	}
	// e.Unlock() - unlocked in cache.UnmarshalNodeCache
	ncCh <- e.NodeCache
}

// TODO : is this used. Nodes are locked when "Fetched"
func (g *GraphCache) LockNode(uid uuid.UID) {

	fmt.Printf("** Cache LockNode  Key Value: [%s]\n", uid.EncodeBase64())

	g.Lock()
	uidb64 := uid.EncodeBase64()
	e := g.cache[uidb64]

	if e == nil {
		e = &entry{ready: make(chan struct{})}
		e.NodeCache = &NodeCache{}
		g.cache[uidb64] = e
		g.Unlock()
		close(e.ready)
	} else {
		g.Unlock()
		<-e.ready
	}
	//
	// lock e . Note: e can only be acquired from outside of this package via the Fetch* api.
	//
	e.Lock()

}
func (g *GraphCache) FetchForUpdate(uid uuid.UID, sortk string, ty ...string) (*NodeCache, error) {
	return g.FetchForUpdateContext(context.TODO(), uid, sortk, ty...)
}

// FetchForUpdate is used as a substitute for database lock. Provided all db access is via this API (or similar) then all updates will be serialised preventing
// mutliple concurrent updates from corrupting each other. It also guarantees consistency between cache and storage copies of the data.
// FetchForUpdate performs a Query so returns multiple items. Typically we us this to access all scalar node attribute (e.g. sortk of "A#A#")
// or all uid-pred and its propagated data (e.g. "A#:G")
func (g *GraphCache) FetchForUpdateContext(ctx context.Context, uid uuid.UID, sortk string, ty ...string) (*NodeCache, error) {

	g.Lock()
	uidb64 := uid.EncodeBase64()
	e := g.cache[uidb64]

	if e == nil { //|| e.NodeCache == nil || e.NodeCache.m == nil {
		e = &entry{}
		g.addNode(uid, e)
		g.Unlock()
		// usually perform some IO operation here...now performed in isCached under an entry lock.
		e.NodeCache = &NodeCache{m: make(map[SortKey]*blk.DataItem), Uid: uid, gc: g}
		e.ready = make(chan struct{})
		close(e.ready)
	} else {
		// all other threads wait on nonbuffered channel - until it is closed
		g.touchNode(uid, e)
		g.Unlock()
		<-e.ready
	}
	e.Lock()

	e.lastAccess = time.Now()
	// check if cache has been cleared while waiting to acquire lock, so try again
	// check sortk is cached
	//
	en := e.NodeCache
	if cached, fetch := en.isCached(ctx, sortk, ty...); !cached {
		var err error
		switch fetch {
		case single:
			err = en.dbFetchItemContext(ctx, sortk)
		case multi:
			err = en.dbFetchSortKContext(ctx, sortk)
		}
		if err != nil {
			return nil, err
		}
	}
	return en, nil
}

// func (g *GraphCache) FetchForUpdate2(uid uuid.UID, sortk ...string) (*NodeCache, error) {
// 	var (
// 		sortk_ string
// 	)
// 	//
// 	//	g lock protects global cache with UID key
// 	//
// 	g.Lock()
// 	if len(sortk) > 0 {
// 		sortk_ = sortk[0]
// 	} else {
// 		sortk_ = types.GraphSN() + "|" + "A#"
// 	}

// 	slog.Log("FetchForUpdate: ", fmt.Sprintf("** Cache FetchForUpdate Cache Key Value: [%s]   sortk: %s", uid.EncodeBase64(), sortk_))
// 	e := g.cache[uid.EncodeBase64()]
// 	// e is nill when UID not in cache (map), e.NodeCache is nill when node cache is cleared.
// 	if e == nil || e.NodeCache == nil {
// 		if e == nil {
// 			e = &entry{ready: make(chan struct{})}
// 			g.cache[uid.EncodeBase64()] = e
// 		}
// 		if e.NodeCache == nil {
// 			e.NodeCache = &NodeCache{m: make(map[SortKey]*blk.DataItem), Uid: uid, gc: g}
// 		}
// 		if e.NodeCache.m == nil {
// 			e.NodeCache.m = make(map[SortKey]*blk.DataItem)
// 		}
// 		g.Unlock()
// 		// nb: type blk.NodeBlock []*DataItem
// 		nb, err := ggdb.FetchNode(uid, sortk_)
// 		if err != nil {
// 			slog.Log("FetchForUpdate: ", fmt.Sprintf("db fetchnode error: %s", err.Error()))
// 			return nil, err
// 		}
// 		en := e.NodeCache
// 		en.Uid = uid
// 		for _, v := range nb {
// 			en.m[v.Sortk] = v
// 		}
// 		close(e.ready)
// 	} else {
// 		g.Unlock()
// 		<-e.ready
// 	}
// 	//
// 	// e lock protects Node Cache with sortk key.
// 	// lock e to prevent updates from other routines. Must explicitly Unlock() some stage later.
// 	//  Note: e can only be acquired from outside of this package via the Fetch* api.
// 	//
// 	e.Lock()
// 	defer e.Unlock()
// 	// check if cache has been cleared while waiting to acquire lock, so try again
// 	if e.NodeCache == nil {
// 		g.FetchForUpdate(uid, sortk_)
// 	}
// 	e.fullLock = true
// 	// check sortk is cached
// 	// TODO: check only looks for one sortk prefix entry when
// 	// a node would typically have many. Need to sophisticated check ideal check each of the node predicate's
// 	// are cached.
// 	var cached bool
// 	for k := range e.m {
// 		if strings.HasPrefix(k, sortk_) {
// 			cached = true
// 			break
// 		}
// 	}
// 	if !cached {
// 		// perform a db fetch of sortk
// 		e.dbFetchSortK(sortk_)
// 	}
// 	return e.NodeCache, nil
// }

// FetchNodeNonCache will perform a db fetch for each execution.
// Why? For testing purposes it's more realistic to access non-cached node data.
// This API is used in GQL testing where I want to be testing the db IO performance not the benefits of caching.
// TODO: rename to FetchNodeNoCache
// func (g *GraphCache) FetchNodeNonCache(uid uuid.UID, sortk ...string) (*NodeCache, error) {
// 	// 	if len(sortk) > 0 {
// 	// 		return g.FetchNode(uid, sortk...)
// 	// 	}
// 	// 	return g.FetchNode(uid)
// 	// }

// 	var sortk_ string

// 	g.Lock()
// 	if len(sortk) > 0 {
// 		sortk_ = sortk[0]
// 	} else {
// 		sortk_ = types.GraphSN() + "|A#" //"A#"
// 	}
// 	uidb64 := uid.EncodeBase64()
// 	e := g.cache[uidb64]
// 	//
// 	// force db read by setting e to nil
// 	//
// 	e = nil
// 	//
// 	if e == nil {
// 		e = &entry{ready: make(chan struct{})}
// 		g.cache[uidb64] = e
// 		g.Unlock()
// 		// nb: type blk.NodeBlock []*DataIte
// 		nb, err := ggdb.FetchNode(uid, sortk_)
// 		if err != nil {
// 			return nil, err
// 		}
// 		if len(nb) == 0 {
// 			// uid not found
// 			return nil, nil
// 		}
// 		e.NodeCache = &NodeCache{m: make(map[SortKey]*blk.DataItem), gc: g}
// 		en := e.NodeCache
// 		en.Uid = uid
// 		for _, v := range nb {
// 			en.m[v.Sortk] = v
// 		}
// 		close(e.ready)
// 	} else {
// 		g.Unlock()
// 		<-e.ready
// 	}
// 	//e.Lock()
// 	// check ifcache has been cleared while waiting to acquire lock..try again
// 	//e.fullLock = true
// 	return e.NodeCache, nil
// }

// ========================== TODO ===============================================

// TODO: FetchNode(uid uuid.UID, sortk string, nTy ...string) (*NodeCache, error) {
// whre nTy is the "expected" or "known" node type.

// option to specify node type as in most (or a lot) of cases when querying a node the type is known
// this will save a lookup of the node type in isCached.

func (g *GraphCache) FetchNode(uid uuid.UID, sortk string, ty ...string) (*NodeCache, error) {
	return g.FetchNodeContext(context.TODO(), uid, sortk, ty...)
}

// FetchNode complete or a subset of node data (for SORTK value) into cache.
// Performs lock for sync (shared resource) and transactional perhpas.
// If cache is already populated with Node data checks SORTK entry exists in cache - if not calls dbFetchSortK().
// Lock is held long enough to read cached result then unlocked in calling routine.
// ty - node type (short name)
func (g *GraphCache) FetchNodeContext(ctx context.Context, uid uuid.UID, sortk string, ty ...string) (*NodeCache, error) {

	logid := "FetchNodeContext"
	slog.Log(logid, fmt.Sprintf("enter, UID %s, sortk [%s]", uid.Base64(), sortk))
	g.Lock()
	uidb64 := uid.EncodeBase64()
	e := g.cache[uidb64]

	if e == nil { //|| e.NodeCache == nil || e.NodeCache.m == nil {
		e = &entry{}
		g.addNode(uid, e)
		g.Unlock()
		// usually perform some IO operation here...now performed in isCached under an entry lock.
		e.NodeCache = &NodeCache{m: make(map[SortKey]*blk.DataItem), Uid: uid, gc: g}
		e.ready = make(chan struct{})
		close(e.ready)
	} else {
		// all other threads wait on nonbuffered channel - until it is closed
		g.touchNode(uid, e)
		g.Unlock()
		<-e.ready
	}
	e.Lock()
	defer e.Unlock()

	e.lastAccess = time.Now()
	// check if cache has been cleared while waiting to acquire lock, so try again
	// check sortk is cached
	//
	en := e.NodeCache
	if cached, fetch := en.isCached(ctx, sortk, ty...); !cached {
		var err error
		switch fetch {
		case single:
			err = en.dbFetchItemContext(ctx, sortk)
		case multi:
			err = en.dbFetchSortKContext(ctx, sortk)
		}
		if err != nil {
			return nil, err
		}
	}
	return en, nil
}

func (nc *NodeCache) dbFetchItem(sortk string) error {
	return nc.dbFetchItemContext(context.TODO(), sortk)
}

//dbFetchItem loads sortk attribute from database and enters into cache
func (nc *NodeCache) dbFetchItemContext(ctx context.Context, sortk string) error {

	slog.Log("dbFetchItem", fmt.Sprintf("dbFetchItem for %q UID: [%s] \n", sortk, nc.Uid.EncodeBase64()))
	nb, err := ggdb.FetchNodeItemContext(ctx, nc.Uid, sortk)
	if err != nil {
		return err
	}
	nc.m[sortk] = nb
	return nil
}

func (nc *NodeCache) dbFetchSortK(sortk string) error {
	return nc.dbFetchSortKContext(context.TODO(), sortk)
}

//dbFetchSortK loads sortk attribute from database and enters into cache
func (nc *NodeCache) dbFetchSortKContext(ctx context.Context, sortk string) error {

	nb, err := ggdb.FetchNodeContext(ctx, nc.Uid, sortk)
	if err != nil {
		return err
	}
	slog.Log("dbFetchSortK", fmt.Sprintf("Fetched %d items for %q\n", len(nb), nc.Uid.EncodeBase64()))
	// add data items to node cache
	for _, v := range nb {
		nc.m[v.Sortk] = v
	}

	return nil
}

// isCached - check if sortk is cached.
// Node type is required as the type determines the sortk entries for the node to check against
// Node type, ty (long name) maybe provided in which case it does not need to be sourced from db/cache (GetType())
func (nc *NodeCache) isCached(ctx context.Context, sortk string, ty ...string) (bool, byte) {
	// 	return true, single
	// }
	logid := "isCached"
	var ty_ string
	if len(ty) > 0 {
		ty_ = ty[0]
	}

	// node type
	i := strings.Index(sortk, "|") + 1
	graph := sortk[:i]
	sortk_ := sortk[i:]
	slog.Log(logid, fmt.Sprintf("sortk_ : %q", sortk_))
	if sortk_ == "A#A#T" {
		if _, ok := nc.m[sortk]; ok {
			return true, single
		}
		if _, ok := nc.m[sortk_]; ok {
			return true, single
		}
		return false, single
	}

	// check if cache is empty (affects all sortk values)
	if len(nc.m) == 0 {
		// specific UID-PRED - A#G#:?",  A#G#:?#"
		if len(sortk_) > 5 && sortk_[:5] == "A#G#:" && (strings.Count(sortk_, "#") == 2 || strings.Count(sortk_, "#") == 3) {
			return false, single
		}
		// specific single propagation scalar data - A#G#:?#:N"
		if len(sortk_) >= 5 && sortk_[:5] == "A#G#:" && strings.Count(sortk_, "#") == 3 && strings.Count(sortk_, ":") == 2 {
			return false, single
		}
		slog.Log(logid, fmt.Sprintf("cache is empty return false, multi"))
		return false, multi
	}
	// get node type from argument or query from cache/db.
	if len(ty) == 0 {
		// long type name of current node
		slog.Log(logid, fmt.Sprintf("About to run GetType()"))
		ty_, _ = nc.GetType()
	}
	if sortk_ == "A" || sortk_ == "A#" {
		// complete node data in partition A - presume this is the first request.
		slog.Log(logid, "return false, multi")
		return false, multi
	}

	// all scalar data in partition A
	if sortk_ == "A#A" || sortk_ == "A#A#" {
		cached := true
		for _, v := range types.GetScalars(ty_, graph+"A#A#:") {
			slog.Log(logid, fmt.Sprintf("*** getScalars : [%s]", v))
			if _, ok := nc.m[v]; !ok {
				cached = false
				break
			}
		}
		slog.Log(logid, fmt.Sprintf("*** getScalars cached %v", cached))
		return cached, multi
	}

	// specific UID-PRED - A#G#:?",  A#G#:?#"
	if len(sortk_) > 5 && sortk_[:5] == "A#G#:" && (strings.Count(sortk_, "#") == 2 || strings.Count(sortk_, "#") == 3) {
		if _, ok := nc.m[sortk]; ok {
			return true, single
		}
		return false, single
	}

	// all single propagation data -"A#G#"
	if len(sortk_) == 4 && sortk_[:4] == "A#G#" {
		cached := true
		//slog.Log(logid, fmt.Sprintf("GetSinglePropagatedScalarsAll : %s [%s]", ty_, sortk))
		for _, v := range types.GetSinglePropagatedScalarsAll(ty_, sortk[:4]+":") {
			if _, ok := nc.m[v]; !ok {
				cached = false
				break
			}
		}
		return cached, multi
	}

	// all single propagation scalar data for specific uid-pred "A#G#:?#"
	if len(sortk_) >= 4 && sortk_[:4] == "A#G#" && (strings.Count(sortk_, "#") == 2 || strings.Count(sortk_, "#") == 3) {

		cached := true
		u := strings.Split(sortk, "#")
		uidpred := u[3][1:]
		for _, v := range types.GetSinglePropagatedScalars(ty_, uidpred, sortk) {
			if _, ok := nc.m[v]; !ok {
				cached = false
				break
			}
		}
		return cached, multi
	}

	// specific single propagation scalar data - A#G#:?#:N"
	if len(sortk_) >= 5 && sortk_[:5] == "A#G#:" && strings.Count(sortk_, "#") == 3 && strings.Count(sortk_, ":") == 2 {
		if _, ok := nc.m[sortk]; ok {
			return true, single
		}
		return false, single
	}

	// double propagation is not requested anywhere so far.

	// // specific double propagation uid-pred data - A#G#:?#G#:A"
	// if len(sortk_) >= 5 && sortk_[:5] == "A#G#:" && strings.Count(sortk_, "#") == 4 {
	// 	if _, ok := nc.m[sortk]; ok {
	// 		return true, single
	// 	}
	// 	return false, single
	// }

	// // all double propagation scalar data - "A#G#:?#G#:", "A#G#:?#G#:?#:?"
	// if len(sortk_) >= 5 && sortk_[:5] == "A#G#:" && strings.Count(sortk_, "#") == 5 {
	// 	uidpred := sortk[strings.Index(sortk, "#:"):]
	// 	cached := true
	// 	for _, v := range types.GetDoublePropagatedScalars(ty_, uidpred[2:], sortk) {
	// 		if _, ok := nc.m[v]; !ok {
	// 			cached = false
	// 			break
	// 		}
	// 	}
	// 	return cached, multi
	// }

	// // specific double propagation scalar data -  A#G#:P#G#:A#:N
	// if len(sortk_) >= 5 && sortk_[:5] == "A#G#:" && strings.Count(sortk_, "#") == 3 && strings.Count(sortk_, ":") == 2 {
	// 	if _, ok := nc.m[sortk]; ok {
	// 		return true, single
	// 	}
	// 	return false, single
	// }

	elog.Add(logid, fmt.Errorf("[%s] not processed", sortk))
	return false, multi
}

func (n *NodeCache) ClearCache(sortk string, subs ...bool) error {

	slog.Log("ClearCache: ", fmt.Sprintf("sortk %s", sortk))
	//
	// check if node is cached
	//
	delete(n.m, sortk)
	// if clear all sub-sortk's
	if len(subs) > 0 && subs[0] {
		for k := range n.m {
			if strings.HasPrefix(sortk, k) {
				delete(n.m, k)
			}
		}
	}

	return nil
}

func (n *NodeCache) CachePurge() {
	n.gc.ClearNodeCache(n.Uid)
}

func (n *NodeCache) ClearNodeCache() {
	n.gc.ClearNodeCache(n.Uid)
}

// func (n *NodeCache) ClearNodeCache(sortk ...string) {
// 	n.gc.ClearNodeCache(n.Uid)
// }
func (g *GraphCache) ClearNodeCache(uid uuid.UID) {

	// purging a node UID from global cache.
	// GC will purge the entry value, once all variables referencing it go out-of-scope.
	// Until then there can be upto two versions of the node cache present in memory.
	// the new cache populated with fresh database fetch, and the previous cache entry that
	// will be active until it the variables referencing it go out-of-scope.
	// These variables should test the cache each time they require access to the node data by
	// checking the value of nc.stale.
	// Also, the NodeCache i.e. e.NodeCache is a shared variable. You cannot
	// e=nil or e.NodeCache=nil as other routines maybe accessing it. Hence
	// the cache purge will only remove the UID from the Global Cache, which
	// can be safely done under the g.Lock().
	var (
		ok bool
		e  *entry
	)
	g.Lock()
	defer g.Unlock()

	if e, ok = g.cache[uid.EncodeBase64()]; !ok {
		//	slog.Log("ClearNodeCache: ", fmt.Sprintf("node %q not in cache", uid.Base64()))
		return
	}
	e.Lock()
	e.stale = true
	//	slog.Log("ClearNodeCache: ", fmt.Sprintf("delete %q from cache", uid.Base64()))
	delete(g.cache, uid.EncodeBase64())
	e.Unlock()

}

// if e == nil {
// 	slog.Log("ClearNodeCache: ", "e nil Nothing to clear")
// }
// e.Lock()
// nc := e.NodeCache

// if len(sortk) > 0 {
// 	// optional: remove overflow blocks for supplied sortk (UID-PRED) they exist
// 	for _, uid := range nc.GetOvflUIDs(sortk[0]) {
// 		if _, ok = g.cache[uid.EncodeBase64()]; ok {
// 			// delete map entry will mean e is unassigned and allow GC to purge e and associated node cache.
// 			delete(g.cache, uid.EncodeBase64())
// 		}
// 	}
// 	// clear dataitem associated with sortk
// 	vv, ok := nc.m[sortk[0]]
// 	if ok && vv != nil {
// 		vv = nil
// 	}

// } else {
// 	// search for UIDPRED sortk's and clear all overflow blocks??
// 	for k, v := range nc.m {
// 		if i := strings.Index(k, "#G#"); i > 0 && i < 5 { // TODO: what about H,I,J,K
// 			for _, uid := range nc.GetOvflUIDs(k) {
// 				if e, ok = g.cache[uid.EncodeBase64()]; ok {
// 					// delete map entry will mean e is unassigned and allow GC to purge e and associated node cache.
// 					delete(g.cache, uid.EncodeBase64())
// 					e = nil
// 				}
// 			}
// 		}
// 		if v != nil {
// 			v = nil
// 		}
// 	}
// }
// if nc != nil {
// 	nc.m = nil
// }
// e.NodeCache = nil
// e = nil
// nc.Unlock()
//}

// Unlock method shadows the RWMutex Unlock
// func (nd *NodeCache) Unlock(s ...string) {

// 	// if len(s) > 0 {
// 	// 	slog.Log("Unlock: ", fmt.Sprintf("******* IN UNLOCK NC ********************  %s", s[0]))
// 	// } else {
// 	// 	slog.Log("Unlock: ", "******* IN UNLOCK NC ********************")
// 	// }

// 	if nd == nil {
// 		return
// 	}

// 	if nd.m != nil && len(nd.m) == 0 {
// 		// locked by LockNode() - without caching daa
// 		nd.RWMutex.Unlock()
// 		return
// 	}
// 	//
// 	if nd.fullLock {
// 		//
// 		// Locked for update - full lock
// 		//
// 		nd.fullLock = false
// 		nd.RWMutex.Unlock()

// 	} else {
// 		//
// 		//	Read Lock
// 		//
// 		nd.RWMutex.RUnlock()
// 		//slog.Log("Unlock: ", "Success RUnlock()")
// 		slog.Log("Unlock: ", "Success Unlock()")
// 		//nd.locked = false
// 	}
// }

// func (nc *NodeCache) CrEmptyNodeItem(uid uuid.UID, sortk string, noLock ...bool) {

// 	if len(noLock) > 0 && !noLock[0] {
// 		nc.Lock()
// 		defer nc.Unlock()
// 	}

// 	// create empty node item, if not present in cache
// 	if _, ok := nc.m[sortk]; !ok {
// 		nc.m[sortk] = &blk.DataItem{Pkey: []byte(uid), Sortk: sortk}
// 	}
// }

// FetchNode complete or a subset of node data (for SORTK value) into cache.
// Performs lock for sync (shared resource) and transactional perhpas.
// If cache is already populated with Node data checks SORTK entry exists in cache - if not calls dbFetchSortK().
// Lock is held long enough to read cached result then unlocked in calling routine.
func (g *GraphCache) FetchAndLockNode(uid uuid.UID, sortk ...string) (*NodeCache, error) {
	var sortk_ string

	if len(sortk) > 0 {
		sortk_ = sortk[0]
	} else {
		sortk_ = types.GraphSN() + "|A#"
	}

	g.Lock()
	uidb64 := uid.EncodeBase64()
	e := g.cache[uidb64]

	if e == nil {
		e = &entry{ready: make(chan struct{})}
		g.cache[uidb64] = e
		// stop anyone reading cache while its being loaded.
		g.Unlock()
		// only single threaded access to all shared variables at this point
		nb, err := ggdb.FetchNode(uid, sortk_)
		if err != nil {
			return nil, err
		}
		en := e.NodeCache
		for _, v := range nb {
			en.m[v.Sortk] = v
			//fmt.Printf("\n***FetchNode: map entries: %#v\n", *v)
		}
		close(e.ready)
	} else {
		// all other threads wait on nonbuffered channel - until it is closed
		// by load cache thread.
		g.Unlock()
		<-e.ready
	}
	// lock e.NodeCache.RWMutex for duration of this function. Meant for readonly client.
	e.RLock()
	e.lastAccess = time.Now()

	// check sortk is cached
	var cached bool
	for k := range e.m {
		if strings.HasPrefix(k, sortk_) {
			cached = true
			break
		}
	}
	if !cached {
		// perform a db fetch of sortk
		e.dbFetchSortK(sortk_)
	}
	return e.NodeCache, nil
}
