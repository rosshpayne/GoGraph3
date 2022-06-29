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

// FetchBatch reads the cUIDs within an overflow block batch.
// Does not read from cache but uses cache as the lock entity.
// i - overflow block. 1..n. 0 is embedded uid-pred which does not execute FetchBatch()
// ouid - overflow block uid
// bid - max overflow batch (5 batches say) within a overflow block
func (g *GraphCache) FetchBatch(i int, bid int64, ouid uuid.UID, wg *sync.WaitGroup, sortk string, bCh chan<- BatchPy) {

	defer wg.Done()

	g.Lock()
	ouidb64 := ouid.EncodeBase64()
	e := g.cache[ouidb64]
	syslog(fmt.Sprintf("FetchBatch: i %d bid %d ouid %s  sortk: %s", i, bid, ouid.EncodeBase64(), sortk))
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
	for b := 1; b < int(bid)+1; b++ {
		// each bid has its own sortk value: <sortk>%<bid>
		sk = sortk + "%" + strconv.Itoa(b)
		nb, err := ggdb.FetchNode(ouid, sk)
		if err != nil {
			elog.Add("FetchBatch: ", err)
			break
		}
		syslog(fmt.Sprintf("FetchBatch: ouid %s  b, sk: %d %s len(nb) %d ", ouid, b, sk, len(nb)))
		e.lastAccess = time.Now()
		// an overflow batch consists of 1 item with an attribute array containing cuids.
		bCh <- BatchPy{Bid: i, Batch: b, Puid: ouid, DI: nb[0]}
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
func (g *GraphCache) FetchForUpdate(uid uuid.UID, sortk ...string) (*NodeCache, error) {
	return g.FetchForUpdateContext(context.TODO(), uid, sortk...)
}

// FetchForUpdate is used as a substitute for database lock. Provided all db access is via this API (or similar) then all updates will be serialised preventing
// mutliple concurrent updates from corrupting each other. It also guarantees consistency between cache and storage copies of the data.
// FetchForUpdate performs a Query so returns multiple items. Typically we us this to access all scalar node attribute (e.g. sortk of "A#A#")
// or all uid-pred and its propagated data (e.g. "A#:G")
func (g *GraphCache) FetchForUpdateContext(ctx context.Context, uid uuid.UID, sortk ...string) (*NodeCache, error) {
	var (
		sortk_ string
	)

	if len(sortk) > 0 {
		sortk_ = sortk[0]
	} else {
		sortk_ = types.GraphSN() + "|" + "A#"
	}
	uidb64 := uid.EncodeBase64()

	g.Lock()
	e := g.cache[uidb64]

	// e is nill when UID not in cache (map), e.NodeCache is nill when node cache is cleared.
	if e == nil { //|| e.NodeCache == nil || e.NodeCache.m == nil {
		//slog.Log("FetchForUpdate", fmt.Sprintf("NOT Cached. Cache Key Value: [%s]   sortk: %s", uid.EncodeBase64(), sortk_))
		e = &entry{ready: make(chan struct{})}
		g.cache[uidb64] = e
		g.Unlock()
		e.NodeCache = &NodeCache{m: make(map[SortKey]*blk.DataItem), Uid: uid, gc: g}
		// nb: type blk.NodeBlock []*DataItem
		nb, err := ggdb.FetchNodeContext(ctx, uid, sortk_)
		if err != nil {
			slog.Log("FetchForUpdate", fmt.Sprintf("db fetchnode error: %s %s %s", uid.EncodeBase64(), sortk_, err.Error()))
			return nil, err
		}
		//	slog.Log("FetchForUpdate", fmt.Sprintf("Successful fetch %s %s", uid.EncodeBase64(), sortk_))
		en := e.NodeCache
		for _, v := range nb {
			en.m[v.Sortk] = v
		}
		close(e.ready)
	} else {
		//	slog.Log("FetchForUpdate", fmt.Sprintf("Is Cached. Cache Key Value: [%s]   sortk: %s", uid.EncodeBase64(), sortk_))
		g.Unlock()
		<-e.ready
	}
	//
	// e lock protects Node Cache with sortk key.
	// lock e to prevent updates from other routines. Must explicitly Unlock() some stage later.
	//  Note: e can only be acquired from outside of this package via the Fetch* api.
	//
	e.Lock()

	// check if cache has been cleared while waiting to acquire lock, so try again
	if e.m == nil {
		g.FetchForUpdateContext(ctx, uid, sortk_)
	}
	e.fullLock = true
	// check sortk is cached
	// TODO: check only looks for one sortk prefix entry when
	// a node would typically have many. Need to sophisticated check ideal check each of the node predicate's
	// are cached.
	var cached bool
	for k := range e.m {
		if strings.HasPrefix(k, sortk_) {
			cached = true
			break
		}
	}
	if !cached {
		//	slog.Log("FetchForUpdate", fmt.Sprintf("dbFetchSortK for %s %s", uid.EncodeBase64(), sortk_))
		// perform a db fetch of sortk
		e.dbFetchSortKContext(ctx, sortk_)
	}
	return e.NodeCache, nil
}

func (g *GraphCache) FetchForUpdate2(uid uuid.UID, sortk ...string) (*NodeCache, error) {
	var (
		sortk_ string
	)
	//
	//	g lock protects global cache with UID key
	//
	g.Lock()
	if len(sortk) > 0 {
		sortk_ = sortk[0]
	} else {
		sortk_ = types.GraphSN() + "|" + "A#"
	}

	slog.Log("FetchForUpdate: ", fmt.Sprintf("** Cache FetchForUpdate Cache Key Value: [%s]   sortk: %s", uid.EncodeBase64(), sortk_))
	e := g.cache[uid.EncodeBase64()]
	// e is nill when UID not in cache (map), e.NodeCache is nill when node cache is cleared.
	if e == nil || e.NodeCache == nil {
		if e == nil {
			e = &entry{ready: make(chan struct{})}
			g.cache[uid.EncodeBase64()] = e
		}
		if e.NodeCache == nil {
			e.NodeCache = &NodeCache{m: make(map[SortKey]*blk.DataItem), Uid: uid, gc: g}
		}
		if e.NodeCache.m == nil {
			e.NodeCache.m = make(map[SortKey]*blk.DataItem)
		}
		g.Unlock()
		// nb: type blk.NodeBlock []*DataItem
		nb, err := ggdb.FetchNode(uid, sortk_)
		if err != nil {
			slog.Log("FetchForUpdate: ", fmt.Sprintf("db fetchnode error: %s", err.Error()))
			return nil, err
		}
		en := e.NodeCache
		en.Uid = uid
		for _, v := range nb {
			en.m[v.Sortk] = v
		}
		close(e.ready)
	} else {
		g.Unlock()
		<-e.ready
	}
	//
	// e lock protects Node Cache with sortk key.
	// lock e to prevent updates from other routines. Must explicitly Unlock() some stage later.
	//  Note: e can only be acquired from outside of this package via the Fetch* api.
	//
	e.Lock()
	defer e.Unlock()
	// check if cache has been cleared while waiting to acquire lock, so try again
	if e.NodeCache == nil {
		g.FetchForUpdate(uid, sortk_)
	}
	e.fullLock = true
	// check sortk is cached
	// TODO: check only looks for one sortk prefix entry when
	// a node would typically have many. Need to sophisticated check ideal check each of the node predicate's
	// are cached.
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

// FetchNodeNonCache will perform a db fetch for each execution.
// Why? For testing purposes it's more realistic to access non-cached node data.
// This API is used in GQL testing where I want to be testing the db IO performance not the benefits of caching.
// TODO: rename to FetchNodeNoCache
func (g *GraphCache) FetchNodeNonCache(uid uuid.UID, sortk ...string) (*NodeCache, error) {
	// 	if len(sortk) > 0 {
	// 		return g.FetchNode(uid, sortk...)
	// 	}
	// 	return g.FetchNode(uid)
	// }

	var sortk_ string

	g.Lock()
	if len(sortk) > 0 {
		sortk_ = sortk[0]
	} else {
		sortk_ = types.GraphSN() + "|A#" //"A#"
	}
	uidb64 := uid.EncodeBase64()
	e := g.cache[uidb64]
	//
	// force db read by setting e to nil
	//
	e = nil
	//
	if e == nil {
		e = &entry{ready: make(chan struct{})}
		g.cache[uidb64] = e
		g.Unlock()
		// nb: type blk.NodeBlock []*DataIte
		nb, err := ggdb.FetchNode(uid, sortk_)
		if err != nil {
			return nil, err
		}
		if len(nb) == 0 {
			// uid not found
			return nil, nil
		}
		e.NodeCache = &NodeCache{m: make(map[SortKey]*blk.DataItem), gc: g}
		en := e.NodeCache
		en.Uid = uid
		for _, v := range nb {
			en.m[v.Sortk] = v
		}
		close(e.ready)
	} else {
		g.Unlock()
		<-e.ready
	}
	//e.Lock()
	// check ifcache has been cleared while waiting to acquire lock..try again
	//e.fullLock = true
	return e.NodeCache, nil
}

func (g *GraphCache) FetchNode(uid uuid.UID, sortk ...string) (*NodeCache, error) {
	return g.FetchNodeContext(context.TODO(), uid, sortk...)
}

// FetchNode complete or a subset of node data (for SORTK value) into cache.
// Performs lock for sync (shared resource) and transactional perhpas.
// If cache is already populated with Node data checks SORTK entry exists in cache - if not calls dbFetchSortK().
// Lock is held long enough to read cached result then unlocked in calling routine.
func (g *GraphCache) FetchNodeContext(ctx context.Context, uid uuid.UID, sortk ...string) (*NodeCache, error) {
	var sortk_ string

	g.Lock()
	if len(sortk) > 0 {
		sortk_ = sortk[0]
	} else {
		sortk_ = types.GraphSN() + "|A#"
	}
	uidb64 := uid.EncodeBase64() // TODO: rename to Base64()
	e := g.cache[uidb64]

	if e == nil { //|| e.NodeCache == nil || e.NodeCache.m == nil {
		e = &entry{ready: make(chan struct{})}
		g.cache[uidb64] = e
		g.Unlock()
		e.NodeCache = &NodeCache{m: make(map[SortKey]*blk.DataItem), Uid: uid, gc: g}
		// only single threaded access at this point so no need to e.Lock()
		// all concurrent read threads will be waiting on the ready channel
		nb, err := ggdb.FetchNodeContext(ctx, uid, sortk_)
		if err != nil {
			return nil, err
		}
		en := e.NodeCache
		for _, v := range nb {
			en.m[v.Sortk] = v
		}
		close(e.ready)
	} else {
		// all other threads wait on nonbuffered channel - until it is closed
		// by load cache thread.
		g.Unlock()
		<-e.ready
	}
	// lock e.NodeCache.RWMutex for duration of this function. Meant for readonly client.
	e.Lock()
	defer e.Unlock()

	e.lastAccess = time.Now()
	// check if cache has been cleared while waiting to acquire lock, so try again
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
		e.dbFetchSortKContext(ctx, sortk_)
	}
	return e.NodeCache, nil
}

func (nc *NodeCache) dbFetchSortK(sortk string) error {
	return nc.dbFetchSortKContext(context.TODO(), sortk)
}

//dbFetchSortK loads sortk attribute from database and enters into cache
func (nc *NodeCache) dbFetchSortKContext(ctx context.Context, sortk string) error {

	slog.LogAlert("dbFetchSortK", fmt.Sprintf("dbFetchSortK for %s UID: [%s] \n", sortk, nc.Uid.EncodeBase64()))
	nb, err := ggdb.FetchNodeContext(ctx, nc.Uid, sortk)
	if err != nil {
		return err
	}
	// add data items to node cache
	for _, v := range nb {
		nc.m[v.Sortk] = v
	}

	return nil
}

// func (n *NodeCache) ClearCache(sortk string, subs ...bool) error {

// 	slog.Log("ClearCache: ", fmt.Sprintf("sortk %s", sortk))
// 	//
// 	// check if node is cached
// 	//
// 	delete(n.m, sortk)
// 	// if clear all sub-sortk's
// 	if len(subs) > 0 && subs[0] {
// 		for k := range n.m {
// 			if strings.HasPrefix(sortk, k) {
// 				delete(n.m, k)
// 			}
// 		}
// 	}

// 	return nil
// }

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
		slog.Log("ClearNodeCache: ", fmt.Sprintf("node %q not in cache", uid))
		return
	}
	e.Lock()
	e.stale = true
	slog.Log("ClearNodeCache: ", fmt.Sprintf("delete %q from cache", uid))
	delete(g.cache, uid.EncodeBase64())
	e.Unlock()

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
}

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
