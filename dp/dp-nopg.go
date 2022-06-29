//go:build nopaginate
// +build nopaginate

package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	blk "github.com/GoGraph/block"
	"github.com/GoGraph/cache"
	param "github.com/GoGraph/dygparam"
	elog "github.com/GoGraph/errlog"
	"github.com/GoGraph/grmgr"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/tx/query"
	"github.com/GoGraph/types"
	"github.com/GoGraph/uuid"
)

var (
	ctxEnd sync.WaitGroup
	cancel context.CancelFunc
)

//	DP design
// ----------
//
// pUID block
// 	 ...						    Assoc Read						  	 Assoc DP
// 	uid-pred    Nd    Id  XF        GoRoutine(1) Channel(2)     		 GoRoutine(3) PKey   Tx(4)
// 				cuid   0  1   -|
// 				cuid   0  1    |
// 				cuid   0  1    |---- rgr0  -->    ch0	->cache.BatchPy ->  gr0       pUID	 ptx
// 				..			   |
// 				cuid   0  1   -|
// 				ovbid  1  4          rgr1  -->    ch1	->cache.BatchPy ->  gr1		  ovbid	 ptx
// 				...
// 				ovbid  1  4          rgrn  -->    chn	->cache.BatchPy ->  grn		  ovbid  ptx
//
//
// (1) - routine cache.FetchBatch()
// (2) - created in cache.MakeChannels().
// (3) - defined below
// (4) - ptx is protected by an associated mutex lock variable - ptxlk. Defined below.

func Propagate(ctx context.Context, limit *grmgr.Limiter, wg *sync.WaitGroup, pUID uuid.UID, ty string, has11 map[string]struct{}) {

	defer limit.EndR()
	defer wg.Done()

	concat := func(s1, s2 string, sn ...string) string {
		var s strings.Builder
		s.WriteString(s1)
		s.WriteString(s2)
		for _, v := range sn {
			s.WriteString(v)
		}
		return s.String()
	}

	var (
		nc          *cache.NodeCache
		err         error
		wgc         sync.WaitGroup
		NoDataFound = query.NoDataFoundErr
	)
	gc := cache.GetCache()

	var b bool
	// TODO: remove test cases
	// ty = "Fm"
	// pUID = uuid.FromString("eJiBYEpbReKaJaJKzAJ+rA==")
	// ty = "P"
	// pUID = uuid.FromString("Jz95dMC7T0aOgoDJaAZLEQ==")

	ty, b = types.GetTyLongNm(ty)
	if b == false {
		panic(fmt.Errorf("cache.GetType() errored. Could not find long type name for short name %s", ty))
	}

	tyAttrs := types.TypeC.TyC[ty]

	for _, v := range tyAttrs {

		v := v
		if _, ok := has11[v.Ty]; !ok {
			continue
		}
		psortk := concat(types.GraphSN(), "|A#G#:", v.C)
		syslog(fmt.Sprintf("Propagate top loop : pUID %s ,   Ty %s ,  psortk %s ", pUID.Base64(), v.Ty, psortk))
		//	fmt.Printf("Propagate top loop : pUID %s ,   Ty %s ,  psortk %s   ", pUID.String(), v.Ty, psortk)
		// lock parent node and load  UID-PRED into cache

		nc, err = gc.FetchForUpdateContext(ctx, pUID, psortk)
		if err != nil {
			if nc != nil {
				nc.Unlock()
				nc.CachePurge()
			}
			if errors.Is(err, NoDataFound) {
				syslog(fmt.Sprintf("No items found for pUID:  %s, sortk: %s ", pUID.Base64(), psortk))
				err = nil
				break
			}
			elog.Add("dp: ", err)
			break
		}
		// in case of lots of children reaches overflow limits - create a channel for each overflow block so
		// processing of child nodes can be conducted in parallel.
		bChs, err := nc.MakeChannels(psortk)
		if err != nil {
			elog.Add("MakeChannels", fmt.Errorf("Puid error: %s %s", pUID.Base64(), err))
			break
		}

		ptx := tx.NewBatchContext(ctx, "DP")
		var ptxlk sync.Mutex

		// create a goroutine to read from each channel created in UnmarshalEdge()
		// there will be one goroutine to handle the uid-pred in the node and one goroutine for each overflow block
		for i, k := range bChs {

			ii, kk := i, k
			wgc.Add(1)

			// block goroutine
			go func(oid int, rch <-chan cache.BatchPy, v blk.TyAttrD) {

				defer wgc.Done()
				// make a grmgr label

				var blimiter *grmgr.Limiter
				if oid == 0 {
					blimiter = nil // embedded
				} else {
					blimiter = grmgr.New(string(pUID.Base64())[:8], 2) // TODO: create parameter for degree of concurrentcy
				}
				var wgd sync.WaitGroup

				// read cache.BatchPy from channel
				for py := range rch {

					py := py
					if blimiter != nil {

						blimiter.Ask()
						<-blimiter.RespCh()
					}
					wgd.Add(1)
					// batch goroutine - enables concurrent processing of batches (embedded and overflow)
					go func(py cache.BatchPy, v blk.TyAttrD) {

						defer wgd.Done()
						var (
							nd [][]byte
							//	xf []int64
							//xf     []int64
							mutdml mut.StdMut
						)
						if blimiter != nil {
							defer blimiter.EndR()
						}
						// transaction for each channel. TODO: log failure
						// ptx := tx.New(concat(pUID.Base64(), "-", strconv.Itoa(oid)))
						// fmt.Println("****************** ptx label: ", concat(pUID.Base64(), "-", strconv.Itoa(oid)))
						pUID := py.Puid // either parent node or overflow UID
						switch py.Bid {
						case 0: //embedded
							nd, _, _ = py.DI.GetNd() // TODO: what about xf - for child node edges that have been soft deleted
						//	nd, xf, = py.DI.GetNd()
						default: // overflow batch
							nd, _ = py.DI.GetOfNd()
						}
						mutdml = mut.Insert
						// process each cuid within embedded or each overflow batch array
						// only handle 1:1 attribute types which means only one entity defined in propagated data
						for _, cuid := range nd {

							// if xf[i] == 1 {
							// 	continue
							// }
							//fmt.Printf("cuid: %s\n", uuid.UID(cuid).String())
							// fetch 1:1 node propagated data and assign to pnode
							// load cache with node's uid-pred and propagated data
							ncc, err := gc.FetchNodeContext(ctx, cuid, types.GraphSN()+"|A#G#")
							if err != nil {
								if errors.Is(err, NoDataFound) {
									syslog(fmt.Sprintf("No items found for cUID:  %s, sortk: %s ", cuid, types.GraphSN()+"|A#G#"))
									fmt.Printf("YYY No items found for cUID:  %s, sortk: %s\n", cuid, types.GraphSN()+"|A#G#")
									err = nil
									continue
								} else {
									panic(fmt.Errorf("FetchNode error: %s", err.Error()))
								}
							}
							// prevent any async process from purging or modifying the cache

							// fetch propagated scalar data from parant's uid-pred child node.
							for _, t := range types.TypeC.TyC[v.Ty] {

								// ignore scalar attributes and 1:M UID predicates
								if t.Card != "1:1" {
									continue
								}
								//sk := concat(types.GraphSN(), "|A#G#:", t.C) // Actor, Character, Film UUIDs [uid-pred]
								sk := "A#G#:" + t.C
								var (
									psk string
								)
								//fmt.Printf(" cuid sk %s    %s\n", uuid.UID(cuid).String(), sk)
								switch py.Batch {
								case 0: // batch 0 -  embedded cuids
									psk = concat(psortk, "#", sk[2:])
								default: // overflow
									psk = concat(psortk, "%", strconv.Itoa(py.Batch), "#", sk[2:])
								}
								//fmt.Printf(" cuid sk %s    %s    %s\n", cuid, sk, psk)
								//
								// this commened out section populates the Nd values of the child nodes. Not sure what value this is.
								//
								sk_ := concat(types.GraphSN(), "|", sk)
								for k, m := range ncc.GetMap() {
									//search for uid-pred entry in cache
									if k == sk_ {
										// because of 1:1 there will only be one child uid for uid node.
										ptxlk.Lock()
										n, xf, _ := m.GetNd()
										xf_ := make([]int64, 1)
										xf_[0] = xf[0] //block.ChildUID
										v := make([][]byte, 1)
										v[0] = n[0]
										//fmt.Printf("PromoteUID: %s %s %T [%s] %v \n", psortk+"#"+sk[2:], k, m, n[1], xf[1])
										merge := ptx.MergeMutation(tbl.EOP, pUID, psk, mutdml)
										merge.AddMember("Nd", v).AddMember("XF", xf_) //.AddMember("Id", nl)
										ptxlk.Unlock()
									}
								}

								// for each attribute of the 1:1 predicate P.A, P.C, P.F
								for _, t_ := range types.TypeC.TyC[t.Ty] {

									// find propagated scalar in cache
									compare := concat(types.GraphSN(), "|", sk, "#:", t_.C)

									for k, m := range ncc.GetMap() {

										//fmt.Println("k, compare ", k, compare)
										if k == compare {

											sk := concat(sk, "#:", t_.C)
											//fmt.Println("=============== k, compare , sk ", k, compare, sk)
											switch py.Batch {
											case 0: // batchId 0 is embedded cuids
												psk = concat(psortk, "#", sk[2:])
											default: // overflow
												psk = concat(psortk, "%", strconv.Itoa(py.Batch), "#", sk[2:])
											}

											// MergeMutation will combine all operations on PKey, SortK into a single PUT
											// rather than a PUT followed by lots of UPDATES.
											// As all operations are PUTs we can configure TX BATCH operation.
											// As all operations are PUTs load is idempotent, meaning repeated operations on same Pkey, Sortk is safe.

											ptxlk.Lock()
											switch t_.DT {

											case "S":

												s, bl := m.GetULS()
												v := make([]string, 1, 1)
												// for 1:1 there will only be one entry in []string
												v[0] = s[0]
												nv := make([]bool, 1, 1)
												nv[0] = bl[0]
												ptx.MergeMutation(tbl.EOP, pUID, psk, mutdml).AddMember("LS", v).AddMember("XBl", nv)

											case "I":

												s, bl := m.GetULI()
												v := make([]int64, 1, 1)
												v[0] = s[0]
												nv := make([]bool, 1, 1)
												nv[0] = bl[0]
												ptx.MergeMutation(tbl.EOP, pUID, psk, mutdml).AddMember("LI", v).AddMember("XBl", nv)

											case "F":
												s, bl := m.GetULF()
												v := make([]float64, 1, 1)
												v[0] = s[0]
												nv := make([]bool, 1, 1)
												nv[0] = bl[0]
												ptx.MergeMutation(tbl.EOP, pUID, psk, mutdml).AddMember("LF", v).AddMember("XBl", nv)

											case "B":
												s, bl := m.GetULB()
												v := make([][]byte, 1, 1)
												v[0] = s[0]
												nv := make([]bool, 1, 1)
												nv[0] = bl[0]
												ptx.MergeMutation(tbl.EOP, pUID, psk, mutdml).AddMember("LB", v).AddMember("XBl", nv)

											case "Bl":
												s, bl := m.GetULBl()
												v := make([]bool, 1, 1)
												v[0] = s[0]
												nv := make([]bool, 1, 1)
												nv[0] = bl[0]
												ptx.MergeMutation(tbl.EOP, pUID, psk, mutdml).AddMember("LBl", v).AddMember("XBl", nv)
											}
											ptxlk.Unlock()
										}
									}
								}
							}

							mutdml = mut.Update // update insert doesn't matter it will all be merged into the original insert
							//ncc.RUnlock()
							ncc.CachePurge()
						}
					}(py, v)

				}
				wgd.Wait()

				if blimiter != nil {
					blimiter.Unregister()
				}

			}(ii, kk, v)
		}
		wgc.Wait()

		// not all Performances edges are connected - e.g. Person Director has no Actor(Performance) edges
		// if !ptx.HasMutations() {
		// 	panic(fmt.Errorf("Propagate: for %s %s", pUID, ty))
		// }
		err = ptx.Execute()
		if err != nil {
			panic(err)
			if !strings.HasPrefix(err.Error(), "No mutations in transaction") {
				elog.Add(logid, err)
			}
		}

	}

	//
	// post: update IX to Y - TODO incorporate in above ptx not separate as below.
	//
	ptx := tx.NewSingle("IXFlag")
	if err != nil {
		// update IX to E (errored) TODO: could create a Remove API
		ptx.NewUpdate(tbl.Block).AddMember("PKey", pUID, mut.IsKey).AddMember("SortK", "A#A#T", mut.IsKey).AddMember("IX", "E")
		fmt.Printf("x")

	} else {
		//TODO: implement Remove. In Spanner set attribute to NULL, in DYnamodb  delete attribute from item ie. update expression: REMOVE "<attr>"
		//syslog(fmt.Sprintf("Propagate: remove IX attribute for %s %s", pUID, ty))
		ptx.NewUpdate(tbl.Block).AddMember("PKey", pUID, mut.IsKey).AddMember("SortK", "A#A#T", mut.IsKey).AddMember("IX", nil, mut.Remove)
		fmt.Printf(".")
	}

	ptx.Execute()
	if err != nil {
		if !strings.HasPrefix(err.Error(), "No mutations in transaction") {
			elog.Add(logid, err)
		}
	}
	//
	if nc != nil {
		nc.Unlock()
		nc.CachePurge()
	}
}

func FetchNodeCh(ctx context.Context, ty string, stateId uuid.UID, restart bool, finishDPch chan<- struct{}, DPbatchCompleteCh <-chan struct{}) <-chan uuid.UID {

	dpCh := make(chan uuid.UID)

	go ScanForDPitems(ty, dpCh, finishDPch, DPbatchCompleteCh)

	return dpCh

}

type Unprocessed struct {
	PKey uuid.UID
}
type PKey []byte

// ScanForDPitems fetches candiate items to which DP will be applied. Items fetched in batches and sent on channel to be picked up by main process DP loop.
func ScanForDPitems(ty string, dpCh chan<- uuid.UID, finishDPch chan<- struct{}, DPbatchCompleteCh <-chan struct{}) {

	// Option 1: uses an index query. For restartability purposes we mark each processed item, in this case by deleting its IX attribute, which has the
	//           affect of removing the item from the index. The index therefore will contain only unprocessed items.
	//
	// 1. Fetch a batch (200) of items from TyIX (based on Ty & IX)
	// 2. for each item
	//   2.1   DP it.
	//   2.2   Post DP: for restartability and identify those items in the tbase that have already been processed,
	//         remove IX attribute from assoicated item (using PKey, Sortk). This has the affect of removing entry from TyIX index.
	// 3. Go To 1.
	//
	// the cose of restartability can be measured in step 2.2 in terms of WCUs for each processed item.
	//
	// Option 2: replace index query with a paginated scan.
	//
	// 1. Paginated scan of index. Use page size of 200 items (say)
	// 2. DP it.
	// 3. Go To 1.
	//
	// The great advantage of the paginated scan is it eliminates the expensive writes of step 2.2 in option 1. Its costs is in more RCUs - so its a matter of determining
	// whether more RCUs is worth it. This will depend on the number of items fetched to items scanned ratio. If it is less than 25% the paginated scan is more efficient.
	// However the granularity of restartability is now much larger, at the size of a page of items rather than the individual items in option 1.
	// This will force some items to be reprocessed in the case of restarting after a failed run. Is this an issue?
	// As DP uses a PUT (merge mutation will force all mutations into original put mutation) it will not matter, as put will overwrite existing item(s) - equiv to SQL's merge.
	// The cost is now in the processing of items that have already been processed after a failed run.
	// For the case of no failure the cost is zero, whereas in option 1, the cost remains the same for a failured run as a non-failed run (step 2.2)
	// So paginated scan is much more efficient for zero failure runs, which will be the case for the majority of executions.

	var (
		logid = "ScanForDPitems"
		stx   *tx.QHandle
		err   error
		b     int
	)

	defer close(dpCh)

	for {

		slog.Log(logid, fmt.Sprintf("ScanForDPitems for type %q started. Batch %d", ty, b))

		rec := []Unprocessed{}

		stx = tx.NewQuery2("dpScan", tbl.Block, "TyIX")
		if err != nil {
			close(dpCh)
			elog.Add(logid, err)
			return
		}
		stx.Select(&rec).Key("Ty", types.GraphSN()+"|"+ty).Key("IX", "X").Limit(param.DPbatch).Consistent(false) // GSI (not LSI) cannot have consistent reads. TODO: need something to detect GSI.

		err = stx.Execute()
		if err != nil {
			elog.Add(logid, err)
			break
		}

		for _, v := range rec {
			dpCh <- v.PKey
		}

		// exit loop when #fetched items less than limit
		if len(rec) < param.DPbatch {
			slog.LogAlert(logid, fmt.Sprintf("#items %q exiting fetch loop", ty))
			break
		}
		b++

		slog.LogAlert(logid, "Waiting on last DP to finish..")
		<-DPbatchCompleteCh
		slog.LogAlert(logid, "Waiting on last DP to finish..Done")

	}
	slog.LogAlert(logid, fmt.Sprintf("About to close dpCh "))

}
