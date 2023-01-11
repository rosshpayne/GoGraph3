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
	elog "github.com/GoGraph/errlog"
	"github.com/GoGraph/grmgr"
	slog "github.com/GoGraph/syslog"
	tbl "github.com/GoGraph/tbl"
	"github.com/GoGraph/tx"
	"github.com/GoGraph/tx/key"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/tx/query"
	txtbl "github.com/GoGraph/tx/tbl"
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
// 				cuid   0  1    |---- rgr0  -->    ch0	->cache.BatchPy ->  gr0       pUID	 ptx ---|
// 				..			   |                                                                    |
// 				cuid   0  1   -|                                                                    |
// 				ovbid  1  4          rgr1  -->    ch1	->cache.BatchPy ->  gr1		  ovbid	 ptx ---|----- ptx.Execute()
//                                   ...                                    ...                     |
// 				...                                                         gr1n                    |
//|                                                                                                 |
// 				ovbid  1  4          rgrn  -->    chn	->cache.BatchPy ->  grn		  ovbid  ptx ---|
//                                                                          ...
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
	mergeMutation := func(h *tx.Handle, tbl txtbl.Name, pk uuid.UID, sk string, opr mut.StdMut) *tx.Handle {
		keys := []key.Key{key.Key{"PKey", pk}, key.Key{"SortK", sk}}
		return h.MergeMutation(tbl, opr, keys)
	}
	var (
		nc          *cache.NodeCache
		err         error
		wgc         sync.WaitGroup
		NoDataFound = query.NoDataFoundErr
		found       bool
	)
	gc := cache.GetCache()

	var b bool

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
		found = true
		psortk := concat(types.GraphSN(), "|A#G#:", v.C)
		slog.LogAlert("Propagate", fmt.Sprintf("Propagate top loop : pUID %s ,   Ty %s ,  psortk %s ", pUID.Base64(), v.Ty, psortk))

		nc, err = gc.FetchForUpdateContext(ctx, pUID, psortk)
		if err != nil {
			if errors.Is(err, NoDataFound) {
				slog.LogAlert("Propagate", fmt.Sprintf("No items found for pUID:  %s, sortk: %s ", pUID.Base64(), psortk))
				err = nil
				break
			}
			elog.Add(logid, fmt.Errorf("dp FetchForUpdate() error for pUID %q sortk: %q: %w", pUID.Base64(), psortk, err))
			slog.LogAlert("Propagate", fmt.Sprintf("Error in FetchForUpdate... : pUID %s ,   Ty %s ,  psortk %s %s ", pUID.Base64(), v.Ty, psortk, err.Error()))
			break
		}
		defer nc.Unlock()
		// in case of lots of children reaches overflow limit (param: EmbeddedChildNodes) - create a channel for each overflow block
		bChs, err := nc.MakeChannels(psortk)
		if err != nil {
			elog.Add(logid, err)
			break
		}
		ptx := tx.NewBatchContext(ctx, "DP")
		var ptxlk sync.Mutex

		//wgc.Add(len(bChs))
		// create a goroutine to read from each channel created in UnmarshalEdge()
		// one bChs for uid-pred and each overflow block. OvBatch are sent on bChs.
		// there will be one goroutine to handle the uid-pred in the node and one goroutine for each overflow block

		for i, k := range bChs {

			//	slog.LogAlert("Propagate", fmt.Sprintf("range bChs :  %s sortk: %s ", pUID.Base64(), psortk))
			ii, kk := i, k
			wgc.Add(1)

			// a goroutine for embedded child uids and one for each overflow block
			go func(oid int, rch <-chan cache.BatchPy, v blk.TyAttrD) {

				defer wgc.Done() // annonymous func gives access to surrounding vars. Concurrent safe operation
				// make a grmgr label

				var blimiter *grmgr.Limiter
				if oid == 0 {
					blimiter = nil // embedded cuids
				} else {
					// handle  overflow batches in parallel
					blimiter = grmgr.New(string(pUID.Base64())[:8], 2) // TODO: create parameter for degree of concurrentcy. # of overflow batches read concurrently
				}
				var wgd sync.WaitGroup

				// read cache.BatchPy from channel 0  - uidpred , channel 1..n, ovfl block batch 1..m
				for py := range rch { // channel

					py := py
					if blimiter != nil {

						blimiter.Ask()
						<-blimiter.RespCh()
					}
					wgd.Add(1)

					// goroutine for DP - upto 2 concurrently on for each ovflblock batch (see blimiter above) TODO: make as parameter
					go func(py cache.BatchPy, v blk.TyAttrD) {

						defer wgd.Done()

						if blimiter != nil {
							defer blimiter.EndR()
						}

						var (
							nd     [][]byte
							mutdml mut.StdMut
						)

						// transaction for each channel. TODO: log failure
						pUID := py.Puid // either parent node or overflow block UID

						switch py.Bid {
						case 0: //embedded
							nd, _, _ = py.DI.GetNd() // TODO: what about xf - for child node edges that have been soft deleted
							slog.Log("dp", fmt.Sprintf("About to propagate to embedded pUID %s   Ty %q ,  psortk %s ", pUID.Base64(), v.Ty, psortk))
						default: // overflow batch
							nd, _ = py.DI.GetOfNd()
							slog.Log("dp", fmt.Sprintf("About to propagate to overflow pUID %s   Ty %q ,  psortk %s ", pUID.Base64(), v.Ty, psortk))
						}
						mutdml = mut.Insert

						// process each cuid within embedded or each overflow batch array
						// only handle 1:1 attribute types which means only one entity defined in propagated data
						for _, cuid := range nd {

							// fetch 1:1 node propagated data and assign to pnode
							// load cache with node's propagated data representing the grandchild's (of puid) scalar data
							ncc, err := gc.FetchNodeContext(ctx, cuid, types.GraphSN()+"|A#G#")
							if err != nil {
								if errors.Is(err, NoDataFound) {
									slog.LogAlert(logid, fmt.Sprintf("DP error: No items found for cUID:  %s, sortk: %s ", cuid, types.GraphSN()+"|A#G#"))
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
								// this commened out section populates the Nd values of the child nodes. Not sure of the value of this
								//
								sk_ := concat(types.GraphSN(), "|", sk)
								//
								// define keys as defined in table. This is checked and changed if necessary in MergeMutation
								ptxlk.Lock()
								for k, m := range ncc.GetMap() {
									//search for uid-pred entry in cache - TODO replace loop with map[sortk] access -if k,ok:=ncc.GetMap()[sk_];ok {
									if k == sk_ {
										// because of 1:1 there will only be one child uid for uid node.
										//	ptxlk.Lock()
										n, xf, _ := m.GetNd()
										xf_ := make([]int64, 1)
										xf_[0] = xf[0] //block.ChildUID
										v := make([][]byte, 1)
										v[0] = n[0]
										//fmt.Printf("PromoteUID: %s %s %T [%s] %v \n", psortk+"#"+sk[2:], k, m, n[1], xf[1])
										merge := mergeMutation(ptx, tbl.EOP, pUID, psk, mutdml)
										merge.AddMember("Nd", v).AddMember("XF", xf_) //.AddMember("Id", nl)
										//	ptxlk.Unlock()
									}
								}

								// for each attribute of the 1:1 predicate P.A, P.C, P.F
								for _, t_ := range types.TypeC.TyC[t.Ty] {

									// find propagated scalar in cache
									compare := concat(types.GraphSN(), "|", sk, "#:", t_.C)

									for k, m := range ncc.GetMap() {

										if k != compare {
											continue
										}

										sk := concat(sk, "#:", t_.C)

										switch py.Batch {
										case 0: // batchId 0 is embedded cuids
											psk = concat(psortk, "#", sk[2:])
										default: // overflow
											psk = concat(psortk, "%", strconv.Itoa(py.Batch), "#", sk[2:])
										}

										// MergeMutation will combine all operations on PKey, SortK into a single PUT
										// rather than a PUT followed by lots of UPDATES.
										// As all operations are PUTs we can configure TX BATCH operation.
										// As all operations are PUTs load is idempotent

										switch t_.DT {

										case "S":

											s, bl := m.GetULS()
											mergeMutation(ptx, tbl.EOP, pUID, psk, mutdml).AddMember("LS", s[:1]).AddMember("XBl", bl[:1])

										case "I":

											s, bl := m.GetULI()
											mergeMutation(ptx, tbl.EOP, pUID, psk, mutdml).AddMember("LI", s[:1]).AddMember("XBl", bl[:1])

										case "F":
											s, bl := m.GetULF()
											mergeMutation(ptx, tbl.EOP, pUID, psk, mutdml).AddMember("LF", s[:1]).AddMember("XBl", bl[:1])

										case "B":
											s, bl := m.GetULB()
											mergeMutation(ptx, tbl.EOP, pUID, psk, mutdml).AddMember("LB", s[:1]).AddMember("XBl", bl[:1])

										case "Bl":
											s, bl := m.GetULBl()
											mergeMutation(ptx, tbl.EOP, pUID, psk, mutdml).AddMember("LBl", s[:1]).AddMember("XBl", bl[:1])
										}
									}
								}
								ptxlk.Unlock()
							}

							mutdml = mut.Update // update or insert doesn't matter it will all be merged into the original insert
							//ncc.RUnlock()
							ncc.CachePurge()
						}
					}(py, v)
				} // by  ovfl block batch reader sends -> channel for each batch in ovfl blck
				wgd.Wait()

				if blimiter != nil {
					blimiter.Unregister()
				}

			}(ii, kk, v) // by  ovfl block reader equates to channel
		}
		wgc.Wait()

		// not all Performances edges are connected - e.g. Person Director has no Actor(Performance) edges
		// if !ptx.HasMutations() {
		// 	panic(fmt.Errorf("Propagate: for %s %s", pUID, ty))
		// }
		// err = ptx.Execute()
		// if err != nil {
		// 	panic(err)
		// 	if !strings.HasPrefix(err.Error(), "No mutations in transaction") {
		// 		elog.Add(logid, err)
		// 	}
		// }
	}
	if !found {
		elog.Add(logid, fmt.Errorf("DP -  1:1 attribute not found for type %q in node %q ", ty, pUID))
		return
	}

	// nolonger remove IX attribute for each successful propagation. Good for non-indempotent mutations and recovery size of 0.
	// Use Limit to determine recovery work/size. Indempotent mutation so safe.
	if err != nil {
		elog.Add(logid, err)
		// update IX to E (errored) TODO: could create a Remove API
		// etx := tx.New("IXFlag")
		// etx.NewUpdate(tbl.Block).AddMember("PKey", pUID, mut.IsKey).AddMember("SortK", "A#A#T", mut.IsKey).AddMember("IX", "E")
		// err = etx.Execute()
		// if err != nil {
		// 	elog.Add(logid, err)
		// }
		slog.LogAlert("Propagate", fmt.Sprintf("Propagate Errored : pUID %s  %s", pUID.Base64(), err.Error()))
		panic(err)
	}
	slog.LogAlert("Propagate", fmt.Sprintf("Finished loop : pUID %s ,   Ty %s", pUID.Base64(), ty))

}
