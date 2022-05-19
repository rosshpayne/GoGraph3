package dp

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	blk "github.com/GoGraph/block"
	"github.com/GoGraph/cache"
	"github.com/GoGraph/rdf/dp/internal/db"
	elog "github.com/GoGraph/rdf/errlog"
	"github.com/GoGraph/rdf/grmgr"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/types"
	"github.com/GoGraph/util"
)

const (
	dpPart = "G"
	logid  = "processDP:"
)

var (
	ctx    context.Context
	ctxEnd sync.WaitGroup
	cancel context.CancelFunc
)

func syslog(s string) {
	slog.Log("dp: ", s)
}

func FetchNodeCh(ty string) <-chan util.UID {

	dpCh := make(chan util.UID, 4)

	go db.ScanForDPitems(ty, dpCh)

	return dpCh

}

func Process(limit *grmgr.Limiter, wg *sync.WaitGroup, pUID util.UID, ty string, has11 map[string]struct{}) {

	defer limit.EndR()
	defer wg.Done()

	var (
		nc  *cache.NodeCache
		err error
		wgc sync.WaitGroup
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
		syslog(fmt.Sprintln("In top loop : ", v.Ty))

		psortk := "A#" + dpPart + "#:" + v.C
		// lock parent node and load performance UID-PRED into cache
		nc, err = gc.FetchForUpdate(pUID, psortk)
		if err != nil {
			panic(fmt.Errorf("FetchForUpdate error: %s", err.Error()))
		}
		// unmarshal uid-pred returning  slice of channels to read overflow batches containing an array of cUIDs.
		bChs := nc.UnmarshalEdge(psortk)

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
					blimiter = nil
				} else {
					blimiter = grmgr.New(pUID.String()[:8], 2) // TODO: create parameter for degree of concurrentcy
				}
				var wgd sync.WaitGroup

				// read batch item from channel
				for py := range rch {

					py := py
					if blimiter != nil {

						blimiter.Ask()
						<-blimiter.RespCh()
					}
					wgd.Add(1)
					// batch goroutine - enables concurrent processing of batches
					go func(py cache.BatchPy, v blk.TyAttrD) {

						defer wgd.Done()
						var (
							nd [][]byte
							//xf     []int64
							mutdml mut.StdDML
						)
						if blimiter != nil {
							defer blimiter.EndR()
						}
						// transaction for each channel. TODO: log failure
						ptx := tx.New(pUID.String() + "-" + strconv.Itoa(oid))

						pUID := py.Puid // either parent node or overflow UID
						switch py.Bid {
						case 0: //embedded
							nd, _, _ = py.DI.GetNd()
						default: // overflow batch
							nd, _ = py.DI.GetOfNd()
						}
						mutdml = mut.Insert
						// process each cuid within embedded or each overflow batch array
						// only handle 1:1 attribute types which means only one entity defined in propagated data
						for _, cuid := range nd {

							// fetch 1:1 node propagated data and assign to pnode
							// load cache with node's uid-pred and propagated data
							ncc, err := gc.FetchNode(cuid, "A#G#")
							if err != nil {
								panic(fmt.Errorf("FetchNode error: %s", err.Error()))
							}
							// fetch propagated scalar data from parant's uid-pred child node.

							for _, t := range types.TypeC.TyC[v.Ty] {

								// ignore scalar attributes and 1:M UID predicates
								if t.Card != "1:1" {
									continue
								}
								sk := "A#G#:" + t.C // Actor, Character, Film UUIDs [uid-pred]
								var (
									psk string
								)

								switch py.Batch {
								case 0: // batchId 0 is embedded cuids
									psk = psortk + "#" + sk[2:]
								default: // overflow
									psk = psortk + "%" + strconv.Itoa(py.Batch) + "#" + sk[2:]
								}
								for k, m := range ncc.GetMap() {
									//search for uid-pred entry in cache
									if k == sk {
										// because of 1:1 there will only be one child uid for uid node.
										n, xf, _ := m.GetNd()
										xf_ := make([]int64, 1)
										xf_[0] = xf[0] //block.ChildUID
										v := make([][]byte, 1)
										v[0] = n[0]
										//fmt.Printf("PromoteUID: %s %s %T [%s] %v \n", psortk+"#"+sk[2:], k, m, n[1], xf[1])
										merge := mut.NewMutation(tbl.EOP, pUID, psk, mutdml)
										merge.AddMember("Nd", v).AddMember("XF", xf_) //.AddMember("Id", nl)
										ptx.Add(merge)
									}
								}
								// for each attribute of the 1:1 predicate P.A, P.C, P.F
								for _, t_ := range types.TypeC.TyC[t.Ty] {

									// find propagated scalar in cache
									compare := sk + "#:" + t_.C

									for k, m := range ncc.GetMap() {

										if k == compare {

											sk := sk + "#:" + t_.C
											switch py.Batch {
											case 0: // batchId 0 is embedded cuids
												psk = psortk + "#" + sk[2:]
											default: // overflow
												psk = psortk + "%" + strconv.Itoa(py.Batch) + "#" + sk[2:]
											}

											switch t_.DT {

											case "S":
												s, bl := m.GetULS()
												v := make([]string, 1, 1)
												// for 1:1 there will only be one entry in []string
												v[0] = s[0]
												nv := make([]bool, 1, 1)
												nv[0] = bl[0]
												merge := mut.NewMutation(tbl.EOP, pUID, psk, mutdml)
												merge.AddMember("LS", v).AddMember("XBl", nv)
												ptx.Add(merge)

											case "I":
												s, bl := m.GetULI()
												v := make([]int64, 1, 1)
												v[0] = s[0]
												nv := make([]bool, 1, 1)
												nv[0] = bl[0]
												merge := mut.NewMutation(tbl.EOP, pUID, psk, mutdml)
												merge.AddMember("LI", v).AddMember("XBl", nv)
												ptx.Add(merge)

											case "F":
												s, bl := m.GetULF()
												v := make([]float64, 1, 1)
												v[0] = s[0]
												nv := make([]bool, 1, 1)
												nv[0] = bl[0]
												merge := mut.NewMutation(tbl.EOP, pUID, psk, mutdml)
												merge.AddMember("LF", v).AddMember("XBl", nv)
												ptx.Add(merge)

											case "B":
												s, bl := m.GetULB()
												v := make([][]byte, 1, 1)
												v[0] = s[0]
												nv := make([]bool, 1, 1)
												nv[0] = bl[0]
												merge := mut.NewMutation(tbl.EOP, pUID, psk, mutdml)
												merge.AddMember("LB", v).AddMember("XBl", nv)
												ptx.Add(merge)

											case "Bl":
												s, bl := m.GetULBl()
												v := make([]bool, 1, 1)
												v[0] = s[0]
												nv := make([]bool, 1, 1)
												nv[0] = bl[0]
												merge := mut.NewMutation(tbl.EOP, pUID, psk, mutdml)
												merge.AddMember("LBl", v).AddMember("XBl", nv)
												ptx.Add(merge)
											}
										}
									}
								}
							}
							// if err := ptx.MakeBatch(); err != nil {
							// 	elog.Add(logid, err)
							// }
							mutdml = mut.Update
							ncc.Unlock()
						}

						// commit each batch separately - TODO: log sucess against batch id for pUID
						err = ptx.Execute()
						if err != nil {
							if !strings.HasPrefix(err.Error(), "No mutations in transaction") {
								elog.Add(logid, err)
							}
						}
					}(py, v)

				}
				wgd.Wait()

				if blimiter != nil {
					blimiter.Finish()
				}

			}(ii, kk, v)
		}
		wgc.Wait()
	}
	//
	// post: update IX to Y
	//
	//	db.UpdateIX(pUID)
	ptx := tx.New("IXFlag")
	merge := mut.NewMutation(tbl.Block, pUID, "", mut.Update)
	merge.AddMember("IX", "Y")
	ptx.Add(merge)
	//ptx.MakeBatch() // will be preformed in Execute anyway

	ptx.Execute()

	if err != nil {
		if !strings.HasPrefix(err.Error(), "No mutations in transaction") {
			elog.Add(logid, err)
		}
	}

	gc.ClearNodeCache(pUID)

}
