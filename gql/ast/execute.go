package ast

import (
	"fmt"
	"strings"
	"sync"

	blk "github.com/GoGraph/block"
	"github.com/GoGraph/cache"
	"github.com/GoGraph/ds"
	"github.com/GoGraph/grmgr"
	mon "github.com/GoGraph/monitor"
	"github.com/GoGraph/types"
	"github.com/GoGraph/uuid"
)

type rootResult struct {
	uid   uuid.UID    //returned from root query
	tyS   string      //returned from root query
	sortk string      // returned from root query
	path  string      // #root (R) #root#Siblings#Friends - not currently used
	nv    ds.ClientNV // the data from the uid item. Populated during execution phase with results of filter operation.
}

// index into node data UL structures (see UnmarshalNodeCache). i,j points to node scalar data for a child node UID defined in the uid-pred (Nd)
// UL data is represented by slice of slice data.
// The outer slice (i) represents the data spread over root(node uid-pred) and overflow batches.
// The inner slice (j) represents the data within a uid-pred (embedded data) or an overflow batch
//
type index struct {
	i, j int
}

func (r *RootStmt) Execute() {
	//
	// execute root func - get back slice of unfiltered results
	//
	result := r.RootFunc.F(r.RootFunc.Farg, r.RootFunc.Value)
	if len(result) == 0 {
		return
	}

	var wgRoot sync.WaitGroup

	stat := mon.Stat{Id: mon.Candidate, Value: len(result)}
	mon.StatCh <- stat
	stat2 := mon.Stat{Id: mon.TouchNode, Lvl: 0}

	limiterRootSearch := grmgr.New("rootSearch", 2)

	for _, v := range result {

		limiterRootSearch.Ask()
		<-limiterRootSearch.RespCh()

		mon.StatCh <- stat2
		wgRoot.Add(1)
		result := &rootResult{uid: v.PKey, tyS: v.Ty[strings.Index(v.Ty, "|")+1:], sortk: v.SortK, path: "root"}

		go r.filterRootResult(&wgRoot, limiterRootSearch, result)
		//r.filterRootResult(&wgRoot, result)

	}
	wgRoot.Wait()
	limiterRootSearch.Unregister()

}

func (r *RootStmt) filterRootResult(wg *sync.WaitGroup, lmtr *grmgr.Limiter, result *rootResult) {
	var (
		err error
		nc  *cache.NodeCache
	)
	defer lmtr.EndR()
	defer wg.Done()
	//
	// save: filter-visit-node uid
	//
	// generate NV from GQL stmt - will also hold data from query response once UmarshalNodeCache is run.
	// query->cache->unmarshal(nv)
	//
	//	fmt.Println("result.tyS: ", result.tyS)
	nvc := r.genNV(result.tyS)
	//fmt.Println("==== Root genNV_ =====")
	for _, n := range nvc {
		fmt.Println("Root genNV__: ", n.Name, n.Ignore)
	}
	//
	// generate sortk - determines extent of node data to be loaded into cache. Tries to keep it as norrow (specific) as possible.
	//
	sortkS := cache.GenSortK(nvc, result.tyS)
	// for _, s := range sortkS {
	// 	fmt.Println("Ysortk: ", s)
	// }
	//
	// fetch data - with optimised fetch - perform queries sequentially becuase of mutex lock on node map
	//
	gc := cache.GetCache()
	for _, sortk := range sortkS {
		stat := mon.Stat{Id: mon.NodeFetch}
		mon.StatCh <- stat
		//fmt.Printf("FetchNodeNonCache for uid: %s  sortk: %s\n", result.uid, sortk)
		nc, _ = gc.FetchNodeNonCache(result.uid, sortk)
		if nc == nil {
			syslog(fmt.Sprintf("*** no data found for: root uid %s sortk %s does not exist", result.uid.String(), sortk))
		}
	}
	if nc == nil {
		syslog(fmt.Sprintf("*** no data found for: root uid %s ", result.uid.String()))
		return
	}
	// for k, _ := range nc.GetMap() {
	// 	fmt.Println("GetMap sortk: ", k)
	// }
	fmt.Println("result.tyS: ", result.tyS)
	//
	// assign cached data to NV
	//
	// assign the cached data to the Value field in the nvc for each sortkS
	err = nc.UnmarshalNodeCache(nvc, result.tyS)
	if err != nil {
		syslog("no entry cound in cache - root uid does not exist")
		return
	}
	//fmt.Println("==== Unmarshalled genNV_ =====")
	for _, n := range nvc {
		fmt.Printf(" ^^ root nvc: %v %T\n", n.Name, n.Value)
		switch x := n.Value.(type) {
		case [][][]uint8:
			fmt.Println("[][][]uint8 value: len = ", len(x))
			// for _, v := range x {
			// 	for k, m := range v {
			// 		fmt.Printf("uid: %d  %s\n", k, uuid.UID(m).String())
			// 	}
			// }
		case [][]string:
			fmt.Println("[][]string: len = ", len(x))
			// for _, v := range x {
			// 	for k, m := range v {
			// 		fmt.Printf("uid: %d  %s\n", k, m)
			// 	}
			// }
		}
	}
	//
	// root filter
	//
	if r.Filter != nil && !r.Filter.RootApply(nvc, result.tyS) {
		nc.ClearNodeCache()
		return
	}
	//
	// save result node data (represented by uid - nvm) to root stmt
	//
	nvm := r.assignData(result.uid.ToUIDString(), nvc, index{0, 0})
	//
	//
	stat := mon.Stat{Id: mon.PassRootFilter}
	mon.StatCh <- stat
	//
	var wgNode sync.WaitGroup

	for _, p := range r.Select {

		fmt.Printf("// p is: %s\n", p.Edge.Name())

		switch x := p.Edge.(type) {

		case *ScalarPred:
			// do nothing as part of propagated data in the cache
			fmt.Println("Do nothing - root scalar ")

		case *UidPred: // child of child, R.N.N - this data is cached in parent node
			var (
				aty blk.TyAttrD
				ok  bool
			)
			x.l.Lock()
			x.lvl = 1
			x.l.Unlock()

			fmt.Printf("%s ** root edge - %s\n", strings.Repeat("\t", 0), x.Name())

			if aty, ok = types.TypeC.TyAttrC[result.tyS+":"+x.Name()]; !ok {
				panic(fmt.Errorf("%s not in %s", result.tyS, x.Name()))
				continue // ignore this attribute as it is in current type
			}
			// filter by setting STATE value for each edge in NVM. NVM has been saved to root stmt
			// and is used by MarshalJSON to output edges from the root node.
			if x.Filter != nil {
				x.d.Lock()
				x.Filter.Apply(nvm, aty.Ty, x.Name()) // AAA - on first uid-pred - on each edge mark as EdgeFiltered true|false
				x.d.Unlock()
			}

			for _, p := range x.Select {

				//fmt.Printf("%s ** attribute of root edge. p is: %s\n", strings.Repeat("\t", 1), p.Edge.Name())

				switch y := p.Edge.(type) {

				case *ScalarPred, *Variable:
					// do nothing as UnmarshalNodeCode has already assigned scalar results in n
					//fmt.Println(" ** Do nothing - scalar value, data cached")

				case *UidPred:
					// data will need to be sourced from db
					// execute query on each x.Name() item and use the propagated uid-pred data to resolve this uid-pred
					var (
						idx index
						nds [][][]byte
					)
					//
					// to get the scalar data in y (current AST element) we need perform a search on y's uid-pred.
					// y scalar will be part of the x queries propagated data
					//
					//fmt.Printf("%s ** root->edge->edge %s  - not cached, unless its double propagated, must be fetched if not.\n", strings.Repeat("\t", 1), p.Edge.Name())
					data, ok := nvm[x.Name()+":"]
					if !ok {
						panic(fmt.Errorf("%q not in NV map", x.Name()+":"))
					}
					if nds, ok = data.Value.([][][]byte); !ok {
						panic(fmt.Errorf("filterRootResult: data.Value is of wrong type")) // TODO: replace panic with error msg???
					}
					sortk := "A#G#:" + aty.C
					// for each Nd uid (on uid edge)
					for i, u := range nds {
						// for each child in outer uid-pred (x.Name)
						for j, uid := range u {

							// check the result of the filter condition on x determined at AAA ie. filter on child nodes whose age > 62
							if data.State[i][j] == blk.UIDdetached || data.State[i][j] == blk.EdgeFiltered { // soft delete set
								continue
							}
							// i,j - defined key for looking up child node UID in cache block.

							wgNode.Add(1)
							idx = index{i, j}
							//
							// y is current AST element (uid-pred)
							// arg      description
							// 2        uid of parent uid-pred (x) to current uid-pred (y) e.g. root-friends-siblings
							//          then current is siblings and parent is friends. Query Friends UID to get siblings data.
							// 3        type of parent uid-pred (person)
							// 4        level in graph
							// 5        name of current AST element e.g. "siblings", from example in 2
							// 6        index into nds (uid array from parent )
							//                      2.          3.     4.  5.        6
							y.execNode(&wgNode, uuid.UID(uid), aty.Ty, 2, y.Name(), idx, result.uid, sortk)
							//go y.execNode(&wgNode, uuid.UID(uid), aty.Ty, 2, y.Name(), idx, result.uid, sortk)
						}
					}
				}
			}
		}
	}

	wgNode.Wait()

}

// execNode takes parent node (depth-1)and performs UmarshalCacheNode on its uid-preds.
// propagated data is the data we are interested in- as it will contain the scalar data for the current AST element
// ty   type of parent node
// us is the current uid-pred from filterRootResult
// uidp is uid current node - not used anymore.
func (u *UidPred) execNode(wg *sync.WaitGroup, uid_ uuid.UID, ty string, lvl int, uidp string, idx index, ruid uuid.UID, sortk_ string) {

	var (
		err error
		nc  *cache.NodeCache
		nvm ds.NVmap // where map key is NV.Name
		nvc ds.ClientNV
		ok  bool
		uty blk.TyAttrD // uid-pred and parent-to-uid-pred type
	)
	uid := uid_.ToUIDString()

	//fmt.Printf("\n%s ** execNode() fetch node u:%s uid: %s,  ty: %s Depth: %d  current uidpred: %s ruid: %s\n", strings.Repeat("\t", lvl), u.Name(), uid, ty, lvl, uidp, ruid)
	uty = types.TypeC.TyAttrC[ty+":"+uidp]
	//fmt.Printf("%s uty: %#v\n", strings.Repeat("\t", lvl), uty)
	// note: source of data (nvm) for u is sourced from u's parent propagated data ie. u's data is in the list structures of u-parent (propagated data)
	//
	defer wg.Done()
	//
	u.l.Lock()
	u.lvl = lvl // depth in graph as determined from GQL stmt
	u.l.Unlock()

	if nvm, nvc, ok = u.Parent.getData(uid); !ok {
		//
		// first instance of a uid-pred in node to be executed. All other uid-preds in this node can ignore fetching data from db as its data was included in the first uid-pred query.
		//
		// generate NV from GQL stmt - for each uid-pred even though this is not strictly necessary. If nvm, nvc was saved to u then it would only need to be generated once for u.
		// query->cache->unmarshal(nv). Generate from parent of u, as it contains u. The uid is sourced from the parent node's relevant uid-pred attribute.
		// we need to perform a query on each uid as it represents the children containing u.
		//
		// as the data is sourced from u-parent so must the NV listing. Only interested in the uid-preds and its scalar types, as this includes the data for u (and its uid-pred siblings)
		//
		nvc = u.Parent.genNV(ty)
		//
		//fmt.Printf("%s ==== uid-pred genNV_ =====  %s\n", strings.Repeat("\t", lvl), u.Name_)
		// for _, n := range nvc {
		// 	fmt.Printf("%s uid-pred u.Parent.genNV(): %s   Ignore: %v\n", strings.Repeat("\t", lvl), n.Name, n.Ignore)
		// }
		// generate sortk - source from node type and NV - merge of two.
		//                  determines extent of node data to be loaded into cache. Tries to keep it as norrow (specific) as possible to minimise RCUs.
		//                  ty is the type of the parent uid-pred (uid passed in)
		//
		sortkS := cache.GenSortK(nvc, ty)
		// for _, s := range sortkS {
		// 	fmt.Printf("%s getSortK sortkS: %s\n", strings.Repeat("\t", lvl), s)
		// }
		//
		switch uty.Card {

		case "1:N":
			// fetch data - with optimised fetch - perform queries sequentially because of mutex lock on node map
			// uid is sourced from u's parent uid-pred.
			// this will populate the cache with the node data (scalar + uid-pred in arrays) for the current AST element (U) for the current root source

			gc := cache.GetCache()
			for _, sortk := range sortkS {
				stat := mon.Stat{Id: mon.NodeFetch}
				mon.StatCh <- stat
				nc, _ = gc.FetchNodeNonCache(uid_, sortk) // BBB
			}
			// for k, _ := range nc.GetMap() {
			// 	fmt.Println("GetMap sortk: ", k)
			// }
			//
			// unmarshal cache contents into nvc
			//
			err = nc.UnmarshalNodeCache(nvc, ty)
			if err != nil {
				panic(err)
			}
			// fmt.Println("==== 1:N Unmarshalled genNV_ =====")
			// for _, n := range nvc {
			// 	fmt.Printf(" genNV__: %s %v\n", n.Name, n.Value)
			// }

		case "1:1":

			var (
				nvc_ ds.ClientNV
			)
			switch x := u.Parent.(type) {

			case *UidPred:
				switch x := x.Parent.(type) {
				case *UidPred:
					_, nvc_, ok = x.getData(ruid.ToUIDString())
					//fmt.Println("UidPred: %#v\n", nvc_)
				case *RootStmt:
					//fmt.Printf("ruid.String() %s\n", ruid.String())
					_, nvc_, ok = x.getData(ruid.ToUIDString())
					// for k, v := range nvc_ {
					// 	v := *v
					// 	fmt.Printf("RootStmt 1: nvc_: %d name: %s value: %s\n", k, v.Name, (v.Value).(string))
					// }
				}

			case *RootStmt:
				_, nvc_, ok = x.getData(ruid.ToUIDString())
				//fmt.Println("RootStmt 2: %#v\n", nvc_)
			}
			//
			// load data from nvm_, nvc_ in parent
			// note: there is a one to one correspondence in valudes between list type
			// in A#G#? and nvc_
			type by [][]byte
			for _, n := range nvc { // needs to be populated
				for _, v := range nvc_ { // sourced from parent NVC
					if strings.HasSuffix(v.Name, n.Name) {

						switch n.Name[len(n.Name)-1] {
						case ':': // uid-pred
							// search for uid
							// idx entry
							//fmt.Println("v.Name = ", v.Name, n.Name)

							uids := v.Value.([][][]byte)
							uu := make([][]byte, 1, 1)
							uuu := make([][][]byte, 1, 1)
							//
							uu[0] = uids[idx.i][idx.j] // one to one between uids and A#G#? values
							uuu[0] = uu
							n.Value = uuu
							n.State = [][]int64{{v.State[idx.i][idx.j]}}

						default: // scalar
							switch x := v.Value.(type) {
							case [][]string:
								val := make([]string, 1, 1)
								val2 := make([][]string, 1, 1)
								val[0] = x[idx.i][idx.j]
								val2[0] = val
								//
								s := make([]int64, 1, 1)
								s2 := make([][]int64, 1, 1)
								s[0] = v.State[idx.i][idx.j]
								s2[0] = s
								n.Value = val2
								n.State = s2
							}
						}
					}
				}
			}
		}
		//
		// now save the nvm to the uid in the parent AST element. Remember the nvm will contain the data (scalar and uid)
		// relevent to the current AST element (u)
		//
		nvm = u.Parent.assignData(uid, nvc, idx)
	}

	// for _, v := range nvc {
	// 	fmt.Printf("execnode nvc: %#v\n\n", v)
	// }

	//
	// for a filter: update nvm edges related to u. Note: filter  is the only component  we make use of u directly. Most other access is via u's parent uid-pred
	// as u.Filter will modify the map elements (which are pointers to NV), any change will be visible to u's parent, where NV has been assigned.
	//

	if u.Filter != nil {
		u.d.Lock()
		u.Filter.Apply(nvm, uty.Ty, u.Name())
		u.d.Unlock()
	}

	for _, p := range u.Select {
		//
		switch x := p.Edge.(type) {

		case *ScalarPred, *Variable: // R.p ignore, already processed

		case *UidPred:
			// NV entry contains child UIDs i.e nv[upred].Value -> [][][]byte
			var (
				nds [][][]byte
				//	aty blk.TyAttrD
				idx index
			)

			// //xxfmt.Println("uty+x.Name()  ", p, u.Name(), u.Name())
			// // get type of the uid-pred
			// if aty, ok = types.TypeC.TyAttrC[uty.Ty+":"+x.Name()]; !ok {
			// 	panic(fmt.Errorf("%s.%s not exists", uty, x.Name()))
			// 	continue // ignore this attribute as it is not in current type
			// }
			// //xxfmt.Println("aty.Ty : ", aty.Ty)
			// results not in nv for this depth in graph. Must query uids stored in nv[i].Value -> [][][]byte
			data, ok := nvm[u.Name()+":"]
			if !ok {
				// for k, v := range nvm {
				// 	fmt.Printf("nvm: %s  %#v\n", k, *v)
				// }
				panic(fmt.Errorf("%q not in NV map", x.Name()+":"))
			}
			if nds, ok = data.Value.([][][]byte); !ok {
				panic(fmt.Errorf(": data.Value is of wrong type"))
			}
			sortk := sortk_ + "#" + uty.C
			for i, k := range nds {
				for j, cUid := range k {

					if data.State[i][j] == blk.UIDdetached || data.State[i][j] == blk.EdgeFiltered {
						continue // soft delete set or failed filter condition
					}

					wg.Add(1)
					idx = index{i, j}
					//fmt.Printf("\n>>>Uid: u.Name(): %s   %s  x.Name(): %s  sortk: %s\n", u.Name(), uuid.UID(cUid).String(), x.Name(), sortk)
					x.execNode(wg, uuid.UID(cUid), uty.Ty, lvl+1, x.Name(), idx, uid_, sortk)
				}
			}
		}
	}
}
