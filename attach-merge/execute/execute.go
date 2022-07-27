package execute

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	atds "github.com/GoGraph/attach-merge/ds"
	//"github.com/GoGraph/attach-merge/execute/event"
	blk "github.com/GoGraph/block"
	"github.com/GoGraph/cache"
	"github.com/GoGraph/ds"
	param "github.com/GoGraph/dygparam"
	"github.com/GoGraph/errlog"
	"github.com/GoGraph/grmgr"
	mon "github.com/GoGraph/monitor"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tbl/key"
	"github.com/GoGraph/tx"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/types"
	//	"github.com/GoGraph/rdf/uuid"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/uuid"
)

type action byte

const (
	logid         = "AttachNode"
	ADD    action = 'A'
	DELETE action = 'D'
)

func logerr(e error, panic_ ...bool) {

	if len(panic_) > 0 && panic_[0] {
		slog.Log("Client: ", e.Error(), true)
		panic(e)
	}
	slog.Log("Attach-merge: ", e.Error())
}

func UpdateValue(cUID uuid.UID, sortK string) error {
	// for update node predicate (sortk)
	// 1. perform cache update first.
	// 2. synchronous update to dynamo plus add stream CDI
	// 4. streams process: to each parent of cUID propagate value.(Streams api: propagateValue(pUID, sortk, v interface{}).

	// for AttachNode
	// for each child scalar create a CDI triggering api propagateValue(pUID, sortk, v interface{}).
	return nil
}

var edgeExistsErr error = errors.New("Edge is already attached")

func GetStringValue(cUID uuid.UID, sortK string) (string, error) { return "", nil }

func IndexMultiValueAttr(cUID uuid.UID, sortK string) error { return nil }

func AttachNodeEdges(ctx context.Context, edges []*atds.Edge, wg_ *sync.WaitGroup, lmtr *grmgr.Limiter, checkMode bool, nextBatchCh chan<- struct{}, once *sync.Once) {

	var (
		//	eAN     *event.AttachNode
		pTyName string
		err     error
		mt      mut.StdMut
		t0      time.Time
		ok      bool
		op      *AttachOp
		//		eAN     *event.AttachNode
	)
	defer lmtr.EndR()
	defer wg_.Done()

	// remove following defer as it impacts performance.
	// log Attach Event via defer
	// defer func() func() {
	// 	eAN = event.NewAttachNodeContext(ctx, edges[0].Puid, len(edges)) // 0:"EV$event",  1:EV$edge
	// 	//	eAN.LogStart()
	// 	return func() {
	// 		err = eAN.EventCommit(err)
	// 		if err != nil {
	// 			panic(err) //TODO: replace panic
	// 		}
	// 	}
	// }()()

	// update state via defer
	defer func() func() {
		// ideally op state should be part of edge join commit, but as its in a different database it must be a separate tx.
		// Two tx are compensated by having the join process check on existence of edge before creating - only for the batch at restart though.
		oTx := tx.New("op state").DB("mysql-GoGraph")
		return func() {
			//	et := eAN.NewTask("op-StateChange") // EV$task
			// Persist state
			oTx.Add(op.End(err)...) // Edge_Movies
			err = oTx.Execute()
			// log finish event
			//	et.Finish(err) // 2: "EV$task"
			if err != nil {
				errlog.Add(logid, fmt.Errorf("Error in execute oTx for attach node operation state: %w ", err))
			}
		}
	}()()

	handleErr := func(err error) {
		//pnd.Unlock()
		errlog.Add(logid, err)
		//panic(err)
	}

	// cTx := tx.NewTx("AttachEdges")// should use transactions as attaching nodes involves multiple puts (inserts) any one of which could fail.
	cTx := tx.NewBatchContext(ctx, "AttachEdges").DB("dynamodb") // as mutations are configured to use Insert (Put) - batch can be used. Issue - no transaction control. Cannot be used with merge stmt
	//cTx := tx.New("AttachEdges")
	// state tx

	//cTx := tx.NewTx("AttachEdges")

	// option 1:  mut.Insert:
	// NoSQL - dynamodb:  an insert/put  performs a merge  operation of all mutations across all edges.  This is good for restartability. Also insert can be batched where as updates cannot.
	//                    Defining NewTx transaction means all edges will be created or all none if errored on one. This level of consistency is unecessary in reality as restarting
	//                    a non-transactional operation will do a merge operation (insert if doesn't exist otherwise update) - all without the overhead associated of transactions.
	//					  Having a check for edge existence of an edge is unecessary because of the merge property of a Dynamodb Put.
	// SQL - spanner: insert or update is restartable because of the transactional property of SQL databases. Either all edges are created or none to a parent node on error.
	//                The restart design needs to  check for the existence of an edge and if it exists abort the entire attach process for a Puid.
	//                The check need only be performed on the initial batch on restarting.
	//				  If insert is used uid-pred must not exist whereas for update (with Set operator - not Append) works if uid-pred does exist.
	//                At this stage (4 Apri 2022) update is preferred.
	//
	// Initial load of edges:
	//           *  Dynamodb: Put for UID-PRED and all propagated items. How does this affect Overflow? Need to maintain Id, XF, Nd attributes in memory. Responsibility
	//                        of propagationTarget().
	//                        As most mutations are merged (with source/original mutation) and Put is used which can be batched, the db is only updated at after all
	//                        child nodes have been attached in memory. Single Put will attach hundreds of edges in some cases.
	//
	//           *  Spanner:  insert to create UID-PRED's and all propagation rows. MergeMutation will combine all mutations into a single update with array entries
	//                      for each edge.
	//
	// Post initial load:
	//
	//			 * Dynamodb: Update with Append for UID-PREDS & propagation items and attrExists (PKey) condition. When propagation items don't exist condition will fail
	//                 then use Put - as above.
	//
	//           * Spanner: Update with Append to Array. Like Dynamodb, update will fail when item doesn't exist, then use Insert.
	//
	//           * What if we presume the parent has children, then use Update (with APPEND). This will fail if there is no children - then we use the same
	//             technique as in the initial load, inserts/puts. Atleast this eliminates the parent fetch for each attach, which for a parent with lots of children
	//             being attached amounts to a lot of unecessary reads ie. money.
	//
	// option 2: all UID-PREDs exist with NULL equiv values in Nd, XF, Id. No Propagation items exist intially.
	//       Dynamodb: Initial: Put includes NULL entry. Everything else is the same as above.
	//                 Post: Update with Append for UID-PREDS & propagation items and attrExists (PKey) condition. When propagation items don't exist condition will fail
	//                 then use Put - as above.
	//       Spanner:  Initial: Update with Append for UID-PREDs. Insert for all propagation items. This is more complicated then above.
	//                 Post: Update with Append - same as "what if" option above. Advantage now is that update will always work for UID-PREDs. However progation items
	//                 will not initially so an insert will be used when the update fails.
	//
	// Conclusion: go with Option 1. No UID_PREDS and propagation items initially.

	mt = mut.Update
	if param.DB == param.Dynamodb {
		mt = mut.Insert
	}
	// mt = mut.Update
	//   cannot be used with tx.NewBatch(). Updates will append to array attributes so is good for creating edges on nodes that allready of edges, which is not the case of insert/put.
	//
	// mut.Merge (do not use as most operations are MergeMutation) //mut.Update - either works. However, update cannot use Batch load wherease Insert can be batched for Dynamo.

	//apAttrTySK := types.GraphSN() + "|A#G"

	// the use of MergeMutation means  all child nodes are attached in memory first and then persisted to the database in a single update
	// after all child nodes have been "virtuall" attached in memory.
	// The database commit unit contains all child nodes that are attached.
	// source node type item from cache

	// populate op with first entry in edges in case of an early error
	//	op = &AttachOp{Puid: edges[0].Puid, Cuid: edges[0].Cuid, Sortk: edges[0].Sortk, Bid: edges[0].Bid}

	gc := cache.GetCache()
	pnd, err := gc.FetchNodeContext(ctx, edges[0].Puid, types.GraphSN()+"|A#A#T")
	if err != nil {
		handleErr(err)
		return
	}

	sk := make(map[string]struct{})
	for _, e := range edges {
		if _, ok := sk[e.Sortk]; !ok {
			sk[e.Sortk] = struct{}{}
		}
	}
	dip := make(map[string]*blk.DataItem)
	// create virtual propagation target cache entries (uidpred's). No longer stored in cache, now in a function variable.
	// The data in dip drives the creation of overflow blocks/batches.
	for k, _ := range sk {
		//	pnd.CrEmptyNodeItem(edges[0].Puid, k, true)
		dip[k] = &blk.DataItem{Pkey: []byte(edges[0].Puid), Sortk: k}
	}

	sk = nil

	// get type e.g "Person" of node from A#A#T item or ty attribute if present
	if pTyName, ok = pnd.GetType(); !ok {
		err := fmt.Errorf(fmt.Sprintf("AttachNode: Error in GetType of parent node"))
		handleErr(err)
		return
	}

	pTySN, _ := types.GetTyShortNm(pTyName)

	// get type details from type table for child node based on sortk (edge attribute)
	var pty blk.TyAttrBlock
	if pty, err = types.FetchType(pTyName); err != nil {
		err := fmt.Errorf("AttachNode main: Error in types.FetchType : %w", err)
		handleErr(err)
		return
	}
	pTySN = types.GraphSN() + "|" + pTySN

	//st := eAN.NewTask("AtachExecute")
	//defer st.Finish(err)

	for _, e := range edges {

		t0 = time.Now()
		op = &AttachOp{Puid: e.Puid, Cuid: e.Cuid, Sortk: e.Sortk, Bid: e.Bid}

		// all mutations will comprise put/inserts
		err = AttachEdge(ctx, cTx, mt, pnd, uuid.UID(e.Cuid), uuid.UID(e.Puid), e.Sortk, pty, pTySN, checkMode, dip)
		if err != nil {
			if errors.Is(err, edgeExistsErr) {
				// database must be Spanner in checkMode. Abort attach process for this parent node.
				// cancel edgeExistsErr and return
				slog.LogErr("AttachEdges: ", fmt.Sprintf("Abort attach process for pUID %s as an edge exists - %s", e.Puid, err))
				return
			}
			errlog.Add(logid, err)
			// if this edge exists than all edges for parent node must
			return
		}

		slog.Log("AttachEdges: ", fmt.Sprintf(" Joined cUID --> pUID       %s -->  %s  %s Elapsed: %s  DML: %s\n", uuid.UID(e.Cuid).EncodeBase64(), uuid.UID(e.Puid).Base64(), e.Sortk, time.Now().Sub(t0), mt))
		// monitor: increment attachnode counter
		stat := mon.Stat{Id: mon.AttachNode}
		mon.StatCh <- stat

	}

	slog.Log("AttachEdges: ", fmt.Sprintf(" Tx dump:  %s", cTx))

	// execute all mutations accumulated in the above for loop. This includes all mutations invovled in Nd and Overflow block and batches.
	// when determining the propagation target
	// NOTE: cTx is Batch currently. This is risky as execute will invole multiple calls to the db and consequently should be transactional.
	// The batch will contain a single update for the target SortK in the Parent Node, inserts for each scalar attribute and optionally inerts/updates for
	// any Overflow blocks or batches, and update to the overflow batch

	// Execute()
	err = cTx.Commit()
	if err != nil {
		//cTx.Dump() // serialise cTx to mysql dump table
		errlog.Add(logid, fmt.Errorf("Error in execute of attach node transaction: %w ", err))
	}

}

// sortK is parent's uid-pred to attach child node too. E.g. G#:S (sibling) or G#:F (friend) or A#G#:F It is the parent's attribute to attach the child node.
// pTy is child type i.e. "Person". This could be derived from child's node cache data.
func AttachEdge(ctx context.Context, cTx *tx.Handle, mutop mut.StdMut, pnd *cache.NodeCache, cUID, pUID uuid.UID, sortK string, pty blk.TyAttrBlock, pTySN string, checkMode bool, dip map[string]*blk.DataItem) error {
	//

	var (
		//	eAN              *event.AttachNode // ev.Event - use concrete type instead to simplify method calls to one.
		cTyName  string
		ok       bool
		err      error
		apAttrTy string // datatype (short Name) of attachpoint (sortk)
		apAttrNm string
	)

	if param.DB == param.Spanner && checkMode {
		// check if edge already exists - which may exists on a restart.
		// If it does the attach process for this edge will be aborted as the program is designed for initial loads only.
		gc := cache.GetCache()
		_, err := gc.FetchNode(cUID, genReverseEdgeSK(sortK, pUID))
		if err == nil {
			// reverse edge must already exists other error would be no-data-found - abort attach process for this edge
			return edgeExistsErr
		}
	}

	// Select child scalar data (sortk: A#A#, non-scalars start with A#G).  ALL SCALARS SHOUD BEGIN WITH sortk "A#A#"
	// A node may not have any scalar values (its a connecting node in that case), but there should always be a A#A#T item defined which defines the type of the node
	// Scalar sortk: "A#A#" will include type of node as well
	// No need to lock (nc.Lock()) as cnd will never be made null eplicitly. Purging the node from the cache will only remove it from the global cache map. It will not set NodeCache=nil or e.nil.
	// So any routines that have a cnd defined can continue to use it even after the node is purged. When all old cnd's have left their scope, the GC will be able to free it, along with the map entries, as the cnd
	// will have no other object's referencing it - the ncd and all its contents can be freed.
	// However, any read access to the cnd.m will require a read lock as a concurrent fetch may add to the cnd.m entries - it will never update an existing entry.The only way
	// an existing nc.m[sortk] entry can be changed is by purging the node from the cache first, then perform a fetch. This will not impact any routines that have an odl cnd.m
	// active, but that old cnd.m will now be stale.
	// The purge process could mark old NodeCache as stale.
	//    ncOld= e.nc
	//    ncOld.Lock()
	//    ncOld.Stale=true
	//    ncOld.Unlock()
	//
	//    Routine with active nc should check Stale condition before reading nc.m
	//
	//gc := cache.NewCache()
	//
	gc := cache.GetCache()
	//
	slog.Log("AttachEdge", fmt.Sprintf("Fetch Child node scalar data for cUID: %s  SortK: [%s]", cUID.Base64(), types.GraphSN()+"|A#A#"))
	cnd, err := gc.FetchNodeContext(ctx, cUID, types.GraphSN()+"|A#A#")
	if err != nil {
		err := fmt.Errorf("Error fetching child scalar data: %w", err)
		errlog.Add(logid, err)
		return err
	}
	//	fmt.Println("ABout GetType for child node: ", cUID.String())
	// get type of child node from A#T sortk e.g "Person"
	if cTyName, ok = cnd.GetType(); !ok {
		return cache.NoNodeTypeDefinedErr
	}
	//	fmt.Println(" GetType done child node: ", cUID.String())
	// check child node type matches type of attachpoint (last entry in sortk)
	s := strings.LastIndex(sortK, "#")
	// grab attachpoint (ap) attribute short name from sortk
	attachPoint := sortK[s+2:]
	for _, v := range pty {
		if v.C == attachPoint {
			apAttrNm = v.Name
			apAttrTy, _ = types.GetTyShortNm(v.Ty)
		}
	}
	//check child node (cn) type matches parent attachpoint type
	cnTy, ok := types.GetTyShortNm(cTyName)
	if !ok {
		return fmt.Errorf("AttachEdge: not ok returned from types.GetTyShortNm for arg: %s", cTyName)
	}

	if cnTy != apAttrTy {
		// should return or panic??
		return fmt.Errorf("Child node type %q does not match parent node attachpoint type %q", attachPoint, cnTy)
	}

	// get type definition for child node
	var cty blk.TyAttrBlock
	if cty, err = types.FetchType(cTyName); err != nil {
		return err
	}

	// determine target for propagation either parent node UID or Overflow block UID.
	// py is legacy from older attach process which was used to exchange data between two goroutines.
	py := &blk.ChPayload{PTy: pty}
	err = propagationTarget(cTx, pnd, py, sortK, pUID, cUID, dip)
	if err != nil {
		return err // panic(err)
	}

	// add child UID to parent node's uid-pred or one of its overflow batches
	if bytes.Equal(py.TUID, pUID) {

		// in parent block (embedded)
		c := make([][]byte, 1, 1)
		c[0] = cUID
		x := make([]int64, 1, 1)
		x[0] = blk.ChildUID
		i := make([]int64, 1, 1)
		i[0] = 0
		// this will create an insert mutation if no mutation exists. If mutation exists then member data will be merged (usually appended)into existing mutation member values.
		// As its an insert the MergeMutation will overwrite the associated item in the db, hence should only be used for initial load unless a read operation is undertaken to load the mutation values.
		cTx.MergeMutation(tbl.EOP, pUID, sortK, mutop).AddMember("Nd", c).AddMember("XF", x).AddMember("Id", i).AddMember("N", 1, mut.Inc).AddMember("P", types.GraphSN()+"|"+apAttrNm).AddMember("Ty", pTySN)
		// for spanner, use stdMut of update with Set mutopr for all arrays. mutopr of Set works with dynamodb (stdMut Insert), as it will the value of mutopr is ignored.
		//	cTx.MergeMutation(tbl.EOP, pUID, sortK, mutop).AddMember("Nd", c,mut.Set).AddMember("XF", x,mut.Set).AddMember("Id", i,mut.Set).AddMember("N", 1, mut.Inc)

	} else {

		// Randomly chooses an overflow block. However before it can choose random it must create a set of overflow blocks
		// which relies upon an Overflow batch limit being reached and a new batch created.
		if py.Random {

			// all overflow blocks have been created. randomly select one.
			c := make([][]byte, 1, 1)
			c[0] = cUID
			x := make([]int64, 1, 1)
			x[0] = blk.ChildUID
			// again, MergeMutation for Overflow is insert which will overwrite associated pkey,sortk item. This is fine for initial load but cannot be used
			// for incremental loads as it does not first source from database - TODO: maybe should implement this.
			cTx.MergeMutation(tbl.EOP, py.TUID, py.Osortk, mutop).AddMember("Nd", c).AddMember("XF", x).AddMember("ASZ", 1, mut.Inc)

			//cTx.MergeMutation(tbl.EOP, py.TUID, py.Osortk, mutop).AddMembers(&<struct>)

		} else {

			// in overflow block - special case of tx.Append as it will set XF to OvflItemFull if Nd/XF exceeds  params.OvfwBatchSize  .
			// propagateTarget() will use OvfwBatchSize to create a new batch next time it is executed.
			cuid := make([][]byte, 1)
			cuid[0] = cUID
			xf := make([]int64, 1)
			xf[0] = int64(blk.ChildUID)

			// append to overflow batch - ASZ records number of items in overflow batch (max: param.OvfwBatchSize (~300))
			cTx.MergeMutation(tbl.EOP, py.TUID, py.Osortk, mutop).AddMember("Nd", cuid).AddMember("XF", xf).AddMember("ASZ", 1, mut.Inc)

			// update XF flag in parent UID-PRED if overflow batch size exceeds limit. This will result in a new batch being created in propagationTarget().
			err = checkOBatchSizeLimitReached(cTx, cUID, py)
			if err != nil {
				return err // panic(err)
			}
		}
		// increment N in parent UID-PRED, which maintains total of all attached nodes.
		// Note it is not just the number of elements in the UID-PRED Nd (which could be mantained with ASZ - maybe)
		cTx.MergeMutation(tbl.EOP, pUID, sortK, mutop).AddMember("N", 1, mut.Inc)

	}

	// build NV (Name:Value) based on Type info - either all scalar types or only those  declared in IncP attruibte for the attachment type define in sortk
	// Value will be supplied by cache.UnmarshallCache()
	var cnv ds.ClientNV

	// find attachment data type from sortK eg. A#G#:S
	// here S is simply the abreviation for the Ty field which defines the child type  e.g 	"Person"
	// TODO: code following cnv population as method/func ??.GenScalarAttributes(cnv)

	var found bool
	for _, v := range py.PTy {
		if v.C == attachPoint {
			found = true
			// grab all scalars from child type if the attribute has propagaton enabled or the attribute is nullable (meaning it may or may not be defined)
			// we need to propagate not nulls to support the has() as its the only to know if its defined for the child as the XF(?) attribute will be true if its defined or false if not.
			for _, v := range cty {
				switch v.DT {
				case "I", "F", "Bl", "S", "DT": // Scalar types. TODO: these attributes should belong to pUpred type only. Can a node be made up of more than one type? Pesuming at this stage only 1, so all scalars are relevant.
					if v.Pg || v.N {
						nv := &ds.NV{Name: v.Name}
						cnv = append(cnv, nv)
					}
				}
			}
		}
	}
	if !found {
		// should return or panic??
		return fmt.Errorf("did not find attachPoint %q in child node type ", attachPoint)
	}

	if len(cnv) > 0 {
		//
		// copy cache data into cnv and unlock child node.
		// As cache is being accessed it must be locked before hand to protect against concurrent updates
		// which it has with gc.FetchForUpdate(cUID)
		//
		// check cnd has not been marked as stale by cache purge or database writer routines
		// as stale is a variable within the shared ncd (which may be updated at anytime) must take a lock before reading its value.
		// Read Lock taken in IsStale().
		for cnd.IsStaleAndLock() {
			cnd.RUnlock()
			cnd, err = gc.FetchNode(cUID, types.GraphSN()+"|A#A#")
			if err != nil {
				err := fmt.Errorf("Error fetching child scalar data: %w", err)
				errlog.Add(logid, err)
				return err
				//panic(err)
			}
		}
		err = cnd.UnmarshalCache(cnv)
		cnd.RUnlock()
		if err != nil {
			return fmt.Errorf("AttachNode (child node): Unmarshal error : %s", err)
		}
		// Propagate scalar fields in Child node to uid-pred-edge of parent node
		for _, t := range cty {

			for _, v := range cnv {

				if t.Name == v.Name {

					propagateMerge(cTx, t, pUID, sortK, py.TUID, py.BatchId, v.Value, mutop)

					break
				}
			}
		}
	}
	// add parent UID to reverse edge on child node
	cTx.Add(updateReverseEdge(cUID, pUID, py.TUID, sortK, py.BatchId))

	return nil
}

// DetachNode: Not implemented for Spanner....
func DetachNode(cUID, pUID uuid.UID, sortK string) error {
	//
	// CEG - Concurrent event gatekeeper.
	//
	// if ok, err = EdgeExists(cUID, pUID, sortK, DELETE); !ok {
	// 	if errors.Is(err, db.ErrConditionalCheckFailed) {
	// 		return gerr.NodesNotAttached
	// 	}
	// }
	// if err != nil {
	// 	return err
	// }
	//err = db.DetachNode(cUID, pUID, sortK)
	// if err != nil {
	// 	var nif db.DBNoItemFound
	// 	if errors.As(err, &nif) {
	// 		err = nil
	// 		fmt.Println(" returning with error NodesNotAttached..............")
	// 		return gerr.NodesNotAttached
	// 	}
	// 	return err
	// }

	return nil
}

func genReverseEdgeSK(sortK string, puid uuid.UID) string {
	var b strings.Builder
	b.WriteString("R#")
	b.WriteString(sortK[strings.LastIndex(sortK, ":"):])
	b.WriteByte('|')
	b.WriteString(puid.String())
	return b.String()
}

func updateReverseEdge(cuid, puid, tUID uuid.UID, sortk string, batchId int64) *mut.Mutation {

	m := mut.NewInsert(tbl.Reverse).AddMember("PKey", cuid).AddMember("SortK", genReverseEdgeSK(sortk, puid))
	// for batch==0 save as NULL otherwise
	if batchId > 0 {
		m.AddMember("batch", batchId)
		m.AddMember("oUID", tUID)
	}

	return m
}

// EdgeExists acts as a sentinel or CEG - Concurrent event gatekeeper, to the AttachNode and DetachNode operations.
// It guarantees the event (operation + data) can only run once.
// Rather than check parent is attached to child, ie. for cUID in pUID uid-pred which may contain millions of UIDs spread over multiple overflow blocks more efficient
// to check child is attached to parent in cUID's #R attribute.
// Solution: specify field "BS" and  query condition 'contains(PBS,pUID+"f")'          where f is the short name for the uid-pred predicate - combination of two will be unique
//           if update errors then node is not attached to that parent-node-predicate, so nothing to delete
//
// func EdgeExists(cuid, puid uuid.UID, sortk string, act action) (bool, error) {

// 	txh := tx.New("EdgeExists")

// 	if tbl.DebugOn {
// 		fmt.Println("In EdgeExists: on ", cuid, puid, sortk)
// 	}
// 	//
// 	fmt.Println("In EdgeExists: on ", cuid, puid, sortk)

// 	mut:=tx.NewMutation(propagated, cuid, "R#")

// 	pred := func(sk string) string {
// 		i := strings.LastIndex(sk, "#")
// 		return sk[i+2:]
// 	}
// 	// note EdgeExists is only called on known nodes - so not necessary to check nodes exist.
// 	//
// 	// if ok, err := NodeExists(cuid); !ok {
// 	// 	if err != nil {
// 	// 		return false, fmt.Errorf("Child node %s does not exist:", cuid)
// 	// 	} else {
// 	// 		return false, fmt.Errorf("Error in NodeExists %w", err)
// 	// 	}
// 	// }
// 	// if ok, err := NodeExists(puid, sortk); !ok {
// 	// 	if err != nil {
// 	// 		return false, fmt.Errorf("Parent node and/or attachment predicate %s does not exist")
// 	// 	} else {
// 	// 		return false, fmt.Errorf("Error in NodeExists %w", err)
// 	// 	}
// 	// }
// 	//
// 	// if the operation is AttachNode we want to ADD the parent node onlyif parent node does not exist otherwise error
// 	// if the operation is DetachNode we want to DELETE parent node only if parent node exists otherwise error
// 	//
// 	//  a mixture of expression and explicit AttributeValue definitions is used - to overcome idiosyncrasies in Dynmaodb sdk handling of Sets
// 	switch act {

// 	case DELETE:
// 		opr = db.NewOperation(db.EdgeExistsDetachNode)

// 		pbs := make([][]byte, 1, 1)
// 		pbs[0] = append(puid, pred(sortk)...)
// 		var pbsC []byte
// 		pbsC = append(puid, pred(sortk)...)
// 		// bs is removed in: removeReverseEdge which requires target UID which is not availabe when EdgeExists is called
// 		input.AddMember("PBS", "@v1", pbs)
// 		//upd = expression.Delete(expression.Name("PBS"), expression.Value(pbs))
// 		input.AddConditiion("Contains", "PBS", pbsC, "Not")
// 		// Contains requires a string for second argument however we want to put a B value. Use X as dummyy to be replaced in explicit AttributeValue stmt
// 		//cond = expression.Contains(expression.Name("PBS"), "X")
// 		// replace gernerated AttributeValue values with corrected ones.
// 		eav = map[string]*dynamodb.AttributeValue{":0": &dynamodb.AttributeValue{B: pbsC}, ":1": &dynamodb.AttributeValue{BS: pbs}}

// 	case ADD:
// 		opr = db.NewOperation(db.EdgeExistsDetachNode)

// 		pbs := make([][]byte, 1, 1)
// 		pbs[0] = append(puid, pred(sortk)...)
// 		var pbsC []byte
// 		pbsC = append(puid, pred(sortk)...)
// 		//
// 		input.AddMember("PBS", pbs)
// 		//upd = expression.Add(expression.Name("PBS"), expression.Value(pbs))
// 		// Contains - sdk requires a string for second argument however we want to put a B value. Use X as dummyy to be replaced by explicit AttributeValue stmt
// 		input.AddConditiion("Contains", "PBS", pbsC, "Not")
// 		//cond = expression.Contains(expression.Name("PBS"), "X").Not()
// 		// workaround: as expression will want Contains(predicate,<string>), but we are comparing Binary will change in ExpressionAttributeValue to use binary attribute value.
// 		// also: compare with binary value "v" which is not base64 encoded as the sdk will encode during transport and dynamodb will decode and compare UID (16byte) with UID in set.
// 		//eav = map[string]*dynamodb.AttributeValue{":0": &dynamodb.AttributeValue{B: pbsC}, ":1": &dynamodb.AttributeValue{BS: pbs}}
// 	}
// 	inputs.Add(input)

// 	inputs.EdgeTest(db.Update) //TODO: implement - see EdgeExists in dynamodb.go
// }

// propagateScalar appends each child node scalar data to the parent edge attribute associated with the scalar attribute (ie. by its own sortk)
// The data is either propagated to the parent node block or the parent nodes overflow block associated with the Child UID.
// Each Scalar attribute(column) is represented by its own item/row in the table with its own sortk value e.g. "A#G#:C", and
// array/list attribute type to which each instance of the scalar value is appended.

func propagateMerge(cTx *tx.Handle, ty blk.TyAttrD, pUID uuid.UID, sortK string, tUID uuid.UID, batchId int64, value interface{}, mutop mut.StdMut) { //, wg ...*sync.WaitGroup) error {
	// **** where does Nd, XF get updated when in Overflow mode.???
	//
	var (
		lty   string
		sortk string
	)

	//lveu-vwfs-xfyd-wgmi
	// syslog := func(s string) {
	// 	slog.Log("propagateScalar: ", s)
	// }
	//syslog(fmt.Sprintf(" pUID tUID    %s %s  batch %d sortk: %s  value %v\n", pUID, tUID, batchId, sortK, value))

	if bytes.Equal(pUID, tUID) {
		if ty.DT != "Nd" {
			// simple scalar e.g. Age
			lty = "L" + ty.DT
			sortk = sortK + "#:" + ty.C // TODO: currently ignoring concept of partitioning data within node block. Is that right?
		} else {
			// TODO: can remove this section
			// uid-predicate e.g. Sibling
			lty = "Nd"
			//	sortk = "A#G#:" + sortK[len(sortK)-1:] // TODO: currently ignoring concept of partitioning data within node block. Is that right? Fix: this presumes single character short name
			sortk = "A#G#:" + sortK[strings.LastIndex(sortK, ":")+1:]
		}
	} else {
		// append data to overflow block batch
		if ty.DT != "Nd" {
			// simple scalar e.g. Age
			lty = "L" + ty.DT
			sortk = sortK + "#:" + ty.C + "%" + strconv.FormatInt(batchId, 10) // TODO: currently ignoring concept of partitioning data within node block. Is that right?
		}
		// else {
		// 	// TODO: can remove this section
		// 	// uid-predicate e.g. Sibling
		// 	lty = "Nd"
		// 	//sortk = "A#G#:" + sortK[len(sortK)-1:] // TODO: currently ignoring concept of partitioning data within node block. Is that right? Fix: this presumes single character short name
		// 	sortk = "A#G#:" + sortK[strings.LastIndex(sortK, ":")+1:]
		// }
	}
	//
	keys := []key.Key{key.Key{"SortK", sortk}, key.Key{"PKey", tUID}} // correct keys wrong order. Order is corrected in processing.
	//keys := []key.Key{key.Key{"Sort", sortk}, key.Key{"PKey", tUID}} // incorrect key name. Passed
	// keys := []key.Key{key.Key{"SortK", sortk}, key.Key{"PKey", sortk}} // incorrect key value. Passed
	// keys = []key.Key{key.Key{"PKey", sortk}}                           // too few keys. Passed
	//keys := []key.Key{key.Key{"Sortk", sortk}, key.Key{"PKey", tUID}, key.Key{"Key", tUID}} // too many keys
	//merge := cTx.MergeMutation(tbl.EOP, tUID, sortk, mutop)
	merge := cTx.MergeMutation2(tbl.EOP, mutop, keys)

	//merge := cTx.NewInsert(tbl.EOP).AddMember("PKey", tUID).AddMember("SortK", sortk)

	// add condition that detects if item/row already exists. If condition is false ie. item does not exist then condition error is raised.
	// merge.AddCondition(mut.AttrExists, "PKey")
	//
	// shadow XBl null identiier. Here null means there is no predicate specified in item, so its value is necessarily null (ie. not defined)
	//
	null := make([]bool, 1, 1)
	// no predicate value in item - set associated null flag, XBl, to true
	if value == nil {
		null[0] = true
	}
	// append child attr value to parent uid-pred list

	switch lty {

	case "LI", "LF":
		// null value for predicate ie. not defined in item. Set value to 0 and use XB to identify as null value
		if value == nil {
			//null[0] = true // performed above
			switch ty.DT {
			case "I":
				value = int64(0)
			case "F":
				value = float64(0)
			}
		}

		switch x := value.(type) {
		case int:
			v := make([]int, 1, 1)
			v[0] = x
			merge.AddMember("LI", v)
		case int32:
			v := make([]int32, 1, 1)
			v[0] = x
			merge.AddMember("LI", v)
		case int64:
			v := make([]int64, 1, 1)
			v[0] = x
			merge.AddMember("LI", v)
		case float64:
			v := make([]float64, 1, 1)
			v[0] = x
			merge.AddMember("LF", v)
		default:
			// TODO: check if string - ok
			errlog.Add(logid, fmt.Errorf("data type must be a number, int64, float64"))
			return
		}

	case "LBl":
		if value == nil {
			value = false
		}
		if x, ok := value.(bool); !ok {
			logerr(fmt.Errorf("data type must be a bool"), true)
		} else {
			v := make([]bool, 1, 1)
			v[0] = x
			merge.AddMember(lty, v)
		}

	case "LS":
		if value == nil {
			value = "__NULL__"
		}
		if x, ok := value.(string); !ok {
			logerr(fmt.Errorf("data type must be a string"), true)
		} else {
			v := make([]string, 1, 1)
			v[0] = x
			merge.AddMember(lty, v)
		}

	case "LDT":

		if value == nil {
			value = "__NULL__"
		}
		if x, ok := value.(time.Time); !ok {
			logerr(fmt.Errorf("data type must be a time"), true)
		} else {
			v := make([]string, 1, 1)
			v[0] = x.String()
			merge.AddMember(lty, v)
		}

	case "LB":

		if value == nil {
			value = []byte("__NULL__")
		}
		if x, ok := value.([]byte); !ok {
			logerr(fmt.Errorf("data type must be a byte slice"), true)
		} else {
			v := make([][]byte, 1, 1)
			v[0] = x
			merge.AddMember(lty, v)
		}

	}
	//
	// bool represents if entry passed in is defined or not. True means it is not defined equiv to null entry.
	//
	merge.AddMember("XBl", null)
	//merge.AddMember("CNT", 1, mut.Inc) // TODO: what about ASZ. CNT required for search function.

}
