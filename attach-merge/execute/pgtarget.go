package execute

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	//"time"

	blk "github.com/GoGraph/block"
	//"github.com/GoGraph/cache"
	param "github.com/GoGraph/dygparam"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx"
	"github.com/GoGraph/tx/key"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/types"
	"github.com/GoGraph/uuid"
)

// PropagationTarget determines if target is embedded array or array in overflow block
// of attach-merge package and should only be used straight after the loader process.
// This process batches the reqired DML in memory rather than populate/update the database for each attach operation.
// Attach-merge introduces a special mutation method for this purpose, MergeMutation, which is used to batch and handle
// appending of values.  The results are executed against the database in a single execute that may involve hundreds of child node being attached.
func propagationTarget(txh *tx.Handle, cpy *blk.ChPayload, sortK string, pUID, cUID uuid.UID, dip map[string]*blk.DataItem) error {
	var (
		err      error
		embedded int // embedded cUIDs in <parent-UID-Pred>.Nd
		oBlocks  int // overflow blocks
		//
		di *blk.DataItem // existing item
		//
		oUID  uuid.UID // new overflow block UID
		index int      // index in parent UID-PRED attribute Nd
		batch int64    // overflow batch id
	)
	syslog := func(s string) {
		slog.Log("propagationTarget: ", s)
	}

	// generates the Sortk for an overflow batch item based on the batch id and original sortK
	batchSortk := func(id int64) string {
		var s strings.Builder
		s.WriteString(sortK)
		s.WriteByte('%')
		s.WriteString(strconv.FormatInt(id, 10))
		return s.String()
	}
	// crOBatch - creates a new overflow batch and initial item to establish List/Array data
	crOBatch := func(index int) string { // return batch sortk
		//
		syslog(fmt.Sprintf("PropagationTarget: create Overflow Batch - sortk %s index %d", sortK, index))
		// increment overflow batch id (id attribute of UID-PRED in parent node)
		di.Id[index] += 1
		id := di.Id[index]
		// nilItem := []byte{'X'}
		// nilUID := make([][]byte, 1, 1)
		// nilUID[0] = nilItem
		// // xf
		// xf := make([]int64, 1, 1)
		// xf[0] = blk.UIDdetached // this is a nil (dummy) entry so mark it deleted.
		// entry 2: Nill batch entry - required for Dynamodb to establish List attributes
		s := batchSortk(id)

		// add new overflow batch to overflow block
		txh.AddMutation2(tbl.EOP, oUID, s, mut.Insert)

		// update batch Id in parent UID (parent Node block)
		//txh.NewMutation(tbl.EOP, pUID, sortK, mut.Update).AddMember2("Id", di.Id, mut.Set)
		keys := []key.Key{key.Key{"PKey", pUID}, key.Key{"SortK", sortK}}
		//txh.MergeMutation(tbl.EOP, pUID, sortK, mut.Update).AddMember("Id", di.Id, mut.Set)
		txh.MergeMutation(tbl.EOP, mut.Update, keys).AddMember("Id", di.Id, mut.Set)

		return s
	}
	// crOBlock - create a new Overflow block
	crOBlock := func() string {
		// create an Overflow block UID
		oUID, err = uuid.MakeUID()
		if err != nil {
			panic(err)
		}
		syslog(fmt.Sprintf("PropagationTarget: create Overflow Block %v\n", oUID))

		// add overflow block first item which identifes its parent node
		m := txh.NewInsert(tbl.Block).AddMember("PKey", oUID).AddMember("Graph", types.GraphSN()).AddMember("OP", di.GetPkey())
		if param.DB == param.Dynamodb {
			m.AddMember("SortK", "OV")
		}

		// add oblock to parent UID-PRED, N
		o := make([][]byte, 1, 1)
		o[0] = oUID
		x := make([]int64, 1, 1)
		x[0] = blk.OvflBlockUID
		i := make([]int64, 1, 1)
		i[0] = 1
		keys := []key.Key{key.Key{"PKey", pUID}, key.Key{"SortK", sortK}}
		// update OUID to parent node's UID-PRED  - note: default operation for update is to append to array types
		//txh.MergeMutation(tbl.EOP, pUID, sortK, mut.Update).AddMember("Nd", o).AddMember("XF", x).AddMember("Id", i) //.AddMember("N", 1, mut.Inc) only icrement N when adding actual child nodes not overflow blocks
		txh.MergeMutation(tbl.EOP, mut.Update, keys).AddMember("Nd", o).AddMember("XF", x).AddMember("Id", i) //.AddMember("N", 1, mut.Inc) only icrement N when adding actual child nodes not overflow blocks

		// update cache with overflow block entry to keep state in sync.
		di.Nd = append(di.Nd, oUID)
		di.XF = append(di.XF, blk.OvflBlockUID)
		di.Id = append(di.Id, 1) // crObatch will increment to 1

		// create first Obatch item
		// - do not use crOBatch as this will add multiple mutations to same item which is not allowed in Dynamodb ???
		syslog(fmt.Sprintf("PropagationTarget: create Overflow Batch - sortk %s index %d", sortK, index))
		s := batchSortk(1)
		txh.AddMutation2(tbl.EOP, oUID, s, mut.Insert)

		return s
	}

	clearXF := func(index int) {
		// batch may be set to blk.OBatchSizeLimit (set when cUID is added in client.AttachNode())
		if di.XF[index] == blk.OBatchSizeLimit {
			// keep adding oBatches until OBatchSizeLimit reached. Update cache entry, which represents current UID-PRED state.
			di.XF[index] = blk.OvflBlockUID
			// s := mut.XFSet{Value: di.XF}
			// upd := mut.MergeMutation(tbl.EOP, pUID, sortK, s)
			keys := []key.Key{key.Key{"PKey", pUID}, key.Key{"SortK", sortK}}
			//txh.MergeMutation(tbl.EOP, pUID, sortK, mut.Update).AddMember("XF", di.XF, mut.Set)
			txh.MergeMutation(tbl.EOP, mut.Update, keys).AddMember("XF", di.XF, mut.Set)
		}

	}

	// dip replaces data that was kept in the node cache. No locking required to populate the cache now.
	di = dip[sortK]
	cpy.DI = di

	// initially di.XF is nil, see dip[k] = &blk.DataItem{Pkey: []byte(edges[0].Puid), Sortk: k} in execute()
	for _, v := range di.XF {
		switch {
		case v <= blk.UIDdetached:
			// child  UIDs stored in parent UID-Predicate
			embedded++
		case v == blk.OBatchSizeLimit || v == blk.OvflBlockUID:
			// child UIDs stored in node overflow blocks
			oBlocks++
		}
	}

	//
	if embedded < param.EmbeddedChildNodes {
		// keep cache entries(di) uptodate as propagationTarget() references the cache and it needs to have the latest state.
		// so add cUID to cache di. The actual mutation for adding cUID occurs in AttachEdge()

		di.Nd = append(di.Nd, cUID)
		di.XF = append(di.XF, blk.ChildUID)
		di.Id = append(di.Id, 0)

		// child node attachment point is the parent UID
		cpy.TUID = pUID

		return nil
	}
	//
	// overflow blocks required....
	//
	if oBlocks <= param.MaxOvFlBlocks {

		// create oBlocks from 1..param.MaxOvFlBlocks with upto param.OBatchThreshold batches in each
		// only interested in last entry in Nd, Id UID-pred arrays, as that is the one GoGraph
		// is initially filling up until OBatchThreshold batches.
		index = len(di.Nd) - 1
		oUID = di.Nd[index]
		batch = di.Id[index]
		//
		switch {

		case oBlocks == 0:

			s := crOBlock()

			cpy.TUID = oUID
			cpy.BatchId = 1
			cpy.Osortk = s
			cpy.NdIndex = len(di.Nd) - 1

		case di.XF[index] == blk.OBatchSizeLimit && batch < param.OBatchThreshold:

			// create a new Overflow batch and clear XF flag to allow for new batches in the particular overflow block (Nd uid)
			clearXF(index)
			crOBatch(index)
			batch = di.Id[index]
			cpy.TUID = oUID
			cpy.BatchId = batch // batch
			cpy.Osortk = batchSortk(batch)
			cpy.NdIndex = index

		case di.XF[index] == blk.OBatchSizeLimit && batch == param.OBatchThreshold:

			if oBlocks < param.MaxOvFlBlocks {

				// reached OBatchThreshold batches in current OBlock.
				// Add another oBlock - ultimately MaxOvFlBlocks will be reacehd.
				clearXF(index)
				s := crOBlock()
				batch = di.Id[len(di.Id)-1]
				cpy.TUID = oUID
				cpy.BatchId = batch // 1
				cpy.Osortk = s      //batchSortk(batch)
				cpy.NdIndex = len(di.Id) - 1

			} else {

				cpy.Random = true
				//rand.Seed(time.Now().UnixNano())
				//TODO: try rand.Int64n
				index = rand.Intn(len(di.Nd)-param.EmbeddedChildNodes) + param.EmbeddedChildNodes
				cpy.TUID = di.Nd[index]
				//rand.Seed(time.Now().UnixNano())
				bid := rand.Intn(int(di.Id[index])) + 1
				cpy.BatchId = int64(bid)
				cpy.Osortk = batchSortk(int64(bid))
				cpy.NdIndex = index
			}

		case di.XF[index] != blk.OBatchSizeLimit && batch <= param.OBatchThreshold:

			batch = di.Id[index]
			cpy.TUID = oUID
			cpy.BatchId = batch // batch
			cpy.Osortk = batchSortk(batch)
			cpy.NdIndex = index

		default:
			panic("switch in PropagationTarget did not process any case options...")
		}

	}
	return nil
}
