//go:build spanner
// +build spanner

package db

import (
	"context"
	"fmt"
	"strings"
	"time"

	blk "github.com/GoGraph/block"
	//	"github.com/GoGraph/dbConn"
	param "github.com/GoGraph/dygparam"
	//gerr "github.com/GoGraph/dygerror"
	mon "github.com/GoGraph/monitor"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/uuid"

	"cloud.google.com/go/spanner" //v1.21.0
	"google.golang.org/api/iterator"
)

const (
	DELETE = 'D'
	ADD    = 'A'
	logid  = "DB: "
)

type request byte

const (
	scalar         request = 'S'
	edge                   = 'E'
	alledges               = 'a'
	propagated             = 'P'
	reverse                = 'R'
	edgepropagated         = 'D'
	type_                  = 't'
	obatchuid              = 'o'
	obatchpred             = 'p'
	eopcnt                 = 'c'
	all                    = 'l'
)

type gsiResult struct {
	Pkey  []byte
	SortK string
}

var (
	client *spanner.Client
)

//  ItemCache struct is the transition between Dynamodb types and the actual attribute type defined in the DD.
//  Number (dynamodb type) -> float64 (transition) -> int (internal app & as defined in DD)
//  process: dynamodb -> ItemCache -> DD conversion if necessary to application variables -> ItemCache -> Dynamodb
//	types that require conversion from ItemCache to internal are:
//   DD:   int         conversion: float64 -> int
//   DD:   datetime    conversion: string -> time.Time
//  all the other datatypes do not need to be converted.

//  NOTE: tyShortNm is duplicated in cache pkg. It exists in in db package only to support come code in rdfload.go that references the db version rather than the cache which it cannot access
// because of input-cycle issues. Once this reference is rdfload is removed the cache version should be the only one used.

type tyNames struct {
	ShortNm string `json:"Atr"`
	LongNm  string
}

var (
	err       error
	tynames   []tyNames
	tyShortNm map[string]string
)

func GetTypeShortNames() ([]tyNames, error) {
	return tynames, nil
}

func Fetch(sk string) (blk.NodeBlock, error) {
	return FetchNode(nil, sk)
}

// FetchNode used to fetch Scalar data or Edge data or edge-overflow data as determinded by sortk parameter
func FetchNode(uid uuid.UID, subKey ...string) (blk.NodeBlock, error) {

	var (
		sortk string
		sql   string
	)
	// type Block struct {
	// 	PKey []byte `spanner:"PKey"`
	// 	// Node
	// 	IsNode byte   // 'y', null if not
	// 	Ty     string // Node type
	// 	// Overflow Block
	// 	P uuid.UID //  Parent Node
	// 	//Ns    [][]byte ???
	// }
	ts := time.Now()
	type All struct {
		PKey  []byte             `spanner:"PKey"`
		Sortk spanner.NullString `spanner:"Sortk"`
		Ty    string             // parent type
		// Edge
		Nd [][]byte
		Id []int64
		XF []int64
		//
		Bl spanner.NullBool
		S  spanner.NullString
		F  spanner.NullFloat64
		I  spanner.NullInt64
		B  []byte
		DT spanner.NullTime
		//
		XBl []bool
		LS  []string
		LI  []int64
		LF  []float64
		LBl []bool
		LB  [][]byte
		LDT []time.Time
		//
		// SS []string
		// NS []int64
		// BS [][]byte
	}

	type Scalar struct {
		PKey  []byte             `spanner:"PKey"`
		Sortk spanner.NullString `spanner:"Sortk"`
		Ty    string             // parent type
		//Ty string - now in Block
		Bl spanner.NullBool
		S  spanner.NullString
		F  spanner.NullFloat64
		I  spanner.NullInt64
		B  []byte
		DT spanner.NullTime
		//
		LS  []string
		LI  []int64
		LF  []float64
		LBl []bool
		LB  [][]byte
		LDT []time.Time
		//
		// SS []string
		// NS []int64
		// BS [][]byte
	}
	type Type_ struct {
		PKey []byte             `spanner:"PKey"`
		Ty   spanner.NullString // parent type
		Puid []byte             `spanner:"P"` // parent UID
	}
	//type Edges struct {
	// Edge-Overflow-Propagated data
	type Edge struct {
		PKey  []byte `spanner:"PKey"`
		Sortk string `spanner:"Sortk"`
		Ty    string // parent type
		//
		Nd [][]byte
		Id []int64
		XF []int64
	}

	type ObatchUID struct {
		PKey  []byte `spanner:"PKey"`
		Sortk string `spanner:"Sortk"`
		Nd    [][]byte
		XF    []int64
	}

	type ObatchPred struct {
		PKey  []byte `spanner:"PKey"`
		Sortk string `spanner:"Sortk"`
		// P, Ty, N (see SaveUpredState) ???
		LS  []string
		LI  []int64
		LF  []float64
		LBl []bool
		LDT []time.Time
		LB  [][]byte
		// determines if slice entry is null (true), default false
		XBl []bool
	}

	type Propagated struct {
		PKey  []byte `spanner:"PKey"`
		Sortk string `spanner:"Sortk"`
		Ty    string // parent type
		// P, Ty, N (see SaveUpredState) ???
		LS  []string
		LI  []int64
		LF  []float64
		LBl []bool
		LDT []time.Time
		LB  [][]byte
		// determines if slice entry is null (true), default false
		XBl []bool
	}

	type EdgePropagated struct {
		PKey  []byte             `spanner:"PKey"`
		Sortk string             `spanner:"Sortk"`
		Ty    spanner.NullString // parent type
		// Edge
		Nd [][]byte
		Id []int64
		XF []int64
		// P, Ty, N (see SaveUpredState) ???
		LS  []string
		LI  []int64
		LF  []float64
		LBl []bool
		LDT []time.Time
		LB  [][]byte
		// determines if slice entry is null (true), default false
		XBl []bool
	}

	type Reverse struct {
		PKey  []byte `spanner:"PKey"` // child UID
		Sortk string `spanner:"Sortk"`
		Ty    string // parent type
		B     []byte // P value ie. Parent UID
		//
		Puid  []byte
		Ouid  []byte
		Batch spanner.NullInt64
	}

	type EopCnt struct {
		Cnt int64 `spanner:"cnt"`
	}

	if len(subKey) > 0 {
		sortk = subKey[0]
	} else {
		sortk = "A#A#"
	}
	ctx := context.Background()

	//defer client.Close()

	// stmt returns one row

	fetchType := func() request {
		sortk := sortk[strings.Index(sortk, "|")+1:]
		switch sortk {
		case "A#":
			return all
		case "A#A", "A#A#", "A#B#", "A#C#", "A#D#", "A#E#", "A#F#":
			return scalar
		case "A#A#T":
			return type_
		case "EOPCount":
			return eopcnt
		default:
			switch {
			case strings.IndexByte(sortk, '%') > 0:

				if strings.IndexByte(sortk[strings.IndexByte(sortk, '%'):], '#') > 0 {
					return obatchpred
				} else {
					return obatchuid
				}

			case strings.HasPrefix(sortk, "A#G"):
				if sortk == "A#G#" {
					return edgepropagated
				}
				// if sortk == "A#G" {
				// 	sortk = "A#G#"
				// 	return alledges
				// }

				switch strings.Count(sortk, "#") {
				case 2: // "A#G#:?"
					return edge
				case 3: // "A#G#:?#:?"
					return propagated
				}
			case strings.HasPrefix(sortk, "ALL"):
				sortk = sortk[3:]
				return edgepropagated

			case strings.HasPrefix(sortk, "R#"):
				return reverse
			}
		}
		panic(fmt.Errorf("Error in db.FetchNode: fetchType not determined based on sortk of %q", sortk))
	}

	params := map[string]interface{}{"uid": []byte(uid), "sk": sortk}
	fetchtype := fetchType()
	syslog(fmt.Sprintf("FetchNode: fetchtype %c uid %s  params %#v", fetchtype, uid, params))

	switch fetchtype {
	case all:
		// all assigned in cache.GenSortK - both scalar and uid-pred data is required.
		// all implies sortk of "A#"
		sql = `Select n.PKey, n.Ty, ns.SortK, null Nd, null Xf, null Id,  ns.S, ns.I, ns.F, ns.Bl, ns.B, ns.DT, null XBl, null LI, null LF, null LBl, null LB, null LDT, null LS
				from Block n 
				join NodeScalar ns using (PKey)
				where n.Pkey = @uid 
				union all
				Select n.PKey, n.Ty, e.SortK, Nd, Xf, Id, null, null, null, null, null, null, XBl, e.LI, e.LF, e.LBl, e.LB, e.LDT, e.LS
				from Block n 
				join eop e using (PKey)
				where n.Pkey = @uid`

	case scalar: // SortK: A#A#
		sql = `Select n.PKey, n.Ty, ns.SortK, ns.S, ns.I, ns.F, ns.Bl, ns.B, ns.DT
				from Block n 
				left outer join NodeScalar ns using (PKey)
				where n.Pkey = @uid and  (Starts_With(ns.Sortk,@sk) or ns.Sortk is null)`

	case type_: // SortK: A#A#
		sql = `Select n.PKey, n.Ty from Block n `

	case alledges: // UID-PRED SortK: A#G#:?
		// used by attach node to determine target UID for propatated data. Only edge data required, hence this query.
		sql = `Select n.PKey, e.Sortk, n.Ty,e.XF, e.Id, e.Nd
				from Block n 
				join EOP e using (PKey)
				where n.Pkey = @uid and Starts_With(e.Sortk,@sk)`
	case edge: // UID-PRED SortK: A#G#:?
		// used by attach node to determine target UID for propatated data. Only edge data required, hence this query.
		sql = `Select n.PKey, e.Sortk, n.Ty, e.XF, e.Id, e.Nd
				from Block n 
				join EOP e using (PKey)
				where n.Pkey = @uid and  e.Sortk = @sk`
	case obatchuid: // UID-PRED SortK: A#G#:?
		// used by attach node to determine target UID for propatated data. Only edge data required, hence this query.
		sql = `Select e.PKey, e.Sortk, e.Nd, e.XF
				from Block n 
				join EOP e using (PKey)
				where n.Pkey = @uid and  e.Sortk = @sk`
	case obatchpred: // SortK: A#G#:?#
		// used by query execute to query propagated data - all array types
		sql = `Select e.Pkey, e.Sortk,  e.LI, e.LF, e.LBl, e.LB, e.LS, e.LDT
				from Block n 
				join EOP e using (PKey)
				where n.Pkey = @uid and e.Sortk = @sk`
	case propagated: // SortK: A#G#:?#
		// used by query execute to query propagated data - all array types
		sql = `Select n.PKey, n.Ty, ps.SortK, ps.LI, ps.LF, ps.LBl, ps.LB, ps.LS
				from Block n 
				join EOP ps using (PKey)
				where n.Pkey = @uid and Starts_With(ps.Sortk,@sk)`
	case edgepropagated: // UID-PRED + propagated SortK: A#G#
		sql = `Select n.PKey, e.Sortk, n.Ty, e.XF, e.Id, e.Nd, e.LI, e.LF, e.LBl, e.LB, e.LS, e.XBl
				from Block n 
				join EOP e using (PKey)
				where n.Pkey = @uid and Starts_With(e.Sortk,@sk)`
	case reverse: // SortK: R#
		sql = `Select n.PKey, r.Sortk, r.pUID, r.Batch
				from Block n 
				join Reverse r using (PKey)
				where n.Pkey = @uid`
	case eopcnt: // ???
		sql = `Select count(*) cnt from EOP`
	}

	// sql := `Select PKey,"A#A#T" SortK, Ty, P,
	// 				ARRAY (select  PKey from NodeScalar       ns where ns.PKey = @uid and  Starts_With(ns.Sortk,@sk)) as ns
	// 				ARRAY (select as struct * from Edge             e  where  e.PKey = @uid and  Starts_With(e.Sortk,@sk)) as e,
	// 				ARRAY (select as struct * from PropagatedScalar ps where ps.PKey = @uid and  Starts_With(ps.Sortk,@sk)) as ps
	// 		   from Block n
	// 		   where n.Pkey = @uid`

	var (
		nb  blk.NodeBlock
		err error
	)

	t0 := time.Now()
	iter := client.Single().Query(ctx, spanner.Statement{SQL: sql, Params: params})
	defer iter.Stop()

	t1 := time.Now()
	tsf := time.Now()

	switch fetchtype {
	case all:

		// Note: use the for { rather then iter.Do(
		// as the former works when parrallel executions of fetch is used.
		// The iter.Do will probably cause race conditions as any variable souced outside the function
		// passed in iter.Do is equivalent to a package variable in a go function.

		for {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			}
			rec := All{}
			err = row.ToStruct(&rec)
			if err != nil {
				panic(err)
			}
			if len(nb) == 0 {
				nbrow := &blk.DataItem{}
				nbrow.Pkey = rec.PKey
				nbrow.Sortk = "A#A#T"
				nbrow.Ty = rec.Ty
				nb = append(nb, nbrow)
			}
			//
			nbrow := &blk.DataItem{}
			nbrow.Pkey = rec.PKey
			if !rec.Sortk.IsNull() {
				nbrow.Sortk = rec.Sortk.StringVal
			}
			switch {
			case !rec.S.IsNull():
				nbrow.S = rec.S.StringVal
			case !rec.I.IsNull():
				nbrow.I = rec.I.Int64
			case !rec.F.IsNull():
				nbrow.F = rec.F.Float64
			case !rec.DT.IsNull():
				nbrow.DT = rec.DT.Time
			case !rec.Bl.IsNull():
				nbrow.Bl = rec.Bl.Bool
			}
			nbrow.Nd = rec.Nd
			nbrow.XF = rec.XF
			nbrow.Id = rec.Id
			nbrow.XBl = rec.XBl
			//
			nbrow.B = rec.B
			nbrow.LS = rec.LS
			nbrow.LI = rec.LI
			nbrow.LF = rec.LF
			nbrow.LB = rec.LB
			nbrow.LBl = rec.LBl
			nbrow.LDT = rec.LDT
			//
			nb = append(nb, nbrow)

		}

	case scalar:

		for {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			}
			rec := Scalar{}
			err = row.ToStruct(&rec)
			if err != nil {
				panic(err)
			}
			if len(nb) == 0 {
				nbrow := &blk.DataItem{}
				nbrow.Pkey = rec.PKey
				nbrow.Sortk = "A#A#T"
				nbrow.Ty = rec.Ty
				nb = append(nb, nbrow)
			}
			//
			nbrow := &blk.DataItem{}
			nbrow.Pkey = rec.PKey
			if !rec.Sortk.IsNull() {
				nbrow.Sortk = rec.Sortk.StringVal
			}
			switch {
			case !rec.S.IsNull():
				nbrow.S = rec.S.StringVal
			case !rec.I.IsNull():
				nbrow.I = rec.I.Int64
			case !rec.F.IsNull():
				nbrow.F = rec.F.Float64
			case !rec.DT.IsNull():
				nbrow.DT = rec.DT.Time
			case !rec.Bl.IsNull():
				nbrow.Bl = rec.Bl.Bool
			}
			nbrow.B = rec.B
			nbrow.LS = rec.LS
			nbrow.LI = rec.LI
			nbrow.LF = rec.LF
			nbrow.LB = rec.LB
			nbrow.LBl = rec.LBl
			nbrow.LDT = rec.LDT
			//
			nb = append(nb, nbrow)
		}

	case type_:

		for {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			}
			rec := Type_{}
			err = row.ToStruct(&rec)
			if err != nil {
				panic(err)
			}
			nbrow := &blk.DataItem{}
			nbrow.Pkey = rec.PKey
			nbrow.Sortk = "A#A#T"
			if !rec.Ty.IsNull() {
				nbrow.Ty = rec.Ty.StringVal
			} else {
				nbrow.Ty = param.OVFL // should return type of parent maybe?
			}
			nbrow.P = rec.Puid
			nb = append(nb, nbrow)

		}

	case edge, alledges:

		for {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			}
			rec := Edge{}
			err = row.ToStruct(&rec)
			if err != nil {
				panic(err)
			}
			if len(nb) == 0 {
				nbrow := &blk.DataItem{}
				nbrow.Pkey = rec.PKey
				nbrow.Sortk = "A#A#T"
				nbrow.Ty = rec.Ty
				nb = append(nb, nbrow)
			}
			nbrow := &blk.DataItem{}
			nbrow.Pkey = rec.PKey
			nbrow.Sortk = rec.Sortk
			nbrow.Nd = rec.Nd
			nbrow.XF = rec.XF
			nbrow.Id = rec.Id

			nb = append(nb, nbrow)

		}

	case obatchuid:

		for {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			}
			rec := ObatchUID{}
			err = row.ToStruct(&rec)
			if err != nil {
				panic(err)
			}
			nbrow := &blk.DataItem{}
			nbrow.Pkey = rec.PKey
			nbrow.Sortk = rec.Sortk
			nbrow.Nd = rec.Nd
			nbrow.XF = rec.XF

			nb = append(nb, nbrow)

		}

	case obatchpred:

		for {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			}
			rec := ObatchPred{}
			err = row.ToStruct(&rec)
			if err != nil {
				panic(err)
			}
			nbrow := &blk.DataItem{}
			nbrow.Pkey = rec.PKey
			nbrow.Sortk = rec.Sortk
			nbrow.LS = rec.LS
			nbrow.LI = rec.LI
			nbrow.LF = rec.LF
			nbrow.LBl = rec.LBl
			nbrow.LDT = rec.LDT
			nbrow.LB = rec.LB
			nbrow.XBl = rec.XBl

			nb = append(nb, nbrow)

		}

	case propagated:

		for {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			}
			rec := Propagated{}
			err = row.ToStruct(&rec)
			if err != nil {
				panic(err)
			}
			if len(nb) == 0 {
				nbrow := &blk.DataItem{}
				nbrow.Pkey = rec.PKey
				nbrow.Sortk = "A#A#T"
				nbrow.Ty = rec.Ty
				nb = append(nb, nbrow)
			}

			nbrow := &blk.DataItem{}
			nbrow.Pkey = rec.PKey
			nbrow.Sortk = rec.Sortk
			nbrow.LS = rec.LS
			nbrow.LI = rec.LI
			nbrow.LF = rec.LF
			nbrow.LBl = rec.LBl
			nbrow.LDT = rec.LDT
			nbrow.LB = rec.LB
			nbrow.XBl = rec.XBl

			nb = append(nb, nbrow)

		}

	case edgepropagated:

		for {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			}
			rec := EdgePropagated{}
			err = row.ToStruct(&rec)
			if err != nil {
				panic(err)
			}
			if len(nb) == 0 {
				nbrow := &blk.DataItem{}
				nbrow.Pkey = rec.PKey
				nbrow.Sortk = "A#A#T"
				if !rec.Ty.IsNull() {
					nbrow.Ty = rec.Ty.StringVal
				}
				//nbrow.Ty = rec.Ty
				nb = append(nb, nbrow)
			}

			nbrow := &blk.DataItem{}
			nbrow.Pkey = rec.PKey
			nbrow.Sortk = rec.Sortk
			nbrow.Nd = rec.Nd
			nbrow.XF = rec.XF
			nbrow.Id = rec.Id
			//
			nbrow.LS = rec.LS
			nbrow.LI = rec.LI
			nbrow.LF = rec.LF
			nbrow.LBl = rec.LBl
			//nbrow.LDT = rec.LDT
			nbrow.LB = rec.LB
			nbrow.XBl = rec.XBl

			nb = append(nb, nbrow)

		}

	case eopcnt:

		for {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			}
			rec := EopCnt{}
			err = row.ToStruct(&rec)
			if err != nil {
				panic(err)
			}
			nbrow := &blk.DataItem{}
			nbrow.I = rec.Cnt
			nb = append(nb, nbrow)

		}

	case reverse:

		for {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			}
			rec := Reverse{}
			err = row.ToStruct(&rec)
			if err != nil {
				panic(err)
			}
			if len(nb) == 0 {
				nbrow := &blk.DataItem{}
				nbrow.Pkey = rec.PKey
				nbrow.Sortk = "A#A#T"
				nbrow.Ty = rec.Ty
				nb = append(nb, nbrow)
			}
			nbrow := &blk.DataItem{}
			nbrow.Pkey = rec.PKey
			nbrow.Sortk = rec.Sortk
			nbrow.LB = make([][]byte, 2, 2)
			nbrow.LB[0] = rec.Puid
			nbrow.LB[1] = rec.Ouid
			if !rec.Batch.IsNull() {
				nbrow.I = rec.Batch.Int64
			}

			nb = append(nb, nbrow)

		}
	}
	if err != nil {
		fmt.Println("=== error in Query ==== fetchtype: ", fetchtype)
		panic(err)
	}
	te := time.Now()
	syslog(fmt.Sprintf("FetchNode: %s subKey: %s  Elapsed - Query: %s  Fetch: %s  Overall: %s  RowCount: %d", uid.String(), sortk, t1.Sub(t0), te.Sub(tsf), te.Sub(ts), len(nb)))

	// fmt.Printf("child nb: len %d \n", len(nb))
	// for _, v := range nb {
	// 	fmt.Printf("data: %#v\n", *v)
	// }

	if len(nb) == 0 {
		// is subKey a G type (uid-predicate) ie. child data block associated with current parent node, create empty dataItem.
		if len(subKey) > 0 && strings.Index(subKey[0], "#G#") != -1 {
			data := make(blk.NodeBlock, 1)
			data[0] = new(blk.DataItem)
			return data, nil
		}
		return nil, newDBNoItemFound("FetchNode", uid.String(), "", "Query")
	}
	//
	// send stats
	//
	v := mon.Fetch{CapacityUnits: 0, Items: len(nb), Duration: te.Sub(t0)}
	stat := mon.Stat{Id: mon.DBFetch, Value: &v}
	mon.StatCh <- stat

	return nb, nil

}

//TODO: replace this function with the one above. Unnecessary to have two.
func FetchNodeItem(uid uuid.UID, sortk string) (blk.NodeBlock, error) {
	return FetchNode(uid, sortk)
}
