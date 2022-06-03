package save

import (
	"fmt"
	"strings"
	"sync"
	"time"

	blk "github.com/GoGraph/block"
	param "github.com/GoGraph/dygparam"
	"github.com/GoGraph/errlog"
	"github.com/GoGraph/grmgr"
	"github.com/GoGraph/rdf/ds"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/types"
	"github.com/GoGraph/uuid"
)

const (
	logid = "rdfSave: "
)

type tyNames struct {
	ShortNm string `json:"Atr"`
	LongNm  string
}

var (
	err     error
	tynames []tyNames
)

func logerr(e error, panic_ ...bool) {

	if len(panic_) > 0 && panic_[0] {
		slog.Log(logid, e.Error(), true)
		panic(e)
	}
	slog.Log(logid, e.Error())
}

func syslog(s string) {
	slog.Log(logid, s)
}

//TODO: this routine requires an error log service. Code below  writes errors to the screen in some cases but not most. Errors are returned but calling routines is a goroutine so thqt get lost.
// sname : node id, short name  aka blank-node-id
// uuid  : user supplied node id (uuid.UIDb64 converted to uuid.UID)
// nv_ : node attribute data
func SaveRDFNode(sname string, suppliedUUID uuid.UID, nv_ []ds.NV, wg *sync.WaitGroup, lmtr *grmgr.Limiter) {

	defer wg.Done()
	defer lmtr.EndR()

	var (
		aTy string // attribute type assigned to attributes of type S, I, F, B. Stored in GSI also.
		err error
	)

	//
	// generate UUID using uuid service
	//
	localCh := make(chan uuid.UID)
	request := uuid.Request{SName: sname, SuppliedUUID: suppliedUUID, RespCh: localCh}

	uuid.ReqCh <- request

	UID := <-localCh

	txh := tx.NewBatch("SaveNode")

	// first: get node type (short name)
	for _, nv := range nv_ {
		if nv.DT == "ty" {
			// null value for predicate ie. not defined in item. Set value to 0 and use XB to identify as null value
			if s, ok := nv.Value.(string); ok {
				if sn, ok := types.GetTyShortNm(s); !ok {
					syslog(fmt.Sprintf("Error: type name %q not found in types.GetTyShortNm \n", nv.Ty))
					return
				} else {
					aTy = types.GraphSN() + "|" + sn
				}
			}

			break
		}
	}

	// second: create mutations for each nv_ entry (node attribute)
	for _, nv := range nv_ {
		var txComplete bool
		var sk strings.Builder
		sk.WriteString(types.GraphSN())
		sk.WriteByte('|')
		sk.WriteString(nv.Sortk)

		//m := mut.NewMutation(tbl.NodeScalar, UID, nv.Sortk, mut.Insert)
		m := mut.NewInsert(tbl.NodeScalar).AddMember("PKey", UID).AddMember("SortK", sk.String())

		// include graph (short) name with attribute name in index
		var ga strings.Builder
		ga.WriteString(types.GraphSN())
		ga.WriteByte('|')
		ga.WriteString(nv.Name)

		switch nv.DT {

		case "I":
			// null value for predicate ie. not defined in item. Set value to 0 and use XB to identify as null value
			if i, ok := nv.Value.(int64); !ok {
				panic(fmt.Errorf("Value is not an Int "))
			} else {
				//(sk string, m string, param string, value interface{}) {
				//m.AddMember("I", i).AddMember("P", ga.String()).AddMember("Ty", aTy)
				m.AddMember("N", float64(i)).AddMember("P", ga.String()).AddMember("Ty", aTy)
			}

		case "F":

			// null value for predicate ie. not defined in item. Set value to 0 and use XB to identify as null value
			if f, ok := nv.Value.(float64); ok {
				//m.AddMember("F", f).AddMember("P", ga.String()).AddMember("Ty", aTy)
				m.AddMember("N", f).AddMember("P", ga.String()).AddMember("Ty", aTy)
				// populate with dummy item to establish LIST
				// switch param.DB {
				// case param.Spanner:
				// 	m.AddMember("F", f).AddMember("P", ga.String())
				// case param.Dynamodb:
				// 	m.AddMember("N", f).AddMember("P", ga.String())
				// }
				//a := Item{PKey: UID, SortK: nv.Sortk, N: f, P: nv.Name, Ty: tyShortNm} //nv.Ty}
			} else {
				panic(fmt.Errorf(" nv.Value is not an string (float) for predicate  %s", nv.Name))
			}

		case "S":

			// null value for predicate ie. not defined in item. Set value to 0 and use XB to identify as null value
			if v, ok := nv.Value.(string); ok {
				//
				// use Ix attribute to determine whether the P attribute (PK of GSI) should be populated.
				//  For Ix value of FT (full text search)the S attribute will not appear in the GSI (P_S) as ElasticSearch has it covered
				//  For Ix value of "x" is used for List or Set types which will have the result of expanding the array of values into
				//  individual items which will be indexed. Usual to be able to query contents of a Set/List.
				//  TODO: is it worthwhile have an FTGSI attribute to have it both index in ES and GSI
				//
				// remove leading and trailing blanks
				v := strings.TrimLeft(strings.TrimRight(v, " "), " ")

				switch nv.Ix {

				case "FTg", "ftg":

					// there is separate load program to load into ES
					// load into GSI by including attribute P in item
					m.AddMember("P", ga.String()).AddMember("S", v).AddMember("E", "S")

				case "FT", "ft":
					// Do not index (no "P" attr).
					m.AddMember("S", v).AddMember("E", "S")

				default:
					// load into GSI by including attribute P in item
					m.AddMember("P", ga.String()).AddMember("S", v)

				}
			} else {
				panic(fmt.Errorf(" nv.Value is not an string "))
			}
			// add type for all cases FT and non-FT
			m.AddMember("Ty", aTy)

		case "DT": // DateTime

			// null value for predicate ie. not defined in item. Set value to 0 and use XB to identify as null value
			if dt, ok := nv.Value.(time.Time); ok {
				// populate with dummy item to establish LIST
				m.AddMember("P", ga.String()).AddMember("DT", dt.String())
				//a := Item{PKey: UID, SortK: nv.Sortk, DT: dt.String(), P: nv.Name, Ty: tyShortNm} //nv.Ty}
			} else {
				panic(fmt.Errorf(" nv.Value is not an String "))
			}

		case "ty": // node type entry

			// null value for predicate ie. not defined in item. Set value to 0 and use XB to identify as null value
			if s, ok := nv.Value.(string); ok {
				if s, ok = types.GetTyShortNm(s); !ok {
					syslog(fmt.Sprintf("Error: type name %q not found in types.GetTyShortNm \n", nv.Ty))
					return
				}
				n := mut.NewInsert(tbl.Block).AddMember("PKey", UID)
				// all type attributes should have vlaue  <graphName>|<ShortTypeName>
				n.AddMember("Graph", types.GraphSN()).AddMember("Ty", s).AddMember("IsNode", "Y")
				// database specific code - TODO: try and eliminate this
				if param.DB == param.Dynamodb {
					n.AddMember("SortK", types.GraphSN()+"|A#A#T")
				}
				txh.Add(n)
				// for dynamodb only
				if param.DB == param.Dynamodb {
					n = mut.NewInsert(tbl.Block).AddMember("PKey", UID)
					// all type attributes should have vlaue  <graphName>|<ShortTypeName>
					n.AddMember("Graph", types.GraphSN()).AddMember("Ty", types.GraphSN()+"|"+s).AddMember("IsNode", "Y").AddMember("IX", "X")
					// database specific code - TODO: try and eliminate this
					n.AddMember("SortK", "A#A#T")
					// hasGSI: Ty,IX
					txh.Add(n)
				}
				txComplete = true
			} else {
				panic(fmt.Errorf(" nv.Value is not an string for attribute %s ", nv.Name))
			}

		case "Bl":

			if f, ok := nv.Value.(bool); ok {
				// populate with dummy item to establish LIST
				m.AddMember("Bl", f).AddMember("P", ga.String())
				//a := Item{PKey: UID, SortK: nv.Sortk, Bl: f, P: nv.Name, Ty: tyShortNm} //nv.Ty}
			} else {
				panic(fmt.Errorf(" nv.Value is not an BL for attribute %s ", nv.Name))
			}

		case "B":

			if f, ok := nv.Value.([]byte); ok {
				// populate with dummy item to establish LIST
				m.AddMember("B", f).AddMember("P", ga.String())
				//a := Item{PKey: UID, SortK: nv.Sortk, B: f, P: nv.Name, Ty: tyShortNm} //nv.Ty}
			} else {
				panic(fmt.Errorf(" nv.Value is not an []byte "))
			}

		case "LI":

			//m.SetOpr(mut.Merge)
			if i, ok := nv.Value.([]int64); ok {
				// populate with dummy item to establish LIST
				m.AddMember("LI", i)
				//a := Item{PKey: UID, SortK: nv.Sortk, LN: f, Ty: tyShortNm}
			} else {
				panic(fmt.Errorf(" nv.Value is not an []int64 for attribute, %s. Type: %T ", nv.Name, nv.Value))
			}

		case "LF":

			//m.SetOpr(mut.Merge)
			if f, ok := nv.Value.([]float64); ok {
				// populate with dummy item to establish LIST
				m.AddMember("LF", f)
				//a := Item{PKey: UID, SortK: nv.Sortk, LN: f, Ty: tyShortNm}
			} else {
				panic(fmt.Errorf(" nv.Value is not an LF for attribute %s ", nv.Name))
			}

		case "LS":

			//m.SetOpr(mut.Merge)
			if s, ok := nv.Value.([]string); ok {
				// populate with dummy item to establish LIST
				m.AddMember("LS", s)
				//a := Item{PKey: UID, SortK: nv.Sortk, LN: f, Ty: tyShortNm}
			} else {
				panic(fmt.Errorf(" nv.Value is not an LF for attribute %s ", nv.Name))
			}

		case "LDT":

			//m.SetOpr(mut.Merge)
			if dt, ok := nv.Value.([]string); ok {
				// populate with dummy item to establish LIST
				m.AddMember("LDT", dt)
				//a := Item{PKey: UID, SortK: nv.Sortk, LN: f, Ty: tyShortNm}
			} else {
				panic(fmt.Errorf(" nv.Value is not an LF for attribute %s ", nv.Name))
			}

			// TODO: others LBl, LB,

		// case "SI":

		// 	if f, ok := nv.Value.([]int); ok {
		// 		// populate with dummy item to establish LIST
		// 		m.AddMember(nv.SortK, f)
		// 		m.AddMember(nv.SortK, tyShortNm)
		// 		//a := Item{PKey: UID, SortK: nv.Sortk, SN: f, Ty: tyShortNm}
		// 	} else {
		// 		panic(fmt.Errorf(" nv.Value is not a slice of SI for attribute %s ", nv.Name))
		// 	}

		// case "SF":

		// 	if f, ok := nv.Value.([]float64); ok {
		// 		// populate with dummy item to establish LIST
		// 		m.AddMember(nv.SortK, f)
		// 		m.AddMember(nv.SortK, tyShortNm)
		// 		//a := Item{PKey: UID, SortK: nv.Sortk, SN: f, Ty: tyShortNm}
		// 	} else {
		// 		panic(fmt.Errorf("SF nv.Value is not an slice float64 "))
		// 	}

		// case "SBl":

		// 	if f, ok := nv.Value.([]bool); ok {
		// 		// populate with dummy item to establish LIST
		// 		m.AddMember(nv.SortK, f)
		// 		m.AddMember(nv.SortK, tyShortNm)
		// 		//a := Item{PKey: UID, SortK: nv.Sortk, SBl: f, Ty: tyShortNm}
		// 	} else {
		// 		panic(fmt.Errorf("Sbl nv.Value is not an slice of bool "))
		// 	}

		// case "SS":

		// 	if f, ok := nv.Value.([]string); ok {
		// 		// populate with dummy item to establish LIST
		// 		m.AddMember(nv.SortK, f)
		// 		m.AddMember(nv.SortK, tyShortNm)
		// 		//a := Item{PKey: UID, SortK: nv.Sortk, SS: f, Ty: tyShortNm}
		// 	} else {
		// 		panic(fmt.Errorf(" SSnv.Value is not an String Set for attribte %s ", nv.Name))
		// 	}

		// case "SB":

		// 	if f, ok := nv.Value.([][]byte); ok {
		// 		// populate with dummy item to establish LIST
		// 		m.AddMember(nv.SortK, f)
		// 		m.AddMember(nv.SortK, tyShortNm)
		// 		//a := Item{PKey: UID, SortK: nv.Sortk, SB: f, Ty: tyShortNm}
		// 	} else {
		// 		panic(fmt.Errorf("SB nv.Value is not an Set of Binary for predicate %s ", nv.Name))
		// 	}

		case "Nd":

			//m.SetOpr(mut.Merge)
			// convert node blank name to UID
			// xf := make([]int64, 1, 1)
			// xf[0] = blk.ChildUID
			// id := make([]int, 1, 1)
			// id[0] = 0
			if f, ok := nv.Value.([]string); ok {
				// populate with dummy item to establish LIST
				// uid := make([][]byte, len(f), len(f))
				// xf := make([]int64, len(f), len(f))
				// id := make([]int64, len(f), len(f))
				for i, n := range f {
					request := uuid.Request{SName: n, RespCh: localCh}
					//syslog(fmt.Sprintf("UID Nd request  : %#v", request))

					uuid.ReqCh <- request

					UID := <-localCh

					// uid[i] = []byte(UID)
					// xf[i] = blk.ChildUID
					// id[i] = 0

				}
				//NdUid = UID // save to use to create a Type item
				//m := mut.NewMutation(tbl.EOP, UID, nv.Sortk, mut.Insert)
				// m := mut.NewInsert(tbl.EOP).AddMember("PKey", UID).AddMember("SortK", sk.String())
				// // add a null value for array types - which allows future any updates, when attaching the node) to use the APPEND attribute operator
				// m.AddMember("Nd", uid)
				// m.AddMember("XF", xf)
				// m.AddMember("Id", id)
				// // populate P,N to capture edge count (child nodes) into P_N GSI
				// m.AddMember("P", ga.String()).AddMember("N", 0)
				// tysn, _ := types.GetTyShortNm(nv.Ty)
				// m.AddMember("Ty", tysn)
				// txh.Add(m)
				txComplete = true
				//a := Item{PKey: UID, SortK: nv.Sortk, Nd: uid, XF: xf, Id: id, Ty: tyShortNm}
			} else {
				panic(fmt.Errorf(" Nd nv.Value is not an string slice "))
			}
		}
		if !txComplete {
			txh.Add(m)
		}
	}

	err = txh.Execute()

	if err != nil {
		//syslog(fmt.Sprintf("Errored for %s: [%s]", sname, err.Error()))
		errlog.Add(logid, err)
	} else {
		syslog(fmt.Sprintf("Finished successfully for %s", sname))
	}
	//
	// expand Set and List types into individual S# entries to be indexed// TODO: what about SN, LN
	//
	// for _, nv := range nv_ {

	// 	input := tx.NewInput()

	// 	// append child attr value to parent uid-pred list
	// 	switch nv.DT {

	// 	case "SS":

	// 		var sk string
	// 		if ss, ok := nv.Value.([]string); ok {
	// 			//
	// 			input := tx.NewInput(UID)

	// 			for i, s := range ss {

	// 				if tyShortNm, ok = types.GetTyShortNm(nv.Ty); !ok {
	// 					syslog(fmt.Sprintf("Error: type name %q not found in types.GetTyShortNm \n", nv.Ty))
	// 					panic(fmt.Errorf("Error: type name %q not found in types.GetTyShortNm \n", nv.Ty))
	// 					return
	// 				}

	// 				sk = "S#:" + nv.C + "#" + strconv.Itoa(i)
	// 				m.SetKey(NodeScalar, UID, sk)
	// 				m.AddMember(sk, nv.Name)
	// 				m.AddMember(sk, s)
	// 				m.AddMember(sk, tyShortNm)
	// 				//a := Item{PKey: UID, SortK: sk, P: nv.Name, S: s, Ty: tyShortNm} //nv.Ty}
	// 				inputs.Add(inputs, input)
	// 			}
	// 		}

	// 	case "SI":

	// 		type Item struct {
	// 			PKey  []byte
	// 			SortK string
	// 			P     string // Dynamo will use AV List type - will convert to SS in convertSet2list()
	// 			N     float64
	// 			Ty    string
	// 		}
	// 		var sk string
	// 		input := tx.NewInput(UID)
	// 		if si, ok := nv.Value.([]int); ok {
	// 			//
	// 			for i, s := range si {

	// 				if tyShortNm, ok = types.GetTyShortNm(nv.Ty); !ok {
	// 					syslog(fmt.Sprintf("Error: type name %q not found in types.GetTyShortNm \n", nv.Ty))
	// 					panic(fmt.Errorf("Error: type name %q not found in types.GetTyShortNm \n", nv.Ty))
	// 					return
	// 				}

	// 				sk = "S#:" + nv.C + "#" + strconv.Itoa(i)
	// 				m.SetKey(NodeScalar, UID, sk)
	// 				m.AddMember(sk, nv.Name)
	// 				m.AddMember(sk, float64(s))
	// 				m.AddMember(sk, tyShortNm)
	// 				//a := Item{PKey: UID, SortK: sk, P: nv.Name, N: float64(s), Ty: tyShortNm} //nv.Ty}
	// 				inputs.Add(inputs, input)
	// 			}
	// 		}

	// case "LS":

	// 	var sk string
	// 	if ss, ok := nv.Value.([]string); ok {
	// 		//
	// 		input := tx.NewInput(UID)
	// 		for i, s := range ss {

	// 			sk = "S#:" + nv.C + "#" + strconv.Itoa(i)
	// 			m.SetKey(NodeScalar, UID, sk)
	// 			m.AddMember(sk, nv.Name)
	// 			m.AddMember(sk, s)
	// 			//m.AddMember(sk,  tyShortNm) //TODO: should this be included?
	// 			inputs.Add(inputs, input)
	// 			//a := Item{PKey: UID, SortK: sk, P: nv.Name, S: s}
	// 		}
	// 	}

	//}
	//
	//}

}
