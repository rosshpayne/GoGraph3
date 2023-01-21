package ds

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	blk "github.com/GoGraph/block"
	"github.com/GoGraph/tx/uuid"
)

// NV (Name-Value) links the graph query to the graph data.
// The Name in NV represents each type of data, <attributeName> for scalar data, <uid-pred:> for each uid-pred
// and <uid-pred:child-scalar-attribute> for each scalar attribute.
// The Value represents the attribute data each a scalar quantity of an array (Nd, []int64 ...) representing the child node data.
// The usual process is to define a set of NV values for a node in the query - which is called generating the NV (genNV())
// For a root node the generated NV will contain the scalar attributes plus the UID-Preds and their scalar values.
// For a UID-Pred node the generated NV will contain the enclosed uid-preds and their scalar values.
// Once a UID's data is fetched from the db it is then unmarshalled from its cache representation into the NV.
// The nodes data in the cache can contain its scalar data, each UID-Pred (child uids) and an array for each scalar attribute
// (representing the child node data).
//
// Usual process when executing a graph query
//  1. generate NVs e.g. Name is populated from graph query but Value is nil
//     Age
//     Name
//     Siblings:
//     Siblings:Name
//     Siblings:Age
//
// Note the above data matches the database data for a node which contains both the scalar and propagated data (child scalar data) for an inidividual node.
// So generateing an NV will only go as far as the scalar attribute of the child nodes.
//
// 2. From the NV name generate a sortk or set of sortk values
// 3. Using the sortk values to query the database for a particular UID that matches graph element from which the NV names were generated
// 4. The cache is now populated with the UID node data - unmarshal this data into the Value of each NV based on the Name.
// 5. For the AST in a query, add the NV to the graph element (method: assignData()) to either the root stmt or uid-pred. map[uid]*[]NV
//
// Note in the AST the scalar data for a node is held in the UID-PRED attribute of the parent node - this matches the
// data format in the database which contains the node scalar data and all the children scalar data (propagated data)
// Scalar data is always queried from the propagated data of the parent node, never at the child node level.
// Similarly the node contains the scalar data of the embedded UID-PREDs of the node.
type NV struct {
	Name     string      // predicate from graphQL stmt (no type required as its based on contents in cache) Name (S), Age (N), Siblings: (Nds), Siblings:Name (list), Friends: (nds), Friends:Name (list)
	Value    interface{} // its value from cache
	Filtered bool        // true indicates item satisifed uid-pred filter condition and should be ignored when marshaling
	ItemTy   string      // populated in UnmarshalCache(). Query root node type. TODO: where used??
	Ignore   bool
	// short name of the type Name belongs to e.g. Pn (for Person)
	//
	// used by UnmarshalCache
	D      sync.Mutex
	Sortk  string
	OfUIDs [][]byte // overflow blocks ids
	// ... for Overflow blocks only
	State [][]int64 // Nd only (propagated child UIDs only) - states: cuid, cuid-detached, cuid-filtered
	Null  [][]bool  // For propagated (child) scalar values only first slice reps Overflow block, second reps each child node in the overflow block
}

type ClientNV []*NV

type NVmap map[string]*NV

func (c ClientNV) MarshalJSON() {

	fmt.Println("MarshalJSON...")

	var s strings.Builder

	for _, v := range c {

		fmt.Println("v.Name ", v.Name)

		switch x := v.Value.(type) {
		case int64: //

			s.WriteString(`{ "`)
			s.WriteString(v.Name)
			s.WriteString(`" : "`)
			i64 := strconv.Itoa(int(x))
			s.WriteString(i64)
			s.WriteString(`"} `)

		case float64: //

			s.WriteString(`{ "`)
			s.WriteString(v.Name)
			s.WriteString(`" : "`)
			s64 := strconv.FormatFloat(x, 'E', -1, 64)
			s.WriteString(s64)
			s.WriteString(`"}`)

		case string: // S, DT

			s.WriteString(`{ "`)
			s.WriteString(v.Name)
			s.WriteString(`" : "`)
			s.WriteString(x)
			s.WriteString(`" }`)

		case bool: // Bl

			s.WriteString(`{ "`)
			s.WriteString(v.Name)
			s.WriteString(`" : "`)
			if x {
				s.WriteString("true")
			} else {
				s.WriteString("false")
			}
			s.WriteString(` } }`)

		case []byte: // B

			s.WriteString(`{ "`)
			s.WriteString(v.Name)
			s.WriteString(`" : "`)
			s.WriteString(string(x))
			s.WriteString(`" }`)

		case []string: // LS, NS

			s.WriteString(`{ "`)
			s.WriteString(v.Name)
			s.WriteString(`" : [`)
			for i, k := range x {
				s.WriteByte('"')
				s.WriteString(k)
				s.WriteByte('"')
				if i < len(x)-1 {
					s.WriteByte(',')
				}
				if i > 10 {
					s.WriteString("...")
					break
				}
			}
			s.WriteString(` ] }`)

		case []float64: // LN, NS

			s.WriteString(`{ "`)
			s.WriteString(v.Name)
			s.WriteString(`" : [ `)
			for i, k := range x {
				s.WriteByte('"')
				s64 := strconv.FormatFloat(k, 'E', -1, 64)
				s.WriteString(s64)
				s.WriteByte('"')
				if i < len(x)-1 {
					s.WriteByte(',')
				}
				if i > 10 {
					s.WriteString("...")
					break
				}
			}
			s.WriteString(` ] }`)

		case []int64: //

			s.WriteString(`{ "`)
			s.WriteString(v.Name)
			s.WriteString(`" : [ `)
			for i, k := range x {
				s.WriteByte('"')
				i64 := strconv.Itoa(int(k))
				s.WriteString(i64)
				s.WriteByte('"')
				if i < len(x)-1 {
					s.WriteByte(',')
				}
				if i > 10 {
					s.WriteString("...")
					break
				}
			}
			s.WriteByte('}')

		case []bool: // LBl

			s.WriteString(`{ "`)
			s.WriteString(v.Name)
			s.WriteString(`" : [ `)
			for i, k := range x {
				if k {
					s.WriteString("true")
				} else {
					s.WriteString("false")
				}
				if i < len(x)-1 {
					s.WriteByte(',')
				}
				if i > 10 {
					s.WriteString("...")
					break
				}
			}
			s.WriteString(` ] }`)

		case [][]byte: // BS

			s.WriteString(`{ "`)
			s.WriteString(v.Name)
			s.WriteString(`" : [ `)
			for i, k := range x {
				s.WriteByte('"')
				s.WriteString(string(k))
				s.WriteByte('"')
				if i < len(x)-1 {
					s.WriteByte(',')
				}
				if i > 10 {
					s.WriteString("...")
					break
				}
			}
			s.WriteString(` ] }`)

		case [][]int64: // Nd

			s.WriteString(`{ "`)
			s.WriteString(v.Name)
			s.WriteString(`" : [ `)
			for i, k := range x {
				//
				for ii, kk := range k {
					// is node not attached - hence its values should not be printed.
					if v.State[i][ii] == blk.UIDdetached {
						continue
					}
					// is value null
					if v.Null[i][ii] {
						s.WriteString("_NULL_")
					} else {
						// each int matches to one child UID
						s.WriteByte('"')
						i64 := strconv.Itoa(int(kk))
						s.WriteString(i64)
						s.WriteByte('"')
					}
					if ii < len(k)-1 {
						s.WriteByte(',')
					}
					if ii > 10 {
						s.WriteString("... ]")
						break
					}
				}
			}
			s.WriteString(` ] }`)

		case [][]float64: // Nd

			s.WriteString(`{ "`)
			s.WriteString(v.Name)
			s.WriteString(`" : [ `)
			for i, k := range x {
				// check if null
				for ii, kk := range k {
					// is node not attached - hence its values should not be printed.
					if v.State[i][ii] == blk.UIDdetached {
						continue
					}
					// is value null
					if v.Null[i][ii] {
						s.WriteString("_NULL_")
					} else {
						// each int matches to one child UID
						s.WriteByte('"')
						s64 := strconv.FormatFloat(kk, 'E', -1, 64)
						s.WriteString(s64)
						s.WriteByte('"')
					}
					if ii < len(k)-1 {
						s.WriteByte(',')
					}
					if ii > 10 {
						s.WriteString("... ]")
						break
					}
				}
			}
			s.WriteString(`] }`)

		case [][]string:

			s.WriteString(`{ "`)
			s.WriteString(v.Name)
			s.WriteString(`" : [ `)

			// for i, v := range x {
			// 	fmt.Println("X: ", i, v)
			// }
			// for i, v := range v.State {
			// 	fmt.Println("State: ", i, v)
			// }
			for i, k := range x {
				//
				for ii, kk := range k {
					// is node attached .
					if v.State[i][ii] == blk.UIDdetached {
						continue
					}
					// is value null
					if v.Null[i][ii] {
						s.WriteString("_NULL_")
					} else {
						// each int matches to one child UID
						s.WriteByte('"')
						s.WriteString(kk)
						s.WriteByte('"')
					}
					if ii < len(k)-1 {
						s.WriteByte(',')
					}
					if ii > 10 {
						s.WriteString("... ]")
						break
					}
				}
			}
			s.WriteString(`] }`)

		case []uuid.UID: // Nd

			s.WriteString(`{ "`)
			s.WriteString(v.Name)
			s.WriteString(`" : [ `)
			for i, k := range x {
				s.WriteByte('"')
				//	s.WriteString("__XX__")

				kk := k.String()
				s.WriteString(kk)
				s.WriteByte('"')
				if i < len(x)-1 {
					s.WriteByte(',')
				}
				if i > 10 {
					s.WriteString("...")
					break
				}
			}
			s.WriteString(` ] }`)

		case nil:
			// no interface value assigned
			s.WriteString(`{ "`)
			s.WriteString(v.Name)
			s.WriteString(" : Null")
			s.WriteString(`}"`)

		}
		s.WriteByte('\n')
	}
	fmt.Println(s.String())

}

// referenced in gql package

type NodeResult struct {
	PKey  uuid.UID
	SortK string
	Ty    string
}

type (
	QResult  []NodeResult
	AttrName = string
)
