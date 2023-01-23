//go:build dynamodb
// +build dynamodb

package block

import (
	"strings"
	"time"

	"github.com/ros2hp/method-db/uuid"
)

type DynaGType uint8

//type UIDstate int8

const (
	// Propagation UID flags. XF flag
	ChildUID        int64 = iota + 1
	CuidInuse             // deprecated
	UIDdetached           // soft delete. Child detached from parent.
	OvflBlockUID          // this entry represents an overflow block. Current batch id contained in Id.
	OuidInuse             // deprecated
	OBatchSizeLimit       // overflow batch reached max entries - stop using. Will force creating of new overflow block or a new batch.
	EdgeFiltered          // set to true when edge fails GQL uid-pred  filter
)

const (
	//
	// scalar types
	//
	N  DynaGType = iota // number
	S                   // string
	Bl                  // bool
	B                   // []byte
	DT                  // DateTime
	//
	// List (ordered set of any type but constrainted to a single type in DynaGraph)
	//
	LS  // []string
	LN  // []string
	LB  // [][]byte
	LBl // []bool
	LDT
	Nd // [][]byte // list of node UIDs
	//
	// Set (unordered set of a single type)
	//
	NS // []string
	SS // []string
	BS // [][]byte
	//
)

// DataItem  maps database attribute (and its associated type) to golang type.
// Used during the database fetch of node block data into the memory cache.
// ToStruct(<dataItem>) for Spanner and dynamodbattribute.UnmarshalListOfMaps(result.Items, &data) for dynamodb.
// Data in then used to populate the NV.value attribute in the UnmarshalCache() ie. from []DataItem -> NV.value
// via the DataItem Get methods
type DataItem struct {
	Pkey  []byte `dynamodbav:"PKey"` // uuid.UID
	Sortk string `dynamodbav:"SortK"`
	//
	Graph  string
	IsNode string
	IX     string // used during double-propagation load "X": not processed, "Y": processed
	//
	OP []byte // parent UID in overflow blocks
	//attrName string
	//
	// scalar types
	//
	// F float64 // spanner float
	// I int64   // spanner int
	N float64 //edge count (dynamodb, spanner), integer, float DTs (dynamodb)
	//N []big.Rat      Spanner: if Numeric is chosen to store all F and I types
	S  string
	Bl bool
	B  []byte
	DT time.Time // DateTime
	//
	// node type - listed in GSI so value can be associated with type for "has" operator
	Ty string // type of node
	// child node counters
	ASZ int64 // attribute of  overflow batch UID-PRED.
	//
	// List (ordered set of any type but constrainted to a single type in DynaGraph)
	//
	LS []string
	LF []float64
	//LN []big.Rat       if Numeric is chosen to store all F and I types
	LI  []int64
	LB  [][]byte
	LBl []bool
	LDT []time.Time
	//
	PBS [][]byte
	BS  [][]byte
	//
	Nd [][]byte //uuid.UID // list of node UIDs, overflow block UIDs, oveflow index UIDs
	//
	// Set (unordered set of a single type)
	//
	// NS []float64
	// SS []string
	// BS [][]byte
	// base64.StdEncoding.Decode requires []byte argument hence XB [][]byte (len 1) rather thab []byte
	// also Dynamo ListAppend only works with slices so []byte to [][]byte
	// note: I use XB rather than X as X causes a Unmarshalling error. See description of field in doco.
	XBl []bool  // used for propagated child scalars (List data). True means associated child value is NULL (ie. is not defined)
	XF  []int64 // used in uid-predicate 1 : c-UID, 2 : c-UID is soft deleted, 3 : ovefflow UID, 4 : overflow block full
	Id  []int64 // current maximum overflow batch id. Maps to the overflow item number in Overflow block e.g. A#G:S#:A#3 where Id is 3 meaning its the third item in the overflow block. Each item containing 500 or more UIDs in Lists.
	//
}
type NodeBlock []*DataItem

// channel payload used in client.AttachNode()
type ChPayload struct {
	TUID    uuid.UID // target UID for scalar propagation
	CUID    uuid.UID
	NdIndex int   // index into nd,xf,id
	BatchId int64 // overflow block batch id
	// nodedata for uid-predicate
	DI *DataItem
	//
	Osortk string // overflow sortk
	// parent node type
	PTy TyAttrBlock
	//
	Random bool
}

// keys
func (dgv *DataItem) GetPkey() []byte {
	return dgv.Pkey
}
func (dgv *DataItem) GetSortK() string {
	return dgv.Sortk
}

// Scalars - scalar data has no associated XBl null inidcator. Absense of item/predicate means it is null.
func (dgv *DataItem) GetS() string {
	return dgv.S
}

func (dgv *DataItem) GetTy() string {
	return dgv.Ty[strings.Index(dgv.Ty, "|")+1:]
	//	return dgv.Ty
}

// Dynamodb TODO: use compiler directives to use slightly differnt GetI()
// func (dgv *DataItem) GetI() int64 {
// 	//	return dgv.I // TODO: how to handle. SQL uses I & F no-SQL uses N. Maybe both SQL & non-SQL use I & F which are both Number type in no-SQL ???
// 	return int64(dgv.N)

// }

// Spanner
func (dgv *DataItem) GetI() int64 {
	//return dgv.I // TODO: how to handle. SQL uses I & F no-SQL uses N. Maybe both SQL & non-SQL use I & F which are both Number type in no-SQL ???
	return int64(dgv.N) // for dynamodb only. All numbers (int, floats) stored in N attribute (dtype: Number)
}

func (dgv *DataItem) GetF() float64 {
	return dgv.N
}

func (dgv *DataItem) GetN() float64 {
	return dgv.N
}

func (dgv *DataItem) GetDT() time.Time {
	//t, _ := time.Parse(time.RFC3339, dgv.DT)
	//return t
	return dgv.DT
}
func (dgv *DataItem) GetB() []byte {
	return dgv.B
}
func (dgv *DataItem) GetBl() bool {
	return dgv.Bl
}

// func (dgv *DataItem) GetIS() []int64 {
// 	is := make([]int64, len(dgv.NS), len(dgv.NS))
// 	for i, _ := range dgv.NS {
// 		is[i] = int64(dgv.NS[i])
// 	}
// 	//dgv.NS = nil // free
// 	return is
// }

// func (dgv *DataItem) GetFS() []float64 {
// 	return dgv.NS
// }
// func (dgv *DataItem) GetBS() [][]byte {
// 	return dgv.BS
// }

// Lists - embedded in item
func (dgv *DataItem) GetLS() []string {
	return dgv.LS
}

// TODO - should this be []int??
func (dgv *DataItem) GetLI() []int64 {
	return dgv.LI
}

func (dgv *DataItem) GetLF() []float64 {
	return dgv.LF
}

func (dgv *DataItem) GetLB() [][]byte {
	return dgv.LB
}
func (dgv *DataItem) GetLBl() []bool {
	return dgv.LBl
}

// Lists - only used for containing propagated values
func (dgv *DataItem) GetULS() ([]string, []bool) {
	return dgv.LS, dgv.XBl
}

// TODO - should this be []int??
func (dgv *DataItem) GetULI() ([]int64, []bool) {
	// is := make([]int64, len(dgv.LN), len(dgv.LN))
	// for i, _ := range dgv.LN {
	// 	is[i] = int64(dgv.LN[i])
	// }
	//dgv.LN = nil // free
	return dgv.LI, dgv.XBl
}

func (dgv *DataItem) GetULF() ([]float64, []bool) {
	return dgv.LF, dgv.XBl
}
func (dgv *DataItem) GetULB() ([][]byte, []bool) {
	return dgv.LB, dgv.XBl
}
func (dgv *DataItem) GetULBl() ([]bool, []bool) {
	return dgv.LBl, dgv.XBl
}

func (dgv *DataItem) GetId() []int64 {
	//return dgv.Id[1:]
	return dgv.Id[:]
}

func (dgv *DataItem) GetXF() []int64 {
	//return dgv.XF[1:]
	return dgv.XF[:]
}

// Propagated Scalars - all List based (UID-pred stuff)
//
//	func (dgv *DataItem) GetULS() ([]string, []bool) {
//		return dgv.LS, dgv.XBl
//	}
//
//	func (dgv *DataItem) GetULI() ([]int64, []bool) {
//		is := make([]int64, len(dgv.LN), len(dgv.LN))
//		for i, _ := range dgv.LN {
//			is[i] = int64(dgv.LN[i])
//		}
//		//dgv.LN = nil // free
//		return is
//	}
//
//	func (dgv *DataItem) GetULF() ([]float64, []bool) {
//		return dgv.LN
//	}
//
//	func (dgv *DataItem) GetULB() ([][]byte, []bool) {
//		return dgv.LB
//	}
//
//	func (dgv *DataItem) GetULBl() ([]bool, []bool) {
//		return dgv.LBl
//	}
func (dgv *DataItem) GetNd() (nd [][]byte, xf []int64, ovfl [][]byte) {

	// nd_ := dgv.Nd[1:]
	// xf_ := dgv.XF[1:]
	nd_ := dgv.Nd[:]
	xf_ := dgv.XF[:]
	for i, v := range nd_ {

		if x := xf_[i]; x <= UIDdetached {
			//	ChildUID ,	CuidInuse  ,UIDdetached
			nd = append(nd, uuid.UID(v))
			xf = append(xf, x)
		} else {
			// 	OvflBlockUID  ,	OuidInuse      ,OBatchSizeLimit
			ovfl = append(ovfl, uuid.UID(v))
		}
	}
	return
}

// GetOfNd() takes a copy of the data cache result and returns the copy
// The data cache should be protected by a read lock when the copy is taken
// After the cache is read it should be unlocked. The copy can be accessed after
// the lock is released as its a different memory object.
func (dgv *DataItem) GetOfNd() ([][]byte, []int64) {

	//return dgv.Nd[1:], dgv.XF[1:]
	return dgv.Nd[:], dgv.XF[:]
}

type OverflowItem struct {
	PKey  []byte
	SortK string // "A#G#:N@1","A#G#:N@2"
	//
	// List (ordered set of any type but constrainted to a single type in DynaGraph)
	//
	Nd [][]byte // list of child node UIDs
	B  []byte   // parent UID
	// scalar data
	LS []string
	LI []int64
	LF []float64
	//LN  []float64
	LB  [][]byte
	LBl []bool
	LDT []string // DateTime
	// flags
	XBl []bool
}

type OverflowBlock [][]*OverflowItem

type Index struct {
	PKey  []byte
	SortK string
	//
	Ouid [][]byte // overflow block UIDs
	//
	XB [][]byte
	XF [][]int
}

type IndexBlock []*Index

//
// ClientNV from AttachNode is persisted using this struct
//
// type StreamCnv struct {
// 	PKey  []byte // ClientNV UID (not node UID)
// 	SortK string // predicate
// 	//attrName string
// 	//
// 	// scalar value to propagate
// 	//
// 	N  float64 // numbers are represented by strings and converted on the fly to dynamodb number
// 	S  string
// 	Bl bool
// 	B  []byte
// }

// func (nv *StreamCnv) GetB() []byte {
// 	return nv.B
// }
// func (nv *StreamCnv) GetBl() bool {
// 	return nv.Bl
// }
// func (nv *StreamCnv) GetDT() time.Time {
// 	t, _ := time.Parse(time.RFC3339, nv.DT)
// 	return t
// }
// func (nv *StreamCnv) GetS() string {
// 	return nv.S
// }
// func (nv *StreamCnv) GetI() int64 {
// 	i := int64(nv.N)
// 	return i
// }
// func (nv *StreamCnv) GetF() float64 {
// 	return nv.N
// }

// type dictionary
type TyItem struct {
	Nm   string   `dynamodbav:"PKey"`  // type name
	Atr  string   `dynamodbav:"SortK"` // attribute name
	Ty   string   // DataType
	F    []string // facets name#DataType#CompressedIdentifer
	C    string   // short name for attribute
	P    string   // data partition containig attribute data - TODO: is this obselete???
	Pg   bool     // true: propagate scalar data to parent
	N    bool     // NULLABLE. False : not null (attribute will always exist ie. be populated), True: nullable (attribute may not exist)
	Cd   int      // cardinality - NOT USED
	Sz   int      // average size of attribute data - NOT USED
	Ix   string   // supported indexes: FT=Full Text (S type only), "x" combined with Ty will index in GSI Ty_Ix
	IncP []string // (optional). List of attributes to be propagated. If empty all scalars will be propagated.
	//	cardinality string   // 1:N , 1:1
}

type TyIBlock []*TyItem

// type attribute-block-derived from TyItem
type TyAttrD struct {
	Name string // Attribute Identfier
	DT   string // Derived value. Attribute Data Type - Nd (for uid-pred attribute only), (then scalars) DT,I,F,S etc
	C    string // Attribute short identifier
	Ty   string // For uid-pred only, the type it respresents e.g "Person"
	P    string // data partition (aka shard) containing attribute
	N    bool   // true: nullable (attribute may not exist) false: not nullable
	Pg   bool   // true: propagate scalar data to parent
	IncP []string
	Ix   string // index type
	Card string
}

type TyAttrBlock []TyAttrD

func (t TyAttrBlock) GetUIDpredC() []string {
	var predC []string
	for _, v := range t {
		if v.DT == "Nd" {
			predC = append(predC, v.C)
		}
	}
	return predC
}

//
// type TyCache map[Ty]blk.TyAttrBlock
// var TyC TyCache
// type TyAttrCache map[Ty_Attr]blk.TyAttrD // map[Ty_Attr]blk.TyItem
// var TyAttrC TyAttrCache

// ***************** rdf ***********************

type ObjT struct {
	Ty    string      // type def from Type
	Value interface{} // rdf object value
}

// RDF SPO
type RDF struct {
	Subj string //[]byte // subject
	Pred string // predicate
	Obj  ObjT   // object
}
