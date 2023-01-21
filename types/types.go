package types

import (
	"fmt"
	"strings"

	blk "github.com/GoGraph/block"
	slog "github.com/GoGraph/syslog"
)

//
// TODO: types should be a service because of global package variables (cache maps)
//       not because of goroutines but potentially multi-sessions issuing SetGraph() etc
//       multi-session out of scope for the moment.

const (
	logid = "types"
)

type Ty = string     // type
type TyAttr = string // type:attr
type AttrTy = string // attr#ty

// type FacetIdent string // type:attr:facet
//
// Derived Type Attributes cache
type TyCache map[Ty]blk.TyAttrBlock

// caches for type-attribute and type-attribute-facet
type TyAttrCache map[TyAttr]blk.TyAttrD // map[TyAttr]blk.TyItem

//TODO: create a cache for lookup via attribute long name to get type, type-short-name, attribute-short-name. This map will be used to support the Has function.

type AttrTyCache map[AttrTy]string

//var TyAttrC TyAttrCache

type TypeCache struct {
	//sync.RWMutex // as all types are loaded at startup - no concurrency control required
	TyAttrC TyAttrCache
	TyC     TyCache
	AttrTy  AttrTyCache
}

var (
	graph     string
	graphSN   string // graph short name
	TypeC     TypeCache
	tyShortNm map[string]string
)

func logerr(e error, panic_ ...bool) {

	if len(panic_) > 0 && panic_[0] {
		slog.LogFail(logid, e)
		panic(e)
	}
	slog.Log(logid, e.Error())
}

// GraphName returns graph's name (long name)
func GraphName() string {
	return graph
}

// GraphSN returns graph's short name
func GraphSN() string {
	return graphSN[:len(graphSN)-1]
}

func GetAllTy() map[string]string {
	return tyShortNm
}

func GetTyShortNm(longNm string) (string, bool) {
	s, ok := tyShortNm[longNm]
	return s, ok
}

func GetTyLongNm(shortTyNm string) (string, bool) {
	// for shortNm, longNm := range tyShortNm {
	// 	if tyNm == longNm {
	// 		return shortNm, true
	// 	}
	// }
	for longNm, shortNm := range tyShortNm {
		if shortTyNm == shortNm {
			return longNm, true
		}
	}
	return "", false
}

func syslog(s string) {
	slog.Log(logid, s)
}

func GetTyAttr(ty string, attr string) TyAttr {
	var s strings.Builder
	// generte key for TyAttrC:  <typeName>:<attrName> e.g. Person:Age
	s.WriteString(ty)
	s.WriteByte(':')
	s.WriteString(attr)
	return s.String()
}

func SetGraph(graph_ string) error {

	var err error

	graph = graph_
	graphSN, err = setGraph(graph)
	if err != nil {
		return err
	}
	//
	// cache holding the attributes belonging to a type
	///
	TypeC.TyC = make(TyCache)
	//
	// DataTy caches for type-attribute and type-attribute-facet
	//
	TypeC.TyAttrC = make(TyAttrCache)
	//
	TypeC.AttrTy = make(AttrTyCache)
	//
	tynames, err := GetTypeShortNames()
	if err != nil {
		panic(err)
	}
	if len(tynames) == 0 {
		panic(fmt.Errorf("No short name type data loaded"))
	}
	//
	// populate type short name cache. This cache is conccurent safe as it is readonly from now on.
	//
	tyShortNm = make(map[string]string)
	for _, v := range tynames {
		tyShortNm[v.LongNm] = v.ShortNm
	}
	//
	// Load data dictionary (i.e ALL type info) - makes for concurrent safe FetchType()
	//
	{
		dd, err := LoadDataDictionary() // type TyIBlock []TyItem
		if err != nil {
			panic(err)
		}
		populateTyCaches(dd)
	}

	return nil
}

func populateTyCaches(allTypes blk.TyIBlock) {
	var (
		tyNm  string
		a     blk.TyAttrD
		tc    blk.TyAttrBlock
		tyMap map[string]bool // contains aggregate/nested type e.g. Person, Film, Genre, Performance
	)
	tyMap = make(map[string]bool)

	for _, v := range allTypes {
		tyNm = v.Nm[strings.Index(v.Nm, ".")+1:] // r.Person becomes tyNm=Person
		v.Nm = tyNm
		if _, ok := tyMap[tyNm]; !ok {
			tyMap[tyNm] = true
		}
		//allTypes[k] = v // TODO: unecessary now that allTypes a slice of pointers. Check???
	}

	// for k, v := range tyMap {
	// 	fmt.Println("tyMap: ", k, v)
	// }

	// for k, v := range allTypes {
	// 	fmt.Printf("allTypes: %d %#v\n", k, v)
	// }

	for ty, _ := range tyMap {

		for _, v := range allTypes {
			// if not current ty then
			if v.Nm != ty {
				continue
			}
			//
			TypeC.AttrTy[v.Atr+"#"+v.Nm] = v.C // support attribute lookup for Has(<attribute>) function
			//
			// checl of DT is a UID attribute and gets its base type
			//	fmt.Printf("DT:%#v \n", v)
			if len(v.Ty) == 0 {
				panic(fmt.Errorf("DT not defined for %#v", v))
			}
			//
			// scalar type or abstract type e.g [person]
			//
			if v.Ty[0] == '[' {
				a = blk.TyAttrD{Name: v.Atr, DT: "Nd", C: v.C, Ty: v.Ty[1 : len(v.Ty)-1], P: v.P, Pg: v.Pg, N: v.N, IncP: v.IncP, Ix: v.Ix, Card: "1:N"}
			} else {
				// check if Ty is a known Type
				if _, ok := tyMap[v.Ty]; ok {
					a = blk.TyAttrD{Name: v.Atr, DT: "Nd", C: v.C, Ty: v.Ty, P: v.P, Pg: v.Pg, N: v.N, IncP: v.IncP, Ix: v.Ix, Card: "1:1"}
				} else {
					// scalar
					a = blk.TyAttrD{Name: v.Atr, DT: v.Ty, C: v.C, P: v.P, N: v.N, Pg: v.Pg, IncP: v.IncP, Ix: v.Ix}
				}
			}
			tc = append(tc, a)
			//
			TypeC.TyAttrC[GetTyAttr(ty, v.Atr)] = a
			tyShortNm, ok := GetTyShortNm(ty)
			if !ok {
				panic(fmt.Errorf("Error in populateTyCaches: Type short name not found"))
			}
			TypeC.TyAttrC[GetTyAttr(tyShortNm, v.Atr)] = a

			// fc, _ := FacetCache[tyAttr]
			// for _, vf := range v.F {
			// 	vfs := strings.Split(vf, "#")
			// 	if len(vfs) == 3 {
			// 		f := FacetTy{Name: vfs[0], DT: vfs[1], C: vfs[2]}
			// 		fc = append(fc, f)
			// 	} else {
			// 		panic(fmt.Errorf("%s", "Facet type information must contain 3 elements: <facetName>#<datatype>#<compressedIdentifer>"))
			// 	}
			// }
			// FacetCache[tyAttr] = fc
		}
		//
		TypeC.TyC[ty] = tc
		tc = nil
	}
	//	if param.DebugOn {
	syslog("==== TypeC.AttrTy")
	for k, v := range TypeC.AttrTy {
		syslog(fmt.Sprintf("%s   shortName: %s\n", k, v))
	}
	syslog("==== TypeC.TyC")
	for k, v := range TypeC.TyC {
		for _, v2 := range v {
			syslog(fmt.Sprintf("%s       %#v\n", k, v2))
		}
	}
	syslog("==== TypeC.TyAttrC")
	for k, v := range TypeC.TyAttrC {
		syslog(fmt.Sprintf("%s       %#v\n", k, v))
	}
	//	}
	// confirm caches are populated
	if len(TypeC.TyC) == 0 {
		panic(fmt.Errorf("typeC.TyC is empty"))
	}
	if len(TypeC.AttrTy) == 0 {
		panic(fmt.Errorf("typeC.AttrTy is empty"))
	}
	if len(TypeC.TyAttrC) == 0 {
		panic(fmt.Errorf("typeC.TyAttrC is empty"))
	}
	//panic(fmt.Errorf("Testing load of DD"))
}

func FetchType(ty Ty) (blk.TyAttrBlock, error) {

	ty = ty[strings.Index(ty, "|")+1:]

	// check if ty is long name using GetTyShortNm which presumes the input is a long name
	if _, ok := GetTyShortNm(ty); !ok {
		// must be a short name - check it exists using GetTyLongNm which only accepts a short name
		if longTy, ok := GetTyLongNm(ty); !ok {
			return nil, fmt.Errorf("FetchType: error %q type not found or short name not defined", ty)
		} else {
			ty = longTy
		}
	}
	if ty, ok := TypeC.TyC[ty]; ok { // ty= Person
		return ty, nil
	}
	return nil, fmt.Errorf("No type %q found", ty)

}

func IsScalarPred(pred string) bool { //TODO: pass in Type so uid-pred is checked against type not whole data dictionary
	for _, v := range TypeC.TyC {
		for _, vv := range v {
			if vv.Name == pred && len(vv.Ty) == 0 {
				// is a scalar in one type so presume its ok
				return true
			}
		}
	}
	return false
}

func ScalarDT(pred string) string { //TODO: pass in Type so uid-pred is checked against type not whole data dictionary
	for _, v := range TypeC.TyC {
		for _, vv := range v {
			if vv.Name == pred && len(vv.Ty) == 0 {
				return vv.DT
			}
		}
	}
	return ""
}

func IsUidPred(pred string) bool { //TODO: pass in Type so uid-pred is checked against type not whole data dictionary

	for _, v := range TypeC.TyC {
		for _, vv := range v {
			if vv.Name == pred && len(vv.Ty) > 0 && vv.DT == "Nd" {
				// is a uid-pred in one type so presume its ok
				return true
			}
		}
	}
	return false
}

func IsScalarInTy(ty string, pred string) bool { //TODO: pass in Type so uid-pred is checked against type not whole data dictionary
	if t, ok := TypeC.TyAttrC[ty+":"+pred]; !ok {
		return false
	} else if len(t.Ty) != 0 {
		return false
	}
	return true
}

func IsUidPredInTy(ty string, pred string) bool { //TODO: pass in Type so uid-pred is checked against type not whole data dictionary

	if t, ok := TypeC.TyAttrC[ty+":"+pred]; !ok {
		return false
	} else if len(t.Ty) == 0 {
		return false
	}
	return true
}

// GetScalars returns the SortK value of each scalar for a given type (ty: long type name)
func GetScalars(ty string, prefix string) []string {
	var attr []string

	for _, v := range TypeC.TyC[ty] {
		if len(v.Ty) == 0 && v.DT[0] != 'L' {
			attr = append(attr, prefix+v.C)
		}
	}
	return attr
}

// GetUidPreds returns the SortK value of each UID-PRED for a given type  (ty: long type name)
func GetUidPreds(ty string, prefix string) []string {
	var attr []string

	for _, v := range TypeC.TyC[ty] {
		if v.Ty == "Nd" {
			attr = append(attr, prefix+v.C)
		}
	}
	return attr
}

// GetSingleProgagatedScalars returns the SortK value for each propagated scalar a given type (UID-PRED)
func GetSinglePropagatedScalarsAll(ty string, prefix string) []string {
	var attr []string

	for _, v := range TypeC.TyC[ty] { // ty = Performance
		if v.DT == "Nd" {
			prefix := prefix + v.C
			attr = append(attr, prefix)
			for _, c := range GetScalars(v.Ty, prefix+"#:") { // Person, Character, Film
				attr = append(attr, c)
			}
		}
	}
	if len(attr) == 0 {
		panic(fmt.Errorf("GetSingleProgagatedScalarsAll error. Failed to find for type [%s]", ty))
	}

	return attr
}

// GetSingleProgagatedScalars returns the SortK value for each propagated scalar a given type (UID-PRED)
func GetSinglePropagatedScalars(ty string, uidpred string, prefix string) []string {
	var attr []string

	for _, v := range TypeC.TyC[ty] { // ty = Performance
		if v.DT == "Nd" && uidpred == v.C {
			prefix := prefix + v.C
			attr = append(attr, prefix)
			for _, c := range GetScalars(v.Ty, prefix+"#:") { // Person, Character, Film
				attr = append(attr, c)
			}
		}
	}
	if len(attr) == 0 {
		panic(fmt.Errorf("GetSingleProgagatedScalarsAll error. Failed to find for type [%s]", ty))
	}

	return attr
}

// GetDoubleProgagatedScalars: "A#G#:P" type: Fm,  "A#G#:?#G#:?#:?"
// func GetDoubleProgagatedScalars(ty string, uidpred string, prefix string) []string {
// 	var attr []string

// 	//ty, _ := GetTyLongNm(ty)

// 	for _, v := range TypeC.TyC[ty] {
// 		if v.Ty != "Nd" {
// 			continue
// 		}
// 		if uidpred != v.C {
// 			continue
// 		}

// 		for _, v := range TypeC.TyC[v.Ty] {
// 			prefix += v.C + "#:"
// 			if v.Ty == "Nd" && v.Card == "1:1" {
// 				for _, c := range GetScalars(v.Ty, prefix) {
// 					attr = append(attr, c)
// 				}
// 			}
// 		}
// 	}

// 	return attr
// }
