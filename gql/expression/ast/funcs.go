package ast

import (
	"bufio"
	"fmt"
	"strings"

	blk "github.com/GoGraph/block"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/types"
	//"github.com/GoGraph/db"
	"github.com/GoGraph/ds"
)

const (
	logid = "exprFunc"
	fatal = true
)

type inEQ uint8

const (
	eq inEQ = iota
	le
	lt
	ge
	gt
)

func syslog(s string, panic_ ...bool) {
	if len(panic_) > 0 && panic_[0] {
		slog.Log(logid, s, panic_[0])
		panic(fmt.Errorf(s))
	}
	slog.Log(logid, s)
}

// eq(predicate, value)
// eq(val(varName), value)
// eq(predicate, val(varName))
// eq(count(predicate), value)
// eq(predicate, [val1, val2, ..., valN])
// eq(predicate, [$var1, "value", ..., $varN])

// type QResultT struct {
// 	PKey  util.UID
// 	SortK string
// 	Ty    string
// }

// ==========================================================================================================

// func Count(p Predicate) int {
// 	// parsing will have confirmed p is a uid-pred only
// 	cnt, err := db.GetCount(p)
// 	if err != nil {
// 		panic(err)
// 	}
// 	return cnt

// }

// type ValOut interface{}

// func Val(v Variable) ValOut {
// 	if v, ok := variable[v]; !ok {
// 		panic(fmt.Errorf("%q does not exist as variable", v))
// 	} else {
// 		return v
// 	}
// 	return nil
// }

// Filter EQ function called during execution-filter phase
// predfunc can be either the expression predicate or a modifying function (val, count)
// value is the predicate value as appears in the expression e.g. eq(<pred>, 5) where 5 is the value.
// nv is the contents of the cache (predicate,value) for the UID returned from the root func
// ty is the type of the cache entry which is the same as the root item type.
// i,k index into slice ([][]) of edge interface values (overflow representation of edge values)
// bool dictates whether QResult element (represented by nv argument) will be ignored or displayeda
func EQ(predfunc FargI, value interface{}, nv ds.NVmap, ty string, j, k int) bool {
	return ieq(eq, predfunc, value, nv, ty, j, k)
}
func GT(predfunc FargI, value interface{}, nv ds.NVmap, ty string, j, k int) bool {
	return ieq(gt, predfunc, value, nv, ty, j, k)
}
func GE(predfunc FargI, value interface{}, nv ds.NVmap, ty string, j, k int) bool {
	return ieq(ge, predfunc, value, nv, ty, j, k)
}
func LT(predfunc FargI, value interface{}, nv ds.NVmap, ty string, j, k int) bool {
	return ieq(lt, predfunc, value, nv, ty, j, k)
}
func LE(predfunc FargI, value interface{}, nv ds.NVmap, ty string, j, k int) bool {
	return ieq(le, predfunc, value, nv, ty, j, k)
}

//ieq represents the common logic for inequality functions e.g. eq, lt, gt, ge, le
// ie - inequality value from calling func
// predFunc - argument to inequality function. consists of a predicate or innerfunction like count()
// value - literal value from GQL statement to be compared to data-cache (nv) value
// nv - GQL predicates and their associated node cache data
// ty - type of result item from root query. It is also appended with uid-pred name (as a workaround) e.g. Person|Sibling or Person|Friend which is used to get access to the node data relevant to the uid-pred..
// j,k - index into node cache map for uid-pred predicates
//
func ieq(ie inEQ, predfunc FargI, value interface{}, nv ds.NVmap, ty string, j, k int) bool {
	var (
		pTy blk.TyAttrD
		ok  bool
	)

	if ty == "" {
		panic("Error in Func: expected a type, got nil")
	}

	switch x := predfunc.(type) {

	case CountFunc:

		switch x.Arg.(type) {

		case *UidPred:
			var nds [][][]byte
			// get data from nv
			data := nv[x.Name()] //data.Value [][][]byte

			if nds, ok = data.Value.([][][]byte); !ok {
				panic(fmt.Errorf("Expression EQ: Expected [][][]byte for NV value"))
			}
			// count child nodes
			n := 0
			for i, k := range nds {
				n += len(k[i])
			}
			if v, ok := value.(int); ok {
				return v == n
			}

			// case Variable: //TODO: implement
		}

	case ScalarPred:
		// find value for this predicate
		var (
			nm   string
			data *ds.NV
		)
		switch j {

		case -1: // root filter
			nm = x.Name()
			data, ok = nv[nm]
			if !ok {
				panic(fmt.Errorf("Error in inequality func: predicate %q not found in ds.NV", nm))
			}
			//
			// get type of predicate from type info
			//
			if pTy, ok = types.TypeC.TyAttrC[ty+":"+nm]; !ok {
				// root result type does not contain filter predicate, so root item fails the filter
				panic(fmt.Errorf("Error in inequality func: predicate %q not found in TypeC.TyAttr", ty+":"+nm))
				return false
			}

		default: // uid-pred filter
			fd := strings.Split(ty, "|")
			ty = fd[0]
			predicate := fd[1]
			//
			nm = predicate + ":" + x.Name()
			data, ok = nv[nm]
			if !ok {
				panic(fmt.Errorf("Error in inequality func: predicate %q not found in ds.NV", nm))
			}
			//
			// get type of predicate from type info
			//
			//fmt.Println("ieq func: ", ty, x.Name())
			if pTy, ok = types.TypeC.TyAttrC[ty+":"+x.Name()]; !ok {
				// root result type does not contain filter predicate, so root item fails the filter
				panic(fmt.Errorf("XX Error in inequality func: predicate %q not found in TypeC.TyAttr", ty+":"+x.Name()))
				return false
			}
		}

		switch pTy.DT {

		case "S":

			switch {

			case j == -1: // root filter
				var (
					ok      bool
					dataVal string
					exprVal string
				)

				// root query filter expression - node scalar data
				// check data value (from NV) type matches type of scalarPred from GQL query
				if dataVal, ok = data.Value.(string); !ok {
					panic(fmt.Errorf("predicate %q value type %q does not match type for predicate in type %q", x.Name(), dataVal, ty))
				}
				// expression value
				if exprVal, ok = value.(string); !ok {
					panic(fmt.Errorf("value %q for predicate %q is not the correct type for type %q", exprVal, x.Name(), ty))
				}
				// compare values
				switch ie {
				case eq:
					return dataVal == exprVal
				case gt:
					return dataVal > exprVal
				case ge:
					return dataVal >= exprVal
				case lt:
					return dataVal < exprVal
				case le:
					return dataVal <= exprVal
				}

			default: // uid-pred filter, args ty, i,j populated have non-zero values
				var (
					ok      bool
					dataVal [][]string
					exprVal string
				)

				// uid-pred filter expression - multiple child scalar data (propagated data format)
				// check if null flag set in data
				if data.Null[j][k] {
					return false
				}
				// data value type
				if dataVal, ok = data.Value.([][]string); !ok {
					panic(fmt.Errorf("predicate %q value type %q does not match type for predicate in type %q", x.Name(), data.Value, ty))
				}
				// expression value
				if exprVal, ok = value.(string); !ok {
					panic(fmt.Errorf("value %q for predicate %q is not the correct type for type %q", exprVal, x.Name(), ty))
				}
				switch ie {
				case eq:
					return dataVal[j][k] == exprVal
				case gt:
					return dataVal[j][k] > exprVal
				case ge:
					return dataVal[j][k] >= exprVal
				case lt:
					return dataVal[j][k] < exprVal
				case le:
					return dataVal[j][k] <= exprVal
				}

			}

			return false

		case "I":

			switch {

			case j == -1: // root filter
				var (
					ok      bool
					dataVal int64
					exprVal int64
				)
				// root query filter expression - node scalar data
				// check data value (from NV) type matches type of scalarPred from GQL query
				if dataVal, ok = data.Value.(int64); !ok {
					panic(fmt.Errorf("predicate %q value type %q does not match type for predicate in type %q", x.Name(), dataVal, ty))
				}
				// expression value
				if exprVal, ok = value.(int64); !ok {
					if y, ok := value.(int); !ok {
						panic(fmt.Errorf("value %q for predicate %q is not the correct type for type %q", exprVal, x.Name(), ty))
					} else {
						exprVal = int64(y)
					}
				}
				// compare values
				switch ie {
				case eq:
					return dataVal == exprVal
				case gt:
					return dataVal > exprVal
				case ge:
					return dataVal >= exprVal
				case lt:
					return dataVal < exprVal
				case le:
					return dataVal <= exprVal
				}

			default: // uid-pred filter, args: ty, i,j contain non-zero values
				var (
					ok      bool
					dataVal int64
					exprVal int64
				)

				// uid-pred filter expression - multiple child scalar data (propagated data format)
				// check if null flag set in data
				if data.Null[j][k] {
					return false
				}
				// data value type
				if y, ok := data.Value.([][]int64); !ok {
					if z, ok := data.Value.([][]int); !ok {
						panic(fmt.Errorf("value %q for predicate %q is not the correct type for type %q", exprVal, x.Name(), ty))
					} else {
						dataVal = int64(z[j][k])
					}
				} else {
					dataVal = y[j][k]
				}
				//fmt.Println("dataVal is : ", dataVal)
				// expression value
				if exprVal, ok = value.(int64); !ok {
					if y, ok := value.(int); !ok {
						panic(fmt.Errorf("value %q for predicate %q is not the correct type for type %q", exprVal, x.Name(), ty))
					} else {
						exprVal = int64(y)
					}
				}
				//fmt.Println("exprVal is : ", exprVal)
				switch ie {
				case eq:
					return dataVal == exprVal
				case gt:
					return dataVal > exprVal
				case ge:
					return dataVal >= exprVal
				case lt:
					return dataVal < exprVal
				case le:
					return dataVal <= exprVal
				}

			}
			// }
		case "F":
		case "Bl":
		default:
			fmt.Printf("DEFAULT : %T\n", predfunc)
		}

	}
	return false
}

// func lt() []ast.Result  { return nil }
// func has() []ast.Result { return nil }

// funcs Var(varNm Variable) ValOut {
// 	item:=variable.Get(varNm)
// 	switch x:=item.(type);x {
// 	case ast.EdgeT:
// 	default:

// 	}
// }

func HAS(predfunc FargI, value interface{}, nv ds.NVmap, ty string, j, k int) bool {
	var (
		nm   string
		data *ds.NV
		//		aTy  blk.TyAttrD
		ok bool
		//
		predicate string
	)

	if value != nil {
		syslog("Error in Has(). value argument should be nil", fatal)
	}
	switch x := predfunc.(type) {
	case ScalarPred, *UidPred:
	default:
		syslog(fmt.Sprintf("Error in Has(). expected a scalar or uid-predicate as argument instead got %q", x.Name(), fatal))
	}

	switch j {

	case -1: // root filter

		predicate = predfunc.Name()
		//  Check ty exists
		if _, err := types.FetchType(ty); err != nil {
			syslog(fmt.Sprintf("Error in Has(). Type %q not found", ty), fatal)
		} else {
			if x, ok := types.TypeC.TyAttrC[ty+":"+predicate]; !ok {
				syslog(fmt.Sprintf("Error in Has(). Attribute %q not found in type %q", predfunc.Name(), ty), fatal)
			} else if !x.N {
				return true // attribute is not nullable  - so must be defined.
			}
		}

		switch predfunc.(type) {
		case ScalarPred:
			if x, ok := nv[predicate]; !ok {
				return false
			} else if x.Value == nil {
				return false
			}

		case *UidPred:
			if x, ok := nv[predicate+":"]; !ok {
				return false
			} else if x.Value == nil {
				return false
			}
		}
		return true

	default: // uid-pred filter

		//fmt.Println("uid-pred filter ty: ", ty)
		// ....uid-pred @filter(<uid-pred-predicate>,<list-of-terms>)  - ty is uid-pred type "Person"

		// retrieve uid-pred name (Sibling, Friend) from ty argument.
		// This is a work around as I forgot to add this data in the original list of arguments.
		fd := strings.Split(ty, "|")
		ty = fd[0]
		predicate := fd[1]
		// retrieve uid-pred data from NV
		switch predfunc.(type) {
		case ScalarPred:

			nm = predicate + ":" + predfunc.Name()
			data, ok = nv[nm]
			if !ok {
				panic(fmt.Errorf("Error in inequality func: predicate %q not found in ds.NV", nm))
			}
			// uid-pred filter expression - multiple child scalar data (propagated data format)
			// check if null flag set in data
			if data.Null[j][k] {
				return false
			}
			return true

		case *UidPred:
			//TODO: implement
			panic(fmt.Errorf(`has(<uid-predicate>) as filter not supported outside of root query`))

		}
	}
	return false
}

func UID(predfunc FargI, value interface{}, nv ds.NVmap, ty string, j, k int) bool    { return false }
func UID_IN(predfunc FargI, value interface{}, nv ds.NVmap, ty string, j, k int) bool { return false }
func VAL(predfunc FargI, value interface{}, nv ds.NVmap, ty string, j, k int) bool    { return false }

func AnyOfTerms(predfunc FargI, value interface{}, nv ds.NVmap, ty string, j, k int) bool {
	// AnyOfTerms(Comment,"Payne Germany") ie. anyofterms(<predicate>,<list of terms>)
	// where comment is a predicate in the type.
	// Type is sourced from GSI in the case of root filter or sourced from the type of the current uid-pred type
	// in the of uid-pred filter. In the above Comment is a predicate belonging to the uid-pred being executed.
	// get string data from NV for relevent predicate
	var (
		nm   string
		data *ds.NV
		//		aTy  blk.TyAttrD
		pred ScalarPred
		ok   bool
	)

	if pred, ok = predfunc.(ScalarPred); !ok {
		syslog("Expected scalar predicate in AnyOfTerms", fatal)
	}
	switch j {

	case -1: // root filter

		//  GSI-item @filter(<gsi-item-type-predicate>, <list-of-terms>)

		//fmt.Printf("in ScalarPred with %q   ty: %s\n", pred.Name(), ty)
		data, ok = nv[pred.Name()]
		if !ok {
			panic(fmt.Errorf("Error in inequality func: predicate %q not found in ds.NV", nm))
		}

	default: // uid-pred filter

		// ....uid-pred @filter(<uid-pred-predicate>,<list-of-terms>)  - ty is uid-pred type "Person"

		// retrieve uid-pred name (Sibling, Friend) from ty argument.
		// This is a work around as I forgot to add this data in the original list of arguments.
		fd := strings.Split(ty, "|")
		ty = fd[0]
		predicate := fd[1]
		// retrieve uid-pred data from NV
		nm = predicate + ":" + pred.Name()
		data, ok = nv[nm]
		if !ok {
			panic(fmt.Errorf("Error in inequality func: predicate %q not found in ds.NV", nm))
		}
	}
	var (
		dataVal string
		exprVal string
	)
	switch {
	case j == -1: // root filter

		// root query filter expression - node scalar data
		// check data value (from NV) type matches type of scalarPred from GQL query
		if dataVal, ok = data.Value.(string); !ok {
			syslog(fmt.Sprintf("predicate %q value type %q does not match type for predicate in type %q", nm, dataVal, ty), fatal)
		}
		// expression value
		if exprVal, ok = value.(string); !ok {
			syslog(fmt.Sprintf("value %q for predicate %q is not the correct type for type %q", exprVal, nm, ty), fatal)
		}

	default: // uid-pred filter, args ty, i,j populated have non-zero values

		// uid-pred filter expression - multiple child scalar data (propagated data format)
		// check if null flag set in data
		if data.Null[j][k] {
			return false
		}
		// data value type
		if dataVal_, ok := data.Value.([][]string); !ok {
			panic(fmt.Errorf("predicate %q value type %q does not match type for predicate in type %q", nm, data.Value, ty))
		} else {
			dataVal = dataVal_[j][k]
		}
		// expression value
		if exprVal, ok = value.(string); !ok {
			panic(fmt.Errorf("value %q for predicate %q is not the correct type for type %q", exprVal, nm, ty))
		}
	}
	// check if any term in exprVal exists in dataVal
	bsExpr := bufio.NewScanner(strings.NewReader(exprVal))
	bsExpr.Split(bufio.ScanWords)

	bsData := bufio.NewScanner(strings.NewReader(dataVal))
	bsData.Split(bufio.ScanWords)

	for bsExpr.Scan() {
		for bsData.Scan() {
			if bsExpr.Text() == bsData.Text() {
				return true
			}
		}
		bsData = bufio.NewScanner(strings.NewReader(dataVal))
		bsData.Split(bufio.ScanWords)
	}
	return false

}

func AllOfTerms(predfunc FargI, value interface{}, nv ds.NVmap, ty string, j, k int) bool {
	// AnyOfTerms(Comment,"Payne Germany") ie. anyofterms(<predicate>,<list of terms>)
	// where comment is a predicate in the type.
	// Type is sourced from GSI in the case of root filter or sourced from the type of the current uid-pred type
	// in the of uid-pred filter. In the above Comment is a predicate belonging to the uid-pred being executed.
	// get string data from NV for relevent predicate
	var (
		nm   string
		data *ds.NV
		//		aTy  blk.TyAttrD
		pred ScalarPred
		ok   bool
		//
		dataVal string
		exprVal string
	)

	if pred, ok = predfunc.(ScalarPred); !ok {
		syslog("Expected Scalar Predicate in AnyOfTerms", fatal)
	}
	switch j {

	case -1: // root filter

		//  GSI-item @filter(<gsi-item-type-predicate>, <list-of-terms>)

		//fmt.Printf("in ScalarPred with %q   ty: %s\n", pred.Name(), ty)
		data, ok = nv[pred.Name()]
		if !ok {
			panic(fmt.Errorf("Error in inequality func: predicate %q not found in ds.NV", nm))
		}

		// root query filter expression - node scalar data
		// check data value (from NV) type matches type of scalarPred from GQL query
		if dataVal, ok = data.Value.(string); !ok {
			syslog(fmt.Sprintf("predicate %q value type %q does not match type for predicate in type %q", nm, dataVal, ty), fatal)
		}
		// expression value
		if exprVal, ok = value.(string); !ok {
			syslog(fmt.Sprintf("value %q for predicate %q is not the correct type for type %q", exprVal, nm, ty), fatal)
		}

	default: // uid-pred filter

		// ....uid-pred @filter(<uid-pred-predicate>,<list-of-terms>)  - ty is uid-pred type "Person"

		// retrieve uid-pred name (Sibling, Friend) from ty argument.
		// This is a work around as I forgot to add this data in the original list of arguments.
		fd := strings.Split(ty, "|")
		ty = fd[0]
		predicate := fd[1]
		// retrieve uid-pred data from NV
		nm = predicate + ":" + pred.Name()
		data, ok = nv[nm]
		if !ok {
			panic(fmt.Errorf("Error in inequality func: predicate %q not found in ds.NV", nm))
		}
		// uid-pred filter expression - multiple child scalar data (propagated data format)
		// check if null flag set in data
		if data.Null[j][k] {
			return false
		}
		// data value type
		if dataVal_, ok := data.Value.([][]string); !ok {
			panic(fmt.Errorf("predicate %q value type %q does not match type for predicate in type %q", nm, data.Value, ty))
		} else {
			dataVal = dataVal_[j][k]
		}
		// expression value
		if exprVal, ok = value.(string); !ok {
			panic(fmt.Errorf("value %q for predicate %q is not the correct type for type %q", exprVal, nm, ty))
		}
	}

	//fmt.Printf("exprVal, dataVal: %s [%s]\n ", exprVal, dataVal)
	// check if any term in exprVal exists in dataVal
	bsExpr := bufio.NewScanner(strings.NewReader(exprVal))
	bsExpr.Split(bufio.ScanWords)
	//fmt.Println("in allofterms: ", exprVal, dataVal)

	bsData := bufio.NewScanner(strings.NewReader(dataVal))
	bsData.Split(bufio.ScanWords)

	var found bool
	for bsExpr.Scan() {
		found = false
		for bsData.Scan() {
			if bsExpr.Text() == bsData.Text() {
				found = true
				break
			}
		}
		if !found {
			return false
		}
		bsData = bufio.NewScanner(strings.NewReader(dataVal))
		bsData.Split(bufio.ScanWords)
	}
	return true

}
