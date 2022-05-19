package ast

import (
	"errors"
	"fmt"
	"strings"

	"github.com/GoGraph/ds"
	"github.com/GoGraph/gql/internal/es"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/tx/query"
	"github.com/GoGraph/types"
)

const (
	logid = "gqlFunc: "
)
const (
	allofterms = " AND "
	anyofterms = " OR "
	exact      = "exact"
)

func syslog(s string) {
	slog.Log(logid, s)
}

// eq function for root query called during execution-root-query phase
// Each ds.QResult will be Fetched then Unmarshalled (via UnmarshalCache) into []NV for each predicate.
// The []NV will then be processed by the Filter function if present to reduce the number of elements in []NV
func EQ(a FargI, value interface{}) ds.QResult {
	return ieq(query.EQ, a, value)
}
func GT(a FargI, value interface{}) ds.QResult {
	return ieq(query.GT, a, value)
}
func GE(a FargI, value interface{}) ds.QResult {
	return ieq(query.GE, a, value)
}
func LT(a FargI, value interface{}) ds.QResult {
	return ieq(query.LT, a, value)
}
func LE(a FargI, value interface{}) ds.QResult {
	return ieq(query.LE, a, value)
}

func ieq(opr query.Equality, a FargI, value interface{}) ds.QResult {

	var (
		err    error
		result ds.QResult
	)

	switch x := a.(type) {

	case *CountFunc:

		// for root stmt only this signature is valid: Count(<uid-pred>)
		var tresult ds.QResult

		result, err = RootCnt(x.Arg.Name(), value.(int), opr)
		if err != nil {
			panic(err) // TODO: pass err to calling routine
		}

		tresult = append(tresult, result...)

		return tresult
	// TODO: implement Variable argument.

	case ScalarPred:

		switch v := value.(type) {
		case int:
			result, err = GSIQueryI(x.Name(), int64(v), opr)
		case float64:
			result, err = GSIQueryF(x.Name(), v, opr)
		case string:
			result, err = GSIQueryS(x.Name(), v, opr)
		case []interface{}:
			//case Variable: // not on root func
		}
		if err != nil {
			if errors.Is(err, query.NoDataFoundErr) {
				if opr == query.EQ {
					// try ElasticSearch exact search (FullText)
					result = ExactTerms(a, value)
				}
			} else {
				panic(fmt.Errorf("ieq func error: %s", err.Error()))
			}
		}

	}

	return result
}

//func Has(a FargI, value interface{}) db.QResult {)

//
// these funcs are used in filter condition only. At the root search ElasticSearch is used to retrieve relevant UIDs.
//
func AllOfTerms(a FargI, value interface{}) ds.QResult {
	return terms(allofterms, a, value)
}

func AnyOfTerms(a FargI, value interface{}) ds.QResult {
	return terms(anyofterms, a, value)
}

func ExactTerms(a FargI, value interface{}) ds.QResult {
	return terms(exact, a, value)
}

func terms(termOpr string, a FargI, value interface{}) ds.QResult {

	// a => predicate
	// value => space delimited list of terms

	fmt.Println("funcs.go: terms")
	type data struct {
		field string
		query string
	}

	var (
		qs     strings.Builder
		t      ScalarPred
		exact_ bool
		ok     bool
	)
	ss := strings.Split(value.(string), " ")
	for i, v := range ss {
		qs.WriteString(v)
		if i < len(ss)-1 {
			qs.WriteString(termOpr)
		}
	}
	if t, ok = a.(ScalarPred); !ok {
		panic(fmt.Errorf("Error in all|any ofterms func: expected a scalar predicate"))
	}
	fmt.Println("Name: ", t.Name())
	fmt.Println("qs.String() ", qs.String())
	if termOpr == exact {
		exact_ = true
	}
	return es.Query(t.Name(), exact_, qs.String())
}

func Has(a FargI, value interface{}) ds.QResult {

	var (
		result ds.QResult
		err    error
	)

	if value != nil {
		panic(fmt.Errorf("Expected nil value. Second argument to has() should be empty"))
	}
	//
	// has(uid-pred) - all uid-preds/edges have a P_N entry (count of edges eminating from uid-predicate). If no edge exist then no entry. So GSI will list only nodes that have the uid-pred
	// has(Actor.film) - nullable uid-pred (not all Person Type are actors)
	// has(Director.film) - nullable uid-pred (because not all Person type are directors) - search for
	//
	// has(<scalar-pred>) - all scalars are indexed in P_N or P_S. If not present (null) in item then there is no index entry. So GSI will list only nodes that have the scalar defined.
	// has(Address) - not-null scalar (everyone must have an address) - search on GSI (P_S) where P="Address" will find all candidates
	// has(Age) - nullable scalar (not everyone gives their age) - search on GSI (P_S) where P="Age" will find all candidates
	//
	switch x := a.(type) {

	case ScalarPred:

		// check P_S, P_N
		result, err = GSIhas(x.Name())
		if err != nil {
			panic(err)
		}

	case *UidPred:
		// Dynamodb: P_N has count of edges for uidPred. Use it to find all associated nodes.
		// Spanner: search for attribute in types and build sortk and query EOP

		// find types containing attribute
		var allresult ds.QResult
		var aty []string
		var sk []string
		for k, vv := range types.TypeC.TyC {
			for _, v := range vv {
				if v.Name == x.Name() {
					tysn, _ := types.GetTyShortNm(k)

					aty = append(aty, tysn)
					sk = append(sk, types.GraphSN()+"|"+"A#G#:"+v.C)
				}
			}
		}

		for i, _ := range aty {
			result, err = GSIhasUpred(x.Name(), aty[i], sk[i])
			if err != nil {
				panic(err)
			}
			allresult = append(allresult, result...)
		}
		result = allresult

	}

	return result
}
