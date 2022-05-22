package ast

import (
	"context"
	"fmt"
	"strings"

	"github.com/GoGraph/ds"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx"
	"github.com/GoGraph/tx/query"
	"github.com/GoGraph/types"
)

var (
	err error
)

type AttrName = string

var ctx context.Context = context.Background() //TODO: replace with actual ctx

func RootCnt(attr string, cnt int, opr query.Equality) (ds.QResult, error) {

	var (
		err error
		all ds.QResult
	)
	rc := tx.NewQuery2(ctx, "rootcnt", tbl.Graph, "P_N")
	rc.Select(&all).Key("P", types.GraphSN()+"|"+attr).Key("N", cnt, opr) // .Filter("Ty", ty)

	err = rc.Execute()
	if err != nil {
		return nil, err
	}

	return all, nil

}

func RootCnt2(ty string, cnt int, sk string, opr query.Equality) (ds.QResult, error) {

	var (
		err error
		all ds.QResult

		found bool
		nm    string
	)

	// convert sk "m|A#G#:A" to P "m|performance.actor"

	tyLn, ok := types.GetTyLongNm(ty)
	if !ok {
		panic(fmt.Errorf("cannot find long name for type %s", ty))
	}

	//ty_ := types.GraphSN() + "|" + tyLn
	//ty_ := types.GraphSN() + "|" + ty
	c_ := sk[strings.Index(sk, ":")+1:]

	for _, v := range types.TypeC.TyAttrC {
		fmt.Printf("v: %#v \n", v)
		//	if v.Ty == ty_ && v.DT == "Nd" && v.C == c_ {
		if v.Ty == tyLn && v.DT == "Nd" && v.C == c_ {
			nm = v.Name
			found = true
		}
		if found {
			break
		}
	}

	p := types.GraphSN() + "|" + nm

	rc := tx.NewQuery2(ctx, "rootcnt", tbl.Graph, "P_N")
	rc.Select(&all).Key("P", p).Key("N", cnt, opr) // .Filter("Ty", ty)

	err = rc.Execute()
	if err != nil {
		return nil, err
	}

	return all, nil

}

func GSIQueryN(attr AttrName, lv float64, op query.Equality) (ds.QResult, error) {

	var qresult ds.QResult
	rc := tx.NewQuery2(ctx, "GSIQueryN", tbl.Graph, "P_N")
	rc.Select(&qresult).Key("P", attr).Key("N", lv, op)

	err := rc.Execute()
	if err != nil {
		return nil, err
	}
	return qresult, nil

}

func GSIQueryI(attr AttrName, lv int64, op query.Equality) (ds.QResult, error) {

	var qresult ds.QResult
	rc := tx.NewQuery2(ctx, "GSIQueryI", tbl.Graph, "P_N")
	rc.Select(&qresult).Key("P", attr).Key("N", lv, op)

	err := rc.Execute()
	if err != nil {
		return nil, err
	}
	return qresult, nil

}
func GSIQueryF(attr AttrName, lv float64, op query.Equality) (ds.QResult, error) {

	var qresult ds.QResult
	rc := tx.NewQuery(tbl.Graph, "GSIQueryF", "P_N")
	rc.Select(&qresult).Key("P", attr).Key("N", lv, op)

	err := rc.Execute()
	if err != nil {
		return nil, err
	}
	return qresult, nil

}

func GSIQueryS(attr AttrName, lv string, op query.Equality) (ds.QResult, error) {

	var qresult ds.QResult

	rc := tx.NewQuery2(ctx, "GSIQueryS", tbl.Graph, "P_S")
	rc.Select(&qresult).Key("P", types.GraphSN()+"|"+attr).Key("S", lv, op)

	err := rc.Execute()
	if err != nil {
		return nil, err
	}
	return qresult, nil
}

func GSIhasS(attr AttrName) (ds.QResult, error) {

	var qresult ds.QResult

	rc := tx.NewQuery2(ctx, "GSIhasS", tbl.Graph, "P_S")
	rc.Select(&qresult).Key("P", types.GraphSN()+"|"+attr)

	err := rc.Execute()
	if err != nil {
		return nil, err
	}
	return qresult, nil
}

func GSIhasB(attr AttrName) (ds.QResult, error) {

	var qresult ds.QResult

	rc := tx.NewQuery2(ctx, "GSIhasB", tbl.Graph, "P_B")
	rc.Select(&qresult).Key("P", types.GraphSN()+"|"+attr)

	err := rc.Execute()
	if err != nil {
		return nil, err
	}
	return qresult, nil
}

func GSIhasN(attr AttrName) (ds.QResult, error) {
	var qresult ds.QResult

	rc := tx.NewQuery2(ctx, "GSIhasN", tbl.Graph, "P_N")
	rc.Select(&qresult).Key("P", types.GraphSN()+"|"+attr)

	err := rc.Execute()
	if err != nil {
		return nil, err
	}
	return qresult, nil
}

func GSIhasF(attr AttrName) (ds.QResult, error) {
	var qresult ds.QResult

	rc := tx.NewQuery2(ctx, "GSIhasF", tbl.Graph, "P_F")
	rc.Select(&qresult).Key("P", types.GraphSN()+"|"+attr)

	err := rc.Execute()
	if err != nil {
		return nil, err
	}
	return qresult, nil
}
func GSIhasI(attr AttrName) (ds.QResult, error) {
	var qresult ds.QResult

	rc := tx.NewQuery2(ctx, "GSIhasF", tbl.Graph, "P_I")
	rc.Select(&qresult).Key("P", types.GraphSN()+"|"+attr)

	err := rc.Execute()
	if err != nil {
		return nil, err
	}
	return qresult, nil
}

func GSIhasUpred(attr AttrName, ty string, sk string) (ds.QResult, error) {
	var qresult ds.QResult

	rc := tx.NewQuery2(ctx, "GSIhasUpred", tbl.Graph, "P_N")
	rc.Select(&qresult).Key("P", types.GraphSN()+"|"+attr)

	err := rc.Execute()
	if err != nil {
		return nil, err
	}
	return qresult, nil

}

func GSIhasChild(attr AttrName) (ds.QResult, error) {

	return nil, nil

}
