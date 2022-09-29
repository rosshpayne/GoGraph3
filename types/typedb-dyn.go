//go:build dynamodb
// +build dynamodb

package types

import (
	"fmt"

	blk "github.com/GoGraph/block"
	dyn "github.com/GoGraph/db"
	//	"github.com/GoGraph/dbConn"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx"
	"github.com/GoGraph/tx/db"
)

const (
	typesTbl  = string(tbl.Type)
	typesTblN = tbl.Name(typesTbl)
)

type tyNames struct {
	ShortNm string `dynamodbav:"SortK"`
	LongNm  string `dynamodbav:"Name"`
}

var (
	gId     string
	tynames []tyNames
)

// func init() {

// 	dynSrv, err = dbConn.New()
// 	if err != nil {
// 		panic(err)
// 	}

// }

// GraphSN returns graph's short name
// func GraphSN() string {
// 	return gId[:len(gId)-1]
// }

func setGraph(graph_ string) (string, error) {
	var err error

	gId, err = getGraphId(graph_)
	if err != nil {
		return "", err
	}

	tynames, err = loadTypeShortNames()
	if err != nil {
		return "", err
	}
	//
	// populate type short name cache. This cache is conccurent safe as it is readonly from now on.
	//
	tyShortNm = make(map[string]string)
	for _, v := range tynames {
		tyShortNm[v.LongNm] = v.ShortNm
	}

	return gId, nil

}

func GetTypeShortNames() ([]tyNames, error) {
	return tynames, nil
}

// getSrv - get default db service. Cannot use an init() as order of execution with db init() cannot be specified. Require a db goroutine service to provide
// getSrv data, which can then be included in a init() in this package.
// dbSrv := SrvCh <- struct{DB: "dynamodb"}
func getSrv() *dyn.DynamodbHandle {
	hdl, err := db.GetDBHdl("dynamodb")
	if err != nil {
		panic(err)
	}
	return hdl.(*dyn.DynamodbHandle)
}

func LoadDataDictionary() (blk.TyIBlock, error) {

	//dynSrv = getSrv()
	var dd blk.TyIBlock

	ldd := tx.NewQuery2("LoadDataDictionary", tbl.Name(typesTblN))
	ldd.Select(&dd).Filter("PKey", gId, "BEGINSWITH")

	if ldd.Error() != nil {
		fmt.Println("Error load dd")
		panic(ldd.Error())
	}
	err := ldd.Execute()
	if err != nil {
		return nil, err
	}

	return dd, nil

}

func loadTypeShortNames() ([]tyNames, error) {

	//	dynSrv = getSrv() // db.GetDBHdl("dynamodb").(db.DynamodbHandle)
	syslog("db.loadTypeShortNames ")
	var tys []tyNames // modify to accept tags

	txg := tx.NewQuery2("TyShortNames", typesTblN)
	txg.Select(&tys).Key("PKey", "#"+gId+"T")
	err := txg.Execute()
	if err != nil {
		return nil, err
	}
	return tys, nil

}

func getGraphId(graphNm string) (string, error) {

	type graphMeta struct {
		SortK string
	}

	var sk []graphMeta

	txg := tx.NewQuery2("GraphName", typesTblN)
	txg.Select(&sk).Key("PKey", "#Graph").Filter("Name", graphNm)
	err := txg.Execute()

	if err != nil {
		return "", newDBUnmarshalErr("getGraphId", "", "", "UnmarshalListOfMaps", err)
	}
	if len(sk) == 0 {
		return "", newDBUnmarshalErr("getGraphId", "", "", "No data returned in getGraphId", err)
	}
	if len(sk) > 1 {
		return "", newDBUnmarshalErr("getGraphId", "", "", "More than one item found in database", err)
	}

	return sk[0].SortK + ".", nil

}

type DBUnmarshalErr struct {
	routine string
	pkey    string
	sortk   string
	api     string // DB statement
	err     error  // aws database error
}

func newDBUnmarshalErr(rt string, pk string, sk string, api string, err error) error {
	e := DBUnmarshalErr{routine: rt, pkey: pk, sortk: sk, api: api, err: err}
	logerr(e)
	return e
}

func (e DBUnmarshalErr) Error() string {
	if len(e.sortk) > 0 {
		return fmt.Sprintf("Unmarshalling error during %s in %s. [%q, %q]. Error: %s ", e.api, e.routine, e.pkey, e.sortk, e.err.Error())
	}
	return fmt.Sprintf("Unmarshalling error during %s in %s. [%q]. Error: %s ", e.api, e.routine, e.pkey, e.err.Error())
}

func (e DBUnmarshalErr) Unwrap() error {
	return e.err
}
