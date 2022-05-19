// +build spanner

package db

import (
	"context"
	"errors"
	"fmt"

	blk "github.com/GoGraph/block"
	"github.com/GoGraph/db"
	slog "github.com/GoGraph/syslog"

	"cloud.google.com/go/spanner" //v1.21.0
	"google.golang.org/api/iterator"
)

const (
	logid = "TypesDB: "
)

type tyNames struct {
	ShortNm string `spanner:"SName"`
	LongNm  string `spanner:"Name"`
}

var (
	client  *spanner.Client
	graphNm string
	gId     string // graph Identifier (graph short name). Each Type name is prepended with the graph id. It is stripped off when type data is loaded into caches.
	//err       error
	tynames   []tyNames
	tyShortNm map[string]string
)

// func init() {
// 	//client, err = dbConn.New()
// 	client=

// }

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

func SetGraph(graph_ string) (string, error) {
	graphNm = graph_

	client = db.GetClient()

	if client == nil {
		syslog("db client is nil")
		return "", errors.New("DB Client is nil")
	}
	gsn, err := graphShortName(graphNm)
	if err != nil {
		return "", err
	}
	fmt.Print("Types: gsn: ", gsn)
	tynames, err = loadTypeShortNames()
	if err != nil {
		panic(err)
	}
	//
	// populate type short name cache. This cache is conccurent safe as it is readonly from now on.
	//
	tyShortNm = make(map[string]string)
	for _, v := range tynames {
		tyShortNm[v.LongNm] = v.ShortNm
	}
	for k, v := range tyShortNm {
		fmt.Println("ShortNames: ", k, v)
	}

	return gsn, nil

}

func graphShortName(g string) (string, error) {

	type sn struct {
		ShortName string
	}
	ctx := context.Background()
	params := map[string]interface{}{"graph": graphNm}

	// stmt returns one row
	stmt := `Select   g.SName ShortName
			from Graph g 
			where g.Name = @graph`

	iter := client.Single().Query(ctx, spanner.Statement{SQL: stmt, Params: params})
	var (
		s    sn
		ierr error
	)
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			ierr = err
			break
		}

		err = row.ToStruct(&s)
		if err != nil {
			ierr = err
			break
		}
	}
	if ierr != nil {
		return "", ierr
	}
	// was the graph found
	if s.ShortName == "" {
		return "", errors.New("Graph not found in type database ")
	}
	return s.ShortName, nil
}

func GetTypeShortNames() ([]tyNames, error) {
	return tynames, nil
}

func LoadDataDictionary() (blk.TyIBlock, error) {

	// 	type TyItem struct {
	// 	Nm   string   `json:"PKey"`  // type name e.g m.Film, r.Person
	// 	Atr  string   `json:"SortK"` // attribute name
	// 	Ty   string   // DataType
	// 	F    []string // facets name#DataType#CompressedIdentifer
	// 	C    string   // compressed identifer for attribute
	// 	P    string   // data partition containig attribute data - TODO: is this obselete???
	// 	Pg   bool     // true: propagate scalar data to parent
	// 	N    bool     // NULLABLE. False : not null (attribute will always exist ie. be populated), True: nullable (attribute may not exist)
	// 	Cd   int      // cardinality - NOT USED
	// 	Sz   int      // average size of attribute data - NOT USED
	// 	Ix   string   // supported indexes: FT=Full Text (S type only), "x" combined with Ty will index in GSI Ty_Ix
	// 	IncP []string // (optional). List of attributes to be propagated. If empty all scalars will be propagated.
	// 	//	cardinality string   // 1:N , 1:1
	// }
	ctx := context.Background()
	//defer client.Close()
	params := map[string]interface{}{"graph": graphNm}

	// stmt returns one row
	stmt := `Select   g.SName||"."||nt.Name Nm, 
			atr.Name Atr,
            atr.SName C,
            atr.Ty ,
            atr.Part P,
            atr.Nullable N,
			IFNULL(atr.Ix,"na") Ix,
			IFNULL(atr.Propagate,false) Pg
			from Graph g join Type nt using (GId) join Attribute atr on (nt.GId = atr.GId and nt.SName = atr.TSName)
			where g.Name = @graph`

	iter := client.Single().Query(ctx, spanner.Statement{SQL: stmt, Params: params})
	defer iter.Stop()

	var dd blk.TyIBlock
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			fmt.Println("no more rows....")
			break
		}
		if err != nil {
			fmt.Println("err in iter.Next()")
			panic(err)
			// TODO: Handle error.
		}
		var tyItem blk.TyItem
		err = row.ToStruct(&tyItem)
		if err != nil {
			return nil, err
		}
		dd = append(dd, &tyItem)
	}
	fmt.Println("dd = ", len(dd))
	return dd, nil
}

func loadTypeShortNames() ([]tyNames, error) {

	syslog("db.loadTypeShortNames ")

	var tyNm tyNames

	ctx := context.Background()

	params := map[string]interface{}{"graph": graphNm}
	//
	stmt := `Select t.Name, t.SName
			from Graph g join Type t using (GId)
			where g.Name = @graph`

	iter := client.Single().Query(ctx, spanner.Statement{SQL: stmt, Params: params})
	defer iter.Stop()

	var tyNms []tyNames
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			fmt.Println("Error: in Graph Query Type...")
			panic(err)
		}
		err = row.ToStruct(&tyNm)
		if err != nil {
			return nil, err
		}
		tyNms = append(tyNms, tyNm)
	}
	return tyNms, nil

}
