//go:build spanner
// +build spanner

// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Sample spanner_arrays is a demonstration program which queries Google's Cloud Spanner
// and returns results containing arrays.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/big"
	"regexp"
	"strconv"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"google.golang.org/api/iterator"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

// Country describes a country and the cities inside it.
type testRec struct {
	XXX      int64 `spanner:"idx"`
	Num      big.Rat
	F64      float64 `spanner:"f64"`
	Intarray []int64
	Strarray []string
	Numarray []big.Rat //float64
}

func main() {
	ctx := context.Background()

	dsn := flag.String("database", "projects/banded-charmer-310203/instances/test-instance/databases/test-sdk-db", "Cloud Spanner database name")
	flag.Parse()

	//Connect to the Spanner Admin API.
	admin, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		log.Fatalf("failed to create database admin client: %v", err)
	}
	defer admin.Close()

	err = createDatabase(ctx, admin, *dsn)
	if err != nil {
		log.Fatalf("failed to create database: %v", err)
	}
	//defer removeDatabase(ctx, admin, *dsn)

	// Connect to database.
	client, err := spanner.NewClient(ctx, *dsn)
	if err != nil {
		log.Fatalf("Failed to create client %v", err)
	}
	defer client.Close()

	err = loadPresets(ctx, client)
	if err != nil {
		log.Fatalf("failed to load preset data: %v", err)
	}

	it := client.Single().Query(ctx, spanner.NewStatement(`SELECT idx,num, f64, intarray,strarray,numarray from testArray where idx=1`))
	defer it.Stop()
	var i int
	for {
		i++
		row, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatalf("failed to read results: %v", err)
		}
		fmt.Printf("row: %d, row: %#v \n", i, row)
		var rec testRec
		if err = row.ToStruct(&rec); err != nil {
			log.Fatalf("failed to read row into Country struct: %v", err)
		}
		fmt.Println("Len(Intarray) : ", len(rec.Intarray))
		log.Println("Key: ", rec.XXX)
		numf, pc := rec.Num.Float64()
		log.Printf("Num: %g", numf)
		log.Printf("f64: %g", rec.F64)
		flt := 0.2
		sf64s := strconv.FormatFloat(flt, 'g', 18, 64)
		numfs := strconv.FormatFloat(numf, 'g', 18, 64)
		log.Printf("numf string from database [%g] [%s] %v", numf, numfs, pc)
		log.Printf("FloatString(rec.Num) string from database [%s]", rec.Num.FloatString(38))
		log.Printf("f64s string not from database [%s]", sf64s)
		sf64 := strconv.FormatFloat(rec.F64, 'g', 18, 64)
		log.Printf("f64 string [%s]", sf64)
		for i, v := range rec.Intarray {
			log.Println(i, v)
		}
		fmt.Println("Exit....")
	}
}

// loadPresets inserts some demonstration data into the tables.
func loadPresets(ctx context.Context, db *spanner.Client) error {
	//mx := []*spanner.Mutation{
	var i int64
	var s []string = []string{"black", "red", "gold"}
	var f []*big.Rat
	var m []*spanner.Mutation
	//
	rt := &big.Rat{}
	rt.SetFloat64(3.1467)
	fmt.Println("rt = %v", rt)
	f = append(f, rt)
	i = -1
	flt := 0.2
	t1 := spanner.InsertMap("TestArray", map[string]interface{}{
		"idx":      1,
		"num":      rt,
		"f64":      flt,
		"Intarray": []int64{i},
		"Strarray": s,
		"Numarray": f,
	})
	t2 := spanner.InsertMap("dual", map[string]interface{}{
		"d": true,
	})
	m = append(m, t1, t2)
	// m2:=spanner.UpdateMap("Accounts", map[string]interface{}{
	// 		"user":    "alice",
	// 		"balance": balance + 10,
	// 	}),

	_, err := db.Apply(ctx, m)

	m = nil
	m1 := spanner.InsertMap("Graph", map[string]interface{}{
		"GId":   1,
		"Name":  "Movies",
		"SName": "m",
	})
	m = append(m, m1)
	m1 = spanner.InsertMap("Graph", map[string]interface{}{
		"GId":   2,
		"Name":  "Relationship",
		"SName": "r",
	})
	m = append(m, m1)
	m2 := spanner.InsertMap("Type", map[string]interface{}{
		"GId":   1,
		"Name":  "Character",
		"SName": "Ch",
	})
	m = append(m, m2)
	m2 = spanner.InsertMap("Type", map[string]interface{}{
		"GId":   1,
		"Name":  "Film",
		"SName": "Fm",
	})
	m = append(m, m2)
	m2 = spanner.InsertMap("Type", map[string]interface{}{
		"GId":   1,
		"Name":  "Genre",
		"SName": "Ge",
	})
	m = append(m, m2)
	m2 = spanner.InsertMap("Type", map[string]interface{}{
		"GId":   1,
		"Name":  "Person",
		"SName": "P",
	})
	m = append(m, m2)
	m2 = spanner.InsertMap("Type", map[string]interface{}{
		"GId":   1,
		"Name":  "Performance",
		"SName": "Pf",
	})
	m = append(m, m2)
	m2 = spanner.InsertMap("Type", map[string]interface{}{
		"GId":   2,
		"Name":  "Person",
		"SName": "P",
	})
	m = append(m, m2)
	m2 = spanner.InsertMap("Attribute", map[string]interface{}{
		"GId":       1,
		"TSName":    "Ch",
		"Name":      "name",
		"SName":     "N",
		"Ty":        "S",
		"Nullable":  false,
		"Propagate": true,
		"Part":      "A",
	})
	m = append(m, m2)
	m2 = spanner.InsertMap("Attribute", map[string]interface{}{
		"GId":       1,
		"TSName":    "Fm",
		"Name":      "film.director",
		"SName":     "D",
		"Part":      "A",
		"Ty":        "[Person]",
		"Propagate": true,
		"Nullable":  false,
	})
	m = append(m, m2)
	m2 = spanner.InsertMap("Attribute", map[string]interface{}{
		"GId":       1,
		"TSName":    "Fm",
		"Name":      "film.genre",
		"SName":     "G",
		"Part":      "A",
		"Ty":        "[Genre]",
		"Propagate": true,
		"Nullable":  false,
	})
	m = append(m, m2)
	m2 = spanner.InsertMap("Attribute", map[string]interface{}{
		"GId":       1,
		"TSName":    "Fm",
		"Name":      "netflix",
		"SName":     "Nf",
		"Part":      "A",
		"Propagate": false,
		"Ty":        "I",
		"Nullable":  true,
	})
	m = append(m, m2)
	m2 = spanner.InsertMap("Attribute", map[string]interface{}{
		"GId":      1,
		"TSName":   "Fm",
		"Name":     "film.performance",
		"SName":    "P",
		"Part":     "A",
		"Ty":       "[Performance]",
		"Nullable": false,
	})
	m = append(m, m2)
	m2 = spanner.InsertMap("Attribute", map[string]interface{}{
		"GId":       1,
		"TSName":    "Fm",
		"Name":      "initial_release_date",
		"SName":     "R",
		"Part":      "A",
		"Propagate": true,
		"Ty":        "S",
		"Nullable":  true,
	})
	m = append(m, m2)
	m2 = spanner.InsertMap("Attribute", map[string]interface{}{
		"GId":       1,
		"TSName":    "Fm",
		"Name":      "revenue",
		"SName":     "Rv",
		"Part":      "A",
		"Propagate": true,
		"Ty":        "F",
		"Nullable":  true,
	})
	m = append(m, m2)
	m2 = spanner.InsertMap("Attribute", map[string]interface{}{
		"GId":       1,
		"TSName":    "Fm",
		"Name":      "title",
		"SName":     "N",
		"Part":      "A",
		"Propagate": true,
		"Ty":        "S",
		"Nullable":  false,
		"Ix":        "FTg",
	})
	m = append(m, m2)
	m2 = spanner.InsertMap("Attribute", map[string]interface{}{
		"GId":       1,
		"TSName":    "Ge",
		"Name":      "name",
		"SName":     "N",
		"Part":      "A",
		"Propagate": true,
		"Ty":        "S",
		"Nullable":  false,
	})
	m = append(m, m2)
	m2 = spanner.InsertMap("Attribute", map[string]interface{}{
		"GId":      1,
		"TSName":   "Pf",
		"Name":     "performance.actor",
		"SName":    "A",
		"Part":     "A",
		"Ty":       "Person",
		"Nullable": false,
	})
	m = append(m, m2)
	m2 = spanner.InsertMap("Attribute", map[string]interface{}{
		"GId":      1,
		"TSName":   "Pf",
		"Name":     "performance.character",
		"SName":    "C",
		"Part":     "A",
		"Ty":       "Character",
		"Nullable": false,
	})
	m = append(m, m2)
	m2 = spanner.InsertMap("Attribute", map[string]interface{}{
		"GId":      1,
		"TSName":   "Pf",
		"Name":     "performance.film",
		"SName":    "F",
		"Part":     "A",
		"Ty":       "Film",
		"Nullable": false,
	})
	m = append(m, m2)
	m2 = spanner.InsertMap("Attribute", map[string]interface{}{
		"GId":      1,
		"TSName":   "P",
		"Name":     "actor.performance",
		"SName":    "A",
		"Part":     "A",
		"Ty":       "[Performance]",
		"Nullable": false,
	})
	m = append(m, m2)
	m2 = spanner.InsertMap("Attribute", map[string]interface{}{
		"GId":       1,
		"TSName":    "P",
		"Name":      "director.film",
		"SName":     "D",
		"Part":      "A",
		"Propagate": false,
		"Ty":        "[Film]",
		"Nullable":  false,
	})
	m = append(m, m2)
	m2 = spanner.InsertMap("Attribute", map[string]interface{}{
		"GId":       1,
		"TSName":    "P",
		"Name":      "name",
		"SName":     "N",
		"Part":      "A",
		"Propagate": true,
		"Ty":        "S",
		"Nullable":  false,
	})
	m = append(m, m2)
	//
	m2 = spanner.InsertMap("Attribute", map[string]interface{}{
		"GId":       2,
		"TSName":    "P",
		"Name":      "Address",
		"SName":     "E",
		"Part":      "A",
		"Propagate": true,
		"Ty":        "S",
		"Nullable":  true,
	})
	m = append(m, m2)
	m2 = spanner.InsertMap("Attribute", map[string]interface{}{
		"GId":       2,
		"TSName":    "P",
		"Name":      "Age",
		"SName":     "A",
		"Part":      "A",
		"Propagate": true,
		"Ty":        "I",
		"Nullable":  true,
	})
	m = append(m, m2)
	m2 = spanner.InsertMap("Attribute", map[string]interface{}{
		"GId":      2,
		"TSName":   "P",
		"Name":     "Cars",
		"SName":    "C",
		"Part":     "A",
		"Ty":       "LS",
		"Nullable": true,
	})
	m = append(m, m2)
	m2 = spanner.InsertMap("Attribute", map[string]interface{}{
		"GId":       2,
		"TSName":    "P",
		"Name":      "Comment",
		"SName":     "Ct",
		"Part":      "A",
		"Propagate": false,
		"Ty":        "S",
		"IX":        "FT",
		"Nullable":  true,
	})
	m = append(m, m2)
	m2 = spanner.InsertMap("Attribute", map[string]interface{}{
		"GId":       2,
		"TSName":    "P",
		"Name":      "DOB",
		"SName":     "D",
		"Part":      "A",
		"Propagate": true,
		"Ty":        "S",
		"Nullable":  true,
	})
	m = append(m, m2)
	m2 = spanner.InsertMap("Attribute", map[string]interface{}{
		"GId":      2,
		"TSName":   "P",
		"Name":     "Friends",
		"SName":    "F",
		"Part":     "A",
		"Ty":       "[Person]",
		"Nullable": true,
	})
	m = append(m, m2)
	m2 = spanner.InsertMap("Attribute", map[string]interface{}{
		"GId":      2,
		"TSName":   "P",
		"Name":     "Jobs",
		"SName":    "J",
		"Part":     "A",
		"Ty":       "LS",
		"Nullable": true,
	})
	m = append(m, m2)
	m2 = spanner.InsertMap("Attribute", map[string]interface{}{
		"GId":       2,
		"TSName":    "P",
		"Name":      "Name",
		"SName":     "N",
		"Part":      "A",
		"Propagate": true,
		"Ty":        "S",
		"IX":        "FTg",
		"Nullable":  false,
	})
	m = append(m, m2)
	m2 = spanner.InsertMap("Attribute", map[string]interface{}{
		"GId":      2,
		"TSName":   "P",
		"Name":     "SalaryLast3Year",
		"SName":    "SLY",
		"Part":     "A",
		"Ty":       "LI",
		"Nullable": true,
	})
	m = append(m, m2)
	m2 = spanner.InsertMap("Attribute", map[string]interface{}{
		"GId":      2,
		"TSName":   "P",
		"Name":     "Siblings",
		"SName":    "S",
		"Part":     "A",
		"Ty":       "[Person]",
		"Nullable": true,
	})
	m = append(m, m2)

	_, err = db.Apply(ctx, m)
	return err
}

// createDatabase uses the Spanner database administration client to create the tables used in this demonstration.
func createDatabase(ctx context.Context, adminClient *database.DatabaseAdminClient, db string) error {
	matches := regexp.MustCompile("^(.*)/databases/(.*)$").FindStringSubmatch(db)
	if matches == nil || len(matches) != 3 {
		log.Fatalf("Invalid database id %s", db)
	}

	var (
		projectID    = matches[1]
		databaseName = matches[2]
	)

	op, err := adminClient.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:          projectID,
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", databaseName),
		ExtraStatements: []string{
			`CREATE TABLE mon_run (
				run BYTES(16)  NOT NULL,
				start TIMESTAMP NOT NULL,
				finish TIMESTAMP ,
				dur String,
				program STRING(32) NOT NULL,
				args ARRAY<STRING(MAX)>,
				status STRING(1) NOT NULL,
				logfile STRING(128),
			) PRIMARY KEY(run)`,
			`CREATE TABLE mon_gr (
				run BYTES(16)  NOT NULL,
				gr STRING(64) NOT NULL,
				s10 float64 NOT NULL,
				s20 float64,
				s40 float64,
				m1 FLOAT64,
				m2 FLOAT64,
				m3 Float64,
				m5 FLOAT64,
				m10 FLOAT64,
				m20 FLOAT64,
				m40 FLOAT64,
				h1 FLOAT64,
				h2 FLOAT64,
			) PRIMARY KEY(run,gr),
			INTERLEAVE IN PARENT mon_run ON DELETE CASCADE`,
			`CREATE TABLE Edge_ (
				Bid INT64 NOT NULL,
				Puid BYTES(16) NOT NULL,
				Cnt INT64 ,
			) PRIMARY KEY(Bid,Puid)`,
			// improve query performance based on CNT - make Cnt nullable and populate with null once reaches 0 - so index only contains items of interest.
			//`CREATE NULL_FILTERED INDEX EdgeIdx_ ON Edge_(Cnt, Puid)`,
			`CREATE TABLE EdgeChild_ (
  				Puid BYTES(16) NOT NULL,
				Cuid BYTES(16) NOT NULL,
				Sortk STRING(64) NOT NULL, 
				Status STRING(1),
				ErrMsg STRING(256),
				) PRIMARY KEY(Puid,Sortk,Cuid)`,
			`CREATE NULL_FILTERED INDEX EdgeChildStatus_ ON EdgeChild_(Status)`,
			// Graph Table: Block + EOP + NodeScalar + ReverseEdge
			// P: for Overflow block only. UID of parent node.
			// IX: populated with X and then Null after bulk DP performed
			// IsNode: Y for is graph node. Overflow blocks are not included.
			`CREATE TABLE Block (
  				PKey BYTES(16) NOT NULL,
				Graph STRING(8) NOT NULL,
				Ty STRING(64) NOT NULL,
				IsNode STRING(1), 
				P BYTES(16),
				IX STRING(1),
				) PRIMARY KEY(PKey)`,
			`CREATE NULL_FILTERED INDEX TyIX ON Block(Ty,IX,PKey)`, // Ty incudes GR
			`CREATE NULL_FILTERED INDEX IsNode ON Block(IsNode)`,
			// EOP: Edge-Overflow-Propagated data
			//      Edge: UID-PRED Nd, Id, XF, N (total edge count)
			//      Overflow: Nd, XBl, L*, ASZ (?)
			//      Propagated: L*, XBl
			`CREATE TABLE EOP (
			PKey BYTES(16) NOT NULL,
			SortK STRING(64) NOT NULL,
			Nd ARRAY<BYTES(16)> ,
			Id ARRAY<INT64> ,
			XF ARRAY<INT64> ,
			Ty STRING(18),
			// CNT INT64, use N
			N INT64, 
			ASZ INT64,
			LS     ARRAY<STRING(MAX)>,
			LI    ARRAY<INT64>,
			LF    ARRAY<FLOAT64>,
			LBl    ARRAY<BOOL>,
			LB     ARRAY<BYTES(MAX)>,
			LDT  ARRAY<TIMESTAMP>,
			XBl	ARRAY<BOOL>,
			) PRIMARY KEY(PKey, SortK),
			INTERLEAVE IN PARENT Block ON DELETE CASCADE`,
			//`CREATE NULL_FILTERED INDEX uidpredcnt ON EOP(SortK, Ty, N, PKey)`,
			`CREATE NULL_FILTERED INDEX EdgeCount ON NodeScalar(P,N)`,
			`CREATE TABLE NodeScalar (
			PKey BYTES(16) NOT NULL,
			SortK STRING(64) NOT NULL,
			P STRING(32),
			Bl BOOL,
			I INT64, 
			F FLOAT64,
			S STRING(MAX),
			B BYTES(MAX),
			DT TIMESTAMP,
			LS   ARRAY<STRING(MAX)>,
			LI   ARRAY<INT64>,
			LF   ARRAY<FLOAT64>,
            LBl  ARRAY<BOOL>,
            LB   ARRAY<BYTES(MAX)>,
            LDT  ARRAY<TIMESTAMP>,
			) PRIMARY KEY(PKey, SortK),
			INTERLEAVE IN PARENT Block ON DELETE CASCADE`,
			`CREATE NULL_FILTERED INDEX NodePredicateF ON NodeScalar(P,F)`,
			`CREATE NULL_FILTERED INDEX NodePredicateS ON NodeScalar(P,S)`,
			`CREATE NULL_FILTERED INDEX NodePredicateI ON NodeScalar(P,N)`,
			`CREATE NULL_FILTERED INDEX NodePredicateBl ON NodeScalar(P,Bl)`,
			`CREATE NULL_FILTERED INDEX NodePredicateB ON NodeScalar(P,B)`,
			`CREATE NULL_FILTERED INDEX NodePredicateDT ON NodeScalar(P,DT)`,
			`Create table dual (
				d bool,
			) PRIMARY KEY (d)`,
			`CREATE TABLE ReverseEdge (
			PKey BYTES(16) NOT NULL, 
			SortK STRING(64) NOT NULL,
			pUID BYTES(16) NOT NULL,
			oUID BYTES(16) ,
			batch INT64, 
			) PRIMARY KEY(PKey,SortK,pUID),
			INTERLEAVE IN PARENT Block ON DELETE CASCADE`,
			`CREATE INDEX Reverse ON ReverseEdge(PKey,pUID)`,
			`Create table TestArray (
				idx INT64 NOT NULL,
				Num  numeric,
				f64 float64,
				i64 int64,
				numarray ARRAY<NUMERIC>,
				intarray ARRAY<INT64>,
				Strarray ARRAY<STRING(100)>,
				Bytearray ARRAY<BYTES(16)>,
			) PRIMARY KEY (idx)`,
			`Create table Graph (
		GId INT64 NOT NULL,
		Name STRING(MAX) NOT NULL,    
		SName STRING(MAX) NOT NULL,
		) PRIMARY KEY(GId)`,
			`create table Type (
		GId INT64 NOT NULL,
		Name STRING(MAX) NOT NULL,
		SName STRING(MAX) NOT NULL,
  		) PRIMARY KEY(GId, Name),
            INTERLEAVE IN PARENT Graph ON DELETE CASCADE`,
			`create table Attribute (
		GId INT64 NOT NULL,
		TSName STRING(MAX) NOT NULL,
		Name STRING(MAX) NOT NULL,
		Ty  STRING(MAX) NOT NULL,
		SName STRING(MAX) NOT NULL, 
		Nullable BOOL ,
		AttributesPropagate ARRAY<STRING(MAX)>,
		Facet  ARRAY<STRING(MAX)>,
		Ix STRING(5) ,
			Part STRING(8),
			Propagate BOOL,
		) PRIMARY KEY(GId,TSName, Name),
            INTERLEAVE IN PARENT Graph ON DELETE CASCADE`,

			`create unique NULL_FILTERED  index GraphName on Graph (GId,SName)`,
			`create unique NULL_FILTERED  index TypeName on Type (GId,SName)`,
			`create unique NULL_FILTERED  index AttrName on Attribute (GId,TSName, SName)`,
			`create table eventlog (
    eid  BYTES(16) NOT NULL,
    event STRING(16) NOT NULL,
    status STRING(1) NOT NULL,
	start  Timestamp NOT NULL,
	finish Timestamp ,
    dur    STRING(30),
    err    STRING(MAX),
) PRIMARY KEY (eid)`,
			`create table NodeAttachDetachEvent (
    eid  BYTES(16) NOT NULL,
    PUID BYTES(16) NOT NULL,
    CUID BYTES(16) NOT NULL,
    SORTK STRING(MAX) NOT NULL,
) PRIMARY KEY (eid),
            INTERLEAVE IN PARENT eventlog ON DELETE CASCADE`,
			`CREATE TABLE esLog (
				PKey BYTES(16) NOT NULL,
				Sortk STRING(64) NOT NULL,
				runid INT64 NOT NULL,
				Ty STRING(18) NOT NULL,
				Graph STRING(12) NOT NULL,
			  ) PRIMARY KEY(PKey),
				INTERLEAVE IN PARENT Block ON DELETE CASCADE`,
			`create table testlog (
					ID STRING(64) NOT NULL,
					Test STRING(32) NOT NULL,
					Status STRING(2) NOT NULL,
					Nodes INT64 NOT NULL,
					Levels STRING(256) NOT NULL,
					ParseET FLOAT64 NOT NULL,
					ExecET FLOAT64  NOT NULL,
					JSON STRING(MAX) ,
					DBread INT64 ,
					Msg STRING(256)
				) Primary Key (ID,Test)`,
			`create index testi on testlog (Test) `,
		},
	})
	if err != nil {
		return err
	}
	if _, err := op.Wait(ctx); err == nil {
		log.Printf("Created database [%s]", db)
	}
	return err
}

// removeDatabase deletes the database which this demonstration program created.
func removeDatabase(ctx context.Context, adminClient *database.DatabaseAdminClient, db string) {
	if err := adminClient.DropDatabase(ctx, &adminpb.DropDatabaseRequest{Database: db}); err != nil {
		log.Fatalf("Failed to remove database [%s]: %v", db, err)
	} else {
		log.Printf("Removed database [%s]", db)
	}
}
