package tx

import (
	"context"
	"strconv"
	"sync"
	"testing"

	dyn "github.com/GoGraph/db"
	"github.com/GoGraph/mysql"
	"github.com/GoGraph/tx/db"
)

type Person struct {
	FirstName string
	LastName  string
	DOB       string
}

type Country struct {
	Name       string
	Population int
	Person
	Nperson Person
}

type Address struct {
	Line1, Line2, Line3 string
	City                string
	Zip                 string
	State               string
	Cntry               Country
}

func TestSelect(t *testing.T) {

	type Input struct {
		Status byte
		Person
		Loc Address
	}

	p := Person{FirstName: "John", LastName: "Payneh", DOB: "13 March 1967"}
	//c := Country{Name: "Australia", Population: 23000}
	c := Country{Name: "Australia", Population: 23000, Person: p, Nperson: p}
	ad := Address{Line1: "Villa 647", Line2: "5 Bukitt St", Line3: "Page", Zip: "2678", State: "ACU", Cntry: c}
	x := Input{Status: 'C', Person: p, Loc: ad}

	nn := NewQuery("label", "table")
	nn.Select(&x)
	for k, v := range nn.GetAttr() {
		t.Logf("%d: name: %s", k, v.Name())
	}

	for i, v := range nn.Split() {
		switch x := v.(type) {
		case *int:
			t.Logf("%d: int: [%d]", i, *x)
		case *uint8:
			t.Logf("%d: uint8: [%d]", i, *x)
		case *string:
			t.Logf("%d: String: [%s]", i, *x)

		}
	}
}

func TestSelectSlice(t *testing.T) {

	type Input struct {
		Status byte
		Person
		Loc Address
	}

	var x []Input
	//x := Input{Status: 'C', Person: p, Loc: ad}

	nn := NewQuery("label", "table")
	nn.Select(&x)

	for k, v := range nn.GetAttr() {
		t.Logf("%d: name: %s", k, v.Name())
	}

	for i := 0; i < 4; i++ {
		for _, v := range nn.Split() {

			switch x := v.(type) {
			case *int:
				*x = 234 * i
				t.Logf("%d: int: [%d]", i, *x)
			case *uint8:
				*x = '$'
				t.Logf("%d: uint8: [%d]", i, *x)
			case *string:
				*x = "abcdefg"
				t.Logf("%d: String: [%s]", i, *x)
			}
		}
	}
}

func TestSelectSlicePtr(t *testing.T) {

	type Input struct {
		Status byte
		Person
		Loc Address
	}

	var x []*Input
	//x := Input{Status: 'C', Person: p, Loc: ad}

	nn := NewQuery("label", "table")
	nn.Select(&x)

	for k, v := range nn.GetAttr() {
		t.Logf("%d: name: %s", k, v.Name())
	}

	for i := 0; i < 4; i++ {
		for _, v := range nn.Split() {

			switch x := v.(type) {
			case *int:
				*x = 234 * i

			case *uint8:
				*x = '$'

			case *string:
				*x = "abcdefg" + strconv.Itoa(i)

			}
		}
	}

	for i, v := range x {
		t.Logf("%d: status: [%d], State: %s", i, v.Loc.Cntry.Population, v.Loc.State)
	}

}

type tyNames struct {
	ShortNm string `dynamodbav:"SortK"`
	LongNm  string `dynamodbav:"Name"`
}

func TestQueryTypesPtrSlice(t *testing.T) {

	type graphMeta struct {
		SortK string
	}
	var wpEnd sync.WaitGroup

	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk []*graphMeta

	txg := NewQuery("GraphName", "GoGraphSS")
	txg.Select(&sk).Key("PKey", "#Graph").Filter("Name", "Movies")
	err := txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query count %d\n", len(sk))
	for _, v := range sk {
		t.Logf("Query count %#v\n", v.SortK)
	}

}

func TestQueryTypesStruct(t *testing.T) {

	type graphMeta struct {
		SortK string
	}
	var wpEnd sync.WaitGroup

	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk graphMeta

	txg := NewQuery("GraphName", "GoGraphSS")
	txg.Select(&sk).Key("PKey", "#Graph").Filter("Name", "Movies")
	err := txg.Execute()

	if err != nil {
		if err.Error() != "Bind variable in Select() must be slice for a query database operation" {
			t.Errorf("Error: %s", err)
		}
	} else {

		t.Logf("Query struct  %s\n", sk.SortK)
	}

}

func TestQueryTypesPtrSliceSQL(t *testing.T) {

	type Testlog struct {
		Test   string
		Logdt  string
		Status string
		Nodes  int
	}

	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mysql.Register(ctx, "mysql-GoGraph", "admin:gjIe8Hl9SFD1g3ahyu6F@tcp(mysql8.cjegagpjwjyi.us-east-1.rds.amazonaws.com:3306)/GoGraph")
	//select test,logdt,status,nodes from Log$GoTest;
	var sk []Testlog

	txg := NewQuery("LogTest", "Log$GoTest").DB("mysql-GoGraph")
	txg.Select(&sk).Key("test", "TestMoviex")
	err := txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query count %d\n", len(sk))
	for _, v := range sk {
		t.Log(v.Test, v.Logdt, v.Status, v.Nodes)
	}
}

func TestQueryTypesPtrSlicePtrSQL(t *testing.T) {

	type Testlog struct {
		Test   string
		Logdt  string
		Status string
		Nodes  int
	}

	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mysql.Register(ctx, "mysql-GoGraph", "admin:gjIe8Hl9SFD1g3ahyu6F@tcp(mysql8.cjegagpjwjyi.us-east-1.rds.amazonaws.com:3306)/GoGraph")
	//select test,logdt,status,nodes from Log$GoTest;
	var sk []*Testlog

	txg := NewQuery("LogTest", "Log$GoTest").DB("mysql-GoGraph")
	txg.Select(&sk).Key("test", "TestMoviex")
	err := txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query count %d\n", len(sk))

	for _, v := range sk {
		t.Log(v.Test, v.Logdt, v.Status, v.Nodes)
	}
}

func TestQueryTypesPtrSlicePtrNestedSQL(t *testing.T) {

	type Nstatus struct {
		Status string
		Nodes  int
	}

	type Testlog struct {
		Test  string
		Logdt string
		Nstatus
	}

	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mysql.Register(ctx, "mysql-GoGraph", "admin:gjIe8Hl9SFD1g3ahyu6F@tcp(mysql8.cjegagpjwjyi.us-east-1.rds.amazonaws.com:3306)/GoGraph")
	//select test,logdt,status,nodes from Log$GoTest;
	var sk []*Testlog

	txg := NewQuery("LogTest", "Log$GoTest").DB("mysql-GoGraph")
	txg.Select(&sk).Key("test", "TestMoviex")
	err := txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query count %d\n", len(sk))

	for _, v := range sk {
		t.Log(v.Test, v.Logdt, v.Status, v.Nodes)
	}
}

func TestQueryTypesPtrSliceNested2SQL(t *testing.T) {

	type Nstatus struct {
		Status string
		Nodes  int
	}

	type Ntest struct {
		Test  string `mdb:"Test"`
		Logdt string `mdb:"Logdt"`
	}

	type Testlog struct {
		Xyz Ntest
		Nstatus
	}

	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mysql.Register(ctx, "mysql-GoGraph", "admin:gjIe8Hl9SFD1g3ahyu6F@tcp(mysql8.cjegagpjwjyi.us-east-1.rds.amazonaws.com:3306)/GoGraph")
	//select test,logdt,status,nodes from Log$GoTest;
	var sk []Testlog

	txg := NewQuery("LogTest", "Log$GoTest").DB("mysql-GoGraph")
	txg.Select(&sk).Key("test", "TestMoviex")
	err := txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query count %d\n", len(sk))

	for _, v := range sk {
		t.Log(v.Xyz.Test, v.Xyz.Logdt, v.Status, v.Nodes)
	}
}

func TestQueryTypesPtrSlicePtrNested2SQL(t *testing.T) {

	type Nstatus struct {
		Status string
		Nodes  int
	}

	type Ntest struct {
		Test  string `mdb:"Test"`
		Logdt string `mdb:"Logdt"`
	}

	type Testlog struct {
		Xyz Ntest
		Nstatus
	}

	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mysql.Register(ctx, "mysql-GoGraph", "admin:gjIe8Hl9SFD1g3ahyu6F@tcp(mysql8.cjegagpjwjyi.us-east-1.rds.amazonaws.com:3306)/GoGraph")
	//select test,logdt,status,nodes from Log$GoTest;
	var sk []*Testlog

	txg := NewQuery("LogTest", "Log$GoTest").DB("mysql-GoGraph")
	txg.Select(&sk).Key("test", "TestMoviex")
	err := txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query count %d\n", len(sk))

	for _, v := range sk {
		t.Log(v.Xyz.Test, v.Xyz.Logdt, v.Status, v.Nodes)
	}
}

func TestQueryTypesPtrSlicePtrNested2SQL2(t *testing.T) {

	type Nstatus struct {
		Status string
		Nodes  int
	}

	type Ntest struct {
		Test  string `mdb:"-"`
		Logdt string `mdb:"Logdt"`
	}

	type Testlog struct {
		Xyz Ntest
		Nstatus
	}

	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mysql.Register(ctx, "mysql-GoGraph", "admin:gjIe8Hl9SFD1g3ahyu6F@tcp(mysql8.cjegagpjwjyi.us-east-1.rds.amazonaws.com:3306)/GoGraph")
	//select test,logdt,status,nodes from Log$GoTest;
	var sk []*Testlog

	txg := NewQuery("LogTest", "Log$GoTest").DB("mysql-GoGraph")
	txg.Select(&sk).Key("test", "TestMoviex")
	err := txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query count %d\n", len(sk))

	for _, v := range sk {
		t.Log(v.Xyz.Test, v.Xyz.Logdt, v.Status, v.Nodes)
	}
}

func TestQueryTypesPtrSlicePtrNested2SQL3(t *testing.T) {

	type Nstatus struct {
		Status string
		Nodes  int
	}

	type Ntest struct {
		Test string `mdb:"-"`
		Fred string `mdb:"Logdt"`
	}

	type Testlog struct {
		Xyz Ntest
		Nstatus
	}

	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mysql.Register(ctx, "mysql-GoGraph", "admin:gjIe8Hl9SFD1g3ahyu6F@tcp(mysql8.cjegagpjwjyi.us-east-1.rds.amazonaws.com:3306)/GoGraph")
	//select test,logdt,status,nodes from Log$GoTest;
	var sk []*Testlog

	txg := NewQuery("LogTest", "Log$GoTest").DB("mysql-GoGraph")
	txg.Select(&sk).Key("test", "TestMoviex")
	err := txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query count %d\n", len(sk))

	for _, v := range sk {
		t.Log(v.Xyz.Test, v.Xyz.Fred, v.Status, v.Nodes)
	}
}
