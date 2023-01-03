package tx

import (
	"context"
	//	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"

	dyn "github.com/GoGraph/db"
	"github.com/GoGraph/mysql"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx/db"
	"github.com/GoGraph/tx/mut"
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
	if len(sk) != 1 {
		t.Errorf(`Expected 1 got %d`, len(sk))
	}
	for _, v := range sk {
		t.Logf("Query count %#v\n", v.SortK)
		if v.SortK != "m" {
			t.Errorf(`Expected "m" got %q`, v.SortK)
		}
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
			t.Errorf("Expected error: %s", err)
		}
	} else {

		t.Logf("Expected error: Bind variable in Select() must be slice for a query database operation")
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

	if len(sk) != 28 {
		t.Errorf("Expected len(sk) to be 28, got %d", len(sk))
	}

	t.Logf("Query count %d\n", len(sk))
	for _, v := range sk {
		t.Log(v.Test, v.Logdt, v.Status, v.Nodes)
	}
}

func TestQueryThreeBindvars(t *testing.T) {

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
	var (
		sk  []Testlog
		sk2 []Testlog
		sk3 []Testlog
	)

	txg := NewQuery("LogTest", "Log$GoTest").DB("mysql-GoGraph")
	txg.Select(&sk, &sk2, &sk3).Key("test", "TestMoviex")
	err := txg.Execute()

	if err != nil {
		if err.Error() != `no more than two bind variables are allowed.` {
			t.Errorf(`Expected Error: "no more than two bind variables are allowed." got %q`, err)
		}
	} else {
		t.Errorf(`Expected Error: "no more than two bind variables are allowed."`)
	}

}

func TestQueryZeroBindvars(t *testing.T) {

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

	txg := NewQuery("LogTest", "Log$GoTest").DB("mysql-GoGraph")
	txg.Select().Key("test", "TestMoviex")
	err := txg.Execute()

	if err != nil {
		if err.Error() != `requires upto two bind variables` {
			t.Errorf(`Expected Error: "requires upto two bind variables" got %q`, err)
		}
	} else {
		t.Errorf(`Expected Error: "requires upto two bind variables"`)
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

func TestQueryPop1(t *testing.T) {

	type City struct {
		Pop int `dynamodbav:"Population"`
	}
	var wpEnd sync.WaitGroup

	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", "GoUnitTest")
	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err := txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

}

func TestQueryPop2(t *testing.T) {

	type City struct {
		Pop int `mdb:"Population"` // will not convert to dynamodbav
	}
	var wpEnd sync.WaitGroup

	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", "GoUnitTest")
	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err := txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

}

func TestQueryPopUpdate(t *testing.T) {

	var tbl tbl.Name = "GoUnitTest"
	type City struct {
		Pop int `dynamodbav:"Population"`
	}
	var wpEnd sync.WaitGroup

	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", tbl)

	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err := txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

	utx := New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	utx.NewUpdate(tbl).AddMember("PKey", 1001, mut.IsKey).AddMember("SortK", "Sydney", mut.IsKey).AddMember("Population", sk.Pop+1)

	err = utx.Execute()
	if err != nil {
		t.Errorf("Update error: %s", err.Error())
	}
	var sk2 City
	txg = NewQuery("pop", tbl)
	txg.Select(&sk2).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk2.Pop)
	if sk2.Pop != sk.Pop+1 {
		t.Fail()
	}

}

func TestQueryPopUpdateKey(t *testing.T) {

	var tbl tbl.Name = "GoUnitTest"
	type City struct {
		Pop int `dynamodbav:"Population"`
	}
	var wpEnd sync.WaitGroup

	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", tbl)

	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err := txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

	utx := New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	utx.NewUpdate(tbl).Key("PKey", 1001, "GT").Key("SortK", "Sydney").Set("Population", sk.Pop+1)

	err = utx.Execute()
	if err != nil {
		if err.Error() != `Expression error in txUpdate. Equality operator for Dynamodb Partition Key must be "EQ"` {
			t.Errorf("Update error: %s", err.Error())
		}
	}
	var sk2 City
	txg = NewQuery("pop", tbl)
	txg.Select(&sk2).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk2.Pop)
	if sk2.Pop != sk.Pop {
		t.Fail()
	}

}
func TestQueryPopUpdate2(t *testing.T) {

	var tbl tbl.Name = "GoUnitTest"
	type City struct {
		Pop int `dynamodbav:"Population"`
	}
	var wpEnd sync.WaitGroup

	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", tbl)

	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err := txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

	utx := New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	utx.NewUpdate(tbl).AddMember("PKey", 1001).AddMember("SortK", "Sydney").AddMember("Population", sk.Pop+1)

	err = utx.Execute()
	if err != nil {
		t.Errorf("Update error: %s", err.Error())
	}
	var sk2 City
	txg = NewQuery("pop", tbl)
	txg.Select(&sk2).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk2.Pop)
	if sk2.Pop != sk.Pop+1 {
		t.Fail()
	}

}

func TestQueryPopUpdate3(t *testing.T) {

	var tbl tbl.Name = "GoUnitTest"
	type City struct {
		Pop int `dynamodbav:"Population"`
	}
	var wpEnd sync.WaitGroup

	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", tbl)

	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err := txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

	utx := New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	utx.NewUpdate(tbl).Key("PKey", 1001).Key("SortK", "Sydney").AddMember("Population", sk.Pop+1)

	err = utx.Execute()
	if err != nil {
		t.Errorf("Update error: %s", err.Error())
	}
	var sk2 City
	txg = NewQuery("pop", tbl)
	txg.Select(&sk2).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk2.Pop)

	if sk2.Pop != sk.Pop+1 {
		t.Fail()
	}

}

func TestQueryPopUpdateSet(t *testing.T) {

	var tbl tbl.Name = "GoUnitTest"
	type City struct {
		Pop int `dynamodbav:"Population"`
	}
	var wpEnd sync.WaitGroup

	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", tbl)

	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err := txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

	utx := New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	utx.NewUpdate(tbl).Key("PKey", 1001).Key("SortK", "Sydney").Set("Population", sk.Pop+1)

	err = utx.Execute()
	if err != nil {
		t.Errorf("Update error: %s", err.Error())
	}
	var sk2 City
	txg = NewQuery("pop", tbl)
	txg.Select(&sk2).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk2.Pop)

	if sk2.Pop != sk.Pop+1 {
		t.Fail()
	}

}

func TestQueryPopUpdateSetKeyWrong(t *testing.T) {

	var tbl tbl.Name = "GoUnitTest"
	type City struct {
		Pop int `dynamodbav:"Population"`
	}
	var wpEnd sync.WaitGroup

	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", tbl)

	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err := txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

	utx := New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	utx.NewUpdate(tbl).Key("PKey", 1001).Key("SortK", "Sydney").Set("Population", sk.Pop+1)

	err = utx.Execute()
	if err != nil {
		t.Errorf("Update error: %s", err.Error())
	}
	var sk2 City
	txg = NewQuery("pop", tbl)
	txg.Select(&sk2).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk2.Pop)

	if sk2.Pop != sk.Pop+1 {
		t.Fail()
	}

}

func TestQueryPopUpdateError(t *testing.T) {
	var err error
	//start syslog services (if any)
	// err := slog.Start()
	// if err != nil {
	// 	panic(fmt.Errorf("Error starting syslog services: %w", err))
	// }

	err = slog.Start()
	if err != nil {
		t.Errorf("Error starting syslog services: %s", err)
	}

	var tbl tbl.Name = "GoUnitTest"
	type City struct {
		Pop int `dynamodbav:"Population"`
	}
	var wpEnd sync.WaitGroup

	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", tbl)

	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

	utx := New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	utx.NewUpdate(tbl).Key("PKey2", 1001).Key("SortK", "Sydney").AddMember("Population", sk.Pop+1)

	err = utx.Execute()
	if err != nil {
		if err.Error() != `Error in Tx setup (see system log for complete list): Error: "PKey2" is not a key in table` {
			t.Errorf("Update error: %s", err.Error())
		}
	}
	var sk2 City
	txg = NewQuery("pop", tbl)
	txg.Select(&sk2).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk2.Pop)

	if sk2.Pop != sk.Pop {
		t.Fail()
	}

}

func TestQueryPopUpdateSAdd(t *testing.T) {

	var tbl tbl.Name = "GoUnitTest"
	type City struct {
		Pop int `dynamodbav:"Population"`
	}
	var wpEnd sync.WaitGroup

	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", tbl)

	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err := txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

	utx := New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	utx.NewUpdate(tbl).Key("PKey", 1001).Key("SortK", "Sydney").Add("Population", 1)

	err = utx.Execute()
	if err != nil {
		t.Errorf("Update error: %s", err.Error())
	}
	var sk2 City
	txg = NewQuery("pop", tbl)
	txg.Select(&sk2).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk2.Pop)

	if sk2.Pop != sk.Pop+1 {
		t.Fail()
	}

}

func TestQueryPopUpdateWhere22(t *testing.T) {

	var tbl tbl.Name = "GoUnitTest"
	type City struct {
		SortK string
		Pop   int `dynamodbav:"Population"`
	}
	var wpEnd sync.WaitGroup

	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", tbl)

	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err := txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

	utx := New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	utx.NewUpdate(tbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation)`).Subtract("Population", 1)

	err = utx.Execute()
	if err != nil {
		t.Errorf("Update error: %s", err.Error())
	}
	var sk2 City
	txg = NewQuery("pop", tbl)
	txg.Select(&sk2).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk2.Pop)

	if sk2.Pop != sk.Pop-1 {
		t.Fail()
	}

}

func TestQueryPopUpdateWhere23(t *testing.T) {

	var tbl tbl.Name = "GoUnitTest"
	type City struct {
		Pop int `dynamodbav:"Population"`
	}
	var wpEnd sync.WaitGroup

	err := slog.Start()
	if err != nil {
		t.Errorf("Error starting syslog services: %s", err)
	}

	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", tbl)

	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

	utx := New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//utx.NewUpdate(tbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists("MaxPopulation") and Population<MaxPopulation`).Subtract("Population", 1)
	utx.NewUpdate(tbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) `).Subtract("Population", 1)

	err = utx.Execute()
	if err != nil {
		if strings.Index(err.Error(), "ConditionalCheckFailedException") == -1 {
			t.Errorf("Update error: %s", err.Error())
		}
		t.Logf("xxxx error: %s", err.Error())
	}
	var sk2 City
	txg = NewQuery("pop", tbl)
	txg.Select(&sk2).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk2.Pop)

	if sk2.Pop != sk.Pop-1 {
		t.Fail()
	}

}

func TestQueryPopUpdateWhere23a(t *testing.T) {

	var tbl tbl.Name = "GoUnitTest"
	type City struct {
		Pop int `dynamodbav:"Population"`
	}
	var wpEnd sync.WaitGroup

	err := slog.Start()
	if err != nil {
		t.Errorf("Error starting syslog services: %s", err)
	}

	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", tbl)

	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

	utx := New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//utx.NewUpdate(tbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists("MaxPopulation") and Population<MaxPopulation`).Subtract("Population", 1)
	utx.NewUpdate(tbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_not_exists(MaxPopulation) `).Subtract("Population", 1)

	err = utx.Execute()
	if err != nil {
		if strings.Index(err.Error(), "ConditionalCheckFailedException") == -1 {
			t.Errorf("Update error: %s", err.Error())
		}
		//	t.Logf("xxxx error: %s", err.Error())
	}
	var sk2 City
	txg = NewQuery("pop", tbl)
	txg.Select(&sk2).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk2.Pop)

	if sk2.Pop != sk.Pop {
		t.Fail()
	}

}

// func TestShortNames(t *testing.T) {
// 	var (
// 		past []byte
// 	)
// 	past = append(past, 'a'-1)
// 	// aa, ab...az, ba,..bz, ca, .. cz, da,..dz, ea
// 	for i := 0; i < 1800; i++ {

// 		for i := len(past) - 1; i >= 0; i-- {
// 			past[i]++
// 			if past[i] == 'z'+1 {
// 				if i == 0 && past[0] == 'z'+1 {
// 					past = append(past, 'a')
// 					for ii := i; ii > -1; ii-- {
// 						past[ii] = 'a'
// 					}
// 					break
// 				} else {
// 					past[i] = 'a'
// 				}
// 			} else {
// 				break
// 			}
// 		}
// 		t.Logf("subnn: %s\n", past)
// 	}

// }

func TestQueryPopUpdateWhere24(t *testing.T) {

	var tbl tbl.Name = "GoUnitTest"
	type City struct {
		Pop int `dynamodbav:"Population"`
	}
	var wpEnd sync.WaitGroup

	err := slog.Start()
	if err != nil {
		t.Errorf("Error starting syslog services: %s", err)
	}

	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", tbl)

	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

	utx := New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(tbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewUpdate(tbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>20000`).Subtract("Population", 1)
	err = utx.Execute()
	if err != nil {
		if strings.Index(err.Error(), "ConditionalCheckFailedException") == -1 {
			t.Errorf("Update xtz error: %s", err.Error())
		}
	}
	//tce := &types.TransactionCanceledException{}

	// if errors.As(err, &tce) {
	// 	for _, e := range tce.CancellationReasons {

	// 		if *e.Code == "ConditionalCheckFailed" {
	// 			t.Errorf("Update xtz error: %s", tce.Error())
	// 		}
	// 	}
	// }
	var sk2 City
	txg = NewQuery("pop", tbl)
	txg.Select(&sk2).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk2.Pop)

	if sk2.Pop != sk.Pop-1 {
		t.Fail()
	}

}

func TestQueryPopUpdateWhere25(t *testing.T) {

	var (
		tbl    tbl.Name = "GoUnitTest"
		plimit          = 20000
	)
	type City struct {
		Pop int `dynamodbav:"Population"`
	}
	var wpEnd sync.WaitGroup

	err := slog.Start()
	if err != nil {
		t.Errorf("Error starting syslog services: %s", err)
	}

	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", tbl)

	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

	utx := New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(tbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewUpdate(tbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>?`).Values(plimit).Subtract("Population", 1)
	err = utx.Execute()
	if err != nil {
		if strings.Index(err.Error(), "ConditionalCheckFailedException") == -1 {
			t.Errorf("Update xtz error: %s", err.Error())
		}
	}
	//tce := &types.TransactionCanceledException{}

	// if errors.As(err, &tce) {
	// 	for _, e := range tce.CancellationReasons {

	// 		if *e.Code == "ConditionalCheckFailed" {
	// 			t.Errorf("Update xtz error: %s", tce.Error())
	// 		}
	// 	}
	// }
	var sk2 City
	txg = NewQuery("pop", tbl)
	txg.Select(&sk2).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk2.Pop)

	if sk2.Pop != sk.Pop-1 {
		t.Fail()
	}

}

func TestQueryPopUpdateWhere26(t *testing.T) {

	var (
		tbl    tbl.Name = "GoUnitTest"
		plimit          = 20000
	)
	type City struct {
		Pop int `dynamodbav:"Population"`
	}
	var wpEnd sync.WaitGroup

	err := slog.Start()
	if err != nil {
		t.Errorf("Error starting syslog services: %s", err)
	}

	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", tbl)

	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

	utx := New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(tbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewUpdate(tbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and (Population>? or Population<?)`).Values(plimit).Subtract("Population", 1)
	err = utx.Execute()
	if err != nil {
		if strings.Index(err.Error(), "expected 2 bind variables in Values, got 1") == -1 {
			t.Errorf("Update error: %s", err.Error())
		}
	}
	//tce := &types.TransactionCanceledException{}

	// if errors.As(err, &tce) {
	// 	for _, e := range tce.CancellationReasons {

	// 		if *e.Code == "ConditionalCheckFailed" {
	// 			t.Errorf("Update xtz error: %s", tce.Error())
	// 		}
	// 	}
	// }
	var sk2 City
	txg = NewQuery("pop", tbl)
	txg.Select(&sk2).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk2.Pop)

	if sk2.Pop != sk.Pop {
		t.Fail()
	}

}

func TestQueryPopUpdateWhere28(t *testing.T) {

	var (
		tbl     tbl.Name = "GoUnitTest"
		plimit           = 20000000
		plimit2          = 2000000
	)
	type City struct {
		Pop int `dynamodbav:"Population"`
	}
	var wpEnd sync.WaitGroup

	err := slog.Start()
	if err != nil {
		t.Errorf("Error starting syslog services: %s", err)
	}

	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", tbl)

	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

	utx := New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(tbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewUpdate(tbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and (Population>? or Population<?)`).Values(plimit, plimit2).Subtract("Population", 1)
	err = utx.Execute()
	if err != nil {
		if strings.Index(err.Error(), "ConditionalCheckFailedException") == -1 {
			t.Errorf("Update xtz error: %s", err.Error())
		}
	}
	//tce := &types.TransactionCanceledException{}

	// if errors.As(err, &tce) {
	// 	for _, e := range tce.CancellationReasons {

	// 		if *e.Code == "ConditionalCheckFailed" {
	// 			t.Errorf("Update xtz error: %s", tce.Error())
	// 		}
	// 	}
	// }
	var sk2 City
	txg = NewQuery("pop", tbl)
	txg.Select(&sk2).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk2.Pop)

	// expect no change
	if sk2.Pop == sk.Pop-1 {
		t.Fail()
	}

}

func TestQueryPopUpdateWhere29(t *testing.T) {

	var (
		tbl     tbl.Name = "GoUnitTest"
		plimit           = 2000000
		plimit2          = 200000
	)
	type City struct {
		Pop int `dynamodbav:"Population"`
	}
	var wpEnd sync.WaitGroup

	err := slog.Start()
	if err != nil {
		t.Errorf("Error starting syslog services: %s", err)
	}

	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", tbl)

	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

	utx := New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(tbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewUpdate(tbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and (Population>? or Population<?)`).Values(plimit, plimit2).Subtract("Population", 1)
	err = utx.Execute()
	if err != nil {
		if strings.Index(err.Error(), "ConditionalCheckFailedException") == -1 {
			t.Errorf("Update xtz error: %s", err.Error())
		}
	}
	//tce := &types.TransactionCanceledException{}

	// if errors.As(err, &tce) {
	// 	for _, e := range tce.CancellationReasons {

	// 		if *e.Code == "ConditionalCheckFailed" {
	// 			t.Errorf("Update xtz error: %s", tce.Error())
	// 		}
	// 	}
	// }
	var sk2 City
	txg = NewQuery("pop", tbl)
	txg.Select(&sk2).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk2.Pop)

	if sk2.Pop != sk.Pop-1 {
		t.Fail()
	}

}
