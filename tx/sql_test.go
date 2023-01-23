package tx

import (
	"context"
	//	"fmt"
	"testing"

	"github.com/ros2hp/method-db/mysql"
	"github.com/ros2hp/method-db/tbl"
)

func TestSQLUpdateStaff(t *testing.T) {

	type Person struct {
		Id       int
		SirName  string
		LastName string
		Age      int
		Salary   float64
		DOB      string
		Height   int
	}
	var (
		staff []Person
		tbl   = tbl.Name("unit$Test")
		err   error
	)
	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mysql.Register(ctx, "mysql-GoGraph", "admin:gjIe8Hl9SFD1g3ahyu6F@tcp(mysql8.cjegagpjwjyi.us-east-1.rds.amazonaws.com:3306)/GoGraph")
	//select test,logdt,status,nodes from Log$GoTest;

	txg := NewQuery("query-test-label", tbl).DB("mysql-GoGraph")

	txg.Select(&staff).Key("Id", 2, "GE").Filter("Age", 30, "GE").Filter("Salary", 100000, "GE")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}
	for i, v := range staff {

		t.Logf("Staff %d  %#v\n", i, v)
	}

	// 	txg := New("LogTest").DB("mysql-GoGraph")
	// 	txg.NewUpdate("unit$Test").Key("test", "TestMoviex")
	// 	err := txg.Execute()

	// 	if err != nil {
	// 		t.Logf("Error: %s", err)
	// 	}

	// t.Logf("Query count %d\n", len(sk))
	//
	//	for _, v := range sk {
	//		t.Log(v.Test, v.Logdt, v.Status, v.Nodes)
	//	}
}

func TestSQLUpdateStaffOr(t *testing.T) {

	type Person struct {
		Id       int
		SirName  string
		LastName string
		Age      int
		Salary   float64
		DOB      string
		Height   int
	}
	var (
		staff []Person
		tbl   = tbl.Name("unit$Test")
		err   error
	)
	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mysql.Register(ctx, "mysql-GoGraph", "admin:gjIe8Hl9SFD1g3ahyu6F@tcp(mysql8.cjegagpjwjyi.us-east-1.rds.amazonaws.com:3306)/GoGraph")
	//select test,logdt,status,nodes from Log$GoTest;

	txg := NewQuery("query-test-label", tbl).DB("mysql-GoGraph")

	txg.Select(&staff).Key("Id", 2, "GE").Filter("Age", 30, "GE").OrFilter("Salary", 100000, "GE")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}
	for i, v := range staff {

		t.Logf("Staff %d  %#v\n", i, v)
	}

	// 	txg := New("LogTest").DB("mysql-GoGraph")
	// 	txg.NewUpdate("unit$Test").Key("test", "TestMoviex")
	// 	err := txg.Execute()

	// 	if err != nil {
	// 		t.Logf("Error: %s", err)
	// 	}

	// t.Logf("Query count %d\n", len(sk))
	//
	//	for _, v := range sk {
	//		t.Log(v.Test, v.Logdt, v.Status, v.Nodes)
	//	}
}

func TestSQLUpdateStaffOrAnd(t *testing.T) {

	type Person struct {
		Id       int
		SirName  string
		LastName string
		Age      int
		Salary   float64
		DOB      string
		Height   int
	}
	var (
		staff []Person
		tbl   = tbl.Name("unit$Test")
		err   error
	)

	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mysql.Register(ctx, "mysql-GoGraph", "admin:gjIe8Hl9SFD1g3ahyu6F@tcp(mysql8.cjegagpjwjyi.us-east-1.rds.amazonaws.com:3306)/GoGraph")
	//select test,logdt,status,nodes from Log$GoTest;

	txg := NewQuery("query-test-label", tbl).DB("mysql-GoGraph")

	txg.Select(&staff).Key("Id", 2, "GE").Filter("Age", 30, "GE").OrFilter("Salary", 100000, "GE").AndFilter("SirName", "Payne")
	err = txg.Execute()

	if err != nil {
		if err.Error() != `Cannot mix OrFilter, AndFilter conditions. Use Where() & Values() instead` {
			t.Errorf("Error: %s", err)
			t.Fail()
		}
		t.Logf("Error: %s", err)
	}
	for i, v := range staff {

		t.Logf("Staff %d  %#v\n", i, v)
	}

	// 	txg := New("LogTest").DB("mysql-GoGraph")
	// 	txg.NewUpdate("unit$Test").Key("test", "TestMoviex")
	// 	err := txg.Execute()

	// 	if err != nil {
	// 		t.Logf("Error: %s", err)
	// 	}

	// t.Logf("Query count %d\n", len(sk))
	//
	//	for _, v := range sk {
	//		t.Log(v.Test, v.Logdt, v.Status, v.Nodes)
	//	}
}

func TestSQLUpdateStaffKeyWhere(t *testing.T) {

	type Person struct {
		Id       int
		SirName  string
		LastName string
		Age      int
		Salary   float64
		DOB      string
		Height   int
	}
	var (
		staff []Person
		tbl   = tbl.Name("unit$Test")
		err   error
	)
	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mysql.Register(ctx, "mysql-GoGraph", "admin:gjIe8Hl9SFD1g3ahyu6F@tcp(mysql8.cjegagpjwjyi.us-east-1.rds.amazonaws.com:3306)/GoGraph")
	//select test,logdt,status,nodes from Log$GoTest;

	txg := NewQuery("query-test-label", tbl).DB("mysql-GoGraph")

	txg.Select(&staff).Key("Id", 2, "GT").Where(" AGE > 30 or salary > 100000")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}
	for i, v := range staff {

		t.Logf("Staff %d  %#v\n", i, v)
	}

	// 	txg := New("LogTest").DB("mysql-GoGraph")
	// 	txg.NewUpdate("unit$Test").Key("test", "TestMoviex")
	// 	err := txg.Execute()

	// 	if err != nil {
	// 		t.Logf("Error: %s", err)
	// 	}

	// t.Logf("Query count %d\n", len(sk))
	//
	//	for _, v := range sk {
	//		t.Log(v.Test, v.Logdt, v.Status, v.Nodes)
	//	}
}

func TestSQLUpdateStaffWhere(t *testing.T) {

	type Person struct {
		Id       int
		SirName  string
		LastName string
		Age      int
		Salary   float64
		DOB      string
		Height   int
	}
	var (
		staff []Person
		tbl   = tbl.Name("unit$Test")
		err   error
	)
	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mysql.Register(ctx, "mysql-GoGraph", "admin:gjIe8Hl9SFD1g3ahyu6F@tcp(mysql8.cjegagpjwjyi.us-east-1.rds.amazonaws.com:3306)/GoGraph")
	//select test,logdt,status,nodes from Log$GoTest;

	txg := NewQuery("query-test-label", tbl).DB("mysql-GoGraph")

	txg.Select(&staff).Where("Id > 2 and AGE > 30 and salary > 100000")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}
	for i, v := range staff {

		t.Logf("Staff %d  %#v\n", i, v)
	}

	// 	txg := New("LogTest").DB("mysql-GoGraph")
	// 	txg.NewUpdate("unit$Test").Key("test", "TestMoviex")
	// 	err := txg.Execute()

	// 	if err != nil {
	// 		t.Logf("Error: %s", err)
	// 	}

	// t.Logf("Query count %d\n", len(sk))
	//
	//	for _, v := range sk {
	//		t.Log(v.Test, v.Logdt, v.Status, v.Nodes)
	//	}
}

func TestSQLUpdateStaffWhereOr(t *testing.T) {

	type Person struct {
		Id       int
		SirName  string
		LastName string
		Age      int
		Salary   float64
		DOB      string
		Height   int
	}
	var (
		staff []Person
		tbl   = tbl.Name("unit$Test")
		err   error
	)
	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mysql.Register(ctx, "mysql-GoGraph", "admin:gjIe8Hl9SFD1g3ahyu6F@tcp(mysql8.cjegagpjwjyi.us-east-1.rds.amazonaws.com:3306)/GoGraph")
	//select test,logdt,status,nodes from Log$GoTest;

	txg := NewQuery("query-test-label", tbl).DB("mysql-GoGraph")

	txg.Select(&staff).Where("Id > 2 and (AGE > 30 or salary > 100000)")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}
	for i, v := range staff {

		t.Logf("Staff %d  %#v\n", i, v)
	}

	// 	txg := New("LogTest").DB("mysql-GoGraph")
	// 	txg.NewUpdate("unit$Test").Key("test", "TestMoviex")
	// 	err := txg.Execute()

	// 	if err != nil {
	// 		t.Logf("Error: %s", err)
	// 	}

	// t.Logf("Query count %d\n", len(sk))
	//
	//	for _, v := range sk {
	//		t.Log(v.Test, v.Logdt, v.Status, v.Nodes)
	//	}
}
func TestSQLUpdateStaffWhereOr2(t *testing.T) {

	type Person struct {
		Id       int
		SirName  string
		LastName string
		Age      int
		Salary   float64
		DOB      string
		Height   int
	}
	var (
		staff []Person
		tbl   = tbl.Name("unit$Test")
		err   error
	)
	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var b int
	var c float64
	//a = 2
	b = 30
	c = 100000

	mysql.Register(ctx, "mysql-GoGraph", "admin:gjIe8Hl9SFD1g3ahyu6F@tcp(mysql8.cjegagpjwjyi.us-east-1.rds.amazonaws.com:3306)/GoGraph")
	//select test,logdt,status,nodes from Log$GoTest;

	txg := NewQuery("query-test-label", tbl).DB("mysql-GoGraph").Prepare()

	for i := 0; i < 4; i++ {
		txg.Select(&staff).Key("Id", i, "GT").Where(" AGE > ? or salary > ?").Values(b+i, c)
		err = txg.Execute()

		if err != nil {
			t.Logf("Error: %s", err)
		}
		for i, v := range staff {

			t.Logf("Staff %d  %#v\n", i, v)
		}
		staff = nil
	}

	// 	txg := New("LogTest").DB("mysql-GoGraph")
	// 	txg.NewUpdate("unit$Test").Key("test", "TestMoviex")
	// 	err := txg.Execute()

	// 	if err != nil {
	// 		t.Logf("Error: %s", err)
	// 	}

	// t.Logf("Query count %d\n", len(sk))
	//
	//	for _, v := range sk {
	//		t.Log(v.Test, v.Logdt, v.Status, v.Nodes)
	//	}
}

func xfW(t *testing.T) {

	type Person struct {
		Id       int
		SirName  string
		LastName string
		Age      int
		Salary   float64
		DOB      string
		Height   int
	}
	var (
		staff []Person
		tbl   = tbl.Name("unit$Test")
		err   error
	)
	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var b int
	var c float64
	//a = 2
	b = 30
	c = 100000

	mysql.Register(ctx, "mysql-GoGraph", "admin:gjIe8Hl9SFD1g3ahyu6F@tcp(mysql8.cjegagpjwjyi.us-east-1.rds.amazonaws.com:3306)/GoGraph")
	//select test,logdt,status,nodes from Log$GoTest;

	txg := NewQuery("query-test-label", tbl).DB("mysql-GoGraph").Prepare()

	for i := 0; i < 4; i++ {
		txg.Select(&staff).Key("Id", i, "GT").Where(" AGE > ? or salary > ?").Values(b+i, c).Limit(1)
		err = txg.Execute()

		if err != nil {
			t.Logf("Error: %s", err)
		}
		for i, v := range staff {

			t.Logf("Staff %d  %#v\n", i, v)
		}
		staff = nil
	}

	// 	txg := New("LogTest").DB("mysql-GoGraph")
	// 	txg.NewUpdate("unit$Test").Key("test", "TestMoviex")
	// 	err := txg.Execute()

	// 	if err != nil {
	// 		t.Logf("Error: %s", err)
	// 	}

	// t.Logf("Query count %d\n", len(sk))
	//
	//	for _, v := range sk {
	//		t.Log(v.Test, v.Logdt, v.Status, v.Nodes)
	//	}
}

func TestSQLUpdateStaff9(t *testing.T) {

	type Person struct {
		Id       int
		SirName  string
		LastName string
		Age      int
		Salary   float64
		DOB      string
		Height   int
	}
	var (
		staff []Person
		tbl   = tbl.Name("unit$Test")
		err   error
	)
	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mysql.Register(ctx, "mysql-GoGraph", "admin:gjIe8Hl9SFD1g3ahyu6F@tcp(mysql8.cjegagpjwjyi.us-east-1.rds.amazonaws.com:3306)/GoGraph")
	//select test,logdt,status,nodes from Log$GoTest;

	txg := NewQuery("query-test-label", tbl).DB("mysql-GoGraph").Prepare()

	txg.Select(&staff).Key("Id", 2)
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}
	for i, v := range staff {

		t.Logf("Staff %d  %#v\n", i, v)
	}

	txu := New("LogTest").DB("mysql-GoGraph")
	txu.NewUpdate("unit$Test").Key("Id", "2").Filter("Salary", 700000, "GE").Add("Age", 1)
	err = txu.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}
	staff = nil
	txg.Select(&staff).Key("Id", 2)
	err = txg.Execute()
	for i, v := range staff {

		t.Logf("Staff %d  %#v\n", i, v)
	}
}

func TestSQLUpdateStaff10(t *testing.T) {

	type Person struct {
		Id       int
		SirName  string
		LastName string
		Age      int
		Salary   float64
		DOB      string
		Height   int
	}
	var (
		staff []Person
		tbl   = tbl.Name("unit$Test")
		err   error
	)
	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mysql.Register(ctx, "mysql-GoGraph", "admin:gjIe8Hl9SFD1g3ahyu6F@tcp(mysql8.cjegagpjwjyi.us-east-1.rds.amazonaws.com:3306)/GoGraph")
	//select test,logdt,status,nodes from Log$GoTest;

	txg := NewQuery("query-test-label", tbl).DB("mysql-GoGraph").Prepare()

	txg.Select(&staff).Key("Id", 2)
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}
	for i, v := range staff {

		t.Logf("Staff %d  %#v\n", i, v)
	}

	txu := New("LogTest").DB("mysql-GoGraph")
	txu.NewUpdate("unit$Test").Key("Id", "2").Filter("Salary", 600000, "GE").Add("Age", 1)
	err = txu.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}
	staff = nil
	txg.Select(&staff).Key("Id", 2)
	err = txg.Execute()
	for i, v := range staff {

		t.Logf("Staff %d  %#v\n", i, v)
	}
}

func TestSQLUpdateStaffWhereOr3(t *testing.T) {

	type Person struct {
		Id       int
		SirName  string
		LastName string
		Age      int
		Salary   float64
		DOB      string
		Height   int
	}
	var (
		staff []Person
		tbl   = tbl.Name("unit$Test")
		err   error
	)
	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var b int
	var c float64
	//a = 2
	b = 23
	c = 100000

	mysql.Register(ctx, "mysql-GoGraph", "admin:gjIe8Hl9SFD1g3ahyu6F@tcp(mysql8.cjegagpjwjyi.us-east-1.rds.amazonaws.com:3306)/GoGraph")
	//select test,logdt,status,nodes from Log$GoTest;

	txg := NewQuery("query-test-label", tbl).DB("mysql-GoGraph").Prepare()

	txg.Select(&staff).Key("Id", 2, "GE").Where(" AGE > ? or salary > ?").Values(b, c)
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}
	for i, v := range staff {

		t.Logf("Staff %d  %#v\n", i, v)
	}
	staff = nil

	txu := New("LogTest").DB("mysql-GoGraph")
	txu.NewUpdate("unit$Test").Key("Id", 2, "GE").Add("Age", 1).Where(" AGE > ? or salary > ?").Values(b, c)
	err = txu.Execute()

	txg.Select(&staff).Key("Id", 2, "GE")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}
	for i, v := range staff {

		t.Logf("Staff %d  %#v\n", i, v)
	}
}

func TestSQLUpdateStaffWhereOr4(t *testing.T) {

	type Person struct {
		Id       int
		SirName  string
		LastName string
		Age      int
		Salary   float64
		DOB      string
		Height   int
	}
	var (
		staff []Person
		tbl   = tbl.Name("unit$Test")
		err   error
	)
	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var b int
	var c float64
	//a = 2
	b = 23
	c = 100000

	mysql.Register(ctx, "mysql-GoGraph", "admin:gjIe8Hl9SFD1g3ahyu6F@tcp(mysql8.cjegagpjwjyi.us-east-1.rds.amazonaws.com:3306)/GoGraph")
	//select test,logdt,status,nodes from Log$GoTest;

	txg := NewQuery("query-test-label", tbl).DB("mysql-GoGraph").Prepare()

	txg.Select(&staff).Key("Id", 2, "GE").Where(" AGE > ? or salary > ?").Values(b, c)
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}
	for i, v := range staff {

		t.Logf("Staff %d  %#v\n", i, v)
	}
	staff = nil

	txu := New("LogTest").DB("mysql-GoGraph")
	txu.NewUpdate("unit$Test").Key("Id", 2, "GE").Add("Age", 1).Where("Salary>=Age")
	err = txu.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	txg.Select(&staff).Key("Id", 2, "GE")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}
	for i, v := range staff {

		t.Logf("Staff %d  %#v\n", i, v)
	}
}

func TestSQLUpdateStaffWhere5(t *testing.T) {

	type Person struct {
		Id       int
		SirName  string
		LastName string
		Age      int
		Salary   float64
		DOB      string
		Height   int
		Ceiling  int
	}
	var (
		staff []Person
		tbl   = tbl.Name("unit$Test")
		err   error
	)
	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var b int
	var c float64
	//a = 2
	b = 23
	c = 100000

	mysql.Register(ctx, "mysql-GoGraph", "admin:gjIe8Hl9SFD1g3ahyu6F@tcp(mysql8.cjegagpjwjyi.us-east-1.rds.amazonaws.com:3306)/GoGraph")
	//select test,logdt,status,nodes from Log$GoTest;

	txg := NewQuery("query-test-label", tbl).DB("mysql-GoGraph").Prepare()

	txg.Select(&staff).Key("Id", 2, "GE").Where(" AGE > ? or salary > ?").Values(b, c)
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}
	for i, v := range staff {

		t.Logf("Staff %d  %#v\n", i, v)
	}
	staff = nil

	txu := New("LogTest").DB("mysql-GoGraph")
	txu.NewUpdate("unit$Test").Key("Id", 2, "GE").Add("Age", 1).Where("Height>=Ceiling")
	err = txu.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	txg.Select(&staff).Key("Id", 2, "GE")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}
	for i, v := range staff {

		t.Logf("Staff %d  %#v\n", i, v)
	}
}

func TestSQLUpdateStaffFilter5(t *testing.T) {

	type Person struct {
		Id       int
		SirName  string
		LastName string
		Age      int
		Salary   float64
		DOB      string
		Height   int
		Ceiling  int
	}
	var (
		staff []Person
		tbl   = tbl.Name("unit$Test")
		err   error
	)
	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var b int
	var c float64
	//a = 2
	b = 23
	c = 100000

	mysql.Register(ctx, "mysql-GoGraph", "admin:gjIe8Hl9SFD1g3ahyu6F@tcp(mysql8.cjegagpjwjyi.us-east-1.rds.amazonaws.com:3306)/GoGraph")
	//select test,logdt,status,nodes from Log$GoTest;

	txg := NewQuery("query-test-label", tbl).DB("mysql-GoGraph").Prepare()

	txg.Select(&staff).Key("Id", 2, "GE").Where(" AGE > ? or salary > ?").Values(b, c)
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}
	for i, v := range staff {

		t.Logf("Staff %d  %#v\n", i, v)
	}
	staff = nil

	txu := New("LogTest").DB("mysql-GoGraph")
	txu.NewUpdate("unit$Test").Key("Id", 2, "GE").Add("Age", 1).Filter("Height", "Ceiling", "GE")
	err = txu.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	txg.Select(&staff).Key("Id", 2, "GE")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}
	for i, v := range staff {

		t.Logf("Staff %d  %#v\n", i, v)
	}
}

func TestSQLPaginate(t *testing.T) {

	type Rec struct {
		Id int
		A  []byte
		B  string
		C  int
		D  float64
	}

	var (
		recs     []*Rec
		tbl      = tbl.Name("test$Page")
		err      error
		pageSize = 5
	)
	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := "B"
	e := "H"
	b := "AI"
	c := 19

	mysql.Register(ctx, "mysql-GoGraph", "admin:gjIe8Hl9SFD1g3ahyu6F@tcp(mysql8.cjegagpjwjyi.us-east-1.rds.amazonaws.com:3306)/GoGraph")
	//select test,logdt,status,nodes from Log$GoTest;

	txg := NewQuery("query-test-label", tbl).DB("mysql-GoGraph").Prepare()
	id := 0
	for i := 0; i < 3; i++ {
		txg.Select(&recs).Key("Id", id, "GT").Where(` A > ? and A < ? or (B >= ? and C < ? )`).Values(a, e, b, c+i).Limit(pageSize).OrderBy("Id")
		err = txg.Execute()

		if err != nil {
			t.Errorf("Error: %s", err)
		}
		for i, v := range recs {
			t.Logf("rec %d  %#v\n", i, v)
		}

		id = recs[len(recs)-1].Id
		t.Log("\nNext page....")
		recs = nil
	}

	if err != nil {
		t.Logf("Error: %s", err)
	}

}
