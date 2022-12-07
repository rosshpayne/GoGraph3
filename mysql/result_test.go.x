package mysql

import (
//	"reflect"
	"fmt"
	"testing"

//	"github.com/GoGraph/tbl"
//	"github.com/GoGraph/tx/query"
)

type resultT struct {
	aa int
	bb string
	cc float64
	dd int
}

func TestScan(t *testing.T) {

//	var result resultT

	rows := []resultT{resultT{aa: 23, bb: "Happy days", cc: 3.245, dd: 4}, resultT{aa: 54, bb: "Orient Express", cc: 11.345, dd: 78}, resultT{aa: 99, bb: "Lost in Space", cc: 23.45, dd: 67}}
	t.Log("here.1")
//	q := query.New(tbl.Name("foo"), "label")
		t.Log("here.2")
//	q.Select(&result)
		t.Log("here.3")

	//out := q.MakeResultSlice(len(rows))

		for _, v := range rows {
			t.Log(v)

			// bindvals := q.Split()
			// fmt.Log("len: ",len(bindvals))
		}
		// if err := rows.Err(); err != nil {
		// 	logerr(err)
		// }
		
	// for i, v := range rows {

	// 	bindvals := Split_(out.Index(i).Addr())

	// 	if err := printFields_(bindvals...); err != nil {
	// 		log.Fatal(err)
	// 	}
	// }
	// if err := rows.Err(); err != nil {
	// 	log.Fatal(err)
	// }

}

func printFields_(f ...interface{}) {
	if len(f) > 0 {
		for i, v := range f {
			fmt.Printf("Field %d %T\n",i, v)
		}
	}
}

// func Split_(o reflect.Value) []interface{} { // )

// 	s := o.Elem().Type() // struct{}
// 	bind := make([]interface{}, s.NumField(), s.NumField())

// 	for i := 0; i < s.NumFields(); i++ {

// 		p := s.Field(i)
// 		vp := o.Pointer() + p.Offset
// 		val := reflect.Indirect(vp).Interface()

// 		bind = append(bind, val)

// 	}

// 	return bind
// }


package main

import (
	"bytes"
    "database/sql"
	"encoding/json"
	"fmt"
	"reflect"
)

func main() {
	typ := reflect.StructOf([]reflect.StructField{
		{
			Name: "Height",
			Type: reflect.TypeOf(sql.NullInt64{1000,false}),
			Tag:  `json:"height"`,
		},
		{
			Name: "Age",
			Type: reflect.TypeOf(int(0)),
			Tag:  `json:"age"`,
		},
	})
    ni:=reflect.New(reflect.TypeOf(sql.NullInt64{1000,false})).Elem()
	ni.Field(0).SetInt(1000)
    ni.Field(1).SetBool(false)

	v := reflect.New(typ).Elem()
	v.Field(0).Set(ni)
	v.Field(1).SetInt(64)
	s := v.Addr().Interface()
	
	w := new(bytes.Buffer)
	if err := json.NewEncoder(w).Encode(s); err != nil {
		panic(err)
	}

	fmt.Printf("value: %+v\n", s)
	fmt.Printf("json:  %s", w.Bytes())


	ss := reflect.New(typ).Elem().Addr().Interface()
	r := bytes.NewReader([]byte(`{"height": {"Int64": 10000, "Valid": true} ,"age":10}`))
	if err := json.NewDecoder(r).Decode(ss); err != nil {
		panic(err)
	}
	fmt.Printf("value: %+v\n", ss)

}