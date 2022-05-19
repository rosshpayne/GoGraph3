package rrr

import (
	"fmt"
	"reflect"
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

	var result resultT

	rows := []resultT{resultT{aa: 23, bb: "Happy days", cc: 3.245, dd: 4}, resultT{aa: 54, bb: "Orient Express", cc: 11.345, dd: 78}, resultT{aa: 99, bb: "Lost in Space", cc: 23.45, dd: 67}}
	t.Log("here.1")
	//	q := query.New(tbl.Name("foo"), "label")
	t.Log("here.2")
	//	q.Select(&result)
	t.Log("here.3")

	//out := q.MakeResultSlice(len(rows))

	for _, v := range rows {
		t.Log(v)

		bindvals := Split(&result)
		t.Log("len: ", len(bindvals))
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
			fmt.Printf("Field %d %T\n", i, v)
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

func Split(a interface{}) []interface{} { // )

	var bind []interface{}
	var v reflect.Value

	// add struct fields to attr list - used to build SQL select list or expression fields in Dynaatyb
	f := reflect.TypeOf(a)
	if f.Kind() != reflect.Ptr {
		panic(fmt.Errorf("Fetch argument: not a pointer"))
	}
	s := f.Elem()
	// if s.Kind() != reflect.Struct {
	// 	panic(fmt.Errorf("Fetch argument: not a struct pointer"))
	// }
	switch s.Kind() {
	case reflect.Struct:
		// used in GetItem (single row select)
		v = reflect.Indirect(reflect.ValueOf(a))
	}

	st := v.Type() // struct{}
	fmt.Printf("st: %s \n", st.Kind())
	var xx interface{}
	//var ptr *int
	for i := 0; i < st.NumField(); i++ {

		p := st.Field(i)
		// fmt.Println("Field: ", p.Name, p.Offset)
		vp := v.Addr().Pointer() + p.Offset
		if p.Type.Kind() == reflect.Int {
			var ii *int
			var cint interface{}
			cint = ii
			var kint interface{}
			kint = vp
			vp := reflect.ValueOf(kint)

			pp := vp.Convert(reflect.TypeOf(cint))

			fmt.Println("pp = ", pp.Kind())
		}
		// ap := p.Addr()
		// fmt.Println("Kind of address: ", ap.Kind())
		fmt.Printf("vp %v, %d type: %s\n", vp, p.Offset, p.Type.Kind())
		continue
		xx = vp

		//fmt.Printf("vp is a %T ptr is a %T %s\n", vp, ptr, reflect.Indirect(reflect.ValueOf(xx)).Type().Kind())
		fmt.Printf("vp %s %s\n", reflect.ValueOf(xx).Type().Kind(), reflect.Indirect(reflect.ValueOf(xx)).Type().Kind())
		if p.Name == "aa" {
			vp = 22
		}
		fmt.Println(vp)

		bind = append(bind, vp)

	}

	return bind
}
