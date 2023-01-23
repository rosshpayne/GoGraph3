package query

import (
	"fmt"
	"testing"

	"github.com/ros2hp/method-db/tx"
)

func TestConst(t *testing.T) {

	var (
		bc  BoolCd = NIL
		bc2 BoolCd = AND
		bc3 BoolCd = OR
	)

	t.Logf("NIL %v %08[1]b", bc)
	t.Logf("AND %v %08[1]b", bc2)
	t.Logf("OR %v %08[1]b", bc3)

}

func TestSelect(t *testing.T) {

	type Person struct {
		FirstName string
		LastName  string
		DOB       string
	}
	type Address struct {
		Line1, Line2, Line3 string
		City                string
		Zip                 string
		State               string
		Cntry               Country
	}

	type Country struct {
		Name       string
		Population int
	}

	type Input struct {
		Status byte
		Person
		Loc Address
	}

	p := Person{FirstName: "Rossj", LastName: "Payneh", DOB: "13 March 1967"}
	c := Country{Name: "Australia", Population: 23000}
	ad := Address{Line1: "Villa 67", Line2: "55 Burkitt St", Line3: "Page", Zip: "2614", State: "ACT", Cntry: c}
	x := Input{Status: 'C', p, Loc: ad}

	nn := tx.NewQuery2("label", "table")
	nn.Select(&x)
	for k, v := range nn.GetAttr {
		fmt.Printf("%d: %d name: %s", v.Name())
	}
}
