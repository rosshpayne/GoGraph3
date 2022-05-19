package ast

import (
	"fmt"
	"testing"

	"github.com/GoGraph/gql/token"
)

func TestAllOfTerms1(t *testing.T) {
	pred := ScalarPred{}
	pos := token.Pos{Line: 2, Col: 17}
	pred.AssignName("Name", pos)
	val := "Payne Ian"

	result := AllOfTerms(pred, val)

	fmt.Printf("result: %#v %s\n", result, result[0].PKey)
}

func TestAllOfTerms2(t *testing.T) {
	pred := ScalarPred{}
	pos := token.Pos{Line: 2, Col: 17}
	pred.AssignName("Comment", pos)
	val := "Payne Germany"

	result := AllOfTerms(pred, val)

	fmt.Printf("result: %#v %s\n", result, result[0].PKey)
}

func TestAnyOfTerms1(t *testing.T) {
	pred := ScalarPred{}
	pos := token.Pos{Line: 2, Col: 17}
	pred.AssignName("Comment", pos)
	val := "Payne Germany"

	result := AnyOfTerms(pred, val)

	for _, v := range result {
		fmt.Printf("result: %#v %s\n", v, v.PKey)
	}
}
