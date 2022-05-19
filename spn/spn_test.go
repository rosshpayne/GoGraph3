package spn

import (
	"fmt"
	"testing"
)

type StmtS []int

// func (s StmtS) AppendSlice(i int) StmtS {

// 	return append(s, i)
// }

func (s *StmtS) AppendSliceP(i int) {
	*s = append(*s, i)
}

func TestSlice(t *testing.T) {
	var ts StmtS
	var tsp StmtS
	var testSlice = []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26}
	var testSlice2 = []int{101, 102, 104, 105}

	// for _, i := range testSlice {
	// 	ts = ts.AppendSlice(i)
	// }

	fmt.Printf("%#v\n", ts)
	//tsp := StmtS{}

	for _, i := range testSlice {
		tsp.AppendSliceP(i)
	}
	for _, i := range testSlice2 {
		tsp.AppendSliceP(i)
	}
	fmt.Printf("%#v\n", tsp)

}
