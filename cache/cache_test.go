package cache

import (
	"fmt"
	"time"

	"testing"
)

func TestTypeFetch(t *testing.T) {
	t0 := time.Now()

	at, err := FetchType("Person")
	if err != nil {
		t.Error(err)
	}

	t1 := time.Now()
	fmt.Println()
	fmt.Println("DB Access: ", t1.Sub(t0))
	// fmt.Printf("\nTest Result: %#v\n", at)
	// fmt.Printf("\nAttribute : %#v\n", AttrTypCache)
	// fmt.Printf("\nFacets : %#v\n\n", FacetCache)
	t1 = time.Now()
	at = FetchType("Person")

	t2 := time.Now()
	fmt.Println("Cache Access: ", t2.Sub(t1))
	for _, v := range at {
		fmt.Println(v.Name, v.C, v.DT)
		fmt.Println(TyAttrC["Person:"+v.Name])
	}

}
