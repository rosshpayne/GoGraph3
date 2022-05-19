package main

import (
	"fmt"
	"testing"

	"github.com/GoGraph/util"
)

func TestUID(t *testing.T) {

	input := `51d84e25-4e71-450d-8cd1-8454dd7871d9`

	uid, err := util.FromLongString(input)
	//uid := util.FromString(input)

	if err != nil {
		fmt.Printf("Error: %s", err)
		return
	}

	fmt.Printf("uid: %s\n", uid.String())

}
