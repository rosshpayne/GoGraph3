package query

import (
	"testing"
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
