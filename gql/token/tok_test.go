package token

import (
	"testing"
)

func TestFeGE(t *testing.T) {
	if "ge" == GE {
		t.Logf("GE type is : %T\n", GE)
	}
	if "IDENT" == IDENT {
		t.Logf("IDENT type is : %T\n", IDENT)
	}
	if "Int" == INT {
		t.Logf("INT type is : %T\n", INT)
	}
}
