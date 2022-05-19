package tx

import (
	"testing"
)

func TestInsertMutation(t *testing.T) {

	uid := []byte{"66PNdV1TSKOpDRlO71+Aow=="}

	tx.New("testInsert")
	tx.Add(tx.NewInsert("GoGraph01").AddMember("PKey", uid).AddMember("Sortk", "A#A#:H").AddMember("P", "r|Height").AddMember("N", 173))
	err := tx.Execute()
	if err != nil {
		t.Error(err)
	}

}
