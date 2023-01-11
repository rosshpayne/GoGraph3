package uuid

import (
	"testing"
)

func TestFromString(t *testing.T) {
	//	s := "75f881d4-ce58-47f4-a9fe-c0bedfcd2b31"
	//s := "400f8599-076f-43a4-bf9d-04c2f89068f1"
	s := "d2dc5ac3-01d4-44cd-a32b-3e15aa2d697e"
	uid := FromString(s)
	t.Logf("uid type: %T", uid)
	t.Logf("UID: %s", uid.EncodeBase64())
}

func TestToUID(t *testing.T) {
	s := UIDb64("pYj5C30HR4mNFnzwEBXEYQ==")
	uid := DecodeBase64(s)
	t.Logf("UID: %s", uid.String())
}
