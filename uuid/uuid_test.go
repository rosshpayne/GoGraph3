package uuid

import (
	"testing"
)

func TestFromString(t *testing.T) {
	//	s := "75f881d4-ce58-47f4-a9fe-c0bedfcd2b31"
	//s := "400f8599-076f-43a4-bf9d-04c2f89068f1"
	s := "2f407f86-6cf5-4c87-9db5-a6f234b80bfb"
	uid := FromString(s)
	t.Logf("uid type: %T", uid)
	t.Logf("UID: %s", uid.EncodeBase64())
}

func TestToUID(t *testing.T) {
	s := UIDb64("pYj5C30HR4mNFnzwEBXEYQ==")
	uid := DecodeBase64(s)
	t.Logf("UID: %s", uid.String())
}
