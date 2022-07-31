package uuid

import (
	"testing"
)

func TestFromString(t *testing.T) {
	s := "75f881d4-ce58-47f4-a9fe-c0bedfcd2b31"
	uid := FromString(s)
	t.Logf("UID: %s", uid.EncodeBase64())
}

func TestToUID(t *testing.T) {
	s := UIDb64("pYj5C30HR4mNFnzwEBXEYQ==")
	uid := DecodeBase64(s)
	t.Logf("UID: %s", uid.String())
}
