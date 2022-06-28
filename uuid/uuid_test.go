package uuid

import (
	"testing"
)

func TestFromString(t *testing.T) {
	s := "a588f90b-7d07-4789-8d16-7cf01015c461"
	uid := FromString(s)
	t.Logf("UID: %s", uid.EncodeBase64())
}

func TestToUID(t *testing.T) {
	s := UIDb64("pYj5C30HR4mNFnzwEBXEYQ==")
	uid := DecodeBase64(s)
	t.Logf("UID: %s", uid.String())
}
