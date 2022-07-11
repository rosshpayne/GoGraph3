package uuid

import (
	"testing"
)

func TestFromString(t *testing.T) {
	s := "76083c49-cbe3-4dab-a53f-309b5a1a6e6a"
	uid := FromString(s)
	t.Logf("UID: %s", uid.EncodeBase64())
}

func TestToUID(t *testing.T) {
	s := UIDb64("pYj5C30HR4mNFnzwEBXEYQ==")
	uid := DecodeBase64(s)
	t.Logf("UID: %s", uid.String())
}
