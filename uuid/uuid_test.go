package uuid

import (
	"testing"
)

func TestFromString(t *testing.T) {
	s := "8ca67674-d948-4145-9d13-c07b933b9e88"
	uid := FromString(s)
	t.Logf("UID: %s", uid.EncodeBase64())
}
