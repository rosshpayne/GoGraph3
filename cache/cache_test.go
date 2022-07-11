package cache

import (
	"testing"
)

func TestMapMake(t *testing.T) {

	mm := make(map[int]int, 10)

	for i := 0; i < 10; i++ {
		mm[i] = i + 100
	}
	for i := 0; i < 10; i++ {
		t.Log(mm[i])
	}
}

func TestMapMakeOverPopulate(t *testing.T) {

	mm := make(map[int]int, 10)

	for i := 0; i < 11; i++ {
		mm[i] = i + 100
	}
	for i := 0; i < 11; i++ {
		t.Log(mm[i])
	}
}

func TestTouchDownUp(t *testing.T) {

	var idx int
	cache := make([]int, 10, 10)
	for i := 0; i < 10; i++ {
		cache[i] = i + 100
	}
	for i, v := range cache {
		t.Logf("i %d ,v %d", i, v)
	}
	// touch
	tc := 102
	for i, v := range cache {
		if v == tc {
			idx = i
		}
	}
	// promote uid to top of list of uids cached
	cache = append(cache, tc)
	// remove old entry
	n := copy(cache[1:], cache[:idx]) // move bottom up
	t.Log("n,idx = ", n, idx)
	if n != idx {
		t.Errorf("copy did not work...")
	}
	// keep slice fixed size - remove oldest entry
	cache = cache[1:]

	for i, v := range cache {
		t.Logf("i %d ,v %d", i, v)
	}
}

func TestTouchTopDown(t *testing.T) {

	var idx int
	cache := make([]int, 10, 10)
	for i := 0; i < 10; i++ {
		cache[i] = i + 100
	}
	for i, v := range cache {
		t.Logf("i %d ,v %d", i, v)
	}
	// touch
	tc := 107
	for i := len(cache) - 1; i >= 0; i-- {
		if cache[i] == tc {
			idx = i
		}
	}
	t.Logf("idx %d ", idx)

	n := copy(cache[idx:], cache[idx+1:]) // move top down

	t.Log("n,idx = ", n, len(cache[idx+1:]))

	if n != len(cache)-idx-1 {
		t.Errorf("copy did not work...")
	}
	cache[len(cache)-1] = tc
	// keep slice fixed size
	//	cache = cache[:len(cache)-1]
	for i, v := range cache {
		t.Logf("i %d ,v %d", i, v)
	}
}
