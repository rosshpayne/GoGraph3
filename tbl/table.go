//go:build spanner
// +build spanner

package tbl

import (
	"fmt"
	"sync"
)

type Name string

const (
	Block      Name = "Block"
	EOP        Name = "EOP" // Edge-Overflow-Propagated
	NodeScalar Name = "NodeScalar"
	Reverse    Name = "ReverseEdge"
	Type       Name = "GoGraphSS"
	Event      Name = "EventLog2"
	Mongr      Name = "mon_gr"
	Monrun     Name = "mon_run"
	Eslog      Name = "esLog"
	State      Name = "state"
	//AttachDetachEvent Name = "NodeAttachDetachEvent" - not used
)

type key struct {
	pk string
	sk string
}

type keyMap map[Name]key

var keysync sync.RWMutex

var (
	err  error
	keys keyMap
)

func init() {

	// only for tables with keys types { util.UID, String} but non standard names. Standard name PKey, SortK
	keys = keyMap{
		Block:      key{pk: "PKey"},
		EOP:        key{"PKey", "SortK"},
		NodeScalar: key{"PKey", "SortK"},
		Event:      key{"eid", "id"},
		Reverse:    key{"PKey", "SortK"},
		//AttachDetachEvent: key{Pk: "eid"},
		State: key{pk: "State"},
	}

}

func Register(t Name, pk string, sk ...string) {
	var k key
	if len(sk) > 0 {
		k = key{pk, sk[0]} // must be of type util.UID
	} else {
		k = key{pk: pk}
	}
	keysync.Lock()
	keys[t] = k
	keysync.Unlock()
}

func RegisterIndex(t Name, i Name, pk string, sk ...string) {
	var k key
	if len(sk) > 0 {
		k = key{pk, sk[0]} // must be of type util.UID
	} else {
		k = key{pk: pk}
	}
	keysync.Lock()
	keys[t] = k
	keysync.Unlock()
}

func IsIdxRegistered(t Name) error {
	return nil
}

func GetKeys(t Name) (string, string, error) {

	keysync.RLock()
	k, ok := keys[t]
	keysync.RUnlock()
	if !ok {
		return "", "", fmt.Errorf("Table %s not found", t)
	}
	return k.pk, k.sk, nil
}
