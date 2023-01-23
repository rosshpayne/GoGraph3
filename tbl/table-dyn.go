//go:build dynamodb
// +build dynamodb

package tbl

import (
	"errors"
	"fmt"
	"sync"

	tx "github.com/ros2hp/method-db/tbl"
)

type (
	Name    = tx.Name
	IdxName = tx.Name
)

var (
	TblName    Name = "GoGraph.Dev"
	Graph      Name = TblName
	Block      Name = TblName
	EOP        Name = TblName
	NodeScalar Name = TblName
	Reverse    Name = TblName
	edgePost   Name //= "-restored"
)

const (
	//
	Type Name = "GoGraphSS"
	//
	Event             Name = "EV$event"
	TaskEv            Name = "EV$task"
	RunStat           Name = "runStats" //internal to db
	Monrun            Name = "runStats"
	Eslog             Name = "esLog"
	State             Name = "State"
	AttachDetachEvent Name = "NodeAttachDetachEvent"
	//
	edge      Name = "Edge_"
	edgeChild Name = "EdgeChild_"
)

type (
	key struct {
		pk string
		sk string
	}

	keyMap    map[Name]key
	idxTblMap map[IdxName]Name
)

var (
	err     error
	keys    keyMap
	idxtbl  idxTblMap
	keysync sync.RWMutex
	//
	tblEdge      Name
	tblEdgeChild Name
)

func init() {
	// TODO: add in any indexes- ie. every table and index that is queried

	// only for tables with keys types { util.UID, String} but non standard names. Standard name PKey, SortK
	keys = keyMap{
		TblName: key{"PKey", "SortK"},
		Block:   key{"PKey", "SortK"},
		Type:    key{"PKey", "SortK"},
		Event:   key{"eid", "id"},
		State:   key{pk: "Id", sk: "Name"},
	}

	idxtbl = make(idxTblMap)

}

func Set(tblname string) {
	t := Name(tblname)
	TblName = t
	Graph = t
	Block = t
	EOP = t
	NodeScalar = t
	Reverse = t
	register(t, "PKey", "SortK")
}

func SetEdgeNames(g string) (Name, Name) {
	//
	// Regstier tables/indexes
	//
	tblEdge = edge + Name(g) + edgePost
	register(tblEdge, "Bid", "Puid")
	registerIndex(IdxName("bid_cnt"), Name(tblEdge), "Bid", "Cnt")

	tblEdgeChild = edgeChild + Name(g) + edgePost
	register(tblEdgeChild, "Puid", "SortK_Cuid")
	registerIndex(IdxName("status_idx"), Name(tblEdgeChild), "Puid", "Status")

	return tblEdge, tblEdgeChild
}

func GetEdgeNames() (Name, Name) {
	return tblEdge, tblEdgeChild
}

func KeyCnt(t Name) int {
	k, ok := keys[t]
	if !ok {
		panic(fmt.Errorf(fmt.Sprintf("table %q does not exist in map", t)))
	}
	switch {
	case len(k.pk) > 0 && len(k.sk) > 0:
		return 2
	case len(k.pk) > 0 && len(k.sk) == 0:
		return 1
	default:
		panic(fmt.Errorf(fmt.Sprintf("table %q has no defined keys", t)))
	}

}

func register(t Name, pk string, sk ...string) {
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

func registerIndex(idx IdxName, t Name, pk string, sk ...string) {
	var k key
	if len(sk) > 0 {
		k = key{pk, sk[0]} // must be of type util.UID
	} else {
		k = key{pk: pk}
	}
	keysync.Lock()
	keys[Name(idx)] = k
	idxtbl[idx] = t
	keysync.Unlock()
}

func GetAssocTable(idx IdxName) (t Name, err error) {
	var ok bool
	if t, ok = idxtbl[idx]; !ok {
		return "", errors.New("Index not registered")
	}
	return t, nil
}

func IsRegistered(t Name) error {
	if _, ok := keys[t]; !ok {
		return fmt.Errorf(fmt.Sprintf("Table %q is not registered", t))
	}
	return nil

}

func IsIdxRegistered(t Name) error {
	var ok bool
	if _, ok = keys[t]; !ok {
		return fmt.Errorf(fmt.Sprintf("Index %q key's are not registered", t))
	}
	if _, ok = idxtbl[IdxName(t)]; !ok {
		return fmt.Errorf(fmt.Sprintf("Index %q is not registered", t))
	}
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

func GetSK(t Name) (string, error) {
	_, sk, err := GetKeys(t)
	return sk, err
}

// 	var k int
// 	if k, ok := keys[t]; ok {
// 		if len(k.pk) > 0 {
// 			k++
// 		}
// 		if len(k.sk) > 0 {
// 			k++
// 		}
// 	}
// 	return k
// }
