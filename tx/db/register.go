package db

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/GoGraph/tbl/key"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/tx/query"
)

type Handle = DBHandle

type Option struct {
	Name string
	Val  interface{}
}

type DBHandle interface {
	Execute(context.Context, []*mut.Mutations, string, API, bool, ...Option) error
	ExecuteQuery(context.Context, *query.QueryHandle, ...Option) error
	Close(*query.QueryHandle) error
	CloseTx([]*mut.Mutations)
	Ctx() context.Context
	String() string
	RetryOp(err error) bool
	//
	GetTableKeys(context.Context, string) ([]key.TableKey, error)
}

type RegistryT struct {
	Name    string
	Default bool
	Handle  DBHandle
}

// const (
// 	DefaultDB int = 0
// )

var (
	mu sync.Mutex
	// zero entry in dbRegistry is for default db.
	// non-default db's use Register()
	dbRegistry []RegistryT = []RegistryT{RegistryT{Name: "dynamodb", Default: true}}
)

// Register is used by non-default database services e.g. mysql
func Register(n string, handle DBHandle, deflt ...bool) {

	mu.Lock()

	d := RegistryT{Name: strings.ToLower(n), Handle: handle}

	if len(deflt) > 0 && deflt[0] {
		dbRegistry[0] = d
	} else {
		dbRegistry = append(dbRegistry, d)
	}

	mu.Unlock()
}

func GetDBHdl(n string) (DBHandle, error) {
	nm := strings.ToLower(n)

	for _, v := range dbRegistry {
		if v.Name == nm {
			return v.Handle, nil
		}
	}

	return nil, fmt.Errorf("%q not found in db registry", n)
}

func GetDefaultDBHdl() DBHandle {
	return dbRegistry[0].Handle
}
