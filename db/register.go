package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/tx/query"
)

type Handle = DBHandle

type Option struct {
	Name string
	Val  interface{}
}

type Throttler interface {
	Up()
	Down()
}

type DBHandle interface {
	Execute(context.Context, []*mut.Mutations, string, API, bool, ...Option) error
	ExecuteQuery(context.Context, *query.QueryHandle, ...Option) error
	Close(*query.QueryHandle) error
	CloseTx([]*mut.Mutations)
	Ctx() context.Context
	String() string
}

type RegistryT struct {
	Name    string
	Default bool
	Handle  DBHandle
}

const (
	DefaultDB int = 0
)

// Register is used by non-default database services e.g. mysql
func Register(n string, handle DBHandle) {

	mu.Lock()

	d := RegistryT{Name: strings.ToLower(n), Handle: handle}

	dbRegistry = append(dbRegistry, d)

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
	return dbRegistry[DefaultDB].Handle
}
