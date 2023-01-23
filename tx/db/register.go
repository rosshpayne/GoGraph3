package db

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/ros2hp/method-db/key"
	"github.com/ros2hp/method-db/mut"
	"github.com/ros2hp/method-db/query"
)

// db defines interfaces to be implemented by dynamodb spanner mysql etc
type Handle = DBHandle

type Option struct {
	Name string
	Val  interface{}
}

type State int

const (
	Enabled State = iota
	Disabled
)

type DBHandle interface {
	Execute(context.Context, []*mut.Mutations, string, API, bool, ...Option) error
	ExecuteQuery(context.Context, *query.QueryHandle, ...Option) error
	Close(*query.QueryHandle) error
	CloseTx([]*mut.Mutations)
	Ctx() context.Context
	String() string
	RetryOp(error) bool
	//
	GetTableKeys(context.Context, string) ([]key.TableKey, error)
	//
}

type RegistryT struct {
	Name       string
	Default    bool
	InstanceOf string // Dynamodb, MySQL, Postgres, Spanner
	Handle     DBHandle
}

// const (
// 	DefaultDB int = 0
// )

var (
	mu sync.Mutex
	// zero entry in dbRegistry is for default db.
	// non-default db's use Register()
	dbRegistry []RegistryT = []RegistryT{RegistryT{InstanceOf: "dynamodb", Default: true}}
)

// Register is used by non-default database services e.g. mysql
func Register(label string, inst string, handle DBHandle) {

	mu.Lock()

	d := RegistryT{Name: strings.ToLower(label), InstanceOf: inst, Handle: handle}

	if strings.ToLower(label) == "default" {
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
	if len(dbRegistry) == 0 {
		panic(fmt.Errorf("No default database assigned"))
	}
	return dbRegistry[0].Handle
}
