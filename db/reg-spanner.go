//go:build spanner
// +build spanner

package db

import (
	"context"
	"sync"

	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/tx/query"

	"cloud.google.com/go/spanner"
)

type SpannerHandle struct {
	opt Options
	ctx context.Context
	*spanner.Client
}

var (
	client *spanner.Client
	// main ctx context
	ctx context.Context
	mu  sync.Mutex
	// zero entry in dbRegistry is for default db.
	// non-default db's use Register()
	dbRegistry []RegistryT = []RegistryT{RegistryT{Name: "Spanner", Default: true}}
)

func newService(ctx context.Context) (*spanner.Client, error) {
	// Note: NewClient does not error if instance is not available. Error is generated when db is accessed in type.graphShortName
	client, err := spanner.NewClient(ctx, "projects/banded-charmer-310203/instances/test-instance/databases/test-sdk-db")
	if err != nil {
		logerr(err)
		return nil, err
	}
	return client, nil
}

func Init(ctx_ context.Context) {

	var err error

	ctx = ctx_
	mu.Lock()
	defer mu.Unlock()

	dbSrv, err := newService(ctx)
	if err != nil {
		logerr(err)
	}
	dbRegistry[DefaultDB].Handle = SpannerHandle{Client: dbSrv}

	// define dbSrv used by db package (dynamodb specific) internals - not ideal solution, would rather
	// source from dbRegistry but this could be expensive at runtime. This works only because
	// we are dealing with the default database, otherwise we would be forced to go through dbRegistry.
}

// Execute dml (see ExecuteQuery). TODO: make prepare a db.Option
func (h SpannerHandle) Execute(ctx context.Context, bs []*mut.Mutations, tag string, api API, prepare bool, opt ...Option) error {

	return execute(ctx, h.Client, bs, tag, api, opt...)
}

func (h SpannerHandle) ExecuteQuery(qh *query.QueryHandle, o ...Option) error {

	return executeQuery(qh, o...)
}

func (h SpannerHandle) Close(q *query.QueryHandle) error {

	return nil
}

func (h SpannerHandle) Ctx() context.Context {
	return h.ctx
}

func (h SpannerHandle) CloseTx(m []*mut.Mutations) {}

func (h SpannerHandle) String() string {
	if dbRegistry[0].Default {
		return dbRegistry[0].Name + " [default]"
	} else {
		return dbRegistry[0].Name + " [not default]"
	}

}
