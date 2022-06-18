//go:build dynamodb
// +build dynamodb

package db

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/tx/query"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	// "github.com/aws/aws-sdk-go/aws"
	// "github.com/aws/aws-sdk-go/aws/session"
	// "github.com/aws/aws-sdk-go/service/dynamodb"
)

type DynamodbHandle struct {
	opt []Option
	ctx context.Context
	*dynamodb.Client
}

var (
	dbSrv *dynamodb.Client
	mu    sync.Mutex
	// zero entry in dbRegistry is for default db.
	// non-default db's use Register()
	dbRegistry []RegistryT = []RegistryT{RegistryT{Name: "dynamodb", Default: true}}
)

func newService(ctx_ context.Context) *dynamodb.Client {

	// Using the SDK's default configuration, loading additional config
	// and credentials values from the environment variables, shared
	// credentials, and shared configuration files
	cfg, err := config.LoadDefaultConfig(ctx_, config.WithRegion("us-east-1"))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	return dynamodb.NewFromConfig(cfg)
}

func Init(ctx_ context.Context, opt ...Option) {

	dbSrv = newService(ctx_)
	if dbSrv == nil {
		panic(fmt.Errorf("dbSrv for dynamodb is nil"))
	}

	dbRegistry[DefaultDB].Handle = DynamodbHandle{Client: dbSrv, ctx: ctx_}

	// define dbSrv used by db package (dynamodb specific) internals - not ideal solution, would rather
	// source from dbRegistry but this could be expensive at runtime. This works only because
	// we are dealing with the default database, otherwise we would be forced to go through dbRegistry.
}

// Execute dml (see ExecuteQuery). TODO: make prepare a db.Option
func (h DynamodbHandle) Execute(ctx context.Context, bs []*mut.Mutations, tag string, api API, prepare bool, opt ...Option) error {

	// ctx as set in tx.NewQuery*
	if ctx == nil {
		ctx = h.ctx // initiated context
	}
	return execute(ctx, h.Client, bs, tag, api, opt...)
}

func (h DynamodbHandle) ExecuteQuery(ctx context.Context, qh *query.QueryHandle, o ...Option) error {

	return executeQuery(ctx, qh, o...)
}

func (h DynamodbHandle) Close(q *query.QueryHandle) error {

	return nil
}

func (h DynamodbHandle) Ctx() context.Context {
	return h.ctx
}

func (h DynamodbHandle) CloseTx(m []*mut.Mutations) {}

func (h DynamodbHandle) String() string {
	if dbRegistry[0].Default {
		return dbRegistry[0].Name + " [default]"
	} else {
		return dbRegistry[0].Name + " [not default]"
	}

}
