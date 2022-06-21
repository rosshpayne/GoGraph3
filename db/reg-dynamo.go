//go:build dynamodb
// +build dynamodb

package db

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/GoGraph/db/internal/throttleSrv"
	thtle "github.com/GoGraph/throttle"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/tx/query"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	// "github.com/aws/aws-sdk-go/aws"
	// "github.com/aws/aws-sdk-go/aws/session"
	// "github.com/aws/aws-sdk-go/service/dynamodb"
)

type DynamodbHandle struct {
	opt []Option
	ctx context.Context
	cfg aws.Config
	*dynamodb.Client
}

var (
	awsConfig aws.Config
	dbSrv     *dynamodb.Client
	mu        sync.Mutex
	// zero entry in dbRegistry is for default db.
	// non-default db's use Register()
	dbRegistry []RegistryT = []RegistryT{RegistryT{Name: "dynamodb", Default: true}}
	//
	wpStart sync.WaitGroup
)

func newService(ctx_ context.Context, opt ...Option) (*dynamodb.Client, aws.Config) {

	var (
		optFuncs []func(*config.LoadOptions) error
	)

	for _, v := range opt {
		if f, ok := v.Val.(func(*config.LoadOptions) error); ok {
			optFuncs = append(optFuncs, f)
		}
	}
	// Using the SDK's default configuration, loading additional config
	// and credentials values from the environment variables, shared
	// credentials, and shared configuration files
	//cfg, err := config.LoadDefaultConfig(ctx_, config.WithRegion("us-east-1"))
	cfg, err := config.LoadDefaultConfig(ctx_, optFuncs...)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	return dynamodb.NewFromConfig(cfg), cfg
}

func Init(ctx_ context.Context, ctxEnd *sync.WaitGroup, opt ...Option) {

	var appThrottle thtle.Throttler

	for _, v := range opt {
		switch strings.ToLower(v.Name) {
		case "throttler":
			appThrottle = v.Val.(Throttler)
		case "region":
			fmt.Println("Region = ", v.Val.(string))
			v.Val = config.WithRegion(v.Val.(string))
		}
	}

	dbSrv, awsConfig := newService(ctx_, opt...)
	if dbSrv == nil {
		panic(fmt.Errorf("dbSrv for dynamodb is nil"))
	}
	dbSrv = dbSrv

	dbRegistry[DefaultDB].Handle = DynamodbHandle{Client: dbSrv, ctx: ctx_, opt: opt, cfg: awsConfig}

	// define dbSrv used by db package (dynamodb specific) internals - not ideal solution, would rather
	// source from dbRegistry but this could be expensive at runtime. This works only because
	// we are dealing with the default database, otherwise we would be forced to go through dbRegistry.

	// start throttler goroutine
	wpStart.Add(1)
	// check verify and saveNode have finished. Each goroutine is responsible for closing and waiting for all routines they spawn.
	ctxEnd.Add(1)
	//
	// start pipeline goroutines
	//
	go throttleSrv.PowerOn(ctx_, &wpStart, ctxEnd, appThrottle)

	syslog(fmt.Sprintf("waiting for db internal service [throttle] to start...."))
	wpStart.Wait()

}

// Execute dml (see ExecuteQuery). TODO: make prepare a db.Option
func (h DynamodbHandle) Execute(ctx context.Context, bs []*mut.Mutations, tag string, api API, prepare bool, opt ...Option) error {

	// ctx as set in tx.NewQuery*
	if ctx == nil {
		ctx = h.ctx // initiated context
	}
	return execute(ctx, h.Client, bs, tag, api, h.cfg, opt...)
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
