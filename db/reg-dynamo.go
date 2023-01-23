//go:build dynamodb
// +build dynamodb

package db

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"

	//	"github.com/GoGraph/db/internal/throttleSrv"
	"github.com/ros2hp/method-db/key"
	//thtle "github.com/GoGraph/throttle"
	"github.com/ros2hp/method-db/db"
	"github.com/ros2hp/method-db/mut"
	"github.com/ros2hp/method-db/query"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	// "github.com/aws/aws-sdk-go/aws"
	// "github.com/aws/aws-sdk-go/aws/session"
	// "github.com/aws/aws-sdk-go/service/dynamodb"
)

type DynamodbHandle struct {
	opt []db.Option
	ctx context.Context
	cfg aws.Config
	*dynamodb.Client
}

var (
	//awsConfig aws.Config // TODO: caused race condition
	dbSrv *dynamodb.Client
	// mu        sync.Mutex
	// // zero entry in dbRegistry is for default db.
	// // non-default db's use Register()
	// dbRegistry []RegistryT = []RegistryT{RegistryT{Name: "dynamodb", Default: true}}
	//
	wpStart sync.WaitGroup

	// runtime options
	DbScan db.State
)

func newService(ctx_ context.Context, opt ...db.Option) (*dynamodb.Client, aws.Config) {

	var optFuncs []func(*config.LoadOptions) error

	for _, v := range opt {
		if f, ok := v.Val.(func(*config.LoadOptions) error); ok {
			optFuncs = append(optFuncs, f)
		}
	}
	// add default RetryMode
	optFuncs = append(optFuncs, (func(*config.LoadOptions) error)(config.WithRetryMode(aws.RetryModeStandard)))

	// Using the SDK's default configuration, loading additional config
	// and credentials values from the environment variables, shared
	// credentials, and shared configuration files
	cfg, err := config.LoadDefaultConfig(ctx_, optFuncs...) // fastest...
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	return dynamodb.NewFromConfig(cfg), cfg
}

func Register(ctx_ context.Context, label string, ctxEnd *sync.WaitGroup, opt ...db.Option) {

	//var appThrottle thtle.Throttler
	DbScan = db.Enabled

	for _, v := range opt {
		switch strings.ToLower(v.Name) {
		// case "throttler":
		// 	appThrottle = v.Val.(thtle.Throttler)
		case "region":
			fmt.Println("Region = ", v.Val.(string))
			v.Val = config.WithRegion(v.Val.(string))
		case "scan":
			DbScan = v.Val.(db.State)
		}
	}

	dbSrv, awsConfig := newService(ctx_, opt...)
	if dbSrv == nil {
		panic(fmt.Errorf("dbSrv for dynamodb is nil"))
	}

	db.Register(label, "dynamodb", &DynamodbHandle{Client: dbSrv, ctx: ctx_, opt: opt, cfg: awsConfig})
	//dbRegistry[DefaultDB].Handle = &DynamodbHandle{Client: dbSrv, ctx: ctx_, opt: opt, cfg: awsConfig}

	//

	// start throttler goroutine
	//wpStart.Add(1)
	// check verify and saveNode have finished. Each goroutine is responsible for closing and waiting for all routines they spawn.
	// ctxEnd.Add(1)
	//
	// *** NOT SURE WHY I PUT THROTTLER HERE......COMMENT OUT TILL I KNOW WHY.
	// *** BUT HOWEVER in PowerON WAITS ON Cancel() which prevents ctxEnd.Done() from executing
	//
	// // start throttler.
	// go throttleSrv.PowerOn(ctx_, &wpStart, ctxEnd, appThrottle)

	// alertlog(fmt.Sprintf("waiting for db internal service [throttle] to start...."))
	// wpStart.Wait()
	// alertlog(fmt.Sprintf("db internal service [throttle] started."))
}

// Execute dml (see ExecuteQuery). TODO: make prepare a db.Option
func (h *DynamodbHandle) Execute(ctx context.Context, bs []*mut.Mutations, tag string, api db.API, prepare bool, opt ...db.Option) error {

	if ctx == nil {
		ctx = h.ctx // initiated context
	}
	return execute(ctx, h.Client, bs, tag, api, h.cfg, opt...)
}

func (h *DynamodbHandle) ExecuteQuery(ctx context.Context, qh *query.QueryHandle, o ...db.Option) error {
	if ctx == nil {
		ctx = h.ctx // initiated context
	}
	return executeQuery(ctx, h, qh, o...)
}

func (h *DynamodbHandle) Close(q *query.QueryHandle) error {

	return nil
}

func (h *DynamodbHandle) Ctx() context.Context {
	return h.ctx
}

func (h *DynamodbHandle) CloseTx(m []*mut.Mutations) {}

func (h *DynamodbHandle) String() string {
	return "dynamdob [default]"
}

func (h *DynamodbHandle) RetryOp(e error) bool {
	return retryOp(e, "db")
}

func (h *DynamodbHandle) GetTableKeys(ctx context.Context, table string) ([]key.TableKey, error) {

	var idx int // dynamodb allows upto two keys

	te, err := tabCache.fetchTableDesc(ctx, h, table)
	if err != nil {
		return nil, fmt.Errorf("GetTableKeys() error: %w", err)
	}
	tabKey := make([]key.TableKey, len(te.dto.Table.KeySchema), len(te.dto.Table.KeySchema))
	for _, v := range te.dto.Table.KeySchema {
		if v.KeyType == types.KeyTypeHash {
			idx = 0
			tabKey[0].Name = *v.AttributeName
		} else {
			idx = 1
			tabKey[1].Name = *v.AttributeName
		}

		// get the key data type
		for _, vv := range te.dto.Table.AttributeDefinitions {
			if *vv.AttributeName == *v.AttributeName {
				tabKey[idx].DBtype = string(vv.AttributeType) // "S","N","B"
			}
		}
	}

	return tabKey, nil

}

// var (
// 	logr    log.Logger
// 	logLvl  mdblog.LogLvl // Alert, Debug, NoLog
// 	errlogr func(e error)
// )

// func logDebug(s string) {
// 	if logLvl == db.Debug {
// 		var out strings.Builder

// 		out.WriteString(":debug:")
// 		out.WriteString(p)
// 		out.WriteByte(' ')
// 		out.WriteString(s)
// 		logr.Print(out.String())
// 	}
// }

// func logAlert(p string, s string) {
// 	if logLvl <= db.Debug {
// 		var out strings.Builder

// 		out.WriteString(":alert:")
// 		out.WriteByte(' ')
// 		out.WriteString(p)
// 		out.WriteByte(' ')
// 		out.WriteString(s)

// 		logr.Print(out.String())
// 	}
// }

// func logAlert(s string) {
// 	if logLvl <= db.Debug {
// 		var out strings.Builder

// 		out.WriteString(":alert:")
// 		out.WriteByte(' ')
// 		out.WriteString(s)

// 		logr.Print(out.String())
// 	}
// }

// func logErr(e error) {
// 	var out strings.Builder

// 	out.WriteString(":error:")
// 	out.WriteString(e.Error())

// 	logr.Print(out.String())
// 	errlogr(e)
// }

// func logFail(e error) {

// 	errlogr(e)

// 	var out strings.Builder

// 	out.WriteString(":error:")
// 	out.WriteString(e.Error())

// 	logr.Fail(out.String())

// }

// func (h *DynamodbHandle) SetLogger(lg log.Logger, level mdblog.LogLvl) {
// 	logr = lg
// 	loglvl = level
// }

// func (h *DynamodbHandle) SetErrLogger(el func(l string, e error)) {
// 	errlogr = el
// }
