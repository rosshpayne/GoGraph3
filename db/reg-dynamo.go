//go:build dynamodb
// +build dynamodb

package db

import (
	"context"
	"sync"

	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/tx/query"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type DynamodbHandle struct {
	opt Options
	*dynamodb.DynamoDB
}

var (
	dbSrv *dynamodb.DynamoDB
	mu    sync.Mutex
	// zero entry in dbRegistry is for default db.
	// non-default db's use Register()
	dbRegistry []RegistryT = []RegistryT{RegistryT{Name: "dynamodb", Default: true}}
)

func newService() (*dynamodb.DynamoDB, error) {

	var err error
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	})
	if err != nil {
		logerr(err, true)
		return nil, err
	}
	return dynamodb.New(sess, aws.NewConfig()), nil
}

func init() {

	var err error

	dbSrv, err = newService()
	if err != nil {
		logerr(err)
	}
	dbRegistry[DefaultDB].Handle = DynamodbHandle{DynamoDB: dbSrv}

	// define dbSrv used by db package (dynamodb specific) internals - not ideal solution, would rather
	// source from dbRegistry but this could be expensive at runtime. This works only because
	// we are dealing with the default database, otherwise we would be forced to go through dbRegistry.
}

// Execute dml (see ExecuteQuery). TODO: make prepare a db.Option
func (h DynamodbHandle) Execute(ctx context.Context, bs []*mut.Mutations, tag string, api API, prepare bool, opt ...Option) error {

	return execute(ctx, h.DynamoDB, bs, tag, api, opt...)
}

func (h DynamodbHandle) ExecuteQuery(qh *query.QueryHandle, o ...Option) error {

	return executeQuery(qh, o...)
}

func (h DynamodbHandle) Close(q *query.QueryHandle) error {

	return nil
}

func (h DynamodbHandle) CloseTx(m []*mut.Mutations) {}

func (h DynamodbHandle) String() string {
	if dbRegistry[0].Default {
		return dbRegistry[0].Name + " [default]"
	} else {
		return dbRegistry[0].Name + " [not default]"
	}

}
