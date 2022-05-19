//go:build dynamodb
// +build dynamodb

package db

import (
	"fmt"
	"time"

	"github.com/DynamoGraph/dbConn"
	param "github.com/DynamoGraph/dygparam"
	slog "github.com/DynamoGraph/syslog"
	"github.com/DynamoGraph/util"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

type Equality int

const (
	logid = "gqlDB: "
)
const (
	EQ Equality = iota + 1
	LT
	GT
	GE
	LE
)

// api for GQL query functions

type NodeResult struct {
	PKey  util.UID
	SortK string
	Ty    string
}

type (
	QResult  []NodeResult
	AttrName = string
)

var (
	dynSrv *dynamodb.DynamoDB
	err    error
	//tynames   []tyNames
	//tyShortNm map[string]string
)

func logerr(e error, panic_ ...bool) {

	if len(panic_) > 0 && panic_[0] {
		slog.Log(logid, e.Error(), true)
		panic(e)
	}
	slog.Log(logid, e.Error())
}

func syslog(s string) {
	slog.Log(logid, s)
}

func init() {
	dynSrv = dbConn.New()
}

func GSIQueryN(attr AttrName, lv float64, op Equality) (QResult, error) {

	var keyC expression.KeyConditionBuilder
	//
	// DD determines what index to search based on Key value. Here Key is Name and DD knows its a string hence index P_S
	//
	switch op {
	case EQ:
		keyC = expression.KeyAnd(expression.Key("P").Equal(expression.Value(attr)), expression.Key("N").Equal(expression.Value(lv)))
	case LT:
		keyC = expression.KeyAnd(expression.Key("P").Equal(expression.Value(attr)), expression.Key("N").LessThan(expression.Value(lv)))
	case GT:
		keyC = expression.KeyAnd(expression.Key("P").Equal(expression.Value(attr)), expression.Key("N").GreaterThan(expression.Value(lv)))
	case GE:
		keyC = expression.KeyAnd(expression.Key("P").Equal(expression.Value(attr)), expression.Key("N").GreaterThanEqual(expression.Value(lv)))
	case LE:
		keyC = expression.KeyAnd(expression.Key("P").Equal(expression.Value(attr)), expression.Key("N").LessThanEqual(expression.Value(lv)))
	}
	expr, err := expression.NewBuilder().WithKeyCondition(keyC).Build()
	if err != nil {
		return nil, newDBExprErr("GSIS", attr, "", err)
	}
	//
	input := &dynamodb.QueryInput{
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	}
	input = input.SetTableName(param.GraphTable).SetIndexName("P_N").SetReturnConsumedCapacity("TOTAL")
	//
	t0 := time.Now()
	result, err := dynSrv.Query(input)
	t1 := time.Now()
	if err != nil {
		return nil, newDBSysErr("GSIS", "Query", err)
	}
	syslog(fmt.Sprintf("GSIS:consumed capacity for Query index P_S, %s.  ItemCount %d  Duration: %s ", result.ConsumedCapacity, len(result.Items), t1.Sub(t0)))
	//
	if int(*result.Count) == 0 {
		return nil, newDBNoItemFound("GSIS", attr, "", "Query") //TODO add lv
	}
	//
	qresult := make(QResult, len(result.Items))
	err = dynamodbattribute.UnmarshalListOfMaps(result.Items, &qresult)
	if err != nil {
		return nil, newDBUnmarshalErr("GSIS", attr, "", "UnmarshalListOfMaps", err)
	}
	//
	return qresult, nil
}

func GSIQueryS(attr AttrName, lv string, op Equality) (QResult, error) {

	var keyC expression.KeyConditionBuilder
	//
	// DD determines what index to search based on Key value. Here Key is Name and DD knows its a string hence index P_S
	//
	switch op {
	case EQ:
		keyC = expression.KeyAnd(expression.Key("P").Equal(expression.Value(attr)), expression.Key("S").Equal(expression.Value(lv)))
	case LT:
		keyC = expression.KeyAnd(expression.Key("P").Equal(expression.Value(attr)), expression.Key("S").LessThan(expression.Value(lv)))
	case GT:
		keyC = expression.KeyAnd(expression.Key("P").Equal(expression.Value(attr)), expression.Key("S").GreaterThan(expression.Value(lv)))
	case GE:
		keyC = expression.KeyAnd(expression.Key("P").Equal(expression.Value(attr)), expression.Key("S").GreaterThanEqual(expression.Value(lv)))
	case LE:
		keyC = expression.KeyAnd(expression.Key("P").Equal(expression.Value(attr)), expression.Key("S").LessThanEqual(expression.Value(lv)))
	}
	expr, err := expression.NewBuilder().WithKeyCondition(keyC).Build()
	if err != nil {
		return nil, newDBExprErr("GSIS", attr, "", err)
	}
	//
	input := &dynamodb.QueryInput{
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	}
	input = input.SetTableName(param.GraphTable).SetIndexName("P_S").SetReturnConsumedCapacity("TOTAL")
	//
	t0 := time.Now()
	result, err := dynSrv.Query(input)
	if err != nil {
		return nil, newDBSysErr("GSIS", "Query", err)
	}
	t1 := time.Now()
	syslog(fmt.Sprintf("GSIS:consumed capacity for Query index P_S, %s.  ItemCount %d  Duration: %s ", result.ConsumedCapacity, len(result.Items), t1.Sub(t0)))
	//
	if int(*result.Count) == 0 {
		return nil, newDBNoItemFound("GSIS", attr, lv, "Query")
	}
	//
	qresult := make(QResult, len(result.Items))
	err = dynamodbattribute.UnmarshalListOfMaps(result.Items, &qresult)
	if err != nil {
		return nil, newDBUnmarshalErr("GSIS", attr, lv, "UnmarshalListOfMaps", err)
	}
	//
	return qresult, nil
}

func GSIhasS(attr AttrName) (QResult, error) {

	syslog("GSIhasS: consumed capacity for Query ")

	var keyC expression.KeyConditionBuilder
	//
	// DD determines what index to search based on Key value. Here Key is Name and DD knows its a string hence index P_S
	//
	keyC = expression.Key("P").Equal(expression.Value(attr))

	expr, err := expression.NewBuilder().WithKeyCondition(keyC).Build()
	if err != nil {
		return nil, newDBExprErr("GSIS", attr, "", err)
	}
	//
	input := &dynamodb.QueryInput{
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	}
	input = input.SetTableName(param.GraphTable).SetIndexName("P_S").SetReturnConsumedCapacity("TOTAL")
	//
	t0 := time.Now()
	result, err := dynSrv.Query(input)
	t1 := time.Now()
	if err != nil {
		return nil, newDBSysErr("GSIhasS", "Query", err)
	}
	syslog(fmt.Sprintf("GSIhasS: consumed capacity for Query index P_S, %s.  ItemCount %d  Duration: %s ", result.ConsumedCapacity, len(result.Items), t1.Sub(t0)))
	if int(*result.Count) == 0 {
		return nil, nil
	}
	qresult := make(QResult, len(result.Items))
	err = dynamodbattribute.UnmarshalListOfMaps(result.Items, &qresult)
	if err != nil {
		return nil, newDBUnmarshalErr("GSIhasS", attr, "", "UnmarshalListOfMaps", err)
	}
	//
	return qresult, nil
}

func GSIhasN(attr AttrName) (QResult, error) {

	syslog("GSIhasN: consumed capacity for Query ")

	var keyC expression.KeyConditionBuilder
	//
	// DD determines what index to search based on Key value. Here Key is Name and DD knows its a string hence index P_S
	//
	keyC = expression.Key("P").Equal(expression.Value(attr))

	expr, err := expression.NewBuilder().WithKeyCondition(keyC).Build()
	if err != nil {
		return nil, newDBExprErr("GSIhasN", attr, "", err)
	}
	//
	input := &dynamodb.QueryInput{
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	}
	input = input.SetTableName(param.GraphTable).SetIndexName("P_N").SetReturnConsumedCapacity("TOTAL")
	//
	t0 := time.Now()
	result, err := dynSrv.Query(input)
	t1 := time.Now()
	if err != nil {
		return nil, newDBSysErr("GSIhasN", "Query", err)
	}
	syslog(fmt.Sprintf("GSIS:consumed capacity for Query index P_S, %s.  ItemCount %d  Duration: %s ", result.ConsumedCapacity, len(result.Items), t1.Sub(t0)))
	//
	if int(*result.Count) == 0 {
		return nil, nil
	}
	//
	qresult := make(QResult, len(result.Items))
	err = dynamodbattribute.UnmarshalListOfMaps(result.Items, &qresult)
	if err != nil {
		return nil, newDBUnmarshalErr("GSIhasN", attr, "", "UnmarshalListOfMaps", err)
	}
	//
	return qresult, nil
}
