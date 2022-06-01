//go:build dynamodb
// +build dynamodb

package db

import (
	"context"
	"fmt"
	"time"

	blk "github.com/GoGraph/block"
	"github.com/GoGraph/db/stats"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/tbl"
	//"github.com/GoGraph/tx/query"
	"github.com/GoGraph/uuid"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	//"github.com/aws/aws-sdk-go/aws"
	// "github.com/aws/aws-sdk-go/service/dynamodb"
	// "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	// "github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

//  ItemCache struct is the transition between Dynamodb types and the actual attribute type defined in the DD.
//  Number (dynamodb type) -> float64 (transition) -> int (internal app & as defined in DD)
//  process: dynamodb -> ItemCache -> DD conversion if necessary to application variables -> ItemCache -> Dynamodb
//	types that require conversion from ItemCache to internal are:
//   DD:   int         conversion: float64 -> int
//   DD:   datetime    conversion: string -> time.Time
//  all the other datatypes do not need to be converted.

var logid = "DB: "

func logerr(e error, panic_ ...bool) {

	if len(panic_) > 0 && panic_[0] {
		slog.Log("DB: ", e.Error(), true)
		panic(e)
	}
	slog.Log("DB: ", e.Error())
}

//  NOTE: tyShortNm is duplicated in cache pkg. It exists in in db package only to support come code in rdfload.go that references the db version rather than the cache which it cannot access
// because of input-cycle issues. Once this reference is rdfload is removed the cache version should be the only one used.

type tyNames struct {
	ShortNm string `json:"Atr"`
	LongNm  string
}

var (
	err       error
	tynames   []tyNames
	tyShortNm map[string]string
)

func GetTypeShortNames() ([]tyNames, error) {
	return tynames, nil
}

type PKey struct {
	PKey  uuid.UID
	SortK string
}

// // NodeExists
// func NodeExists(uid uuid.UID, subKey ...string) (bool, error) {

// 	var (
// 		err    error
// 		dynsrv DBHandle
// 		sortk  string
// 	)

// 	if len(subKey) > 0 {
// 		sortk = subKey[0]
// 	} else {
// 		sortk = "A#T"
// 	}
// 	pkey := PKey{PKey: uid, SortK: sortk}

// 	av, err = dynamodbattribute.MarshalMap(&pkey)
// 	if err != nil {
// 		return false, newDBMarshalingErr("NodeExists", uid.String(), sortk, "MarshalMap", err)
// 	}
// 	//
// 	input := &dynamodb.GetItemInput{
// 		Key: av,
// 	}
// 	input = input.SetTableName(string(tbl.TblName)).SetReturnConsumedCapacity("TOTAL")
// 	//
// 	// GetItem
// 	//
// 	//dbSrv := GetClient()
// 	if param.DefaultDB == "dynamodb" {
// 		dbSrv, err := GetDBHdl(param.DefaultDB).(DynamodbHandle)
// 	}
// 	if err != nil {
// 		panic(err)
// 	}

// 	t0 := time.Now()
// 	result, err := dbSrv.GetItem(input)
// 	t1 := time.Now()
// 	if err != nil {
// 		return false, newDBSysErr("NodeExists", "GetItem", err)
// 	}
// 	syslog(fmt.Sprintf("NodeExists: consumed capacity for GetItem: %s  Duration: %s", result.ConsumedCapacity, t1.Sub(t0)))

// 	if len(result.Item) == 0 {
// 		return false, nil
// 	}
// 	return true, nil
// }

// FetchNode performs a Query with KeyBeginsWidth on the SortK value, so all item belonging to the SortK are fetched.
func FetchNodeContext(ctx_ context.Context, uid uuid.UID, subKey ...string) (blk.NodeBlock, error) {

	var sortk string
	if len(subKey) > 0 {
		sortk = subKey[0]
		//		slog.Log("DB FetchNode: ", fmt.Sprintf(" node: %s subKey: %s", uid.String(), sortk))
	} else {
		sortk = "A#"
		// slog.Log("DB FetchNode: ", fmt.Sprintf(" node: %s subKey: %s", uid.String(), sortk))
	}

	keyC := expression.KeyEqual(expression.Key("PKey"), expression.Value(uid)).And(expression.KeyBeginsWith(expression.Key("SortK"), sortk))
	expr, err := expression.NewBuilder().WithKeyCondition(keyC).Build()
	if err != nil {
		return nil, newDBExprErr("FetchTNode", uid.String(), sortk, err)
	}
	//
	input := &dynamodb.QueryInput{
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		TableName:                 aws.String(string(tbl.TblName)),
		ReturnConsumedCapacity:    types.ReturnConsumedCapacityIndexes,
		ConsistentRead:            aws.Bool(true),
	}
	//input = input.SetTableName(string(tbl.TblName)).SetReturnConsumedCapacity("INDEXES").SetConsistentRead(true)
	//	fmt.Println("FetchNode: input: ", input.String())
	//
	// Query
	//
	t0 := time.Now()
	result, err := dbSrv.Query(ctx_, input)
	t1 := time.Now()
	if err != nil {
		return nil, newDBSysErr("DB FetchNode", "Query", err)
	}
	dur := t1.Sub(t0)
	cc_ := ConsumedCapacity_{result.ConsumedCapacity}
	syslog(fmt.Sprintf("FetchNode:consumed capacity for Query  %s. ItemCount %d  Duration: %s", cc_.String(), len(result.Items), dur.String()))
	//
	if result.Count == 0 {
		return nil, newDBNoItemFound("FetchNode", uid.String(), "", "Query")
	}
	data := make(blk.NodeBlock, result.Count)
	err = attributevalue.UnmarshalListOfMaps(result.Items, &data)
	if err != nil {
		return nil, newDBUnmarshalErr("FetchNode", uid.String(), "", "UnmarshalListOfMaps", err)
	}
	//
	// update stats
	//
	// fmt.Printf("monitor capacity: %#v %#v\n", result.ConsumedCapacity, result.ConsumedCapacity.Table)
	// v := mon.Fetch{CapacityUnits: *result.ConsumedCapacity.CapacityUnits, Items: len(result.Items), Duration: dur}
	// stat := mon.Stat{Id: mon.DBFetch, Value: &v}
	// mon.StatCh <- stat
	// save query statistics
	stats.SaveQueryStat(stats.Query, "FetchNode", result.ConsumedCapacity, result.Count, result.ScannedCount, dur)

	return data, nil
}

// FetchNode performs a Query with KeyBeginsWidth on the SortK value, so all item belonging to the SortK are fetched.
func FetchNode(uid uuid.UID, subKey ...string) (blk.NodeBlock, error) {

	var sortk string
	if len(subKey) > 0 {
		sortk = subKey[0]
		//		slog.Log("DB FetchNode: ", fmt.Sprintf(" node: %s subKey: %s", uid.String(), sortk))
	} else {
		sortk = "A#"
		// slog.Log("DB FetchNode: ", fmt.Sprintf(" node: %s subKey: %s", uid.String(), sortk))
	}

	keyC := expression.KeyEqual(expression.Key("PKey"), expression.Value(uid)).And(expression.KeyBeginsWith(expression.Key("SortK"), sortk))
	expr, err := expression.NewBuilder().WithKeyCondition(keyC).Build()
	if err != nil {
		return nil, newDBExprErr("FetchTNode", uid.String(), sortk, err)
	}
	//
	input := &dynamodb.QueryInput{
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		TableName:                 aws.String(string(tbl.TblName)),
		ReturnConsumedCapacity:    types.ReturnConsumedCapacityIndexes,
		ConsistentRead:            aws.Bool(true),
	}
	//input = input.SetTableName(string(tbl.TblName)).SetReturnConsumedCapacity("INDEXES").SetConsistentRead(true)
	//	fmt.Println("FetchNode: input: ", input.String())
	//
	// Query
	//
	t0 := time.Now()
	result, err := dbSrv.Query(context.Background(), input)
	t1 := time.Now()
	if err != nil {
		return nil, newDBSysErr("DB FetchNode", "Query", err)
	}
	dur := t1.Sub(t0)
	cc_ := ConsumedCapacity_{result.ConsumedCapacity}
	syslog(fmt.Sprintf("FetchNode:consumed capacity for Query  %s. ItemCount %d  Duration: %s", cc_.String(), len(result.Items), dur.String()))
	//
	if int(result.Count) == 0 {
		return nil, newDBNoItemFound("FetchNode", uid.String(), "", "Query")
	}
	data := make(blk.NodeBlock, result.Count)
	err = attributevalue.UnmarshalListOfMaps(result.Items, &data)
	if err != nil {
		return nil, newDBUnmarshalErr("FetchNode", uid.String(), "", "UnmarshalListOfMaps", err)
	}
	//
	// update stats
	//
	// fmt.Printf("monitor capacity: %#v %#v\n", result.ConsumedCapacity, result.ConsumedCapacity.Table)
	// v := mon.Fetch{CapacityUnits: *result.ConsumedCapacity.CapacityUnits, Items: len(result.Items), Duration: dur}
	// stat := mon.Stat{Id: mon.DBFetch, Value: &v}
	// mon.StatCh <- stat
	// save query statistics
	stats.SaveQueryStat(stats.Query, "FetchNode", result.ConsumedCapacity, result.Count, result.ScannedCount, dur)

	return data, nil
}

func FetchNodeItem(uid uuid.UID, sortk string) (blk.NodeBlock, error) {

	// stat := mon.Stat{Id: mon.DBFetch}
	// mon.StatCh <- stat

	// proj := expression.NamesList(expression.Name("SortK"), expression.Name("Nd"), expression.Name("XF"), expression.Name("Id"))
	// expr, err := expression.NewBuilder().WithProjection(proj).Build()
	// if err != nil {
	// 	return nil, newDBExprErr("FetchNodeItem", "", "", err)
	// }
	// TODO: remove encoding when load of data via cli is not used.
	syslog(fmt.Sprintf("FetchNodeItem: uid: %s   sortk: sortk"))
	pkey := PKey{PKey: []byte(uid), SortK: sortk}
	av, err := attributevalue.MarshalMap(&pkey)
	if err != nil {
		return nil, newDBMarshalingErr("FetchNodeItem", "", sortk, "MarshalMap", err)
	}
	//
	input := &dynamodb.GetItemInput{
		Key:            av,
		TableName:      aws.String(string(tbl.TblName)),
		ConsistentRead: aws.Bool(true),
	}
	//input = input.SetTableName(string(tbl.TblName)).SetReturnConsumedCapacity("TOTAL")
	//
	// GetItem
	//
	t0 := time.Now()
	result, err := dbSrv.GetItem(context.Background(), input)
	t1 := time.Now()
	dur := t1.Sub(t0)
	if err != nil {
		return nil, newDBSysErr("FetchNodeItem", "GetItem", err)
	}
	cc_ := ConsumedCapacity_{result.ConsumedCapacity}
	syslog(fmt.Sprintf("FetchNodeItem:consumed capacity for GetItem  %s. Duration: %s", cc_.String(), t1.Sub(t0)))
	//
	if len(result.Item) == 0 {
		return nil, newDBNoItemFound("FetchNodeItem", "", sortk, "GetItem")
	}
	//
	var di blk.DataItem
	err = attributevalue.UnmarshalMap(result.Item, &di)
	if err != nil {
		return nil, newDBUnmarshalErr("FetchNodeItem", "", sortk, "UnmarshalMap", err)
	}
	nb := make(blk.NodeBlock, 1, 1)
	nb[0] = &di

	// v := mon.Fetch{CapacityUnits: *result.ConsumedCapacity.CapacityUnits, Items: len(result.Item), Duration: dur}
	// stat := mon.Stat{Id: mon.DBFetch, Value: &v}
	// mon.StatCh <- stat
	stats.SaveQueryStat(stats.GetItem, "FetchNodeItem", result.ConsumedCapacity, 1, 0, dur)
	return nb, nil
	//
}
