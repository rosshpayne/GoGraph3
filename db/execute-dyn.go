//go:build dynamodb
// +build dynamodb

package db

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	//"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"text/scanner"
	"time"

	throttle "github.com/GoGraph/db/internal/throttleSrv"
	"github.com/GoGraph/db/stats"

	"github.com/ros2hp/method-db/db"
	"github.com/ros2hp/method-db/dbs"
	"github.com/ros2hp/method-db/log"
	"github.com/ros2hp/method-db/mut"
	"github.com/ros2hp/method-db/param"
	"github.com/ros2hp/method-db/query"
	"github.com/ros2hp/method-db/uuid"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type action byte

const (
	fail action = iota
	retry
	shortDelay
	longDelay
	throttle_ // reduce concurrency
)

type ComparOpr string

const (
	EQ         ComparOpr = "EQ"
	NE         ComparOpr = "NE"
	GT         ComparOpr = "GT"
	GE         ComparOpr = "GE"
	LE         ComparOpr = "LE"
	LT         ComparOpr = "LT"
	BEGINSWITH ComparOpr = "BEGINSWITH"
	BETWEEN    ComparOpr = "BETWEEN"
	NOT        ComparOpr = "NOT"
	NA         ComparOpr = "NA"
)

func (c ComparOpr) String() string {
	switch c {
	case EQ:
		return " = "
	case NE:
		return " != "
	case GT:
		return " > "
	case GE:
		return " >= "
	case LE:
		return " <= "
	case LT:
		return " < "
	case BEGINSWITH:
		return "BeginsWith("
	case BETWEEN:
		return "Between("
	case NOT:
		return " !"
	}
	return "NA"
}

func execute(ctx context.Context, client *dynamodb.Client, bs []*mut.Mutations, tag string, api db.API, cfg aws.Config, opt ...db.Option) error {

	var (
		err error
	)

	switch api {

	case db.TransactionAPI:

		if len(bs) > 1 {
			return fmt.Errorf("Error: More than 25 mutations defined when using a Transaction API")
		}
		// distinction between Std and Transaction made in execTranaction()
		err = execTransaction(ctx, client, bs, tag, api)

	case db.StdAPI:

		// distinction between Std and Transaction made in execTranaction()
		err = execTransaction(ctx, client, bs, tag, api)

	case db.BatchAPI:

		err = execBatch(ctx, client, bs, tag, cfg)

	// case ScanAPI: - NA, scan should be a derived quantity based on query methods employeed e.g. lack of Key() method would force a scan

	// 	err = execScan(client, bs, tag)

	case db.OptimAPI:

		err = execOptim(ctx, client, bs, tag, cfg)

	}
	return err

}

// retry determines whether the database operation that caused the error can be retried, based on the type of error.
// All http 500 status codes can be retried whereas only a handful of 400 status codes can be.
func errorAction(err error) []action { // (bool, action string) {
	//
	re := &awshttp.ResponseError{}
	if errors.As(err, &re) {

		switch re.Response.StatusCode {
		case 500:
			// do not override the default Retryer, which will have done its thing, so if error persists fail it.
			return []action{shortDelay, retry}

		case 400:
			// // these are the only errors that the application can retry after receiving
			// icsle := &types.ItemCollectionSizeLimitExceededException{}
			// if errors.As(err, &icsle) {
			// 	return []action{retry}
			// }
			// lee := &types.LimitExceededException{}
			// if errors.As(err, &lee) {
			// 	return []action{longDelay, retry}
			// }
			// ptee := &types.ProvisionedThroughputExceededException{}
			// if errors.As(err, &ptee) {
			// 	return []action{longDelay, retry}
			// }
			// rle := &types.RequestLimitExceeded{}
			// if errors.As(err, &rle) {
			// 	return []action{retry}
			// }

			errString := strings.ToLower(err.Error())
			if strings.Index(errString, "api error throttlingexception") > 0 {
				if strings.Index(errString, "try again shortly") > 0 {
					// // lets wait 30 seconds....
					// log.LogError("throttlingexception", "About to wait 30 seconds before proceeding...")
					// time.Sleep(30 * time.Second)
					return []action{shortDelay, retry}
				} else {
					return []action{fail}
				}
			}
			if strings.Index(errString, "rate of requests exceeds the allowed throughput") > 0 {
				return []action{throttle_, longDelay, retry}
			}
		}
	} else {
		panic(fmt.Errorf("errActions: expected a ResponseError"))
	}
	return []action{fail}
}

func retryOp(err error, tag string, cf ...aws.Config) bool {

	var cfg aws.Config
	if len(cf) > 0 {
		cfg = cf[0]
	}

	//  github.com/aws/aws-sdk-go-v2/aws/Retryer
	retryer := cfg.Retryer

	log.LogAlert(fmt.Sprintf("RetryOp for %s: [%T] %s", tag, err, err))

	if retryer != nil {
		if retryer().IsErrorRetryable(err) {
			log.LogAlert("RetryOp: error is retryable...")
			for _, v := range []int{1, 2, 3} {
				d, err := retryer().RetryDelay(v, err)
				if err != nil {
					fmt.Printf("RetryOp:  Delay attempt %d:  %s", v, d.String())
				}
			}
		} else {
			log.LogAlert("RetryOp: error is NOT retryable...")
		}
		log.LogAlert(fmt.Sprintf("RetryOp: max attempts: %d", retryer().MaxAttempts()))
	} else {
		log.LogAlert(fmt.Sprintf("RetryOp: no Retryer defined. aws.Config max attempts: %d", cfg.RetryMaxAttempts))
	}

	for _, action := range errorAction(err) {

		switch action {
		case shortDelay:
			log.LogAlert("RetryOp: short delay...")
			time.Sleep(20 * time.Second)
		case longDelay:
			log.LogAlert("RetryOp: long delay...")
			time.Sleep(60 * time.Second)
		case retry:
			log.LogAlert("RetryOp: retry...")
			return true
		// case fail:
		// 	return false
		case throttle_:
			log.LogAlert("RetryOp: throttle...")
			// call throttle down api
			throttle.Down()

		}
	}
	log.LogAlert("RetryOp: no retry...")
	return false
}

// func XXX(expr expression.Expression) map[string]*dynamodb.AttributeValue {

// 	var s strings.Builder
// 	values := expr.Values()

// convertBS2List converts Binary Set to List because GoGraph expects a List type not a BS type
// Dynamodb's expression pkg creates BS rather than L for binary array data.
// As GoGraph had no user-defined types it is possible to hardwire in the affected attributes.
// All types in GoGraph are known at compile time.
func convertBS2List(values map[string]types.AttributeValue, names map[string]string) map[string]types.AttributeValue {

	var s strings.Builder

	for k, v := range names { // map[string]string  [":0"]"PKey", [":2"]"SortK"
		switch v {
		//safe to hardwire in attribute name as all required List binaries are known at compile time.
		case "Nd", "LB":
			s.WriteByte(':')
			s.WriteByte(k[1])
			// check if BS is used and then convert if it is

			if bs, ok := values[s.String()].(*types.AttributeValueMemberBS); ok {
				nl := make([]types.AttributeValue, len(bs.Value), len(bs.Value))
				for i, b := range bs.Value {
					nl[i] = &types.AttributeValueMemberB{Value: b}
				}
				values[s.String()] = &types.AttributeValueMemberL{Value: nl}
			}
			s.Reset()
		}
	}
	return values
}

// pkg db must support mutations, Insert, Update, Remove, Merge:
// any other mutations (eg WithOBatchLimit) must be defined outside of DB and passed in (somehow)

func txUpdate(m *mut.Mutation) (*types.TransactWriteItem, error) {

	var (
		err error
		//c    string
		expr expression.Expression
		upd  expression.UpdateBuilder
	)

	// merge := false
	// if len(ismerge) > 0 {
	// 	merge = ismerge[0]
	// }

	for i, col := range m.GetMembers() {
		if col.Name == "__" || col.IsKey() {
			continue
		}

		switch col.Mod {
		// TODO: implement Add (as opposed to inc which is "Add 1")
		case mut.Add:
			if i == 0 {
				upd = expression.Set(expression.Name(col.Name), expression.Name(col.Name).Plus(expression.Value(col.Value)))
			} else {
				upd = upd.Set(expression.Name(col.Name), expression.Name(col.Name).Plus(expression.Value(col.Value)))
			}
		case mut.Subtract:
			if i == 0 {
				upd = expression.Set(expression.Name(col.Name), expression.Name(col.Name).Minus(expression.Value(col.Value)))
			} else {
				upd = upd.Set(expression.Name(col.Name), expression.Name(col.Name).Minus(expression.Value(col.Value)))
			}
		case mut.Remove:
			if i == 0 {
				upd = expression.Remove(expression.Name(col.Name))
			} else {
				upd = upd.Remove(expression.Name(col.Name))
			}

		}

		switch col.Array {

		// array identifes dynamodb List types such as attributes  "Nd", "XF", "Id", "XBl", "L*":
		// default behaviour is to append value to end of array

		case true:

			// Default operation is APPEND unless overriden by SET.
			if col.Mod == mut.Set {

				if i == 0 {
					// on the rare occuassion some mutations want to set the array e.g. XF parameter when creating overflow blocks
					upd = expression.Set(expression.Name(col.Name), expression.Value(col.Value))
				} else {
					upd = upd.Set(expression.Name(col.Name), expression.Value(col.Value))
				}

			} else { // Append

				if i == 0 {
					upd = expression.Set(expression.Name(col.Name), expression.ListAppend(expression.Name(col.Name), expression.Value(col.Value)))
				} else {
					upd = upd.Set(expression.Name(col.Name), expression.ListAppend(expression.Name(col.Name), expression.Value(col.Value)))
				}
			}

		case false:

			if col.Mod != mut.Set {
				// already processed (see above)
				break
			}
			var ct string
			va := col.Value

			if v, ok := col.Value.(string); ok {
				if v == "$CURRENT_TIMESTAMP$" {
					tz, _ := time.LoadLocation(param.TZ)
					ct = time.Now().In(tz).String()
					va = ct
				}
			}

			// Set operation
			if i == 0 {
				upd = expression.Set(expression.Name(col.Name), expression.Value(va))
			} else {
				upd = upd.Set(expression.Name(col.Name), expression.Value(va))
			}
		}
	}

	// add condition expression...

	expr, err = expression.NewBuilder().WithUpdate(upd).Build()
	if err != nil {
		return nil, newDBExprErr("txUpdate", "", "", err)
	}

	exprNames := expr.Names()
	exprValues := expr.Values()

	av := make(map[string]types.AttributeValue)
	// generate key AV
	for _, v := range m.GetKeys() {
		if v.IsPartitionKey() && v.GetOprStr() != "EQ" {
			return nil, newDBExprErr("txUpdate", "", "", fmt.Errorf(`Equality operator for Dynamodb Partition Key must be "EQ"`))
		}
		av[v.Name] = marshalAvUsingValue(v.Value)
	}
	var update *types.Update
	// Where expression defined
	if len(m.GetWhere()) > 0 {

		exprCond, binds := buildConditionExpr(m.GetWhere(), exprNames, exprValues)

		if binds != len(m.GetValues()) {
			return nil, fmt.Errorf("expected %d bind variables in Values, got %d", binds, len(m.GetValues()))
		}

		for i, v := range m.GetValues() {
			ii := i + 1
			exprValues[":"+strconv.Itoa(ii)] = marshalAvUsingValue(v)
		}
		fmt.Println("xxexprCond: ", exprCond, exprNames, exprValues)
		update = &types.Update{
			Key:                       av,
			ExpressionAttributeNames:  exprNames,
			ExpressionAttributeValues: convertBS2List(exprValues, exprNames),
			UpdateExpression:          expr.Update(),
			ConditionExpression:       aws.String(exprCond),
			TableName:                 aws.String(m.GetTable()),
		}

	} else {
		update = &types.Update{
			Key:                       av,
			ExpressionAttributeNames:  exprNames,
			ExpressionAttributeValues: convertBS2List(exprValues, exprNames),
			UpdateExpression:          expr.Update(),
			TableName:                 aws.String(m.GetTable()),
		}
	}

	twi := &types.TransactWriteItem{Update: update}
	return twi, nil

}

func txPut(m *mut.Mutation) (*types.TransactWriteItem, error) {

	av, err := marshalMutation(m)
	if err != nil {
		return nil, err
	}
	put := &types.Put{
		Item:      av,
		TableName: aws.String(m.GetTable()),
	}
	//
	twi := &types.TransactWriteItem{Put: put}

	return twi, nil

}

func crTx(m *mut.Mutation, opr mut.StdMut) ([]types.TransactWriteItem, error) {

	switch opr {

	case mut.Update: //, mut.Append:

		upd, err := txUpdate(m)
		if err != nil {
			return nil, err
		}
		return []types.TransactWriteItem{*upd}, nil

	case mut.Insert:

		put, err := txPut(m)
		if err != nil {
			return nil, err
		}
		return []types.TransactWriteItem{*put}, nil

	case mut.Merge:
		// use in Dynamodb only when list_append or mut.Add on an attribute is required, otherwise a PutItem will implement the merge as a single api call.
		// put-item has no list_append operation as its avaialble only as an update-expression. However, the update-expression requires
		// that the list-append attribute exists (it will not create it). Only the put-item will create the attribute, hence the merge is used for
		// all list-append operations.
		// in the case of spanner, a merge is required to implement the functionality of the no-sql put-item. An update will get a NoDataFoundErr if item doesn't exist.
		// in the case of dynamodb, an update will also put a record if Key values do not already exist in table ie. it will never get a query.NoDataFoundErr.

		upd, err := txUpdate(m)
		if err != nil && !errors.Is(err, query.NoDataFoundErr) {
			return nil, err
		}

		put, err := txPut(m)
		if err != nil {
			return nil, err
		}
		// attr (list type for appendList operation) does not exist - must be explicitly put

		return []types.TransactWriteItem{*upd, *put}, nil

	default:
		panic(fmt.Errorf("cannot mix a %q mutation with normal transaction based mutations ie. insert/update/remove/merge. Change to insert/delete or remove from current transaction.", opr))
		return nil, nil
	}

}

// execBatchMutations: note, all other mutations in this transaction must be either bulkinsert or bulkdelete.
// cannot mix with non-bulk requests ie. insert/update/delete/merge/remove
// NB: batch cannot make use of condition expressions
func execBatchMutations(ctx context.Context, client *dynamodb.Client, bi mut.Mutations, tag string, cfg aws.Config) error {

	//type BatchWriteItemInput struct {
	//             RequestItems map[string][]*types.WriteRequest
	//             .. }
	//type types.WriteRequest {
	//            PutRequest *PutRequest
	//            DeleteRequest *DeleteRequest
	//                  }
	//type PutRequest {
	//             Item map[string]types.AttributeValue
	//
	var (
		//curTbl string
		t0, t1         time.Time
		wrtreq         types.WriteRequest
		api            stats.Source
		unProcRetryCnt int
	)

	muts := func(ri map[string][]types.WriteRequest) int {
		muts := 0
		// sum all writerequests across all tables
		for _, v := range ri {
			muts += len(v)
		}
		return muts
	}

	//	var wrs []*types.WriteRequest

	reqi := make(map[string][]types.WriteRequest)
	req := 0

	// bundle mutations into a batch of RequestItems
	for _, m := range bi {

		m := m.(*mut.Mutation)
		av, err := marshalMutation(m)
		if err != nil {
			return newDBSysErr("genBatchInsert", "", err)
		}

		switch m.GetOpr() {
		case mut.Insert:
			wrtreq = types.WriteRequest{PutRequest: &types.PutRequest{Item: av}}
			api = stats.BatchInsert
		case mut.Delete:
			wrtreq = types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: av}}
			api = stats.BatchDelete
		default:
			panic(fmt.Errorf("Found %q amongst BulkInsert/BulkDelete requests. Do not mix bulk mutations with non-bulk requests", m.GetOpr()))
		}

		wrs := reqi[m.GetTable()]
		wrs = append(wrs, wrtreq)
		reqi[m.GetTable()] = wrs
		req++

		if req > param.MaxMutations {
			panic(fmt.Errorf("BulkMutations: exceeds %q items in a batch write. This should not happen as it should be caught in New*/Add operation", param.MaxMutations))
		}
	}

	// execute batch, checking for no-execution errors or unprocessed items
	{
		var (
			operRetryCnt int
			retryErr     error
			out          *dynamodb.BatchWriteItemOutput
			err          error
		)

		for {

			if operRetryCnt == param.MaxOperRetries {
				return newDBSysErr2("execBatchMutations", tag, fmt.Sprintf("Exceed max retries [%d]", param.MaxOperRetries), MaxOperRetries, retryErr)
			}
			t0 = time.Now()
			out, err = client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: reqi, ReturnConsumedCapacity: types.ReturnConsumedCapacityIndexes}) //aws.String("INDEXES")})
			t1 = time.Now()

			// rleErr := &types.RequestLimitExceeded{Message: aws.String("request limit on table GoGraph exceeded...")}
			// httpResp := &http.Response{StatusCode: 400}
			// smResp := &smithyhttp.Response{httpResp}
			// smRespErr := &smithyhttp.ResponseError{Response: smResp, Err: rleErr}
			// awsRespErr := &awshttp.ResponseError{ResponseError: smRespErr, RequestID: "my-request-id"}
			// err = fmt.Errorf("my error : %w", awsRespErr)

			if err != nil {

				if !retryOp(err, tag, cfg) {
					return newDBSysErr2("BatchWriteItem", tag, "Error type prevents retry of operation or max retries exceeded", NonRetryOperErr, err)
				}
				// wait 1 seconds before processing again...
				operRetryCnt++
				retryErr = err
				continue
			}
			retryErr = nil
			stats.SaveBatchStat(api, tag, out.ConsumedCapacity, t1.Sub(t0), muts(reqi))
			break
		}
		// handle unprocessed items

		unProc := muts(out.UnprocessedItems)

		if unProc > 0 {

			delay := 100 // base expotential backoff time (ms)
			curUnProc := len(bi)

			// execute unprocessed items, checking for no-execution errors or unprocessed items in batch
			for {

				log.LogDebug(fmt.Sprintf("BatchWriteItem: %s, tag: %s: Elapsed: %s Unprocessed: %d of %d [retry: %d]", api, tag, t1.Sub(t0).String(), unProc, curUnProc, unProcRetryCnt+1))

				if unProcRetryCnt == param.MaxUnprocRetries {
					nerr := UnprocessedErr{Remaining: muts(out.UnprocessedItems), Total: curUnProc, Retries: unProcRetryCnt}
					return newDBSysErr2("BatchWriteItem", tag, fmt.Sprintf("Failed to process all unprocessed batched items after %d retries", unProcRetryCnt), MaxUnprocRetries, nerr)
				}
				if operRetryCnt == param.MaxOperRetries {
					return newDBSysErr2("BatchWriteItem", tag, fmt.Sprintf("Exceed max retries [%d] on operation error ", operRetryCnt), MaxOperRetries, retryErr)
				}
				// retry backoff delay
				time.Sleep(time.Duration(delay) * time.Millisecond)
				//
				t0 = time.Now()
				out, err = client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: out.UnprocessedItems, ReturnConsumedCapacity: types.ReturnConsumedCapacityIndexes}) //aws.String("INDEXES")})
				t1 = time.Now()

				if err != nil {
					retryErr = err
					if !retryOp(err, tag, cfg) {
						return newDBSysErr2("BatchWriteItem", tag, "Error type prevents retry of operation.", NonRetryOperErr, err)
					}
					// wait n seconds before reprocessing...
					time.Sleep(1 * time.Second)
					delay *= 2
					operRetryCnt++
					continue
				}

				stats.SaveBatchStat(api, tag, out.ConsumedCapacity, t1.Sub(t0), muts(out.UnprocessedItems))
				unProc = muts(out.UnprocessedItems)
				if unProc == 0 {
					break
				}
				// some items still remain to be processed. Retry...
				curUnProc = unProc
				unProcRetryCnt++
				// increase backoff delay
				delay *= 2
			}
		}
		log.LogDebug(fmt.Sprintf("%s : Batch processed all items [tag: %s]. Mutations %d  Elapsed: %s", api, tag, len(bi), t1.Sub(t0).String()))
	}

	// log 30% of activity
	dur := t1.Sub(t0).String()
	if dot := strings.Index(dur, "."); dur[dot+2] == 57 && (dur[dot+3] == 54 || dur[dot+3] == 55) {
		log.LogDebug(fmt.Sprintf("Bulk Insert [tag: %s]: mutations %d  Unprocessed Retries: %d  Elapsed: %s", tag, len(bi), unProcRetryCnt, dur))
	}

	return nil
}

func execBatch(ctx context.Context, client *dynamodb.Client, bs []*mut.Mutations, tag string, cfg aws.Config) error {
	// merge transaction

	// generate statements for each mutation
	for _, b := range bs {

		for _, m := range *b { // mut.Mutations
			_, ok := m.(*mut.Mutation)
			if !ok {
				return fmt.Errorf("ExecBatch error. Batch contains a non-Mutation type")
			}
		}

		err := execBatchMutations(ctx, client, *b, tag, cfg)
		if err != nil {
			return err
		}
	}
	return nil
}

// execOptim - bit silly. Runs Inserts as a batch and updates as transactional. Maybe illogical. Worth more of a think.
func execOptim(ctx context.Context, client *dynamodb.Client, bs []*mut.Mutations, tag string, cfg aws.Config) error {

	var (
		in  mut.Mutations
		ins []mut.Mutations
	)

	// aggregate put/inserts
	for _, b := range bs {

		for _, m := range *b {

			if m.(*mut.Mutation).GetOpr() == mut.Insert {
				in = append(in, m)
				if len(in) == param.MaxMutations {
					ins = append(ins, in)
					in = nil
				}
			}
		}
	}
	if len(in) > 0 {
		ins = append(ins, in)
	}

	// processes inserts
	for _, v := range ins {
		err := execBatchMutations(ctx, client, v, tag, cfg)
		if err != nil {
			return err
		}
	}

	// process updates/deletes
	return execTransaction(ctx, client, bs, tag, db.OptimAPI)

}

// mut.Mutations = []dbs.Mutation
type txWrite struct {
	// upto to out transactionWriteItems created when a merge is specified
	txwii [2]*dynamodb.TransactWriteItemsInput
	merge bool
}

func execTransaction(ctx context.Context, client *dynamodb.Client, bs []*mut.Mutations, tag string, api db.API) error {

	// handle as a Transaction

	// txwio, err := TransactWriteItems(TransactWriteItemsInput)
	//
	// TransactWriteItemsInput {
	//       ClientRequestToken *string
	//   	 TransactItems []*TransactWriteItem
	//
	// TransactWriteItem {
	//        ConditionCheck *ConditionCheck
	//        Update         *Update ,
	//		  Put            *Put,
	//        Delete         *Delete,
	//
	//  Put {
	//      ConditionExpression ,
	//      ExpressionAttributeNames  map[string]*string
	//      ExpressionAttributeValues map[string]types.AttributeValue
	//      Item                      map[string]types.AttributeValue,
	//      TableName *string
	//
	// generate mutation as Dynamodb Put or Update.
	// note: if opr is merge will generate both, a Put and Update
	var (
		tx txWrite
		//
		btx       []txWrite
		twi, twi2 []types.TransactWriteItem
	)
	// merge transaction
	const (
		merge1 = 0
		merge2 = 1
	)
	var merge bool
	// generate statements for each mutation
	for _, b := range bs {

		merge = false
		for _, m := range *b { // mut.Mutations

			y, ok := m.(*mut.Mutation)
			if ok {

				if api == db.OptimAPI && y.GetOpr() == mut.Insert {
					continue
				}
				txwis, err := crTx(y, y.GetOpr())
				if err != nil {
					return err
				}
				// accumulate into two TransactItems streams, stream 1 (twi) contains non-merge & merge (first op), stream 2 (twi2) has same non-merge but contains merge (second op)
				// if stream 1 generates condition error in case of merge stmt, stream 2 is executed
				// first stmt stream
				twi = append(twi, txwis[0])
				// second stmt stream (if a merge used in any mutation)
				switch len(txwis) {
				case 1:
					twi2 = append(twi2, txwis[0])
				case 2:
					twi2 = append(twi2, txwis[1])
					merge = true
				}

			} else {

				// TODO: GetStatement() feature. Implement - if wanted
			}
		}

		tx = txWrite{txwii: [2]*dynamodb.TransactWriteItemsInput{&dynamodb.TransactWriteItemsInput{TransactItems: twi, ReturnConsumedCapacity: types.ReturnConsumedCapacityIndexes}, &dynamodb.TransactWriteItemsInput{TransactItems: twi2, ReturnConsumedCapacity: types.ReturnConsumedCapacityIndexes}}}
		tx.merge = merge
		// add transaction to the batch transactions
		btx = append(btx, tx)
		//	// reset for next batch of mutations
		twi2 = nil
		twi = nil

	}

	var (
		t0, t1 time.Time
	)

	switch api {

	case db.TransactionAPI:

		for _, tx := range btx {

			t0 = time.Now()
			out, err := client.TransactWriteItems(ctx, tx.txwii[0])
			t1 = time.Now()
			if err != nil {

				if tag == "Target UPred" { // TODO: ??? remove
					panic(err)
				}

				tce := &types.TransactionCanceledException{}

				if errors.As(err, &tce) {

					for _, e := range tce.CancellationReasons {

						switch *e.Code {

						case "None":

						case "ValidationError":

							// previous way of detecting merge alternate op required. Deprecated with conditional-put.
							// switch *e.Message {
							// case "The provided expression refers to an attribute that does not exist in the item":

							return fmt.Errorf("Transaction Validaation error [tag: %s] %w. Item: %#v", tag, errors.New(*e.Message), e.Item)

						case "ConditionalCheckFailed":

							//  triggered by update with "attribute_exists(PKEY)" condition expression. Update will fail if PKEY attribute does not exist in db.
							// second transaction stream (idx 1) contains insert/put operation, as the second part of a merge operation.
							t0 = time.Now()
							out, err := client.TransactWriteItems(ctx, tx.txwii[1])
							t1 = time.Now()
							if err != nil {
								return newDBSysErr2("TransactionAPI (merge part 2)", tag, "", "TxMergeP2", err)
							}
							stats.SaveTransactStat(tag, out.ConsumedCapacity, t1.Sub(t0), len(tx.txwii[1].TransactItems))
							// return nil
							dur := t1.Sub(t0).String()
							if dot := strings.Index(dur, "."); dur[dot+2] == 57 && (dur[dot+3] == 54) { //|| dur[dot+3] == 55) {
								log.LogDebug(fmt.Sprintf("TransactionAPI (merge part2): mutations %d  Elapsed: %s ", len(tx.txwii[1].TransactItems), dur))
							}

						default:

							return newDBSysErr2("TransactionAPI (merge part 2)", tag, "", "TxMergeP2", err)

						}
					}

				} else {

					log.LogDebug(fmt.Sprintf("Transaction error: %s. %#v\n", err, tx.txwii[0]))

					return newDBSysErr2("TransactionAPI", tag, "Not a TransactionCanceledException error in TransactWriteItems", "", err)

				}

			} else {
				// no error, save statistics
				stats.SaveTransactStat(tag, out.ConsumedCapacity, t1.Sub(t0), len(tx.txwii[0].TransactItems))

				dur := t1.Sub(t0).String()
				if dot := strings.Index(dur, "."); dur[dot+2] == 57 && (dur[dot+3] == 54) { //|| dur[dot+3] == 55) {
					log.LogDebug(fmt.Sprintf("TransactionAPI: mutations %d  Elapsed: %s ", len(tx.txwii[0].TransactItems), dur))
				}

			}
		}

	case db.StdAPI, db.OptimAPI:

		for _, tx := range btx {

			t0 = time.Now()
			idx := 0
			for i := 0; i < 2; i++ {
				var mergeContinue bool

				//fmt.Println("loop i,idx,len(tx.txwii[i].TransactItems[idx:]) ", i, idx, len(tx.txwii[i].TransactItems[idx:]), mergeContinue)
				for j, op := range tx.txwii[i].TransactItems[idx:] {

					if op.Update != nil {

						uii := &dynamodb.UpdateItemInput{
							Key:                       op.Update.Key,
							ExpressionAttributeNames:  op.Update.ExpressionAttributeNames,
							ExpressionAttributeValues: op.Update.ExpressionAttributeValues,
							UpdateExpression:          op.Update.UpdateExpression,
							ConditionExpression:       op.Update.ConditionExpression,
							TableName:                 op.Update.TableName,
							ReturnConsumedCapacity:    types.ReturnConsumedCapacityIndexes,
						}

						t0 := time.Now()
						uio, err := client.UpdateItem(ctx, uii)
						t1 := time.Now()
						if err != nil {

							//fmt.Printf("UpdateItem error: %s\n", err)

							tce := &types.TransactionCanceledException{}

							if errors.As(err, &tce) {

								for _, e := range tce.CancellationReasons {

									if *e.Code == "ConditionalCheckFailed" {

										// insert triggered by update with "attribute_exists(PKEY)" condition expression.
										// second transaction stream (i=1) contains insert operations, as the second part of a "merge" operation.
										idx += j
										mergeContinue = true
										// uio.ConsumedCapacity is nil for condition failures
										cc := &types.ConsumedCapacity{TableName: op.Update.TableName}
										stats.SaveStdStat(stats.UpdateItemCF, tag, cc, t1.Sub(t0))
										break
									}
								}
								if !mergeContinue {
									return newDBSysErr2("Standard API", tag, "UpdateItem - TransactionCanceledException", "", err)
								}

							} else {

								return newDBSysErr2("Standard API", tag, "UpdateItem Error", "", err)

							}

						} else {

							stats.SaveStdStat(stats.UpdateItem, tag, uio.ConsumedCapacity, t1.Sub(t0))
						}
					}

					if op.Put != nil {

						pii := &dynamodb.PutItemInput{
							Item:                   op.Put.Item,
							TableName:              op.Put.TableName,
							ReturnConsumedCapacity: types.ReturnConsumedCapacityIndexes,
						}

						t0 := time.Now()
						uio, err := client.PutItem(ctx, pii)
						t1 := time.Now()
						if err != nil {

							tce := &types.TransactionCanceledException{}

							if errors.As(err, &tce) {

								return newDBSysErr2("Execute std api", tag, "PutItem - TransactionCanceledException", "", err)

							} else {

								return newDBSysErr2("Execute std api", tag, "PutItem Error", "", err)
							}
						} else {
							//log.LogDebug(fmt.Sprintf("Execute std api:consumed capacity for PutItem %s.  Duration: %s", uio.ConsumedCapacity, t1.Sub(t0)))
							stats.SaveStdStat(stats.PutItem, tag, uio.ConsumedCapacity, t1.Sub(t0))
						}
					}

					if op.Delete != nil {

						dii := &dynamodb.DeleteItemInput{
							Key:                       op.Delete.Key,
							ExpressionAttributeNames:  op.Delete.ExpressionAttributeNames,
							ExpressionAttributeValues: op.Delete.ExpressionAttributeValues,
							ConditionExpression:       op.Delete.ConditionExpression,
							TableName:                 op.Delete.TableName,
							ReturnConsumedCapacity:    types.ReturnConsumedCapacityIndexes,
						}

						t0 := time.Now()
						uio, err := client.DeleteItem(ctx, dii)
						t1 := time.Now()
						if err != nil {

							tce := &types.TransactionCanceledException{}

							if errors.As(err, &tce) {

								return newDBSysErr2("Execute std api", tag, "DeleteItem - TransactionCanceledException", "", err)

							} else {

								return newDBSysErr2("Execute std api", tag, "DeleteItem", "", err)
							}
						} else {

							stats.SaveStdStat(stats.DeleteItem, tag, uio.ConsumedCapacity, t1.Sub(t0))
						}
					}
				} // for

				if !mergeContinue {
					break
				}

			} // for
		}
	}
	return nil
}

// ////////  QUERY  /////////  QUERY  ///////  QUERY  /////////  QUERY  ///////  QUERY  ////////////  QUERY  /////////  QUERY  ///////////
type scanMode byte

const (
	parallel scanMode = iota
	nonParallel
)

func genKeyAV(q *query.QueryHandle) (map[string]types.AttributeValue, error) {

	// TODO: check against Dynamodb table key definition.
	var (
		err error
		av  map[string]types.AttributeValue
	)

	av = make(map[string]types.AttributeValue)

	// generate key AV
	for _, v := range q.GetAttr() {
		if v.IsKey() {
			av[v.Name()] = marshalAvUsingValue(v.Value())
			if err != nil {
				return nil, err
			}
		}
	}

	return av, nil
}

func crProjectionExpr(q *query.QueryHandle) *expression.ProjectionBuilder {

	// generate projection

	var first = true
	var proj expression.ProjectionBuilder

	for _, v := range q.GetAttr() {
		if v.IsFetch() {
			if first {
				proj = expression.NamesList(expression.Name(v.Name()))
				first = false
			} else {
				proj = proj.AddNames(expression.Name(v.Name()))
			}
		}
	}
	if !first {
		// projection expr created
		return &proj
	}
	return nil

}

func executeQuery(ctx context.Context, dh *DynamodbHandle, q *query.QueryHandle, opt ...db.Option) error {

	if q.Error() != nil {
		return q.Error()
	}

	e, err := queryCache.fetchQuery(ctx, dh, q)
	if err != nil {
		return err
	}
	// options
	for _, o := range opt {
		switch strings.ToLower(o.Name) {
		default:
		}
	}
	// define projection based on struct passed via Select()
	proj := crProjectionExpr(q)
	//
	av, err := genKeyAV(q)
	if err != nil {
		return err
	}

	switch e.access {

	case cGetItem:
		if q.Paginated() {
			return fmt.Errorf("Query is a single item query, remove Parginate() method")
		}
		if q.Worker() > 0 {
			return fmt.Errorf("Query is not a scan, remove Worker() method")
		}
		if q.ExecMode() == q.Func() || q.ExecMode() == q.Channel() {
			return fmt.Errorf("Query is a single item query, use Execute() method as its much more efficient")
		}
		return exGetItem(ctx, dh, q, e, av, proj)
	case cQuery:
		if q.Worker() > 0 {
			return fmt.Errorf("Remove Worker() method. Cannot be assigned to non-scan operations.")
		}
		return exQuery(ctx, dh, q, e, proj)
	case cScan:
		// check if scan has been disabled at either the db or stmt level
		if DbScan == db.Disabled {
			if qscan := q.GetConfig("scan"); qscan != nil {
				if qscan.(db.State) == db.Enabled {
					return exScan(ctx, dh, q, proj)
				}
			}
			return fmt.Errorf("Scan operation is disabled")
		}
		return exScan(ctx, dh, q, proj)
	}

	return fmt.Errorf("Inconsistency in db executeQuery()")

}

// func exGetItem(q *query.QueryHandle, av []types.AttributeValue) error {
func exGetItem(ctx context.Context, client *DynamodbHandle, q *query.QueryHandle, e *qryEntry, av map[string]types.AttributeValue, proj *expression.ProjectionBuilder) error {

	if proj == nil {
		return fmt.Errorf("Select must be specified in GetItem")
	}
	expr, err := expression.NewBuilder().WithProjection(*proj).Build()
	if err != nil {
		return newDBExprErr("exGetItem", "", "", err)
	}

	input := &dynamodb.GetItemInput{
		Key:                      av,
		ProjectionExpression:     expr.Projection(),
		ExpressionAttributeNames: expr.Names(),
		TableName:                aws.String(string(q.GetTable())),
		ReturnConsumedCapacity:   types.ReturnConsumedCapacityIndexes,
		ConsistentRead:           aws.Bool(q.ConsistentMode()),
	}
	//
	//log.LogDebug(fmt.Sprintf("GetItem: %#v\n", input))
	t0 := time.Now()
	result, err := client.GetItem(ctx, input)
	t1 := time.Now()
	if err != nil {
		return newDBSysErr("exGetItem", "GetItem", err)
	}
	dur := t1.Sub(t0)
	dur_ := dur.String()
	if dot := strings.Index(dur_, "."); dur_[dot+2] == 57 {
		log.LogDebug(fmt.Sprintf("exGetItem:consumed capacity for GetItem  %s. Duration: %s", ConsumedCapacity_{result.ConsumedCapacity}.String(), dur_))
	}
	//
	if len(result.Item) == 0 {
		// var (
		// 	s string
		// 	u []byte
		// )
		// for _, v := range av {
		// 	switch x := v.(type) {
		// 	case *types.AttributeValueMemberS:
		// 		s = x.Value
		// 	case *types.AttributeValueMemberB:
		// 		u = x.Value
		// 	}
		// }
		return query.NoDataFoundErr
	}
	err = attributevalue.UnmarshalMap(result.Item, q.GetBind())
	if err != nil {
		return newDBUnmarshalErr("xGetItem", "", "", "UnmarshalMap", err)
	}
	// save query statistics
	stats.SaveQueryStat(stats.GetItem, q.Tag, result.ConsumedCapacity, 1, 0, dur)

	return nil
}

func exQuery(ctx context.Context, client *DynamodbHandle, q *query.QueryHandle, e *qryEntry, proj *expression.ProjectionBuilder) error {
	var err error

	bindvars := len(q.Binds()) // bind vars in Select()

	switch x := q.ExecMode(); x {

	case q.Std():

		if bindvars != 1 {
			panic(fmt.Errorf("Query is not paginated. Specify one bind variable only in Select()"))
		}

		return exQueryStd(ctx, client, q, e, proj)

	default:

		// must be a paginated query as ExecteByChannel() or ExecuteByFunc() is used.
		// all q.Func() will have been configured with 1 worker if no Worker specified.

		if bindvars < 2 {
			panic(fmt.Errorf("Using channels, please specifiy multiple bind variables in Select()"))
		}

		// create wrks number of bind vars (double buf)
		// q.bind == *[]rec
		r := reflect.ValueOf(q.GetBind()).Elem()

		// create channel
		chT := reflect.ChanOf(reflect.BothDir, r.Type()) // Type
		ch := reflect.MakeChan(chT, 0)                   // always unbuffered so we known when receiver has consumed a page/buffer.
		q.SetChannel(ch.Interface())

		// start worker scan service
		go queryChannelSrv(ctx, q, ch, client, e, proj)

		if x == q.Func() {
			// place func on receive end of channel:  func(a interface{}) error, where a is channel created above
			// blocking call
			err = q.GetFunc()(ch.Interface())

		} else {

			// assign slice of channels to original queryHandler
			q.SetChannel(ch.Interface())
		}
	}
	return err
}

func queryChannelSrv(ctx context.Context, q *query.QueryHandle, chv reflect.Value, client *DynamodbHandle, e *qryEntry, proj *expression.ProjectionBuilder) {

	var err error
	var first bool = true

	log.LogAlert(fmt.Sprintf("queryChannelSrv exQuery, scanChannelSrv: Tag [%s]", q.Tag))

	for !q.EOD() {

		if q.IsRestart() {
			first = false
		}

		err = exQueryStd(ctx, client, q, e, proj)

		if err != nil {
			if errors.Is(query.NoDataFoundErr, err) { //TODO: this is never returned for paginated query???
				log.LogAlert("queryChannelSrv no data found..")
				q.SetEOD()
				continue
			}
			log.LogErr(fmt.Errorf("queryChannelSrv: %w", err))
		}
		// check for ctrl-C
		select {
		case <-ctx.Done():
			break
		default:
		}

		//chv.Send(reflect.ValueOf(q.Binds()).Index(q.GetWriteBufIdx()).Elem().Elem())
		chv.Send(reflect.ValueOf(q.GetBind()).Elem())

		// unblocked, receiver has consumed a page, save state (ignore on first loop)
		if q.Paginated() && !first {
			err := savePgState(ctx, client, q.PgStateId(), q.PopPgStateValS())
			if err != nil {
				log.LogErr(fmt.Errorf("savePgState: %w", err))
				q.SetEOD()
				continue
			}
		}
		first = false
	}

	chv.Close()

}

func exQueryStd(ctx context.Context, client *DynamodbHandle, q *query.QueryHandle, e *qryEntry, proj *expression.ProjectionBuilder) error {
	//
	var (
		keyc   expression.KeyConditionBuilder
		flt, f expression.ConditionBuilder

		input *dynamodb.QueryInput
		err   error

		exprProj    *string
		exprKeyCond *string
		exprNames   map[string]string
		exprValues  map[string]types.AttributeValue
		exprFilter  *string
	)

	//log.LogDebug("exQueryStd", fmt.Sprintf("exQuery: Tag [%s]", q.Tag))
	exprNames = make(map[string]string)

	// check bind variable is a slice
	if !q.IsBindVarASlice() {
		return fmt.Errorf("Bind variable in Select() must be slice for a query database operation")
	}

	// pagination
	if q.IsRestart() {
		// read StateVal from table using q.GetStartVal
		// parse contents into map[string]types.AttributeValue
		log.LogAlert("exQueryStd restart: retrieve pg state...")
		pg, err := getPgState(ctx, client, q.PgStateId())
		if err != nil {
			return err
		}
		q.AddPgStateValS(pg)
		q.SetPgStateValI(unmarshalPgState(pg))
		q.SetRestart(false)
	}
	//
	keyAttrs := q.GetKeys()

	switch len(q.GetKeys()) {
	case 1:
		if keyAttrs[0] != e.GetPK() {
			panic(fmt.Errorf(fmt.Sprintf("Expected Partition Key %q got %q", e.GetPK(), keyAttrs[0])))
		}
		// always EQ
		keyc = expression.KeyEqual(expression.Key(e.GetPK()), expression.Value(q.GetKeyValue(e.GetPK())))
	case 2:
		// TODO: this check of keyAttrs is redundant??
		for _, v := range keyAttrs {
			if v != e.GetPK() {
				if v != e.GetSK() {
					return (fmt.Errorf(fmt.Sprintf("Expected key attribute to be one of (%q %q) got %q", e.GetPK(), e.GetSK(), v)))
				}
			}
		}
		//
		keyc = expression.KeyEqual(expression.Key(e.GetPK()), expression.Value(q.GetKeyValue(e.GetPK())))
		switch ComparOpr(q.GetKeyComparOpr(e.GetSK())) {
		case EQ:
			keyc = expression.KeyAnd(keyc, expression.KeyEqual(expression.Key(e.GetSK()), expression.Value(q.GetKeyValue(e.GetSK()))))
		case GT:
			keyc = expression.KeyAnd(keyc, expression.KeyGreaterThan(expression.Key(e.GetSK()), expression.Value(q.GetKeyValue(e.GetSK()))))
		case GE:
			keyc = expression.KeyAnd(keyc, expression.KeyGreaterThanEqual(expression.Key(e.GetSK()), expression.Value(q.GetKeyValue(e.GetSK()))))
		case LT:
			keyc = expression.KeyAnd(keyc, expression.KeyLessThan(expression.Key(e.GetSK()), expression.Value(q.GetKeyValue(e.GetSK()))))
		case LE:
			keyc = expression.KeyAnd(keyc, expression.KeyLessThanEqual(expression.Key(e.GetSK()), expression.Value(q.GetKeyValue(e.GetSK()))))
		case BEGINSWITH:
			keyc = expression.KeyAnd(keyc, expression.KeyBeginsWith(expression.Key(e.GetSK()), q.GetKeyValue(e.GetSK()).(string)))
		case BETWEEN:
			arg := q.GetKeyValue(e.GetSK()).([2]interface{})
			keyc = expression.KeyAnd(keyc, expression.KeyBetween(expression.Key(e.GetSK()), expression.Value(arg[0]), expression.Value(arg[1])))
		default:
			panic(fmt.Errorf(fmt.Sprintf("Key operator %q not supported", keyAttrs[1])))

		}
	default:
		panic(fmt.Errorf(fmt.Sprintf("No more than two Key %q", len(q.GetKeys()))))
	}

	// filter defined
	for i, n := range q.GetFilterAttrs() {

		log.LogDebug(fmt.Sprintf("exQueryStd Filter : %#v\n", n.Name()))

		if q.GetOr() > 0 && q.GetAnd() > 0 {
			panic(fmt.Errorf("Cannot mix OrFilter, AndFilter conditions. Use Where() & Values() instead"))
		}
		if i == 0 {
			switch ComparOpr(n.GetOprStr()) {
			case BETWEEN:
				arg := q.GetKeyValue(e.GetSK()).([2]interface{})
				flt = expression.Between(expression.Name(n.Name()), expression.Value(arg[0]), expression.Value(arg[1]))
			case BEGINSWITH:
				flt = expression.BeginsWith(expression.Name(n.Name()), n.Value().(string))
			case GT:
				flt = expression.GreaterThan(expression.Name(n.Name()), expression.Value(n.Value()))
			case GE:
				flt = expression.GreaterThanEqual(expression.Name(n.Name()), expression.Value(n.Value()))
			case LT:
				flt = expression.LessThan(expression.Name(n.Name()), expression.Value(n.Value()))
			case LE:
				flt = expression.LessThanEqual(expression.Name(n.Name()), expression.Value(n.Value()))
			case EQ:
				flt = expression.Equal(expression.Name(n.Name()), expression.Value(n.Value()))
			case NE:
				flt = expression.NotEqual(expression.Name(n.Name()), expression.Value(n.Value()))
			default:
				panic(fmt.Errorf(fmt.Sprintf("Comparitor %q not supported", ComparOpr(n.GetOprStr()))))
			}

		} else {

			switch ComparOpr(n.GetOprStr()) {
			case BETWEEN:
				arg := q.GetKeyValue(e.GetSK()).([2]interface{})
				f = expression.Between(expression.Name(n.Name()), expression.Value(arg[0]), expression.Value(arg[1]))
			case BEGINSWITH:
				f = expression.BeginsWith(expression.Name(n.Name()), n.Value().(string))
			case GT:
				f = expression.GreaterThan(expression.Name(n.Name()), expression.Value(n.Value()))
			case GE:
				f = expression.GreaterThanEqual(expression.Name(n.Name()), expression.Value(n.Value()))
			case LT:
				f = expression.LessThan(expression.Name(n.Name()), expression.Value(n.Value()))
			case LE:
				f = expression.LessThanEqual(expression.Name(n.Name()), expression.Value(n.Value()))
			case EQ:
				f = expression.Equal(expression.Name(n.Name()), expression.Value(n.Value()))
			case NE:
				f = expression.NotEqual(expression.Name(n.Name()), expression.Value(n.Value()))
			default:
				panic(fmt.Errorf(fmt.Sprintf("xComparitor %q not supported", ComparOpr(n.GetOprStr()))))
			}
			//flt = flt.And(f)
			switch n.BoolCd() {
			case query.AND:
				flt = flt.And(f)
			case query.OR:
				flt = flt.Or(f)
			}
		}

	}
	// build expression.Expression
	b := expression.NewBuilder().WithKeyCondition(keyc)
	if proj != nil {
		b = b.WithProjection(*proj)
	}
	if q.FilterSpecified() {
		b = b.WithFilter(flt)
	}
	expr, err := b.Build()
	if err != nil {
		return newDBExprErr("exQueryStd", "", "", err)
	}
	exprProj = expr.Projection()
	exprKeyCond = expr.KeyCondition()
	exprNames = expr.Names()
	exprValues = expr.Values()
	exprFilter = expr.Filter()

	// Where expression defined
	if len(q.GetWhere()) > 0 {

		if len(q.GetFilterAttrs()) > 0 {
			panic(fmt.Errorf(fmt.Sprintf("Cannot mix Filter and Where methods.")))
		}

		exprStr, binds := buildFilterExpr(q.GetWhere(), exprNames, exprValues)
		if binds != len(q.GetValues()) {
			panic(fmt.Errorf("expected %d bind variables in Values, got %d", binds, len(q.GetValues())))
		}

		for i, v := range q.GetValues() {
			exprValues[":"+strconv.Itoa(i)] = marshalAvUsingValue(v)
		}

		// build expression.Expression
		b := expression.NewBuilder().WithKeyCondition(keyc)
		if proj != nil {
			b = b.WithProjection(*proj)
		}

		expr, err := b.Build()
		if err != nil {
			return newDBExprErr("exQueryStd", "", "", err)
		}
		// append expression Names and Values
		for k, v := range expr.Names() {
			exprNames[k] = v
		}
		for k, v := range expr.Values() {
			exprValues[k] = v
		}
		var s strings.Builder
		s.WriteString(*exprFilter)
		s.WriteString(" AND (")
		s.WriteString(exprStr)
		s.WriteString(" )")
		exprFilter = aws.String(s.String())
		// define QueryInput

	}

	input = &dynamodb.QueryInput{
		KeyConditionExpression:    exprKeyCond,
		FilterExpression:          exprFilter,
		ExpressionAttributeNames:  exprNames,
		ExpressionAttributeValues: exprValues,
		ProjectionExpression:      exprProj,
		TableName:                 aws.String(string(q.GetTable())),
		ReturnConsumedCapacity:    types.ReturnConsumedCapacityIndexes,
		ConsistentRead:            aws.Bool(q.ConsistentMode()),
	}

	if q.IndexSpecified() {
		input.IndexName = aws.String(string(q.GetIndex()))
		//log.LogDebug(fmt.Sprintf("exQueryStd: index specified: %s", q.GetIndex()))
	}
	if l := q.GetLimit(); l > 0 {
		input.Limit = aws.Int32(int32(l))
	}
	if lk := q.PgStateValI(); lk != nil {
		input.ExclusiveStartKey = lk.(map[string]types.AttributeValue)
	}

	input.ScanIndexForward = aws.Bool(q.IsScanForwardSet())
	//
	t0 := time.Now()
	result, err := client.Query(ctx, input)
	t1 := time.Now()
	if err != nil {
		return newDBSysErr("exQueryStd", "Query", err)
	}
	// pagination cont....
	if lek := result.LastEvaluatedKey; len(lek) == 0 {

		// LastEvaluatedKey is definitive EOD - not zero rows returned in paginated query as a filter can eliminate all items.
		if q.PaginatedQuery() {
			q.AddPgStateValS("")
			q.SetPgStateValI(nil)
			q.SetEOD()
			if q.PgStateValI() != nil {
				err := deletePgState(ctx, client, q.PgStateId())
				if err != nil {
					log.LogErr(fmt.Errorf("deletePgState: %w", err))
				}
			}
		}

	} else {

		// if not a paginated query - throw error
		if q.PaginatedQuery() {
			q.AddPgStateValS(stringifyPgState(result.LastEvaluatedKey))
			q.SetPgStateValI(result.LastEvaluatedKey)
		}
	}

	dur := t1.Sub(t0)
	dur_ := dur.String()
	if dot := strings.Index(dur_, "."); dur_[dot+2] == 57 {
		log.LogDebug(fmt.Sprintf("exQueryStd:consumed capacity for Query  %s. ItemCount %d  Duration: %s", ConsumedCapacity_{result.ConsumedCapacity}.String(), result.Count, dur_))
	}
	//
	if result.Count == 0 {
		if !q.PaginatedQuery() {
			//	q.SetEOD() // TODO: could this be removed, as non-paginated query does not use EOD, it uses query.NoDataFoundErr
			return query.NoDataFoundErr
		} else {
			// TODO: should bind be emptied??
			if reflect.ValueOf(q.GetBind()).Elem().Kind() == reflect.Slice {

				if reflect.ValueOf(q.GetBind()).Elem().Len() > 0 {
					log.LogAlert(fmt.Sprintf("exStdQuery Zero out bind variable "))
					t := reflect.ValueOf(q.GetBind()).Type().Elem()
					n := reflect.New(t) // *slice to empty slice
					q.SetBindValue(n.Elem())
				}
			}
			stats.SaveQueryStat(stats.Query, q.Tag, result.ConsumedCapacity, result.Count, result.ScannedCount, dur)
			return nil
		}
	}

	err = attributevalue.UnmarshalListOfMaps(result.Items, q.GetBind())
	if err != nil {
		return newDBUnmarshalErr("exQuery", "", "", "UnmarshalListOfMaps", err)
	}

	// save query statistics
	stats.SaveQueryStat(stats.Query, q.Tag, result.ConsumedCapacity, result.Count, result.ScannedCount, dur)

	return nil
}

func exScan(ctx context.Context, client *DynamodbHandle, q *query.QueryHandle, proj *expression.ProjectionBuilder) error {
	var err error

	switch wrks := q.NumWorkers(); wrks {

	case 0:

		switch q.ExecMode() {

		case q.Channel():

			bindvars := len(q.Binds()) // GetSelect()
			if bindvars < 2 {
				panic(fmt.Errorf("Using channels, please specifiy multiple bind variables in Select()"))
			}
			// ExecuteByChannel()
			r := reflect.ValueOf(q.GetBind()).Elem() // *[]unprocBuf
			// fmt.Println("q.Bind() : ", reflect.ValueOf(q.Bind()).Elem().Kind())
			// fmt.Println("Make chan of ", r.Type().Kind(), reflect.TypeOf(r.Interface()).Kind())
			ch := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, r.Type()), bindvars-2)
			//fmt.Println("chv: ", chv.Kind())
			q.SetChannel(ch.Interface())

			// start scan service
			go scanChannelSrv(ctx, nonParallel, q, ch, client, proj)

		default:

			// normal Execute()
			return exNonParallelScan(ctx, client, q, proj)
		}

	default:

		// all q.Func() will have been configured with 1 worker if no Worker specified.

		if x := q.ExecMode(); x == q.Channel() || x == q.Func() {

			bindvars := len(q.Binds()) // GetSelect()

			if bindvars < 2 {
				panic(fmt.Errorf("Using channels, please specifiy multiple bind variables in Select()"))
			}

			// create wrks number of bind vars (double buf)
			// q.bind == *[]rec
			r := reflect.ValueOf(q.GetBind()).Elem()

			// create slice of channels
			chT := reflect.ChanOf(reflect.BothDir, r.Type()) // Type
			sT := reflect.SliceOf(chT)                       // Type
			chs := reflect.New(sT)
			ichs := reflect.Indirect(chs)

			// create #wrks clones of QueryHandle and assign bind variables and channel to it.
			// Each cloned QueryHandle will be assoicated with one parallel scan worker
			for i := 0; i < wrks; i++ {

				cq := q.Clone()

				//ch := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, r.Type()), 1)
				ch := reflect.MakeChan(chT, 0)
				cq.SetChannel(ch.Interface())
				ichs = reflect.Append(ichs, ch)
				// set bind vars
				var sel []interface{}

				// assign existing or create new bind vars and assign to cloned QueryHandle
				if i == 0 {
					// use bind vars from query method Select() for first clone
					cq.Select(q.Binds()...)

				} else {

					// create bind vars using New() for all other clones except first - based on
					for n := 0; n < len(q.Binds()); n++ {
						bv := reflect.New(r.Type())
						sel = append(sel, bv.Interface())
					}
					// assign bind vars to cloned queryHandler - each will be double buffered if specifed in original Select()
					// TODO: check multiple bind vars specified in Select when paginate or workers specified.
					cq.Select(sel...)
				}
				// configure worker details
				cq.SetTag(q.Tag + "-w" + strconv.Itoa(i))
				cq.SetWorker(i)

				// start worker scan service
				go scanChannelSrv(ctx, parallel, cq, ch, client, proj)
			}

			if x == q.Func() {
				// place func on receive end of channel:  func(a interface{}) error, where a is channel created above
				// blocking call
				var wg sync.WaitGroup
				wg.Add(wrks)
				for i := 0; i < wrks; i++ {
					go exWorker(&wg, q, ichs.Index(i).Interface())
				}
				wg.Wait()

			} else {

				// assign slice of channels to original queryHandler
				q.SetChannel(ichs.Interface())
			}

		} else {

			log.LogErr(fmt.Errorf("exScan: Not supported. Use ExecuteWithChannel() instead"))
		}

	}

	return err
}

// exWorker, a blocking call to scan worker function
func exWorker(wg *sync.WaitGroup, q *query.QueryHandle, ch interface{}) {
	err := q.GetFunc()(ch)
	if err != nil {
		log.LogErr(fmt.Errorf("workerFunc: %w", err))
	}
	wg.Done()
}

// scanChannelSrv is a goroutine (service) created for each worker (on a table partition/segment)
// there is one scanChannelSrv
func scanChannelSrv(ctx context.Context, scan scanMode, q *query.QueryHandle, chv reflect.Value, client *DynamodbHandle, proj *expression.ProjectionBuilder) {

	var err error

	//log.LogAlert("scanChannelSrv", fmt.Sprintf("exQuery: Tag [%s]", q.Tag))

	for !q.EOD() {

		fmt.Println("BD EOD...")
		switch scan {
		case nonParallel:
			err = exNonParallelScan(ctx, client, q, proj)
		case parallel:
			err = exScanWorker(ctx, client, q, proj)
		}

		if err != nil {
			if errors.Is(query.NoDataFoundErr, err) {
				q.SetEOD()
				continue
			}
			log.LogErr(fmt.Errorf("scanChannelSrv: %w", err))
		}
		// check for ctrl-C
		select {
		case <-ctx.Done():
			break
		default:
		}
		// block when all buffers are used - wait for receiver to read one
		bidx := q.GetWriteBufIdx()
		fmt.Println("bidx: ", bidx)
		chv.Send(reflect.ValueOf(q.Binds()).Index(bidx).Elem().Elem())
		//chv.Send(reflect.ValueOf(q.GetBind()).Elem())

		// unblocked, receiver has consumed a page, save state (ignore on first loop)
		if q.Paginated() && len(q.PgStateValS()) > 0 {
			err := savePgState(ctx, client, q.PgStateId(), q.PopPgStateValS(), q.Worker())
			if err != nil {
				log.LogErr(fmt.Errorf("savePgState: %w", err))
				q.SetEOD()
				continue
			}
		}
	}

	chv.Close()

}

// exNonParallelScan - non-parallel scan. Scan of table or index.
func exNonParallelScan(ctx context.Context, client *DynamodbHandle, q *query.QueryHandle, proj *expression.ProjectionBuilder) error {

	log.LogDebug(fmt.Sprintf("exNonParallelScan"))

	if q.IsRestart() {
		// read StateVal from table using q.GetStartVal
		// parse contents into map[string]types.AttributeValue
		log.LogAlert("exNonParallelScan: restart")
		pg, err := getPgState(ctx, client, q.PgStateId())
		if err != nil {
			return err
		}
		q.AddPgStateValS(pg)
		q.SetPgStateValI(unmarshalPgState(pg))
		q.SetRestart(false)

	}

	var flt, f expression.ConditionBuilder

	if proj == nil {
		return fmt.Errorf("Select must be specified in a Scan")
	}
	for i, n := range q.GetFilterAttrs() {
		if i == 0 {
			switch ComparOpr(n.GetOprStr()) {
			case BEGINSWITH:
				flt = expression.BeginsWith(expression.Name(n.Name()), n.Value().(string))
			case GT:
				flt = expression.GreaterThan(expression.Name(n.Name()), expression.Value(n.Value()))
			case LT:
				flt = expression.LessThan(expression.Name(n.Name()), expression.Value(n.Value()))
			case EQ:
				flt = expression.Equal(expression.Name(n.Name()), expression.Value(n.Value()))
			case NE:
				flt = expression.NotEqual(expression.Name(n.Name()), expression.Value(n.Value()))
			default:
				panic(fmt.Errorf(fmt.Sprintf("Comparitor %q not supported", ComparOpr(n.GetOprStr()))))
			}

		} else {

			switch ComparOpr(n.GetOprStr()) {
			case BEGINSWITH:
				f = expression.BeginsWith(expression.Name(n.Name()), n.Value().(string))
			case GT:
				f = expression.GreaterThan(expression.Name(n.Name()), expression.Value(n.Value()))
			case LT:
				f = expression.LessThan(expression.Name(n.Name()), expression.Value(n.Value()))
			case EQ:
				f = expression.Equal(expression.Name(n.Name()), expression.Value(n.Value()))
			case NE:
				f = expression.NotEqual(expression.Name(n.Name()), expression.Value(n.Value()))
			default:
				panic(fmt.Errorf(fmt.Sprintf("Comparitor %q not supported", ComparOpr(n.GetOprStr()))))
			}
			//flt = flt.And(f)
			switch n.BoolCd() {
			case query.AND:
				flt = flt.And(f)
			case query.OR:
				flt = flt.Or(f)
			}
		}
	}

	// build expression.Expression
	b := expression.NewBuilder()
	if proj != nil {
		b = b.WithProjection(*proj)
	}
	if q.FilterSpecified() {
		b = b.WithFilter(flt)
	}

	expr, err := b.Build()
	if err != nil {
		return newDBExprErr("exNonParallelScan", "", "", err)
	}
	// log.LogAlert("exNonParallelScan", fmt.Sprintf("ProjectionExpression  %s", *expr.Projection()))
	// log.LogAlert("exNonParallelScan", fmt.Sprintf("ExpressionAttributeNames  %s", expr.Names()))
	// log.LogAlert("exNonParallelScan", fmt.Sprintf("ExpressionAttributeValues  %s", expr.Values()))
	// log.LogAlert("exNonParallelScan", fmt.Sprintf("Filter  %s", *expr.Filter()))
	// log.LogAlert("exNonParallelScan", fmt.Sprintf("TableName:  %s", q.GetTable()))
	input := &dynamodb.ScanInput{
		ProjectionExpression:      expr.Projection(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		Select:                    types.SelectSpecificAttributes, // aws.String("SPECIFIC_ATTRIBUTES"),
		TableName:                 aws.String(string(q.GetTable())),
		ReturnConsumedCapacity:    types.ReturnConsumedCapacityIndexes,
		ConsistentRead:            aws.Bool(q.ConsistentMode()),
	}

	if q.IndexSpecified() {
		input.IndexName = aws.String(string(q.GetIndex()))
	}

	// fmt.Println(dbScanInput{input}.String())

	if l := q.GetLimit(); l > 0 {
		input.Limit = aws.Int32(int32(l))
	}

	if lk := q.PgStateValI(); lk != nil {
		input.ExclusiveStartKey = lk.(map[string]types.AttributeValue)
		//	log.LogDebug(fmt.Sprintf("exNonParallelScan: SetExclusiveStartKey %s", input.String()))
	}
	//fmt.Print("input: ", input.String(), "\n")
	//
	t0 := time.Now()
	result, err := client.Scan(ctx, input)
	t1 := time.Now()
	if err != nil {
		return newDBSysErr("exNonParallelScan", "Scan", err)
	}

	// save LastEvaluatedKey
	if lek := result.LastEvaluatedKey; len(lek) == 0 {

		log.LogAlert("exNonParallelScan LastEvaluatedKey is nil, setEOD()")
		q.AddPgStateValS("")
		q.SetPgStateValI(nil)
		q.SetEOD()

	} else {

		q.AddPgStateValS(stringifyPgState(result.LastEvaluatedKey))
		q.SetPgStateValI(result.LastEvaluatedKey)
	}

	dur := t1.Sub(t0)
	dur_ := dur.String()
	if dot := strings.Index(dur_, "."); dur_[dot+2] == 57 {
		cc_ := ConsumedCapacity_{result.ConsumedCapacity}
		log.LogDebug(fmt.Sprintf("exNonParallelScan:consumed capacity for Scan  %s. ItemCount %d  Duration: %s", cc_.String(), result.Count, dur_))
	}
	//

	//save query statistics
	stats.SaveQueryStat(stats.Scan, q.Tag, result.ConsumedCapacity, result.Count, result.ScannedCount, dur)

	if result.Count > 0 {

		err = attributevalue.UnmarshalListOfMaps(result.Items, q.GetBind())
		if err != nil {
			return newDBUnmarshalErr("exNonParallelScan", "", "", "UnmarshalListOfMaps", err)
		}
		//log.LogDebug("exNonParallelScan", fmt.Sprintf(" result count  [items]  %d [%d] %d", result.Count, result.Count, reflect.ValueOf(q.GetBind()).Elem().Len()))

	} else {

		//log.LogDebug("exNonParallelScan", fmt.Sprintf(" result count  [items]  %d [%d] %d", result.Count, result.Count, reflect.ValueOf(q.GetBind()).Elem().Len()))

		// zero out q.bind (client select variable) if populated
		if reflect.ValueOf(q.GetBind()).Elem().Kind() == reflect.Slice {

			if reflect.ValueOf(q.GetBind()).Elem().Len() > 0 {
				log.LogAlert("exNonParallelScan Zero out bind variable ")
				t := reflect.ValueOf(q.GetBind()).Type().Elem()
				n := reflect.New(t) // *slice to empty slice
				q.SetBindValue(n.Elem())

			}

		} else {
			panic(fmt.Errorf("Expected a slice for scan output variable."))
		}

		log.LogDebug("exNonParallelScan: return NoDataFoundErr")

		return query.NoDataFoundErr
	}

	return nil
}

// exScanWorker used by parallel scan`
func exScanWorker(ctx context.Context, client *DynamodbHandle, q *query.QueryHandle, proj *expression.ProjectionBuilder) error {

	worker := fmt.Sprintf("exScanWorker: thread %d  totalSegments %d", q.Worker(), q.NumWorkers())
	log.LogAlert(worker)

	if q.IsRestart() {
		// read StateVal from table using q.GetStartVal
		// parse contents into map[string]types.AttributeValue
		pg, err := getPgState(ctx, client, q.PgStateId(), q.Worker())
		if err != nil {
			return err
		}
		q.AddPgStateValS(pg)
		q.SetPgStateValI(unmarshalPgState(pg))
		q.SetRestart(false)
	}

	var flt, f expression.ConditionBuilder
	if proj == nil {
		return fmt.Errorf("Select must be specified in a Scan")
	}
	for i, n := range q.GetFilterAttrs() {
		if i == 0 {
			switch ComparOpr(n.GetOprStr()) {
			case BEGINSWITH:
				flt = expression.BeginsWith(expression.Name(n.Name()), n.Value().(string))
			case GT:
				flt = expression.GreaterThan(expression.Name(n.Name()), expression.Value(n.Value()))
			case LT:
				flt = expression.LessThan(expression.Name(n.Name()), expression.Value(n.Value()))
			case EQ:
				flt = expression.Equal(expression.Name(n.Name()), expression.Value(n.Value()))
			case NE:
				flt = expression.NotEqual(expression.Name(n.Name()), expression.Value(n.Value()))
			default:
				panic(fmt.Errorf(fmt.Sprintf("Comparitor %q not supported", ComparOpr(n.GetOprStr()))))
			}

		} else {

			switch ComparOpr(n.GetOprStr()) {
			case BEGINSWITH:
				f = expression.BeginsWith(expression.Name(n.Name()), n.Value().(string))
			case GT:
				f = expression.GreaterThan(expression.Name(n.Name()), expression.Value(n.Value()))
			case LT:
				f = expression.LessThan(expression.Name(n.Name()), expression.Value(n.Value()))
			case EQ:
				f = expression.Equal(expression.Name(n.Name()), expression.Value(n.Value()))
			case NE:
				f = expression.NotEqual(expression.Name(n.Name()), expression.Value(n.Value()))
			default:
				panic(fmt.Errorf(fmt.Sprintf("xComparitor %q not supported", ComparOpr(n.GetOprStr()))))
			}
			//flt = flt.And(f)
			switch n.BoolCd() {
			case query.AND:
				flt = flt.And(f)
			case query.OR:
				flt = flt.Or(f)
			}
		}
	}
	// build expression.Expression
	b := expression.NewBuilder()
	if proj != nil {
		b = b.WithProjection(*proj)
	}
	if q.FilterSpecified() {
		b = b.WithFilter(flt)
	}

	expr, err := b.Build()
	if err != nil {
		return newDBExprErr("exScanWorker", "", "", err)
	}

	input := &dynamodb.ScanInput{
		ProjectionExpression:      expr.Projection(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		Select:                    types.SelectSpecificAttributes,
		TableName:                 aws.String(string(q.GetTable())),
		ReturnConsumedCapacity:    types.ReturnConsumedCapacityIndexes,
		ConsistentRead:            aws.Bool(q.ConsistentMode()),
	}
	if q.IndexSpecified() {
		input.IndexName = aws.String(string(q.GetIndex()))
	}
	if l := q.GetLimit(); l > 0 {
		input.Limit = aws.Int32(int32(l))
	}
	if lk := q.PgStateValI(); lk != nil {
		//	q.bindState()
		input.ExclusiveStartKey = lk.(map[string]types.AttributeValue)
		//log.LogDebug(fmt.Sprintf("exScanWorker: SetExclusiveStartKey %s", input.String()))
	}

	input.Segment = aws.Int32(int32(q.Worker()))           //int64(q.Worker())
	input.TotalSegments = aws.Int32(int32(q.NumWorkers())) //int64(q.NumWorkers())
	//log.LogDebug(fmt.Sprintf("exScanWorker: thread %d  input: %s", q.Worker(), input.String()))
	//
	t0 := time.Now()
	result, err := client.Scan(ctx, input)
	t1 := time.Now()
	if err != nil {
		return newDBSysErr("exScanWorker", "Scan", err)
	}
	// save LastEvaluatedKey
	if lek := result.LastEvaluatedKey; len(lek) == 0 {

		q.AddPgStateValS("")
		q.SetPgStateValI(nil)
		q.SetEOD()

	} else {

		q.AddPgStateValS(stringifyPgState(result.LastEvaluatedKey))
		q.SetPgStateValI(result.LastEvaluatedKey)
	}

	dur := t1.Sub(t0)
	dur_ := dur.String()
	if dot := strings.Index(dur_, "."); dur_[dot+2] == 57 {
		log.LogAlert(fmt.Sprintf("exScanWorker:consumed capacity for Scan  %s. ItemCount %d  Duration: %s", ConsumedCapacity_{result.ConsumedCapacity}.String(), result.Count, dur_))
	}
	//
	log.LogAlert(fmt.Sprintf("exScanWorker thread: %d  result.Count  %d", q.Worker(), result.Count))

	//save query statistics
	stats.SaveQueryStat(stats.Scan, q.Tag, result.ConsumedCapacity, result.Count, result.ScannedCount, dur)

	if result.Count > 0 {

		err = attributevalue.UnmarshalListOfMaps(result.Items, q.GetBind())
		if err != nil {
			return newDBUnmarshalErr("exScanWorker", "", "", "UnmarshalListOfMaps", err)
		}

	} else {

		// zero out q.bind (client select variable) if populated
		if reflect.ValueOf(q.GetBind()).Elem().Kind() == reflect.Slice {

			if reflect.ValueOf(q.GetBind()).Elem().Len() > 0 {

				t := reflect.ValueOf(q.GetBind()).Type().Elem()
				n := reflect.New(t) // *slice to empty slice
				q.SetBindValue(n.Elem())
			}
		} else {
			panic(fmt.Errorf("Expected a slice for scan output variable."))
		}

		log.LogAlert("exScanWorker return NoDataFoundErr")

		return query.NoDataFoundErr
	}

	return nil
}

func stringifyPgState(d map[string]types.AttributeValue) string {
	var lek strings.Builder
	lek.WriteString(strconv.Itoa(len(d)))
	for k, v := range d {
		lek.WriteByte('{')
		lek.WriteString(k)
		lek.WriteString(" : ")
		if b, ok := v.(*types.AttributeValueMemberB); ok {
			lek.WriteString("{ B : ")
			lek.WriteByte('"')
			if len(b.Value) == 16 {
				lek.WriteString(uuid.UID(b.Value).String())
			} else {
				lek.WriteString(base64.StdEncoding.EncodeToString(b.Value))
			}
			lek.WriteString(`" }`)
		} else {
			switch x := v.(type) {
			case *types.AttributeValueMemberS:
				lek.WriteString("{ S : ")
				lek.WriteByte('"')
				lek.WriteString(x.Value)
				lek.WriteString(`" }`)
			}
		}

		lek.WriteString(" }")
	}
	return lek.String()
}

func savePgState(ctx context.Context, client *DynamodbHandle, id uuid.UID, val string, worker ...int) error {

	var state string = "LastEvaluatedKey"

	log.LogAlert(fmt.Sprintf("savePgState id: %s val: %q  worker: %d", id.Base64(), val, worker))

	m := mut.NewInsert("pgState")
	if len(worker) > 0 {
		state += "-w" + strconv.Itoa(worker[0])
	}
	m.AddMember("Id", id, mut.IsKey).AddMember("Name", state, mut.IsKey).AddMember("Value", val).AddMember("Updated", "$CURRENT_TIMESTAMP$")

	// add single mutation to mulitple-mutation configuration usually performed within a tx.
	mt := mut.Mutations([]dbs.Mutation{m})
	bs := []*mut.Mutations{&mt}

	return execTransaction(ctx, client.Client, bs, "internal-state", db.StdAPI)

}

func deletePgState(ctx context.Context, client *DynamodbHandle, id uuid.UID, worker ...int) error {

	var state string = "LastEvaluatedKey"

	log.LogDebug(fmt.Sprintf("deletePgState id: %s ", id.Base64()))

	m := mut.NewDelete("pgState")
	if len(worker) > 0 {
		state += "-w" + strconv.Itoa(worker[0])
	}
	m.AddMember("Id", id, mut.IsKey).AddMember("Name", state, mut.IsKey)

	// add single mutation to mulitple-mutation configuration usually performed within a tx.
	mt := mut.Mutations([]dbs.Mutation{m})
	bs := []*mut.Mutations{&mt}

	return execTransaction(ctx, client.Client, bs, "internal-state", db.StdAPI)

}

func getPgState(ctx context.Context, client *DynamodbHandle, id uuid.UID, worker ...int) (string, error) {

	var state string = "LastEvaluatedKey"

	if len(worker) > 0 {
		state += "-w" + strconv.Itoa(worker[0])
	}

	log.LogAlert(fmt.Sprintf("getPgState id: %s ", id.String()))

	type Val struct {
		Value string
	}
	var val Val

	q := query.New2("getPgState", "pgState")
	q.Select(&val).Key("Id", id).Key("Name", state)

	err := executeQuery(ctx, client, q)
	if err != nil {
		log.LogErr(fmt.Errorf("getPgState errored: %w ", err))
		return "", err
	}
	log.LogAlert(fmt.Sprintf("getPgState value: %s ", val.Value))
	return val.Value, nil

}

// unmarshalPgState takes scan state data from bundles it into map[string]types.AttributeValue
//
//	4{S : {  S: "17 June 1986"} }{PKey : {  B: "WbjVdFJGTWGpqRpUTEPcPg==" } }{SortK : { S: "r|A#A#:D"} }{P : {  S: "r|DOB"} }
func unmarshalPgState(in string) map[string]types.AttributeValue {
	var (
		attr string
		tok  string
		err  error
	)

	mm := make(map[string]types.AttributeValue)

	log.LogAlert(fmt.Sprintf("unmarshalPgState: %s ", in))
	// in:  4{IX : { S : "X" } }{PKey : { B : "a588f90b-7d07-4789-8d16-7cf01015c461" } }{Ty : { S : "m|P" } }{SortK : { S : "A#A#T" } }

	var s scanner.Scanner
	s.Init(strings.NewReader(in))
	s.Scan()
	elements, err := strconv.ParseInt(s.TokenText(), 10, 32)
	if err != nil {
		panic(err)
	}
	var j int64
	for j = 0; j < elements; j++ {
		var av types.AttributeValue
		s.Scan()
		if s.TokenText() != "{" {

		}
		s.Scan()
		attr = s.TokenText()
		// scan over : {
		for i := 0; i < 2; i++ {
			s.Scan()
			switch s.TokenText() {
			case ":", "{":
			default:
				panic(fmt.Errorf("expected :{ got %s", s.TokenText()))
			}
		}
		s.Scan()
		tok = s.TokenText() // S, B, N
		s.Scan()            // pass over ":"
		s.Scan()
		switch tok {
		case "S":
			s := s.TokenText()[1 : len(s.TokenText())-1] // remove surrounding double quotes
			av = &types.AttributeValueMemberS{Value: s}
		case "B":
			//
			var uid []byte

			u := s.TokenText()[1 : len(s.TokenText())-1] // remove surrounding double quotes
			if len(u) == 36 {
				uid = uuid.FromString(u)
			} else {
				uid, err = base64.StdEncoding.DecodeString(u)
				if err != nil {
					panic(err)
				}
			}
			av = &types.AttributeValueMemberB{Value: []byte(uid)}
		case "N":
			n := s.TokenText()[1 : len(s.TokenText())-1] // remove surrounding double quotes
			av = &types.AttributeValueMemberN{Value: n}
		}

		mm[attr] = av

		for i := 0; i < 2; i++ {
			s.Scan()
			switch s.TokenText() {
			case "}":
			default:
				panic(fmt.Errorf("expected } got %s", s.TokenText()))
			}
		}
	}

	log.LogDebug(fmt.Sprintf("unmarshalPgState  %s", stringifyPgState(mm)))

	return mm
}

type Capacity_ struct {
	*types.Capacity
}

func (c Capacity_) String() string {
	var s strings.Builder

	if c.CapacityUnits != nil {
		s.WriteString(fmt.Sprintf("Total CUs:  %g ", c.CapacityUnits))
	}
	if c.ReadCapacityUnits != nil {
		s.WriteString(fmt.Sprintf("Read CUs:  %g ", c.ReadCapacityUnits))
	}
	if c.WriteCapacityUnits != nil {
		s.WriteString(fmt.Sprintf("Write CUs:  %g ", c.WriteCapacityUnits))
	}
	s.WriteByte('\n')

	return s.String()
}

type ConsumedCapacity_ struct {
	*types.ConsumedCapacity
}

func (w ConsumedCapacity_) String() string {
	var s strings.Builder

	if w.CapacityUnits != nil {
		s.WriteString(fmt.Sprintf("Total CUs: %g ", *w.CapacityUnits))
	}
	if w.TableName != nil {
		s.WriteString(fmt.Sprintf(" Table: %s ", *w.TableName))
	}
	if w.Table != nil {
		w_ := Capacity_{w.Table}
		w_.String()
	}
	if w.WriteCapacityUnits != nil {
		s.WriteString(fmt.Sprintf("Total Write CUs: %g \n", *w.WriteCapacityUnits))
	}
	for k, v := range w.GlobalSecondaryIndexes {
		w_ := Capacity_{&v}
		s.WriteString(fmt.Sprintf(" GIndex: %s  %s ", k, w_.String()))
	}
	for k, v := range w.LocalSecondaryIndexes {
		w_ := Capacity_{&v}
		s.WriteString(fmt.Sprintf(" LIndex: %s  %s ", k, w_.String()))
	}
	s.WriteByte('\n')

	return s.String()

}
