//go:build dynamodb
// +build dynamodb

package db

import (
	"context"
	"errors"
	"fmt"
	//"net/http"
	"reflect"
	"strconv"
	"strings"
	"text/scanner"
	"time"

	throttle "github.com/GoGraph/db/internal/throttleSrv"
	"github.com/GoGraph/db/stats"
	"github.com/GoGraph/dbs"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/uuid"
	//"github.com/GoGraph/tbl"
	param "github.com/GoGraph/dygparam"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/tx/query"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	//	smithyhttp "github.com/aws/smithy-go/transport/http"
	// "github.com/aws/aws-sdk-go/aws"
	// "github.com/aws/aws-sdk-go/aws/awserr"
	// "github.com/aws/aws-sdk-go/service/dynamodb"
	//"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	//"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

type action byte

const (
	fail action = iota
	retry
	shortDelay
	longDelay
	throttle_ // reduce concurrency
)

func syslog(s string) {
	slog.Log(logid, s)
}

func alertlog(s string) {
	slog.LogAlert(logid, s)
}

func execute(ctx context.Context, client *dynamodb.Client, bs []*mut.Mutations, tag string, api API, cfg aws.Config, opt ...Option) error {

	var (
		err error
	)

	awsConfig = cfg

	switch api {

	case StdAPI, TransactionAPI:

		// distinction between Std and Transaction make in execTranaction()
		err = execTransaction(ctx, client, bs, tag, api)

	case BatchAPI:

		err = execBatch(ctx, client, bs, tag)

	// case ScanAPI: - NA, scan should be a derived quantity based on query methods employeed e.g. lack of Key() method would force a scan

	// 	err = execScan(client, bs, tag)

	case OptimAPI:

		err = execOptim(ctx, client, bs, tag)

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
					// slog.LogError("throttlingexception", "About to wait 30 seconds before proceeding...")
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

func retryOp(err error) bool {

	//  github.com/aws/aws-sdk-go-v2/aws/Retryer
	retryer := awsConfig.Retryer

	alertlog(fmt.Sprintf("retryOp: [%T] %s", err, err))

	if retryer != nil {
		if retryer().IsErrorRetryable(err) {
			alertlog("retryOp: error is retryable...")
			for _, v := range []int{1, 2, 3} {
				d, err := retryer().RetryDelay(v, err)
				if err != nil {
					fmt.Printf("retryOp:  Delay attempt %d:  %s", v, d.String())
				}
			}
		} else {
			alertlog("retryOp: error is NOT retryable...")
		}
		alertlog(fmt.Sprintf("retryOp: max attempts: %d", retryer().MaxAttempts()))
	} else {
		alertlog(fmt.Sprintf("retryOp: no Retryer defined. aws.Config max attempts: %d", awsConfig.RetryMaxAttempts))
	}

	for _, action := range errorAction(err) {

		switch action {
		case shortDelay:
			alertlog("retryOp: short delay...")
			time.Sleep(20 * time.Second)
		case longDelay:
			alertlog("retryOp: long delay...")
			time.Sleep(60 * time.Second)
		case retry:
			alertlog("retryOp: retry...")
			return true
		// case fail:
		// 	return false
		case throttle_:
			alertlog("retryOp: throttle...")
			// call throttle down api
			throttle.Down()

		}
	}
	alertlog("retryOp: no retry...")
	return false
}

// func XXX(expr expression.Expression) map[string]*dynamodb.AttributeValue {

// 	var s strings.Builder
// 	values := expr.Values()

// convertBS2List converts Binary Set to List because GoGraph expects a List type not a BS type
// Dynamodb's expression pkg creates BS rather than L for binary array data.
// As GoGraph had no user-defined types it is possible to hardwire in the affected attributes.
// All types in GoGraph are known at compile time.
func convertBS2List(expr expression.Expression) map[string]types.AttributeValue {

	var s strings.Builder
	values := expr.Values() // map[string]types.AttributeValue

	for k, v := range expr.Names() { // map[string]string  [":0"]"PKey", [":2"]"SortK"
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
		cond expression.ConditionBuilder
	)

	// merge := false
	// if len(ismerge) > 0 {
	// 	merge = ismerge[0]
	// }

	for i, col := range m.GetMembers() {
		if col.Name == "__" || col.IsKey() {
			continue
		}

		switch col.Opr {
		// TODO: implement Add (as opposed to inc which is "Add 1")
		case mut.Add:
			if i == 0 {
				upd = expression.Set(expression.Name(col.Name), expression.Name(col.Name).Plus(expression.Value(col.Value)))
			} else {
				upd = upd.Set(expression.Name(col.Name), expression.Name(col.Name).Plus(expression.Value(col.Value)))
			}
		case mut.Inc:
			if i == 0 {
				upd = expression.Set(expression.Name(col.Name), expression.Name(col.Name).Plus(expression.Value(1)))
			} else {
				upd = upd.Set(expression.Name(col.Name), expression.Name(col.Name).Plus(expression.Value(1)))
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
			if col.Opr == mut.Set {

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

			if col.Opr != mut.Set {
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

	// add condition expression if defined and create expression
	if cd := m.GetCondition(); cd != nil {
		switch cd.GetCond() {
		case mut.AttrExists:
			cond = expression.AttributeExists(expression.Name(cd.GetAttr()))
		case mut.AttrNotExists:
			cond = expression.AttributeNotExists(expression.Name(cd.GetAttr()))
		}
		// create expression builder with condition builder
		expr, err = expression.NewBuilder().WithUpdate(upd).WithCondition(cond).Build()
	} else {
		// create expression builder with no condition builder
		expr, err = expression.NewBuilder().WithUpdate(upd).Build()
	}

	if err != nil {
		return nil, newDBExprErr("txUpdate", "", "", err)
	}

	av := make(map[string]types.AttributeValue)

	// generate key AV
	for _, v := range m.GetKeys() {
		av[v.Name] = marshalAvUsingValue(v.Value)
	}
	//
	update := &types.Update{
		Key:                       av,
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: convertBS2List(expr),
		UpdateExpression:          expr.Update(),
		ConditionExpression:       expr.Condition(),
		TableName:                 aws.String(m.GetTable()),
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
func execBatchMutations(ctx context.Context, client *dynamodb.Client, bi mut.Mutations, tag string) error {

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

				if !retryOp(err) {
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

				slog.Log("dbExecute: ", fmt.Sprintf("BatchWriteItem: %s, tag: %s: Elapsed: %s Unprocessed: %d of %d [retry: %d]", api, tag, t1.Sub(t0).String(), unProc, curUnProc, unProcRetryCnt+1))

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
					if !retryOp(err) {
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
		slog.Log("dbExecute:", fmt.Sprintf("%s : Batch processed all items [tag: %s]. Mutations %d  Elapsed: %s", api, tag, len(bi), t1.Sub(t0).String()))
	}

	// log 30% of activity
	dur := t1.Sub(t0).String()
	if dot := strings.Index(dur, "."); dur[dot+2] == 57 && (dur[dot+3] == 54 || dur[dot+3] == 55) {
		slog.Log("dbExecute:", fmt.Sprintf("Bulk Insert [tag: %s]: mutations %d  Unprocessed Retries: %d  Elapsed: %s", tag, len(bi), unProcRetryCnt, dur))
	}

	return nil
}

func execBatch(ctx context.Context, client *dynamodb.Client, bs []*mut.Mutations, tag string) error {
	// merge transaction

	// generate statements for each mutation
	for _, b := range bs {

		for _, m := range *b { // mut.Mutations
			_, ok := m.(*mut.Mutation)
			if !ok {
				return fmt.Errorf("ExecBatch error. Batch contains a non-Mutation type")
			}
		}

		err := execBatchMutations(ctx, client, *b, tag)
		if err != nil {
			return err
		}
	}
	return nil
}

// execOptim - bit silly. Runs Inserts as a batch and updates as transactional. Maybe illogical. Worth more of a think.
func execOptim(ctx context.Context, client *dynamodb.Client, bs []*mut.Mutations, tag string) error {

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
		err := execBatchMutations(ctx, client, v, tag)
		if err != nil {
			return err
		}
	}

	// process updates/deletes
	return execTransaction(ctx, client, bs, tag, OptimAPI)

}

// mut.Mutations = []dbs.Mutation
type txWrite struct {
	// upto to out transactionWriteItems created when a merge is specified
	txwii [2]*dynamodb.TransactWriteItemsInput
	merge bool
}

func execTransaction(ctx context.Context, client *dynamodb.Client, bs []*mut.Mutations, tag string, api API) error {

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

				if api == OptimAPI && y.GetOpr() == mut.Insert {
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

	case TransactionAPI:

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
								slog.Log("dbExecute: ", fmt.Sprintf("TransactionAPI (merge part2): mutations %d  Elapsed: %s ", len(tx.txwii[1].TransactItems), dur))
							}

						default:

							return newDBSysErr2("TransactionAPI (merge part 2)", tag, "", "TxMergeP2", err)

						}
					}

				} else {

					syslog(fmt.Sprintf("Transaction error: %s. %#v\n", err, tx.txwii[0]))

					return newDBSysErr2("TransactionAPI", tag, "Not a TransactionCanceledException error in TransactWriteItems", "", err)

				}

			} else {
				// no error, save statistics
				stats.SaveTransactStat(tag, out.ConsumedCapacity, t1.Sub(t0), len(tx.txwii[0].TransactItems))

				dur := t1.Sub(t0).String()
				if dot := strings.Index(dur, "."); dur[dot+2] == 57 && (dur[dot+3] == 54) { //|| dur[dot+3] == 55) {
					slog.Log("dbExecute: ", fmt.Sprintf("TransactionAPI: mutations %d  Elapsed: %s ", len(tx.txwii[0].TransactItems), dur))
				}

			}
		}

	case StdAPI, OptimAPI:

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

							fmt.Printf("UpdateItem error: %s\n", err)

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
							//syslog(fmt.Sprintf("Execute std api:consumed capacity for PutItem %s.  Duration: %s", uio.ConsumedCapacity, t1.Sub(t0)))
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

//
//
//
//////////  QUERY  /////////  QUERY  ///////  QUERY  /////////  QUERY  ///////  QUERY  ////////////  QUERY  /////////  QUERY  ///////////
//
//
//

func genKeyAV(q *query.QueryHandle) (map[string]types.AttributeValue, error) {

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

func executeQuery(ctx context.Context, q *query.QueryHandle, opt ...Option) error {

	var (
		// options
		err error
	)

	if q.Error() != nil {
		return q.Error()
	}

	for _, o := range opt {
		switch strings.ToLower(o.Name) {
		default:
		}
	}

	// as this is db package (default DB) we can use the dbSrv value
	client := GetDefaultDBHdl().(DynamodbHandle)
	//client := dbSrv

	if q.GetError() != nil {
		return fmt.Errorf(fmt.Sprintf("Cannot execute query because of error %s", q.GetError()))
	}

	// define projection based on struct passed via Select()
	proj := crProjectionExpr(q)
	//
	av, err := genKeyAV(q)
	if err != nil {
		return err
	}
	pk, sk := q.GetPkSk()
	if len(pk) == 0 {

		// if !q.HasFilter() {
		// 	if len(sk) == 0 && !q.HasFilter() {
		// 		return fmt.Errorf("Partition Key must be defined with equality condition")
		// 	}
		// }
		return exScan(ctx, client, q, proj)
	}

	if q.IndexSpecified() {
		// gsi  do not enforce uniqueness so GetItem() is NA.  key() specifications perform query, no keys scan.
		return exQuery(ctx, client, q, proj)
	}

	switch {

	case len(pk) != 0 && len(sk) == 0 && tbl.KeyCnt(q.GetTable()) == 1:

		return exGetItem(ctx, client, q, av, proj)

	default:

		if q.KeyCnt() == tbl.KeyCnt(q.GetTable()) {

			sk, err := tbl.GetSK(q.GetTable())
			if err != nil {
				panic(err)
			}
			switch q.GetKeyComparOpr(sk) {

			case query.EQ:

				return exGetItem(ctx, client, q, av, proj)

			default:

				return exQuery(ctx, client, q, proj)
			}

		} else {

			return exQuery(ctx, client, q, proj)
		}

	}

}

//func exGetItem(q *query.QueryHandle, av []types.AttributeValue) error {
func exGetItem(ctx context.Context, client DynamodbHandle, q *query.QueryHandle, av map[string]types.AttributeValue, proj *expression.ProjectionBuilder) error {

	//fmt.Println("=== GetItem ===")
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
	//syslog(fmt.Sprintf("GetItem: %#v\n", input))
	t0 := time.Now()
	result, err := client.GetItem(ctx, input)
	t1 := time.Now()
	if err != nil {
		return newDBSysErr("exGetItem", "GetItem", err)
	}
	dur := t1.Sub(t0)
	dur_ := dur.String()
	if dot := strings.Index(dur_, "."); dur_[dot+2] == 57 {
		syslog(fmt.Sprintf("exGetItem:consumed capacity for GetItem  %s. Duration: %s", ConsumedCapacity_{result.ConsumedCapacity}.String(), dur_))
	}
	//
	if len(result.Item) == 0 {
		return query.NoDataFoundErr
	}
	err = attributevalue.UnmarshalMap(result.Item, q.GetFetch())
	if err != nil {
		return newDBUnmarshalErr("xGetItem", "", "", "UnmarshalMap", err)
	}
	// save query statistics
	stats.SaveQueryStat(stats.GetItem, q.Tag, result.ConsumedCapacity, 1, 0, dur)

	return nil
}

func exQuery(ctx context.Context, client DynamodbHandle, q *query.QueryHandle, proj *expression.ProjectionBuilder) error {
	//
	var (
		keyc   expression.KeyConditionBuilder
		flt, f expression.ConditionBuilder
	)

	syslog(fmt.Sprintf("exQuery: Tag [%s]", q.Tag))

	// pagination
	if q.IsRestart() {
		// read StateVal from table using q.GetStartVal
		// parse contents into map[string]types.AttributeValue
		//syslog(fmt.Sprintf("exQuery: restart"))
		q.SetPgStateValI(unmarshalPgState(getPgState(ctx, q.PgStateId())))
		q.SetRestart(false)

	} else {

		// if we have got here (without error) then we must have successfully processed last batch, so commit its savepoint (lastevaluatedkey)
		// if we don't get to save this state then the last batch will be reprocessed - as Elasticsearch is idempotent this is not an issue.
		// saving lastEvaluatedKey minimises the amount of reprocessing in the event of an interruption to the ES load.
		if len(q.PgStateValS()) > 0 {
			syslog(fmt.Sprintf("exQuery: load pg state..."))
			err := savePgState(ctx, client, q.PgStateId(), q.PgStateValS())
			if err != nil {
				panic(err)
			}
		}
		syslog(fmt.Sprintf("exQuery: restart false."))
	}

	switch q.KeyCnt() {
	case 1:
		keyc = expression.KeyEqual(expression.Key(q.GetPK()), expression.Value(q.GetKeyValue(q.GetPK())))
	case 2:
		//
		keyc = expression.KeyEqual(expression.Key(q.GetPK()), expression.Value(q.GetKeyValue(q.GetPK())))
		switch q.GetKeyComparOpr(q.GetSK()) {
		case query.EQ:
			keyc = expression.KeyAnd(keyc, expression.KeyEqual(expression.Key(q.GetSK()), expression.Value(q.GetKeyValue(q.GetSK()))))
		case query.GT:
			keyc = expression.KeyAnd(keyc, expression.KeyGreaterThan(expression.Key(q.GetSK()), expression.Value(q.GetKeyValue(q.GetSK()))))
		case query.LT:
			keyc = expression.KeyAnd(keyc, expression.KeyLessThan(expression.Key(q.GetSK()), expression.Value(q.GetKeyValue(q.GetSK()))))
		case query.GE:
			keyc = expression.KeyAnd(keyc, expression.KeyGreaterThanEqual(expression.Key(q.GetSK()), expression.Value(q.GetKeyValue(q.GetSK()))))
		case query.LE:
			keyc = expression.KeyAnd(keyc, expression.KeyLessThanEqual(expression.Key(q.GetSK()), expression.Value(q.GetKeyValue(q.GetSK()))))
		case query.BEGINSWITH:
			//syslog(fmt.Sprintf("BEGINSWITH: %s %s ", q.GetSK(), q.GetKeyValue(q.GetSK()).(string)))
			keyc = expression.KeyAnd(keyc, expression.KeyBeginsWith(expression.Key(q.GetSK()), q.GetKeyValue(q.GetSK()).(string)))
		}
	}

	for i, n := range q.GetFilterAttr() {
		if i == 0 {
			switch n.ComparOpr() {
			case query.BEGINSWITH:
				flt = expression.BeginsWith(expression.Name(n.Name()), n.Value().(string))
			case query.GT:
				flt = expression.GreaterThan(expression.Name(n.Name()), expression.Value(n.Value()))
			case query.LT:
				flt = expression.LessThan(expression.Name(n.Name()), expression.Value(n.Value()))
			case query.EQ:
				flt = expression.Equal(expression.Name(n.Name()), expression.Value(n.Value()))
			case query.NE:
				flt = expression.Equal(expression.Name(n.Name()), expression.Value(n.Value()))
			default:
				panic(fmt.Errorf(fmt.Sprintf("Comparitor %q not supported", n.ComparOpr())))
			}

		} else {

			switch n.ComparOpr() {
			case query.BEGINSWITH:
				f = expression.BeginsWith(expression.Name(n.Name()), n.Value().(string))
			case query.GT:
				f = expression.GreaterThan(expression.Name(n.Name()), expression.Value(n.Value()))
			case query.LT:
				f = expression.LessThan(expression.Name(n.Name()), expression.Value(n.Value()))
			case query.EQ:
				f = expression.Equal(expression.Name(n.Name()), expression.Value(n.Value()))
			case query.NE:
				f = expression.Equal(expression.Name(n.Name()), expression.Value(n.Value()))
			default:
				panic(fmt.Errorf(fmt.Sprintf("xComparitor %q not supported", n.ComparOpr())))
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
		return newDBExprErr("exQuery", "", "", err)
	}

	// define QueryInput
	input := &dynamodb.QueryInput{
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ProjectionExpression:      expr.Projection(),
		TableName:                 aws.String(string(q.GetTable())),
		ReturnConsumedCapacity:    types.ReturnConsumedCapacityIndexes,
		ConsistentRead:            aws.Bool(q.ConsistentMode()),
	}

	if q.IndexSpecified() {
		input.IndexName = aws.String(string(q.GetIndex()))
		//syslog(fmt.Sprintf("exQuery: index specified: %s", q.GetIndex()))
	}
	if l := q.GetLimit(); l > 0 {
		input.Limit = aws.Int32(int32(l))
		//syslog(fmt.Sprintf("exQuery: limit specified %d", l))
	}
	if lk := q.PgStateValI(); lk != nil {
		input.ExclusiveStartKey = lk.(map[string]types.AttributeValue)
	}

	if q.SKset() {
		input.ScanIndexForward = aws.Bool(q.IsScanForwardSet())
	}
	//
	t0 := time.Now()

	result, err := client.Query(ctx, input)
	syslog(fmt.Sprintf("exQuery: returns %d items", result.Count))
	t1 := time.Now()
	if err != nil {
		return newDBSysErr("exQuery", "Query", err)
	}

	// pagination cont....
	if lek := result.LastEvaluatedKey; len(lek) == 0 {

		syslog(fmt.Sprintf("exQuery: LastEvaluatedKey = 0"))
		//EOD
		q.SetPgStateValS("")
		q.SetPgStateValI(nil)
		q.SetEOD()

	} else {

		syslog(fmt.Sprintf("exQuery: LastEvaluatedKey != 0"))
		q.SetPgStateValS(stringifyPgState(result.LastEvaluatedKey))
		q.SetPgStateValI(result.LastEvaluatedKey)
	}

	dur := t1.Sub(t0)
	dur_ := dur.String()
	if dot := strings.Index(dur_, "."); dur_[dot+2] == 57 {
		syslog(fmt.Sprintf("exQuery:consumed capacity for Query  %s. ItemCount %d  Duration: %s", ConsumedCapacity_{result.ConsumedCapacity}.String(), len(result.Items), dur_))
	}
	//
	if result.Count == 0 {
		return query.NoDataFoundErr
	}

	err = attributevalue.UnmarshalListOfMaps(result.Items, q.GetFetch())
	if err != nil {
		return newDBUnmarshalErr("exQuery", "", "", "UnmarshalListOfMaps", err)
	}

	// save query statistics
	stats.SaveQueryStat(stats.Query, q.Tag, result.ConsumedCapacity, result.Count, result.ScannedCount, dur)

	return nil
}

func exScan(ctx context.Context, client DynamodbHandle, q *query.QueryHandle, proj *expression.ProjectionBuilder) error {
	var err error

	switch q.GetParallel() {

	case 0:
		err = exSingleScan(ctx, client, q, proj)

	default:
		err = exWorkerScan(ctx, client, q, proj)
	}

	return err
}

// exSingleScan - non-parallel scan. Scan of table or index.
func exSingleScan(ctx context.Context, client DynamodbHandle, q *query.QueryHandle, proj *expression.ProjectionBuilder) error {

	syslog(fmt.Sprintf("exSingleScan: thread %d", q.Worker()))

	if q.IsRestart() {
		// read StateVal from table using q.GetStartVal
		// parse contents into map[string]types.AttributeValue
		syslog(fmt.Sprintf("exSingleScan: restart"))
		q.SetPgStateValI(unmarshalPgState(getPgState(ctx, q.PgStateId())))
		q.SetRestart(false)

	} else {

		// if we have got here (without error) then we must have successfully processed last batch, so commit its savepoint (lastevaluatedkey)
		// if we don't get to save this state then the last batch will be reprocessed - as Elasticsearch is idempotent this is not an issue.
		// saving lastEvaluatedKey minimises the amount of reprocessing in the event of an interruption to the ES load.
		if len(q.PgStateValS()) > 0 {
			syslog(fmt.Sprintf("exSingleScan: load pg state..."))
			err := savePgState(ctx, client, q.PgStateId(), q.PgStateValS())
			if err != nil {
				panic(err)
			}
		}
	}

	var flt, f expression.ConditionBuilder

	if proj == nil {
		return fmt.Errorf("Select must be specified in a Scan")
	}
	for i, n := range q.GetFilterAttr() {
		if i == 0 {
			switch n.ComparOpr() {
			case query.BEGINSWITH:
				flt = expression.BeginsWith(expression.Name(n.Name()), n.Value().(string))
			case query.GT:
				flt = expression.GreaterThan(expression.Name(n.Name()), expression.Value(n.Value()))
			case query.LT:
				flt = expression.LessThan(expression.Name(n.Name()), expression.Value(n.Value()))
			case query.EQ:
				flt = expression.Equal(expression.Name(n.Name()), expression.Value(n.Value()))
			case query.NE:
				flt = expression.Equal(expression.Name(n.Name()), expression.Value(n.Value()))
			default:
				panic(fmt.Errorf(fmt.Sprintf("Comparitor %q not supported", n.ComparOpr())))
			}

		} else {

			switch n.ComparOpr() {
			case query.BEGINSWITH:
				f = expression.BeginsWith(expression.Name(n.Name()), n.Value().(string))
			case query.GT:
				f = expression.GreaterThan(expression.Name(n.Name()), expression.Value(n.Value()))
			case query.LT:
				f = expression.LessThan(expression.Name(n.Name()), expression.Value(n.Value()))
			case query.EQ:
				f = expression.Equal(expression.Name(n.Name()), expression.Value(n.Value()))
			case query.NE:
				f = expression.Equal(expression.Name(n.Name()), expression.Value(n.Value()))
			default:
				panic(fmt.Errorf(fmt.Sprintf("xComparitor %q not supported", n.ComparOpr())))
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
		return newDBExprErr("exSingleScan", "", "", err)
	}

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
		//	q.FetchState()
		input.ExclusiveStartKey = lk.(map[string]types.AttributeValue)
		//	syslog(fmt.Sprintf("exSingleScan: SetExclusiveStartKey %s", input.String()))
	}
	//fmt.Print("input: ", input.String(), "\n")
	//
	t0 := time.Now()
	result, err := client.Scan(ctx, input)
	t1 := time.Now()
	if err != nil {
		return newDBSysErr("exSingleScan", "Scan", err)
	}

	// save LastEvaluatedKey
	if lek := result.LastEvaluatedKey; len(lek) == 0 {

		q.SetPgStateValS("")
		q.SetPgStateValI(nil)
		q.SetEOD()

	} else {

		q.SetPgStateValS(stringifyPgState(result.LastEvaluatedKey))
		q.SetPgStateValI(result.LastEvaluatedKey)
	}

	dur := t1.Sub(t0)
	dur_ := dur.String()
	if dot := strings.Index(dur_, "."); dur_[dot+2] == 57 {
		cc_ := ConsumedCapacity_{result.ConsumedCapacity}
		syslog(fmt.Sprintf("exSingleScan:consumed capacity for Scan  %s. ItemCount %d  Duration: %s", cc_.String(), len(result.Items), dur_))
	}
	//
	syslog(fmt.Sprintf("exSingleScan: thread: %d  len(result.Items)  %d", q.Worker(), len(result.Items)))

	if len(result.Items) > 0 {

		err = attributevalue.UnmarshalListOfMaps(result.Items, q.GetFetch())
		if err != nil {
			return newDBUnmarshalErr("exSingleScan", "", "", "UnmarshalListOfMaps", err)
		}

	} else {

		// zero out q.fetch (client select variable) if populated
		if reflect.ValueOf(q.GetFetch()).Elem().Kind() == reflect.Slice {

			if reflect.ValueOf(q.GetFetch()).Elem().Len() > 0 {

				t := reflect.ValueOf(q.GetFetch()).Type().Elem()
				n := reflect.New(t) // *slice to empty slice
				q.SetFetchValue(n.Elem())

			} else {
				fmt.Println("=== do nothing ====")
			}
		} else {
			panic(fmt.Errorf("Expected a slice for scan output variable."))
		}
	}

	//save query statistics
	stats.SaveQueryStat(stats.Scan, q.Tag, result.ConsumedCapacity, result.Count, result.ScannedCount, dur)

	return nil
}

// exWorkerScan used by parallel scan`
func exWorkerScan(ctx context.Context, client DynamodbHandle, q *query.QueryHandle, proj *expression.ProjectionBuilder) error {

	syslog(fmt.Sprintf("exWorkerScan: thread %d  totalSegments %d", q.Worker(), q.GetParallel()))
	fmt.Sprintf("exWorkerScan: thread %d totalSegments %d", q.Worker(), q.GetParallel())
	if q.IsRestart() {
		// read StateVal from table using q.GetStartVal
		// parse contents into map[string]types.AttributeValue
		syslog(fmt.Sprintf("exWorkerScan: restart"))
		q.SetPgStateValI(unmarshalPgState(getPgState(ctx, q.PgStateId(), q.Worker())))
		q.SetRestart(false)

	} else {

		// if we have got here (without error) then we must have successfully processed last batch, so commit its savepoint (lastevaluatedkey)
		// if we don't get to save this state then the last batch will be reprocessed - as Elasticsearch is idempotent this is not an issue.
		// saving lastEvaluatedKey minimises the amount of reprocessing in the event of an interruption to the ES load.
		if len(q.PgStateValS()) > 0 {
			syslog(fmt.Sprintf("exWorkerScan: load pg state..."))
			err := savePgState(ctx, client, q.PgStateId(), q.PgStateValS(), q.Worker())
			if err != nil {
				panic(err)
			}
		}
	}

	var flt, f expression.ConditionBuilder
	if proj == nil {
		return fmt.Errorf("Select must be specified in a Scan")
	}
	for i, n := range q.GetFilterAttr() {
		if i == 0 {
			switch n.ComparOpr() {
			case query.BEGINSWITH:
				flt = expression.BeginsWith(expression.Name(n.Name()), n.Value().(string))
			case query.GT:
				flt = expression.GreaterThan(expression.Name(n.Name()), expression.Value(n.Value()))
			case query.LT:
				flt = expression.LessThan(expression.Name(n.Name()), expression.Value(n.Value()))
			case query.EQ:
				flt = expression.Equal(expression.Name(n.Name()), expression.Value(n.Value()))
			case query.NE:
				flt = expression.Equal(expression.Name(n.Name()), expression.Value(n.Value()))
			default:
				panic(fmt.Errorf(fmt.Sprintf("Comparitor %q not supported", n.ComparOpr())))
			}

		} else {

			switch n.ComparOpr() {
			case query.BEGINSWITH:
				f = expression.BeginsWith(expression.Name(n.Name()), n.Value().(string))
			case query.GT:
				f = expression.GreaterThan(expression.Name(n.Name()), expression.Value(n.Value()))
			case query.LT:
				f = expression.LessThan(expression.Name(n.Name()), expression.Value(n.Value()))
			case query.EQ:
				f = expression.Equal(expression.Name(n.Name()), expression.Value(n.Value()))
			case query.NE:
				f = expression.Equal(expression.Name(n.Name()), expression.Value(n.Value()))
			default:
				panic(fmt.Errorf(fmt.Sprintf("xComparitor %q not supported", n.ComparOpr())))
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
		return newDBExprErr("exWorkerScan", "", "", err)
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
	if lk := q.PgStateValI(); lk != nil {
		//	q.FetchState()
		input.ExclusiveStartKey = lk.(map[string]types.AttributeValue)
		//syslog(fmt.Sprintf("exWorkerScan: SetExclusiveStartKey %s", input.String()))
	}

	input.Segment = aws.Int32(int32(q.Worker()))            //int64(q.Worker())
	input.TotalSegments = aws.Int32(int32(q.GetParallel())) //int64(q.GetParallel())
	//syslog(fmt.Sprintf("exWorkerScan: thread %d  input: %s", q.Worker(), input.String()))
	//
	t0 := time.Now()
	result, err := client.Scan(ctx, input)
	t1 := time.Now()
	if err != nil {
		return newDBSysErr("exWorkerScan", "Scan", err)
	}
	// save LastEvaluatedKey
	if lek := result.LastEvaluatedKey; len(lek) == 0 {

		q.SetPgStateValS("")
		q.SetPgStateValI(nil)
		q.SetEOD()

	} else {

		q.SetPgStateValS(stringifyPgState(result.LastEvaluatedKey))
		q.SetPgStateValI(result.LastEvaluatedKey)
	}

	dur := t1.Sub(t0)
	dur_ := dur.String()
	if dot := strings.Index(dur_, "."); dur_[dot+2] == 57 {
		syslog(fmt.Sprintf("exWorkerScan:consumed capacity for Scan  %s. ItemCount %d  Duration: %s", ConsumedCapacity_{result.ConsumedCapacity}.String(), len(result.Items), dur_))
	}
	//
	syslog(fmt.Sprintf("exWorkerScan: thread: %d  len(result.Items)  %d", q.Worker(), len(result.Items)))

	if len(result.Items) > 0 {

		err = attributevalue.UnmarshalListOfMaps(result.Items, q.GetFetch())
		if err != nil {
			return newDBUnmarshalErr("exWorkerScan", "", "", "UnmarshalListOfMaps", err)
		}

	} else {

		// zero out q.fetch (client select variable) if populated
		if reflect.ValueOf(q.GetFetch()).Elem().Kind() == reflect.Slice {

			if reflect.ValueOf(q.GetFetch()).Elem().Len() > 0 {

				t := reflect.ValueOf(q.GetFetch()).Type().Elem()
				n := reflect.New(t) // *slice to empty slice
				q.SetFetchValue(n.Elem())

			} else {
				fmt.Println("=== do nothing ====")
			}
		} else {
			panic(fmt.Errorf("Expected a slice for scan output variable."))
		}
	}

	//save query statistics
	stats.SaveQueryStat(stats.Scan, q.Tag, result.ConsumedCapacity, result.Count, result.ScannedCount, dur)

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
			lek.WriteString(uuid.UID(b.Value).String())
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

func savePgState(ctx context.Context, client DynamodbHandle, id uuid.UID, val string, worker ...int) error {

	var state string = "LastEvaluatedKey"

	syslog(fmt.Sprintf("savePgState id: %s ", id.Base64()))

	m := mut.NewInsert("pgState")
	if len(worker) > 0 {
		state += "-w" + strconv.Itoa(worker[0])
	}
	m.AddMember("Id", id, mut.IsKey).AddMember("Name", state, mut.IsKey).AddMember("Value", val).AddMember("Updated", "$CURRENT_TIMESTAMP$")

	// add single mutation to mulitple-mutation configuration usually performed within a tx.
	mt := mut.Mutations([]dbs.Mutation{m})
	bs := []*mut.Mutations{&mt}

	return execTransaction(ctx, client.Client, bs, "internal-state", StdAPI)

}

func getPgState(ctx context.Context, id uuid.UID, worker ...int) string {

	var state string = "LastEvaluatedKey"

	if len(worker) > 0 {
		state += "-w" + strconv.Itoa(worker[0])
	}

	syslog(fmt.Sprintf("getPgState id: %s ", id.String()))

	type Val struct {
		Value string
	}
	var val Val

	q := query.New("pgState", "getPgState")
	q.Select(&val).Key("Id", id).Key("Name", state)

	err := executeQuery(ctx, q)
	if err != nil {
		panic(err)
	}
	return val.Value

}

// unmarshalPgState takes scan state data from bundles it into map[string]types.AttributeValue
//	4{S : {  S: "17 June 1986"} }{PKey : {  B: "WbjVdFJGTWGpqRpUTEPcPg==" } }{SortK : { S: "r|A#A#:D"} }{P : {  S: "r|DOB"} }
func unmarshalPgState(in string) map[string]types.AttributeValue {
	var (
		attr string
		tok  string
	)
	mm := make(map[string]types.AttributeValue)

	alertlog(fmt.Sprintf("unmarshalPgState: %s ", in))
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
			u := s.TokenText()[1 : len(s.TokenText())-1] // remove surrounding double quotes
			uid := uuid.FromString(u)
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

	syslog(fmt.Sprintf("unmarshalPgState  %s", stringifyPgState(mm)))

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
