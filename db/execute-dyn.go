//go:build dynamodb
// +build dynamodb

package db

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"text/scanner"
	"time"

	"github.com/GoGraph/db/stats"
	"github.com/GoGraph/dbs"
	//	"github.com/GoGraph/dbConn"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/uuid"
	//"github.com/GoGraph/tbl"
	param "github.com/GoGraph/dygparam"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/tx/query"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

func execute(ctx context.Context, client *dynamodb.DynamoDB, bs []*mut.Mutations, tag string, api API, opt ...Option) error {

	var err error

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

// convertBSet2List converts Binary Set to List because GoGraph expects a List type not a BS type
// Dynamodb's expression pkg creates BS rather than L for binary array data.
// As GoGraph had no user-defined types it is possible to hardwire in the affected attributes.
// All types in GoGraph are known at compile time.
func convertBSet2List(expr expression.Expression) map[string]*dynamodb.AttributeValue {

	var s strings.Builder
	values := expr.Values()

	for k, v := range expr.Names() {
		switch *v {
		//safe to hardwire in attribute name as all required List binaries are known at compile time.
		case "Nd", "LB":
			s.WriteByte(':')
			s.WriteByte(k[1])
			// check if BS is used and then convert if it is
			var nl []*dynamodb.AttributeValue

			for i, u := range values[s.String()].BS {
				if i == 0 {
					nl = make([]*dynamodb.AttributeValue, len(values[s.String()].BS), len(values[s.String()].BS))
				}
				nl[i] = &dynamodb.AttributeValue{B: u}
				if i == len(values[s.String()].BS)-1 {
					values[s.String()] = &dynamodb.AttributeValue{L: nl} // this nils AttributeValue{B }
				}
			}
			s.Reset()
		}
	}
	return values
}

// pkg db must support mutations, Insert, Update, Remove, Merge:
// any other mutations (eg WithOBatchLimit) must be defined outside of DB and passed in (somehow)

func txUpdate(m *mut.Mutation) (*dynamodb.TransactWriteItem, error) {

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

	av := make(map[string]*dynamodb.AttributeValue)

	// generate key AV
	for _, v := range m.GetKeys() {
		av[v.Name] = marshalAvUsingValue(v.Value)
	}
	//
	update := &dynamodb.Update{
		Key:                       av,
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: convertBSet2List(expr),
		UpdateExpression:          expr.Update(),
		ConditionExpression:       expr.Condition(),
		TableName:                 aws.String(m.GetTable()),
	}

	twi := &dynamodb.TransactWriteItem{}
	return twi.SetUpdate(update), nil

}

func txPut(m *mut.Mutation) (*dynamodb.TransactWriteItem, error) {

	av, err := marshalMutation(m)
	if err != nil {
		return nil, err
	}
	put := &dynamodb.Put{
		Item:      av,
		TableName: aws.String(m.GetTable()),
	}
	//
	twi := &dynamodb.TransactWriteItem{}

	return twi.SetPut(put), nil

}

func crTx(m *mut.Mutation, opr mut.StdMut) ([]*dynamodb.TransactWriteItem, error) {

	switch opr {

	case mut.Update: //, mut.Append:

		upd, err := txUpdate(m)
		if err != nil {
			return nil, err
		}
		return []*dynamodb.TransactWriteItem{upd}, nil

	case mut.Insert:

		put, err := txPut(m)
		if err != nil {
			return nil, err
		}
		return []*dynamodb.TransactWriteItem{put}, nil

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

		return []*dynamodb.TransactWriteItem{upd, put}, nil

	default:
		panic(fmt.Errorf("cannot mix a %q mutation with normal transaction based mutations ie. insert/update/remove/merge. Change to insert/delete or remove from current transaction.", opr))
		return nil, nil
	}

}

// execBatchMutations: note, all other mutations in this transaction must be either bulkinsert or bulkdelete.
// cannot mix with non-bulk requests ie. insert/update/delete/merge/remove
// NB: batch cannot make use of condition expressions
func execBatchMutations(ctx context.Context, client *dynamodb.DynamoDB, bi mut.Mutations, tag string) error {

	//type BatchWriteItemInput struct {
	//             RequestItems map[string][]*dynamodb.WriteRequest
	//             .. }
	//type dynamodb.WriteRequest {
	//            PutRequest *PutRequest
	//            DeleteRequest *DeleteRequest
	//                  }
	//type PutRequest {
	//             Item map[string]*dynamodb.AttributeValue
	//
	var (
		curTbl string
		wrtreq *dynamodb.WriteRequest
		api    stats.Source
	)

	muts := func(ri map[string][]*dynamodb.WriteRequest) int {
		muts := 0
		for _, v := range ri {
			muts += len(v)
		}
		return muts
	}

	//	var wrs []*dynamodb.WriteRequest

	reqi := make(map[string][]*dynamodb.WriteRequest)
	req := 0
	for _, m := range bi {

		m := m.(*mut.Mutation)
		av, err := marshalMutation(m)
		if err != nil {
			return newDBSysErr("genBatchInsert", "", err)
		}
		switch m.GetOpr() {
		case mut.Insert:
			wrtreq = &dynamodb.WriteRequest{PutRequest: &dynamodb.PutRequest{Item: av}}
			api = stats.BatchInsert
		case mut.Delete:
			wrtreq = &dynamodb.WriteRequest{DeleteRequest: &dynamodb.DeleteRequest{Key: av}}
			api = stats.BatchDelete
		default:
			panic(fmt.Errorf("Found %q amongst BulkInsert/BulkDelete requests. Do not mix bulk mutations with non-bulk requests", m.GetOpr()))
		}
		wrs := reqi[m.GetTable()]
		wrs = append(wrs, wrtreq)
		req++
		if req > param.MaxMutations {
			panic(fmt.Errorf("BulkMutations: exceeds %q items in a batch write. This should not happen as it should be caught in New*/Add operation", param.MaxMutations))
		}
		reqi[m.GetTable()] = wrs
		curTbl = m.GetTable()
		reqi[curTbl] = wrs
	}
	{
		t0 := time.Now()
		out, err := client.BatchWriteItem(&dynamodb.BatchWriteItemInput{RequestItems: reqi, ReturnConsumedCapacity: aws.String("INDEXES")})
		t1 := time.Now()
		stats.SaveBatchStat(api, tag, out.ConsumedCapacity, t1.Sub(t0), muts(reqi))
		if err != nil {
			return newDBSysErr("BatchWriteItem: ", "execBatchMutations", err)
		}

		// handle unprocessed items
		unProc := muts(out.UnprocessedItems)
		if unProc > 0 {
			delay := 100 // expotential backoff time (ms)
			retries := param.UnprocessedRetries
			curTot := len(bi)
			for i := 0; i < retries; i++ {
				// deloay retry
				time.Sleep(time.Duration(delay) * time.Millisecond)
				//
				slog.Log("dbExecute: ", fmt.Sprintf("%s into %s: Elapsed: %s Unprocessed: %d of %d [retry: %d]", api, curTbl, t1.Sub(t0).String(), unProc, curTot, i+1))
				curTot = unProc
				t0 = time.Now()
				out, err = client.BatchWriteItem(&dynamodb.BatchWriteItemInput{RequestItems: out.UnprocessedItems, ReturnConsumedCapacity: aws.String("INDEXES")})
				t1 = time.Now()
				stats.SaveBatchStat(api, tag, out.ConsumedCapacity, t1.Sub(t0), muts(out.UnprocessedItems))
				if err != nil {
					return newDBSysErr("Unprocessed Item", "execBatchMutations", err)
				}
				unProc = muts(out.UnprocessedItems)
				if unProc == 0 {
					break
				}
				delay *= 2
			}
			if unProc != 0 {
				nerr := UnprocessedErr{Remaining: muts(out.UnprocessedItems), Total: curTot, Retries: retries}
				return newDBSysErr(fmt.Sprintf("Failure to process all unprocessed Items after %d retries", retries), "execBatchMutations", nerr)
			}
			slog.Log("dbExecute:", fmt.Sprintf("%s [tbl: %s]: Processed all unprocessed items. Mutations %d  Elapsed: %s", api, curTot, unProc, t1.Sub(t0).String()))
			return nil
		}

		// log 30% of activity
		dur := t1.Sub(t0).String()
		if dot := strings.Index(dur, "."); dur[dot+2] == 57 && (dur[dot+3] == 54 || dur[dot+3] == 55) {
			slog.Log("dbExecute:", fmt.Sprintf("Bulk Insert [tbl: %s]: mutations %d  Elapsed: %s", curTbl, len(bi), dur))
		}
	}

	return nil
}

func execBatch(ctx context.Context, client *dynamodb.DynamoDB, bs []*mut.Mutations, tag string) error {
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
func execOptim(ctx context.Context, client *dynamodb.DynamoDB, bs []*mut.Mutations, tag string) error {

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

func execTransaction(ctx context.Context, client *dynamodb.DynamoDB, bs []*mut.Mutations, tag string, api API) error {

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
	//      ExpressionAttributeValues map[string]*dynamodb.AttributeValue
	//      Item                      map[string]*dynamodb.AttributeValue,
	//      TableName *string
	//
	// generate mutation as Dynamodb Put or Update.
	// note: if opr is merge will generate both, a Put and Update
	var (
		tx txWrite
		//
		btx       []txWrite
		twi, twi2 []*dynamodb.TransactWriteItem
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

		tx = txWrite{txwii: [2]*dynamodb.TransactWriteItemsInput{&dynamodb.TransactWriteItemsInput{TransactItems: twi, ReturnConsumedCapacity: aws.String("INDEXES")}, &dynamodb.TransactWriteItemsInput{TransactItems: twi2, ReturnConsumedCapacity: aws.String("INDEXES")}}}
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

			// if tag == "Target UPred" {
			// 	//fmt.Printf("btx %d: %#v\n", i, tx.txwii[0])
			// 	fmt.Printf("Transaction stmts for Target UPred: #stmts %d   %#v\n", len(tx.txwii[0].TransactItems), tx.txwii[0].TransactItems)
			// }
			t0 = time.Now()
			out, err := client.TransactWriteItems(tx.txwii[0])
			t1 = time.Now()
			if err != nil {

				if tag == "Target UPred" {
					panic(err)
				}

				switch t := err.(type) {

				case *dynamodb.TransactionCanceledException:

					// look for specific errors as code has bee designed to cater for these only
					for _, e := range t.CancellationReasons {

						switch *e.Code {

						case "None":

						case "ValidationError":

							// previous way of detecting merge alternate op required. Deprecated with conditional-put.
							// switch *e.Message {
							// case "The provided expression refers to an attribute that does not exist in the item":

							return fmt.Errorf("Transaction Validaation error %w. Item: %#v", errors.New(*e.Message), e.Item)

						case "ConditionalCheckFailed":

							// insert triggered by update with "attribute_exists(PKEY)" condition expression.
							// second transaction stream (idx 1) contains insert operations, as the second part of a merge operation.
							t0 = time.Now()
							out, err := client.TransactWriteItems(tx.txwii[1])
							t1 = time.Now()
							if err != nil {
								err := newDBSysErr("merge mutation: %q (part 2): %w", tag, err)
								return err
							}
							stats.SaveTransactStat(tag, out.ConsumedCapacity, t1.Sub(t0), len(tx.txwii[1].TransactItems))
							// return nil

						default:

							return errors.New(*e.Message)

						}
					}

				default:

					syslog(fmt.Sprintf("Transaction error. Not a TransactionCanceledException. Caught in default. %T %#v\n %#v\n", t, t, tx.txwii[0]))
					var e awserr.Error
					if errors.As(err, &e) {
						syslog(fmt.Sprintf("e: Error: [%s]\n Code: [%s]\n Message: [%s] \n", e.Error(), e.Code(), e.Message()))
					}
					return newDBSysErr("DB default case...", "", err)

				}

			} else {
				stats.SaveTransactStat(tag, out.ConsumedCapacity, t1.Sub(t0), len(tx.txwii[0].TransactItems))
			}

			// if dot := strings.Index(dur, "."); dur[dot+2] == 57 && (dur[dot+3] == 54) { //|| dur[dot+3] == 55) {
			// 	slog.Log("dbExecute: ", fmt.Sprintf("Transaction: mutations %d  Elapsed: %s ", len(tx.txwii[0].TransactItems), dur))
			// }

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
						}
						uii.SetReturnConsumedCapacity("INDEXES")

						t0 := time.Now()
						uio, err := client.UpdateItem(uii)
						t1 := time.Now()
						if err != nil {
							if aerr, ok := err.(awserr.Error); ok {
								if aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {

									// insert triggered by update with "attribute_exists(PKEY)" condition expression.
									// second transaction stream (i=1) contains insert operations, as the second part of a "merge" operation.
									idx += j
									mergeContinue = true
									// uio.ConsumedCapacity is nil for condition failures
									cc := &dynamodb.ConsumedCapacity{TableName: op.Update.TableName}
									stats.SaveStdStat(stats.UpdateItemCF, tag, cc, t1.Sub(t0))
									break
								}
								fmt.Println("tbl: ", *op.Update.TableName)
								fmt.Println("Key: ", op.Update.Key)
								for k, v := range op.Update.Key {
									if k == "PKey" {
										fmt.Printf("Pkey: [%s]\n", uuid.UID(v.B))
									} else {
										fmt.Printf("%s: [%v]\n", k, v)
									}
								}
								for k, v := range op.Update.ExpressionAttributeNames {
									fmt.Println("Names: ", k, *v)
								}
								fmt.Println("updExp: ", *op.Update.UpdateExpression)
								if op.Update.ConditionExpression != nil {
									fmt.Println("Cond: ", *op.Update.ConditionExpression)
								}

								return newDBSysErr("Execute std api", "UpdateItem?", err)

							} else {

								return newDBSysErr("Execute std api", "UpdateItem", err)
							}
						} else {
							//syslog(fmt.Sprintf("Execute std:consumed capacity for UpdateItem %s.  Duration: %s", uio.ConsumedCapacity, t1.Sub(t0)))
							stats.SaveStdStat(stats.UpdateItem, tag, uio.ConsumedCapacity, t1.Sub(t0))
						}
					}
					if op.Put != nil {
						pii := &dynamodb.PutItemInput{
							Item:      op.Put.Item,
							TableName: op.Put.TableName,
						}
						pii.SetReturnConsumedCapacity("INDEXES")
						fmt.Println("PutITemINput : ", pii.String())
						t0 := time.Now()
						uio, err := client.PutItem(pii)
						t1 := time.Now()
						if err != nil {
							if aerr, ok := err.(awserr.Error); ok {
								if aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
									// ignore condition check failures
									cc := &dynamodb.ConsumedCapacity{TableName: op.Update.TableName}
									stats.SaveStdStat(stats.PutItemCF, tag, cc, t1.Sub(t0))
									break
								}
								return newDBSysErr("Execute std api", "PutItem", err)

							} else {

								return newDBSysErr("Execute std api", "PutItem", err)
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
						}
						dii.SetReturnConsumedCapacity("INDEXES")

						t0 := time.Now()
						uio, err := client.DeleteItem(dii)
						t1 := time.Now()
						if err != nil {
							if aerr, ok := err.(awserr.Error); ok {
								if aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
									// ignore condition check failures
									cc := &dynamodb.ConsumedCapacity{TableName: op.Update.TableName}
									stats.SaveStdStat(stats.DeleteItemCF, tag, cc, t1.Sub(t0))
									break
								}
								return newDBSysErr("Execute std api", "DeleteItem", err)

							} else {

								return newDBSysErr("Execute std api", "DeleteItem", err)
							}
						} else {
							//syslog(fmt.Sprintf("Execute std api:consumed capacity for DeleteItem %s.  Duration: %s", uio.ConsumedCapacity, t1.Sub(t0)))
							stats.SaveStdStat(stats.DeleteItem, tag, uio.ConsumedCapacity, t1.Sub(t0))
						}
					}
				}
				if !mergeContinue {
					break
				}
			}
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

func genKeyAV(q *query.QueryHandle) (map[string]*dynamodb.AttributeValue, error) {

	var (
		err error
		av  map[string]*dynamodb.AttributeValue
	)

	av = make(map[string]*dynamodb.AttributeValue)

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

func executeQuery(q *query.QueryHandle, opt ...Option) error {

	var (
		// options
		err error
	)

	ctx := q.Ctx()
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

	case len(pk) != 0 && len(sk) != 0 && tbl.KeyCnt(q.GetTable()) == 2, len(pk) != 0 && len(sk) == 0 && tbl.KeyCnt(q.GetTable()) == 1:

		// if q.EqyCondition(pk) != query.EQ {
		// 	return fmt.Errorf("Partition Key must be defined with equality condition")
		// }
		//fmt.Println("===========GetItem=======")
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

//func exGetItem(q *query.QueryHandle, av []*dynamodb.AttributeValue) error {
func exGetItem(ctx context.Context, client DynamodbHandle, q *query.QueryHandle, av map[string]*dynamodb.AttributeValue, proj *expression.ProjectionBuilder) error {

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
	}
	input = input.SetTableName(string(q.GetTable())).SetReturnConsumedCapacity("INDEXES").SetConsistentRead(q.ConsistentMode())
	//
	//syslog(fmt.Sprintf("GetItem: %#v\n", input))
	t0 := time.Now()
	result, err := client.GetItem(input)
	t1 := time.Now()
	if err != nil {
		return newDBSysErr("exGetItem", "GetItem", err)
	}
	dur := t1.Sub(t0)
	dur_ := dur.String()
	if dot := strings.Index(dur_, "."); dur_[dot+2] == 57 {
		syslog(fmt.Sprintf("exGetItem:consumed capacity for GetItem  %s. Duration: %s", result.ConsumedCapacity.String(), dur_))
	}
	//
	if len(result.Item) == 0 {
		return query.NoDataFoundErr
	}
	err = dynamodbattribute.UnmarshalMap(result.Item, q.GetFetch())
	if err != nil {
		return newDBUnmarshalErr("xGetItem", "", "", "UnmarshalMap", err)
	}
	// save query statistics
	stats.SaveQueryStat(stats.GetItem, q.Tag, result.ConsumedCapacity, int64(len(result.Item)), 0, dur)

	return nil
}

func exQuery(ctx context.Context, client DynamodbHandle, q *query.QueryHandle, proj *expression.ProjectionBuilder) error {
	//
	var (
		keyc   expression.KeyConditionBuilder
		flt, f expression.ConditionBuilder
	)

	// pagination
	if q.IsRestart() {
		// read StateVal from table using q.GetStartVal
		// parse contents into map[string]*dynamodb.AttributeValue
		syslog(fmt.Sprintf("exQuery: restart"))
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
			keyc = expression.KeyAnd(keyc, expression.KeyBeginsWith(expression.Key(q.GetSK()), q.GetKeyValue(q.GetSK()).(string)))
		}
	}

	for i, n := range q.GetFilter() {
		if i == 0 {
			switch q.GetFilterComparOpr(n) {
			case query.GT:
				flt = expression.GreaterThan(expression.Name(n), expression.Value(q.GetFilterValue(n)))
			case query.LT:
				flt = expression.LessThan(expression.Name(n), expression.Value(q.GetFilterValue(n)))
			case query.EQ:
				flt = expression.Equal(expression.Name(n), expression.Value(q.GetFilterValue(n)))
			case query.NE:
				flt = expression.Equal(expression.Name(n), expression.Value(q.GetFilterValue(n)))
			default:
				panic(fmt.Errorf(fmt.Sprintf("Comparitor %q not supported", q.GetFilterComparOpr(n))))
			}

		} else {

			switch q.GetFilterComparOpr(n) {
			case query.GT:
				f = expression.GreaterThan(expression.Name(n), expression.Value(q.GetFilterValue(n)))
			case query.LT:
				f = expression.LessThan(expression.Name(n), expression.Value(q.GetFilterValue(n)))
			case query.EQ:
				f = expression.Equal(expression.Name(n), expression.Value(q.GetFilterValue(n)))
			case query.NE:
				f = expression.Equal(expression.Name(n), expression.Value(q.GetFilterValue(n)))
			default:
				panic(fmt.Errorf(fmt.Sprintf("xComparitor %q not supported", q.GetFilterComparOpr(n))))
			}
			flt = flt.And(f)
			// TODO: implement others, OR, NOT
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
	}
	input = input.SetTableName(string(q.GetTable())).SetReturnConsumedCapacity("INDEXES").SetConsistentRead(q.ConsistentMode())
	if q.IndexSpecified() {
		input = input.SetIndexName(string(q.GetIndex()))
	}
	if l := q.GetLimit(); l > 0 {
		input = input.SetLimit(int64(l))
	}
	if lk := q.PgStateValI(); lk != nil {
		input = input.SetExclusiveStartKey(lk.(map[string]*dynamodb.AttributeValue))
	}

	if q.SKset() {
		input = input.SetScanIndexForward(q.IsScanForwardSet())
	}
	//
	t0 := time.Now()
	result, err := client.Query(input)
	t1 := time.Now()
	if err != nil {
		return newDBSysErr("exQuery", "Query", err)
	}

	// pagination cont....
	if lek := result.LastEvaluatedKey; len(lek) == 0 {

		//EOD
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
		syslog(fmt.Sprintf("exQuery:consumed capacity for Query  %s. ItemCount %d  Duration: %s", result.ConsumedCapacity.String(), len(result.Items), dur_))
	}
	//
	if int(*result.Count) == 0 {
		return query.NoDataFoundErr
	}

	err = dynamodbattribute.UnmarshalListOfMaps(result.Items, q.GetFetch())
	if err != nil {
		return newDBUnmarshalErr("exQuery", "", "", "UnmarshalListOfMaps", err)
	}

	// save query statistics
	stats.SaveQueryStat(stats.Query, q.Tag, result.ConsumedCapacity, *result.Count, *result.ScannedCount, dur)

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

func exSingleScan(ctx context.Context, client DynamodbHandle, q *query.QueryHandle, proj *expression.ProjectionBuilder) error {

	syslog(fmt.Sprintf("exSingleScan: thread %d", q.Worker()))

	if q.IsRestart() {
		// read StateVal from table using q.GetStartVal
		// parse contents into map[string]*dynamodb.AttributeValue
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
	for i, n := range q.GetFilter() {

		if i == 0 {
			switch q.GetFilterComparOpr(n) {
			case query.GT:
				flt = expression.GreaterThan(expression.Name(n), expression.Value(q.GetFilterValue(n)))
			case query.LT:
				flt = expression.LessThan(expression.Name(n), expression.Value(q.GetFilterValue(n)))
			case query.EQ:
				flt = expression.Equal(expression.Name(n), expression.Value(q.GetFilterValue(n)))
			case query.NE:
				flt = expression.Equal(expression.Name(n), expression.Value(q.GetFilterValue(n)))
			default:
				panic(fmt.Errorf(fmt.Sprintf("Comparitor %q not supported", q.GetFilterComparOpr(n))))
			}

		} else {

			switch q.GetFilterComparOpr(n) {
			case query.GT:
				f = expression.GreaterThan(expression.Name(n), expression.Value(q.GetFilterValue(n)))
			case query.LT:
				f = expression.LessThan(expression.Name(n), expression.Value(q.GetFilterValue(n)))
			case query.EQ:
				f = expression.Equal(expression.Name(n), expression.Value(q.GetFilterValue(n)))
			case query.NE:
				f = expression.Equal(expression.Name(n), expression.Value(q.GetFilterValue(n)))
			default:
				panic(fmt.Errorf(fmt.Sprintf("xComparitor %q not supported", q.GetFilterComparOpr(n))))
			}
			flt = flt.And(f)
			// TODO: implement others, OR, NOT
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
		Select:                    aws.String("SPECIFIC_ATTRIBUTES"),
	}

	input = input.SetTableName(string(q.GetTable())).SetReturnConsumedCapacity("INDEXES").SetConsistentRead(q.ConsistentMode())
	if q.IndexSpecified() {
		input = input.SetIndexName(string(q.GetIndex()))
	}
	if l := q.GetLimit(); l > 0 {
		input = input.SetLimit(int64(l))
	}

	if lk := q.PgStateValI(); lk != nil {
		//	q.FetchState()
		input = input.SetExclusiveStartKey(lk.(map[string]*dynamodb.AttributeValue))
		syslog(fmt.Sprintf("exSingleScan: SetExclusiveStartKey %s", input.String()))

	}
	//fmt.Print("input: ", input.String(), "\n")
	//
	t0 := time.Now()
	result, err := client.Scan(input)
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
		syslog(fmt.Sprintf("exSingleScan:consumed capacity for Scan  %s. ItemCount %d  Duration: %s", result.ConsumedCapacity.String(), len(result.Items), dur_))
	}
	//
	syslog(fmt.Sprintf("exSingleScan: thread: %d  len(result.Items)  %d", q.Worker(), len(result.Items)))

	if len(result.Items) > 0 {

		err = dynamodbattribute.UnmarshalListOfMaps(result.Items, q.GetFetch())
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
	stats.SaveQueryStat(stats.Scan, q.Tag, result.ConsumedCapacity, *result.Count, *result.ScannedCount, dur)

	return nil
}

// exWorkerScan used by parallel scan`
func exWorkerScan(ctx context.Context, client DynamodbHandle, q *query.QueryHandle, proj *expression.ProjectionBuilder) error {

	syslog(fmt.Sprintf("exWorkerScan: thread %d  totalSegments %d", q.Worker(), q.GetParallel()))
	fmt.Sprintf("exWorkerScan: thread %d totalSegments %d", q.Worker(), q.GetParallel())
	if q.IsRestart() {
		// read StateVal from table using q.GetStartVal
		// parse contents into map[string]*dynamodb.AttributeValue
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
	for i, n := range q.GetFilter() {

		if i == 0 {
			switch q.GetFilterComparOpr(n) {
			case query.GT:
				flt = expression.GreaterThan(expression.Name(n), expression.Value(q.GetFilterValue(n)))
			case query.LT:
				flt = expression.LessThan(expression.Name(n), expression.Value(q.GetFilterValue(n)))
			case query.EQ:
				flt = expression.Equal(expression.Name(n), expression.Value(q.GetFilterValue(n)))
			case query.NE:
				flt = expression.Equal(expression.Name(n), expression.Value(q.GetFilterValue(n)))
			default:
				panic(fmt.Errorf(fmt.Sprintf("Comparitor %q not supported", q.GetFilterComparOpr(n))))
			}

		} else {

			switch q.GetFilterComparOpr(n) {
			case query.GT:
				f = expression.GreaterThan(expression.Name(n), expression.Value(q.GetFilterValue(n)))
			case query.LT:
				f = expression.LessThan(expression.Name(n), expression.Value(q.GetFilterValue(n)))
			case query.EQ:
				f = expression.Equal(expression.Name(n), expression.Value(q.GetFilterValue(n)))
			case query.NE:
				f = expression.Equal(expression.Name(n), expression.Value(q.GetFilterValue(n)))
			default:
				panic(fmt.Errorf(fmt.Sprintf("xComparitor %q not supported", q.GetFilterComparOpr(n))))
			}
			flt = flt.And(f)
			// TODO: implement others, OR, NOT
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
		Select:                    aws.String("SPECIFIC_ATTRIBUTES"),
	}

	input = input.SetTableName(string(q.GetTable())).SetReturnConsumedCapacity("INDEXES").SetConsistentRead(q.ConsistentMode())
	if q.IndexSpecified() {
		input = input.SetIndexName(string(q.GetIndex()))
	}
	if lk := q.PgStateValI(); lk != nil {
		//	q.FetchState()
		input = input.SetExclusiveStartKey(lk.(map[string]*dynamodb.AttributeValue))
		syslog(fmt.Sprintf("exWorkerScan: SetExclusiveStartKey %s", input.String()))

	}

	input = input.SetSegment(int64(q.Worker()))
	input = input.SetTotalSegments(int64(q.GetParallel()))
	syslog(fmt.Sprintf("exWorkerScan: thread %d  input: %s", q.Worker(), input.String()))
	//
	t0 := time.Now()
	result, err := client.Scan(input)
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
		syslog(fmt.Sprintf("exWorkerScan:consumed capacity for Scan  %s. ItemCount %d  Duration: %s", result.ConsumedCapacity.String(), len(result.Items), dur_))
	}
	//
	syslog(fmt.Sprintf("exWorkerScan: thread: %d  len(result.Items)  %d", q.Worker(), len(result.Items)))

	if len(result.Items) > 0 {

		err = dynamodbattribute.UnmarshalListOfMaps(result.Items, q.GetFetch())
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
	stats.SaveQueryStat(stats.Scan, q.Tag, result.ConsumedCapacity, *result.Count, *result.ScannedCount, dur)

	return nil
}

func stringifyPgState(d map[string]*dynamodb.AttributeValue) string {
	var lek strings.Builder
	lek.WriteString(strconv.Itoa(len(d)))
	for k, v := range d {
		lek.WriteByte('{')
		lek.WriteString(k)
		lek.WriteString(" : ")
		if len(v.B) != 0 {
			lek.WriteString("{ B : ")
			lek.WriteByte('"')
			lek.WriteString(uuid.UID(v.B).String())
			lek.WriteString(`" }`)
		} else {
			lek.WriteString(v.GoString())
		}
		lek.WriteString(" }")
	}
	return lek.String()
}

func savePgState(ctx context.Context, client DynamodbHandle, id uuid.UID, val string, worker ...int) error {

	var state string = "LastEvaluatedKey"

	//syslog(fmt.Sprintf("exScan: savePgState id: %s ", id.String()))

	m := mut.NewInsert("pgState")
	if len(worker) > 0 {
		state += "-w" + strconv.Itoa(worker[0])
	}
	m.AddMember("Id", id, mut.IsKey).AddMember("Name", state, mut.IsKey).AddMember("Value", val).AddMember("Updated", "$CURRENT_TIMESTAMP$")

	// add single mutation to mulitple-mutation configuration usually performed within a tx.
	mt := mut.Mutations([]dbs.Mutation{m})
	bs := []*mut.Mutations{&mt}

	return execTransaction(ctx, client.DynamoDB, bs, "internal-state", StdAPI)

}

func getPgState(ctx context.Context, id uuid.UID, worker ...int) string {

	var state string = "LastEvaluatedKey"

	if len(worker) > 0 {
		state += "-w" + strconv.Itoa(worker[0])
	}

	syslog(fmt.Sprintf("exScan: getPgState id: %s ", id.String()))

	type Val struct {
		Value string
	}
	var val Val

	q := query.NewCtx(ctx, "pgState", "getPgState")
	q.Select(&val).Key("Id", id).Key("Name", state)

	err := executeQuery(q)
	if err != nil {
		panic(err)
	}
	return val.Value

}

func unmarshalPgState(in string) map[string]*dynamodb.AttributeValue {
	var (
		attr string
		tok  string
	)
	mm := make(map[string]*dynamodb.AttributeValue)

	//	4{S : {  S: "17 June 1986"} }{PKey : {  B: "WbjVdFJGTWGpqRpUTEPcPg==" } }{SortK : { S: "r|A#A#:D"} }{P : {  S: "r|DOB"} }

	fmt.Println("in: ", in)
	var s scanner.Scanner
	s.Init(strings.NewReader(in))
	s.Scan()
	elements, err := strconv.ParseInt(s.TokenText(), 10, 32)
	if err != nil {
		panic(err)
	}
	var j int64
	for j = 0; j < elements; j++ {
		var av dynamodb.AttributeValue
		fmt.Println("loop: ", j)
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
			av = dynamodb.AttributeValue{S: aws.String(s)}
		case "B":
			u := s.TokenText()[1 : len(s.TokenText())-1] // remove surrounding double quotes
			av = dynamodb.AttributeValue{B: uuid.FromString(u)}
		case "N":
			n := s.TokenText()[1 : len(s.TokenText())-1] // remove surrounding double quotes
			av = dynamodb.AttributeValue{N: aws.String(n)}
		}

		mm[attr] = &av

		for i := 0; i < 2; i++ {
			s.Scan()
			switch s.TokenText() {
			case "}":
			default:
				panic(fmt.Errorf("expected } got %s", s.TokenText()))
			}
		}
	}

	syslog(fmt.Sprintf("exScan: unmarshalPgState  %s", stringifyPgState(mm)))

	return mm
}
