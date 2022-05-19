//go:build dynamodb
// +build dynamodb

package db

import (
	"fmt"
	"time"

	//slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/db/stats"
	param "github.com/GoGraph/dygparam"
	"github.com/GoGraph/tx/mut"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

// removeAttr is now part of txUpdate()
func removeAttr(m *mut.Mutation, tag string) error {

	var (
		err  error
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
		if i == 0 {
			upd = expression.Remove(expression.Name(col.Name))
		} else {
			upd = upd.Remove(expression.Name(col.Name))
		}
	}

	expr, err = expression.NewBuilder().WithUpdate(upd).Build()
	if err != nil {
		return newDBExprErr("txUpdate", "", "", err)
	}

	av := make(map[string]*dynamodb.AttributeValue)

	// generate key AV
	for _, v := range m.GetKeys() {
		av[v.Name] = marshalAvUsingValue(v.Value)
	}
	//
	update := &dynamodb.UpdateItemInput{
		Key:                       av,
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: convertBSet2List(expr),
		UpdateExpression:          expr.Update(),
		//ConditionExpression:       expr.Condition(),
		TableName: aws.String(m.GetTable()),
	}
	{
		t0 := time.Now()
		uio, err := client.UpdateItem(update)
		t1 := time.Now()
		if err != nil {
			return newDBSysErr("removeAttr", "UpdateItem", err)
		}
		syslog(fmt.Sprintf("Single:consumed capacity for removeAttr (UpdateItem) %s.  Duration: %s", uio.ConsumedCapacity, t1.Sub(t0)))
		stats.SaveSingleStat(stats.Remove, tag, uio.ConsumedCapacity, t1.Sub(t0))
	}

	return nil

}

func updateItem(m *mut.Mutation, tag string) error {

	var (
		err  error
		c    string
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

		}

		if col.Name[0] == 'L' {
			c = "L*"
		} else {
			c = col.Name
		}

		switch c {

		case "Nd", "XF", "Id", "XBl", "L*":

			// ignore col.Opr and append

			if i == 0 {
				upd = expression.Set(expression.Name(col.Name), expression.ListAppend(expression.Name(col.Name), expression.Value(col.Value)))
			} else {
				upd = upd.Set(expression.Name(col.Name), expression.ListAppend(expression.Name(col.Name), expression.Value(col.Value)))
			}

		default:

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
	// add condition expression if defined
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
		return newDBExprErr("txUpdate", "", "", err)
	}

	av := make(map[string]*dynamodb.AttributeValue)

	// generate key AV
	for _, v := range m.GetKeys() {
		av[v.Name] = marshalAvUsingValue(v.Value)
	}
	//
	update := &dynamodb.UpdateItemInput{
		Key:                       av,
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: convertBSet2List(expr),
		UpdateExpression:          expr.Update(),
		ConditionExpression:       expr.Condition(),
		TableName:                 aws.String(m.GetTable()),
	}
	{
		t0 := time.Now()
		uio, err := client.UpdateItem(update)
		t1 := time.Now()
		if err != nil {
			return newDBSysErr("updateItem", "UpdateItem", err)
		}
		syslog(fmt.Sprintf("Single:consumed capacity for UpdateItem %s.  Duration: %s", uio.ConsumedCapacity, t1.Sub(t0)))
		stats.SaveSingleStat(stats.UpdateItem, tag, uio.ConsumedCapacity, t1.Sub(t0))

	}
	return nil

}

func putItem(m *mut.Mutation, tag string) error {

	av, err := marshalMutation(m)
	if err != nil {
		return err
	}
	//fmt.Printf("txPut: %#v\n table: %s\n", av, m.GetTable())
	//
	putii := &dynamodb.PutItemInput{
		Item:                   av,
		TableName:              aws.String(m.GetTable()),
		ReturnConsumedCapacity: aws.String("TOTAL"),
	}
	//
	t0 := time.Now()
	ret, err := client.PutItem(putii)
	t1 := time.Now()
	syslog(fmt.Sprintf("Single: consumed capacity for PutItem  %s. Duration: %s", ret.ConsumedCapacity, t1.Sub(t0)))
	if err != nil {
		return newDBSysErr("CreateOvflBatch", "PutItem", err)
	}
	stats.SaveSingleStat(stats.PutItem, tag, ret.ConsumedCapacity, t1.Sub(t0))

	return nil

}

func execStd(m *mut.Mutations, tag string) error {

	y := (*m)[0].(*mut.Mutation)

	switch y.GetOpr() {

	case mut.Update:

		return updateItem(y, tag)

	case mut.Insert, mut.Merge:

		return putItem(y, tag)

	case mut.Remove:

		return removeAttr(y, tag)

	default:
		panic(fmt.Errorf("db ExecSingle error: opr is not assigned"))
		return nil
	}

}

// TODO: implement DeleteItem
