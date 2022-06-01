//go:build dynamodb
// +build dynamodb

package db

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/GoGraph/uuid"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

//type queryInput *dynamodb.QueryInput // cannot create a type as a pointer to another package type and create methods on the new type.
// type queryInput dynamodb.QueryInput

// func (q queryInput) String() string {
// 	var s strings.Builder

// 	s.WriteString("\nqueryInput: \n")
// 	if q.KeyConditionExpression != nil {
// 		s.WriteString(fmt.Sprintf("KeyConditionExpression: %s\n", *q.KeyConditionExpression))
// 	}
// 	if q.FilterExpression != nil {
// 		s.WriteString(fmt.Sprintf("FilterExpression: %s\n", *q.FilterExpression))
// 	}
// 	for k, v := range q.ExpressionAttributeNames {
// 		s.WriteString(fmt.Sprintf("ExpressionAttributeNames: %s %v\n", k, v))
// 	}
// 	for k, v := range q.ExpressionAttributeValues {
// 		switch x := v.(type) {
// 		case *types.AttributeValueMemberS:
// 			s.WriteString(fmt.Sprintf("ExpressionAttributeValues: %s %v\n", k, x.Value))
// 		}
// 	}
// 	if q.ProjectionExpression != nil {
// 		s.WriteString(fmt.Sprintf("ProjectionExpression: %s\n", *q.ProjectionExpression))
// 	}
// 	if q.IndexName != nil {
// 		s.WriteString(fmt.Sprintf("IndexName: %s\n", *q.IndexName))
// 	}
// 	if q.TableName != nil {
// 		s.WriteString(fmt.Sprintf("TableName: %s\n", *q.TableName))
// 	}
// 	return s.String()
// }

type dbQueryInput struct {
	*dynamodb.QueryInput
}

func (q dbQueryInput) String() string {
	var s strings.Builder

	s.WriteString("\nqueryInput: \n")
	if q.KeyConditionExpression != nil {
		s.WriteString(fmt.Sprintf("KeyConditionExpression: %s\n", *q.KeyConditionExpression))
	}
	if q.FilterExpression != nil {
		s.WriteString(fmt.Sprintf("FilterExpression: %s\n", *q.FilterExpression))
	}
	for k, v := range q.ExpressionAttributeNames {
		s.WriteString(fmt.Sprintf("ExpressionAttributeNames: %s %v\n", k, v))
	}
	for k, v := range q.ExpressionAttributeValues {
		switch x := v.(type) {
		case *types.AttributeValueMemberS:
			s.WriteString(fmt.Sprintf("ExpressionAttributeValues: %s %v\n", k, x.Value))
		}
	}
	if q.ProjectionExpression != nil {
		s.WriteString(fmt.Sprintf("ProjectionExpression: %s\n", *q.ProjectionExpression))
	}
	if q.IndexName != nil {
		s.WriteString(fmt.Sprintf("IndexName: %s\n", *q.IndexName))
	}
	if q.TableName != nil {
		s.WriteString(fmt.Sprintf("TableName: %s\n", *q.TableName))
	}
	return s.String()
}

type dbScanInput struct {
	*dynamodb.ScanInput
}

func (q dbScanInput) String() string {
	var s strings.Builder

	s.WriteString("\nScanInput: \n")

	if q.FilterExpression != nil {
		s.WriteString(fmt.Sprintf("FilterExpression: %s\n", *q.FilterExpression))
	}
	first := true
	for k, v := range q.ExpressionAttributeNames { // map[string]string
		if first {
			s.WriteString("ExpressionAttributeNames: ")
			first = false
		}
		s.WriteString(fmt.Sprintf("\n\t%s : %s ", k, v))
	}
	first = true
	for k, v := range q.ExpressionAttributeValues { //  map[string]types.AttributeValue
		if first {
			s.WriteString("ExpressionAttributeValues: ")
		}
		s.WriteString(fmt.Sprintf("\n\t %s : %s ", k, dbav{v}.String()))
	}
	if q.ProjectionExpression != nil {
		s.WriteString(fmt.Sprintf("ProjectionExpression: %s\n", *q.ProjectionExpression))
	}
	if q.IndexName != nil {
		s.WriteString(fmt.Sprintf("IndexName: %s\n", *q.IndexName))
	}
	if q.TableName != nil {
		s.WriteString(fmt.Sprintf("TableName: %s\n", *q.TableName))
	}
	return s.String()
}

type dbUpdateItemInput struct {
	*dynamodb.UpdateItemInput
}

// ExpressionAttributeNames:  op.Update.ExpressionAttributeNames,
// ExpressionAttributeValues: op.Update.ExpressionAttributeValues,
// UpdateExpression:          op.Update.UpdateExpression,
// ConditionExpression:       op.Update.ConditionExpression,

func (q dbUpdateItemInput) String() string {
	var s strings.Builder

	s.WriteByte('\n')
	s.WriteString("UpdateItemInput: ")
	s.WriteByte('\n')
	first := true
	for _, v := range q.Key { //  map[string]types.AttributeValue
		if first {
			s.WriteString("\nKeys: ")
		}
		s.WriteString(fmt.Sprintf("\n\t %s : %s", dbav{v}.String()))
	}
	first = true
	for k, v := range q.ExpressionAttributeNames { // map[string]string
		if first {
			s.WriteString("\nExpressionAttributeNames: ")
		}
		s.WriteString(fmt.Sprintf("\n\t%s : %s", k, v))
	}
	for k, v := range q.ExpressionAttributeValues { // map[string]types.AttributeValue
		if first {
			s.WriteString("\nExpressionAttributeValues: ")
		}
		s.WriteString(fmt.Sprintf("\n\t%s : %s", k, dbav{v}.String()))
	}
	if q.UpdateExpression != nil {
		s.WriteString(fmt.Sprintf("UpdateExpression: %s", *q.UpdateExpression))
	}
	if q.ConditionExpression != nil {
		s.WriteString(fmt.Sprintf("ConditionExpression: %s", *q.ConditionExpression))
	}
	if q.TableName != nil {
		s.WriteString(fmt.Sprintf("TableName: %s", *q.TableName))
	}
	return s.String()
}

type dbav struct {
	types.AttributeValue
}

func (d dbav) String() string {
	var s strings.Builder
	switch v := d.AttributeValue.(type) {
	case *types.AttributeValueMemberB: // []byte
		// is a UUID
		if len(v.Value) == 16 {
			return fmt.Sprintf("%s %s  [UID]", uuid.UID(v.Value).EncodeBase64(), strconv.Itoa(len(v.Value)))
		}
		if len(v.Value) < 8 {
			return fmt.Sprintf("%s %s", uuid.UID(v.Value[:]).EncodeBase64, strconv.Itoa(len(v.Value)))
		}
		return fmt.Sprintf("%s %s", uuid.UID(v.Value[:8]).EncodeBase64, strconv.Itoa(len(v.Value)))

	case *types.AttributeValueMemberBOOL:
		return fmt.Sprintf("%v", v.Value)

	case *types.AttributeValueMemberBS: //[][]byte

		for i, j := range v.Value {
			if i == 3 {
				s.WriteString(fmt.Sprintf("... [%d]", len(v.Value)))
			}
			if len(j) < 8 {
				s.WriteString(fmt.Sprintf("%s %s", uuid.UID(j[:]).EncodeBase64, strconv.Itoa(len(j))))
			} else {
				s.WriteString(fmt.Sprintf("%s %s", uuid.UID(j[:8]).EncodeBase64, strconv.Itoa(len(j))))
			}
			s.WriteString(", ")
		}
		return "BS: nil"

	case *types.AttributeValueMemberL:
		//	 Value is []types.AttributeValue
		for i, j := range v.Value {
			if i == 3 {
				break
			}
			s.WriteString(fmt.Sprintf("%s\n", dbav{j}.String()))
		}

	case *types.AttributeValueMemberM:
		_ = v.Value // Value is map[string]types.AttributeValue

	case *types.AttributeValueMemberN:
		_ = v.Value // Value is string

	case *types.AttributeValueMemberNS:
		_ = v.Value // Value is []string

	case *types.AttributeValueMemberNULL:
		_ = v.Value // Value is bool

	case *types.AttributeValueMemberS:
		_ = v.Value // Value is string

	case *types.AttributeValueMemberSS:
		_ = v.Value // Value is []string

	case *types.UnknownUnionMember:
		fmt.Println("unknown tag:", v.Tag)

	default:
		fmt.Println("union is nil or unknown type")

	}
	return s.String()
}
