//go:build dynamodb
// +build dynamodb

package dynamodb

import (
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/ros2hp/method-db/dynamodb/internal/param"
	"github.com/ros2hp/method-db/mut"
	"github.com/ros2hp/method-db/uuid"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	// "github.com/aws/aws-sdk-go/aws"
	// "github.com/aws/aws-sdk-go/service/dynamodb"
)

func marshalMutationKeys(m *mut.Mutation) (map[string]types.AttributeValue, error) {
	return marshalAV_(m, m.GetKeys())
}

func marshalMutation(m *mut.Mutation) (map[string]types.AttributeValue, error) {
	return marshalAV_(m, m.GetMembers())
}

func marshalAV_(m *mut.Mutation, ms []mut.Member) (map[string]types.AttributeValue, error) {

	var (
		err  error
		item map[string]types.AttributeValue
	)

	item = make(map[string]types.AttributeValue, len(m.GetMembers()))

	for _, col := range ms {

		if col.Name == "__" {
			continue
		}
		//item[col.Name], err = marshalAvUsingName(col.Name, col.Value)
		item[col.Name] = marshalAvUsingValue(col.Value)
		// if err != nil {
		// 	return nil, err
		// }
	}
	return item, err
}

func marshalAvUsingValue(val interface{}) types.AttributeValue {

	switch x := val.(type) {

	case uuid.UID:
		return &types.AttributeValueMemberB{Value: x}

	case []byte:
		return &types.AttributeValueMemberB{Value: x}

	case string:
		ss := val.(string)
		if ss == "$CURRENT_TIMESTAMP$" {
			tz, _ := time.LoadLocation(param.TZ)
			ss = time.Now().In(tz).String()
		}
		return &types.AttributeValueMemberS{Value: ss}

	case float64:
		s := strconv.FormatFloat(x, 'g', -1, 64)
		return &types.AttributeValueMemberN{Value: s}

	case int64:
		s := strconv.FormatInt(x, 10)
		return &types.AttributeValueMemberN{Value: s}

	case int32:
		s := strconv.FormatInt(int64(x), 10)
		return &types.AttributeValueMemberN{Value: s}

	case int:
		s := strconv.Itoa(x)
		return &types.AttributeValueMemberN{Value: s}

	case bool:
		bl := val.(bool)
		return &types.AttributeValueMemberBOOL{Value: bl}

	case []uuid.UID:
		lb := make([]types.AttributeValue, len(x), len(x))

		for i, v := range x {
			lb[i] = &types.AttributeValueMemberB{Value: v}
		}
		return &types.AttributeValueMemberL{Value: lb}

	case [][]byte:
		lb := make([]types.AttributeValue, len(x), len(x))

		for i, v := range x {
			lb[i] = &types.AttributeValueMemberB{Value: v}
		}
		return &types.AttributeValueMemberL{Value: lb}

	case []string:
		// represented as List of string

		lb := make([]types.AttributeValue, len(x), len(x))
		for i, s := range x {
			ss := s
			lb[i] = &types.AttributeValueMemberS{Value: ss}
		}
		return &types.AttributeValueMemberL{Value: lb}

	case []int64:
		// represented as List of int64

		lb := make([]types.AttributeValue, len(x), len(x))
		for i, v := range x {
			s := strconv.FormatInt(v, 10)
			lb[i] = &types.AttributeValueMemberN{Value: s}
		}
		return &types.AttributeValueMemberL{Value: lb}

	case []bool:
		lb := make([]types.AttributeValue, len(x), len(x))
		for i, v := range x {
			bl := v
			lb[i] = &types.AttributeValueMemberBOOL{Value: bl}
		}
		return &types.AttributeValueMemberL{Value: lb}

	case []float64:
		lb := make([]types.AttributeValue, len(x), len(x))
		for i, v := range x {
			s := strconv.FormatFloat(v, 'g', -1, 64)
			lb[i] = &types.AttributeValueMemberN{Value: s}
		}
		return &types.AttributeValueMemberL{Value: lb}

	case time.Time:
		//ss := x.Format(time.UnixDate)
		ss := x.String()
		return &types.AttributeValueMemberS{Value: ss}

	default:

		panic(fmt.Errorf("val type %T not supported", val))

	}
	return nil
}

func marshalAvUsingName(name string, val interface{}) (types.AttributeValue, error) {

	switch name {

	case "Pkey", "B": // []byte
		//	return &types.AttributeValue{B: val.([]byte)}, nil
		return &types.AttributeValueMemberB{Value: val.([]byte)}, nil

	case "Sortk", "S", "Ty": // string
		s := val.(string)
		//return &types.AttributeValue{S: &s}, nil
		return &types.AttributeValueMemberS{Value: s}, nil

	case "P": //string
		//return &types.AttributeValue{S: aws.String(val.(string))}, nil
		return &types.AttributeValueMemberS{Value: val.(string)}, nil

	case "F": // float64
		f := val.(string)
		// if s, err := strconv.ParseFloat(f, 64); err != nil {
		// 	syslog(fmt.Sprintf("Error converting float64 to string in marshalAV(): %s", err))
		// 	panic(err)
		// }
		//return &types.AttributeValue{N: &f}, nil
		return &types.AttributeValueMemberN{Value: f}, nil

	case "I", "CNT", "ASZ": // int64
		i := val.(string)
		// if s, err := strconv.ParseInt(i, 10, 64); err != nil {
		// 	syslog(fmt.Sprintf("Error converting int64 to string in marshalAV(): %s", err))
		// 	panic(err)
		// }
		//return &types.AttributeValue{N: &i}, nil
		return &types.AttributeValueMemberN{Value: i}, nil

	case "Bl": // bool
		bl := val.(bool)
		//return &types.AttributeValue{BOOL: &bl}, nil
		return &types.AttributeValueMemberBOOL{Value: bl}, nil

	case "DT":
		//TODO: implement

	case "LB", "PBS", "BS", "Nd":
		bs := val.([][]byte)
		lb := make([]types.AttributeValue, len(bs), len(bs))

		for i, v := range bs {
			lb[i] = &types.AttributeValueMemberB{Value: v}
		}
		//return &types.AttributeValue{L: lb}, nil
		return &types.AttributeValueMemberL{Value: lb}, nil

	case "Xf", "XF", "Id", "LI":
		// represented as List of int64

		switch bi := val.(type) {
		case []string:
			lb := make([]types.AttributeValue, len(bi), len(bi))
			for i, s := range bi {
				lb[i] = &types.AttributeValueMemberN{Value: s}
			}
			//return &types.AttributeValue{L: lb}, nil
			return &types.AttributeValueMemberL{Value: lb}, nil

		case []int64:
			lb := make([]types.AttributeValue, len(bi), len(bi))
			for i, v := range bi {
				s := strconv.FormatInt(v, 10)
				lb[i] = &types.AttributeValueMemberN{Value: s}
			}
			//return &types.AttributeValue{L: lb}, nil
			return &types.AttributeValueMemberL{Value: lb}, nil
		}

	case "LBl": // []bool
		// represent as list of bool
		bi := val.([]bool)
		lb := make([]types.AttributeValue, len(bi), len(bi))

		for i, v := range bi {
			bl := v
			lb[i] = &types.AttributeValueMemberBOOL{Value: bl}
		}
		//return &types.AttributeValue{L: lb}, nil
		return &types.AttributeValueMemberL{Value: lb}, nil

	case "LF": // []float64 or []string
		switch bi := val.(type) {
		case []string:
			lb := make([]types.AttributeValue, len(bi), len(bi))
			for i, s := range bi {
				lb[i] = &types.AttributeValueMemberN{Value: s}
			}
			//return &types.AttributeValue{L: lb}, nil
			return &types.AttributeValueMemberL{Value: lb}, nil

		case []float64:
			lb := make([]types.AttributeValue, len(bi), len(bi))
			for i, v := range bi {
				s := strconv.FormatFloat(v, 'g', -1, 64)
				lb[i] = &types.AttributeValueMemberN{Value: s}
			}
			//return &types.AttributeValue{L: lb}, nil
			return &types.AttributeValueMemberL{Value: lb}, nil
		}

	case "LS": // []string
		// represent as list of bool
		bs := val.([]string)
		ls := make([]types.AttributeValue, len(bs), len(bs))

		for i, v := range bs {
			ls[i] = &types.AttributeValueMemberS{Value: v}
		}
		//return &types.AttributeValue{L: ls}, nil
		return &types.AttributeValueMemberL{Value: ls}, nil

	}
	return nil, nil
}

// func many(i interface{}) (many bool) {

// 	t:=i.TypeOf()
// 	if t.KindOf() != reflect.Ptr {
// 		panic(fmt.Errof("marshalInput: argument must be a pointer"))
// 	}
// 	t2:=t.Elem()
// 	if ty.KindOf() == reflect.Slice {
// 		many=true
// 	}
// 	return
// }

func getField(i interface{}) ([]string, error) {

	t := reflect.TypeOf(i)
	if t.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("getField: argument must be a pointer")
	}
	t2 := t.Elem()
	if t2.Kind() == reflect.Slice {
		t2 = t2.Elem()
	}
	if t2.Kind() != reflect.Struct {
		return nil, fmt.Errorf("getField: base ype must be a struct")
	}
	var flds []string
	for i := 0; i < t2.NumField(); i++ {
		flds = append(flds, t2.Field(i).Name)
	}
	return flds, nil
}
