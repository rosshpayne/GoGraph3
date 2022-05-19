//go:build dynamodb
// +build dynamodb

package mut

import (
	"reflect"
)

func IsArray(value interface{}) bool {
	if value == nil {
		return false
	}
	if ty := reflect.TypeOf(value); ty.Kind() == reflect.Slice {
		// dynamodb "binary" type (represented as a []byte in go) is not an array but a primitive type
		if ty.Elem().Kind() != reflect.Uint8 {
			return true
		}
	}
	return false

}
