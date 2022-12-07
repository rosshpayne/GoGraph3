//go:build dynamodb
// +build dynamodb

package mut

import (
	"reflect"
)

// IsArray determines if the value passed in is a slice and therefore will be used in a DYnamodb array type.
// TODO: Would be better to drive off the name of the attribute and from the table type definition determine its type.
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
