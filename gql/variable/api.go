package variable

import (
	"fmt"
	//	"github.com/DynamoGraph/gql/ast"
)

type storeT map[string]*Item

var varStore storeT

type Item struct {
	Name  string
	Value interface{}
	//	Edge  ast.EdgeI
	Edge interface{}
}

func init() {
	varStore = make(storeT)
}
func (i *Item) Add() {
	varStore[i.Name] = i
}

func (i *Item) Set() {
	i.Value = varStore[i.Name]
}

func Get(n string) *Item {
	return varStore[n]

}

func GetInt(n string) (int, error) {

	if v, ok := varStore[n].Edge.(int); !ok {
		return 0, fmt.Errorf("variable %q is not an int", n)
	} else {
		return v, nil
	}

}

func GetString(n string) (string, error) {
	if v, ok := varStore[n].Edge.(string); !ok {
		return "", fmt.Errorf("variable %q is not a string", n)
	} else {
		return v, nil
	}

}

func GetAll(n string) ([]interface{}, error) {
	if v, ok := varStore[n].Edge.([]interface{}); !ok {
		return nil, fmt.Errorf("variable %q is not a string", n)
	} else {
		return v, nil
	}

}

func Count(n string) int {
	if v, ok := varStore[n].Edge.([]interface{}); !ok {
		return 0 //TODO process error
	} else {
		return len(v)
	}
}

func Avg(n string) int {
	if v, ok := varStore[n].Edge.([]interface{}); !ok {
		return 0
	} else {
		var sum int
		for _, k := range v {
			if i, ok := k.(int); !ok {
				panic(fmt.Errorf("variable: avg not an int"))
			} else {
				sum += i
			}
		}
		return int(sum / len(v))
	}
}
