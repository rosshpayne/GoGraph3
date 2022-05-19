package expression

import (
	"testing"
)

func TestExecute(t *testing.T) {

	type testT struct {
		input  string
		result bool
	}

	var tests []testT = []testT{
		//		{input: `le(initial_release_date, "2000")`, result: 9},
		//		{input: `allofterms(name, "jones indiana") OR allofterms(name, "jurassic park")`, result: 0},
		{input: `false`, result: true},
		{input: `false OR NOT (false AND false)`, result: true},
		{input: `false OR NOT (true AND true)`, result: false},
		{input: `true OR NOT (true AND true)`, result: true},
		{input: `not false `, result: true},
		{input: `not true `, result: false},
		{input: `not ( false OR false) `, result: true},
		{input: `not ( false OR not false) `, result: false},
		{input: `( false OR not false) `, result: true},
		{input: `( not ( false OR not false) )`, result: false},
		{input: `false OR false`, result: false},
		{input: `false OR false`, result: false},
		{input: `true OR false`, result: true},
		{input: `(true OR false) AND false`, result: false},
		{input: `(true AND true) OR false`, result: true},
		{input: `(false AND false) OR true`, result: true},
		{input: `(false OR false) AND true`, result: false},
		{input: `(true OR false) AND true`, result: true},
		{input: `(true OR false) AND false`, result: false},
		{input: `true OR false AND false`, result: true},
		{input: `true OR (false AND false)`, result: true},
		{input: `true OR (false AND false) OR false `, result: true},
		{input: `not (true OR (false AND false) OR false )`, result: false},
		{input: `true OR false AND false `, result: true},
		{input: `(true OR false) AND false `, result: false},
		{input: `true OR false AND false OR false `, result: true},
		{input: `true OR false AND false AND false `, result: true},
	}
	for _, v := range tests {
		t.Log(v.input)
		expr := New(v.input)
		result := expr.Execute()
		if result == v.result {
			t.Log("*** PASSED - ", v.result)
		} else {
			t.Errorf("+++FAILED - Got %v  expected %v\n", result, v.result)
		}
	}
}
