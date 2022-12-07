//go:build dynamodb
// +build dynamodb

package db

import (
	"fmt"
	"strconv"
	"strings"
	"text/scanner"
)

func buildConditionExpr(src string, exprNames map[string]string) (string, int) {

	return buildFilterExpr(src, exprNames)
}

// validateWhere() validates where clause and builds attribute: map[string]strig and value map[string]attribuveValue (based on Value() arguments)
func buildFilterExpr(src string, exprNames map[string]string) (string, int) {
	// 1. validate
	// open paranthesis equals closed
	// count of ?
	// create attribute map and assign substitue variable

	// 2.convert this:
	// (`Name = ? and (Nd in (?,?) or Age > ? and Height < ?)
	// to this:
	//  `#a = :1 and #b in (:2,:3) or #c > :4 and #d < :5
	//
	// 3.populate these two variables
	// attributeNames: string[]string
	// attributeValues: string[]attributeValue
	//
	//   condition:
	//.      attr equality value
	//       value is a ?, identifer (func) containing ?
	//
	//       function()
	//
	// A condition that must be satisfied in order for a conditional DeleteItem to
	// succeed. An expression can contain any of the following:
	//
	// * Functions: <only in dml, not in filter expression>
	// attribute_exists | attribute_not_exists | attribute_type | contains |
	// begins_with | size These function names are case-sensitive.
	//
	// * Comparison
	// operators: = | <> | < | > | <= | >= | BETWEEN | IN
	//
	// * Logical operators: AND |
	// OR | NOT
	//
	//  condition and condition or condition
	////////////////////////////////////////////////////////
	// for DML operations:
	// condition-expression ::=
	//   operand comparator operand
	// | operand BETWEEN operand AND operand
	// | operand IN ( operand (',' operand (, ...) ))
	// | function
	// | condition AND condition
	// | condition OR condition
	// | NOT condition
	// | ( condition )

	var (
		parOpen, parClose, binds int
		sc                       scanner.Scanner
		sub                      rune = 'a'
		s                        strings.Builder
	)

	sc.Init(strings.NewReader(src))

	for tok := sc.Scan(); tok != scanner.EOF; tok = sc.Scan() {

		fmt.Printf("%s: %s\n", sc.Position, sc.TokenText())

		// TODO: check non-keys only

		l := strings.ToLower(sc.TokenText())
		switch l {
		case "(":
			s.WriteString(sc.TokenText())
			parOpen++
		case ")":
			s.WriteString(sc.TokenText())
			parClose++
		case "?":
			// bind variables
			binds++
			s.WriteString(":" + strconv.Itoa(binds))
		case ",", "+", "-", "*", "/", "<>", ">=", "<=", "<", ">", "=":
			s.WriteString(sc.TokenText())
		case "between", "in":
			s.WriteString(sc.TokenText())
		case "not", "or", "and":
			s.WriteString(sc.TokenText())
		case "attribute_exists", "attribute_not_exists", "attribute_type", "begins_with", "contains", "size":
			s.WriteString(sc.TokenText())
			for tok := sc.Scan(); tok != scanner.EOF; tok = sc.Scan() {
				// go to )
				s.WriteString(sc.TokenText())
				switch sc.TokenText() {
				case "(":
					parOpen++
				case ")":
					parClose++
				case "?":
					binds++
				}
				if sc.TokenText() == ")" {
					break
				}
			}
		default:
			// must be a table attribute
			fmt.Println("Must be an attribute: ", sc.TokenText())
			// add to ExpressionNames
			v := "#x" + string(sub)
			exprNames[sc.TokenText()] = v
			sub++
			if sub == 'z' {
				sub = 'a'
			}
			s.WriteString(v)
		}
		// add  whitespace
		s.WriteString(" ")
	}
	if parOpen != parClose {
		panic(fmt.Errorf("paranthesis do not match in query condition %q", s))
	}
	return s.String(), binds
}
