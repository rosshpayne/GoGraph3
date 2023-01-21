//go:build dynamodb
// +build dynamodb

package dynamodb

import (
	"fmt"
	"strconv"
	"strings"
	"text/scanner"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func buildConditionExpr(src string, exprNames map[string]string, exprValues map[string]types.AttributeValue) (string, int) {

	return buildFilterExpr(src, exprNames, exprValues)
}

// buildFilterExpr() validates MethodDB where()  and builds attribute: map[string]strig and value map[string]attribuveValue (based on Value() arguments)
func buildFilterExpr(src string, exprNames map[string]string, exprValues map[string]types.AttributeValue) (string, int) {
	// 1. validate
	// open paranthesis equals closed
	// count of ?
	// create attribute map and assign substitue variable

	// 2.convert this:
	// (`Name = ? and (Nd in (?,?) or Age > ? and Height < ?)
	// to this:
	//  `#a = :1 and #b in (:2,:3) or #c > :4 and #d < :5`
	//
	// (`Name = ? and (Nd in (3,?) or Age  > 55 and Height < ?)
	// to this:
	//  `#a = :3 and #b in (:1,:4) or #c > :2 and #d < :5`
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
	// * Logical operators: AND | OR | NOT
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
		genId                    []byte
		s                        strings.Builder
	)
	genId = append(genId, 'a'-1)

	// gen generates a name and value identifier, a..z, aa..az, ba..bz.,
	genId_ := func() []byte {
		// increment genId
		for i := len(genId) - 1; i >= 0; i-- {
			genId[i]++
			if genId[i] == 'z'+1 {
				if i == 0 && genId[0] == 'z'+1 {
					genId = append(genId, 'a')
					for ii := i; ii > -1; ii-- {
						genId[ii] = 'a'
					}
					break
				} else {
					genId[i] = 'a'
				}
			} else {
				break
			}
		}
		return genId
	}
	sc.Init(strings.NewReader(src))

	for tok := sc.Scan(); tok != scanner.EOF; tok = sc.Scan() {

		//fmt.Printf("%s: %s\n", sc.Position, sc.TokenText())

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
		default:
			// must be a table attribute or a literal
			//	fmt.Println("Must be an attribute: ", sc.TokenText(), tok)
			// add to ExpressionNames sc.TokenText()[0] == '"'
			if tok == scanner.String || tok == scanner.Int || tok == scanner.Float {
				v := ":v" + string(genId_())
				switch tok {
				case scanner.Int:
					av := new(types.AttributeValueMemberN)
					av.Value = sc.TokenText()
					exprValues[v] = av
				case scanner.Float:
					av := new(types.AttributeValueMemberN)
					av.Value = sc.TokenText()
					exprValues[v] = av
				case scanner.String:
					av := new(types.AttributeValueMemberS)
					av.Value = sc.TokenText()
					exprValues[v] = av
				}
				s.WriteString(v)

			} else {

				var n string
				var found bool
				for k, v := range exprNames {
					if v == sc.TokenText() {
						found = true
						n = k
					}
				}
				if !found {
					n = "#n" + string(genId_())
					exprNames[n] = sc.TokenText()
				}
				s.WriteString(n)
			}
		}
		// add  whitespace
		s.WriteString(" ")
	}
	if parOpen != parClose {
		panic(fmt.Errorf("paranthesis do not match in query condition %q", s))
	}
	return s.String(), binds
}
