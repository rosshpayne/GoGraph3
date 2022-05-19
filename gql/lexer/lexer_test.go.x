package lexer

import (
	"fmt"
	"testing"

	"github.com/DynamoGraph/gql/token"
)

func TestTwoFunc(t *testing.T) {
	input := `{
  me(func: eq(count(film.director, 14)) ) {`

	tests := []struct {
		expectedType    token.TokenType
		expectedLiteral string
	}{
		{token.LBRACE, "{"},
		{token.IDENT, "me"},
		{token.LPAREN, "("},
		{token.FUNC, "func"},
		{token.COLON, ":"},
		{token.RFUNC, "eq"},
		{token.LPAREN, "("},
		{token.MFUNC, "count"},
		{token.LPAREN, "("},
		{token.IDENT, "film.director"},
		{token.INT, "14"},
		{token.RPAREN, ")"},
		{token.RPAREN, ")"},
		{token.RPAREN, ")"},
		{token.LBRACE, "{"},
	}

	l := New(input)

	for i, tt := range tests {
		tok := l.NextToken()
		//	fmt.Printf("%v\n", tok)
		fmt.Println(tok.Literal)
		if tok.Type != tt.expectedType {
			t.Fatalf("tests[%d] - tokentype wrong. expected=%q, got=%q Error: %s",
				i, tt.expectedType, tok.Type, l.Error())
		}

		if tok.Literal != tt.expectedLiteral {
			t.Fatalf("tests[%d] - literal wrong. expected=%q, got=%q Error: %s",
				i, tt.expectedLiteral, tok.Literal, l.Error())
		}
	}
}

func TestBooleanExprLiterals(t *testing.T) {
	input := `true OR false`

	tests := []struct {
		expectedType    token.TokenType
		expectedLiteral string
	}{
		{token.BOOLEAN, "true"},
		{token.OR, "OR"},
		{token.BOOLEAN, "false"},
	}

	l := New(input)

	for i, tt := range tests {
		tok := l.NextToken()
		//	fmt.Printf("%v\n", tok)
		fmt.Println(tok.Literal)
		if tok.Type != tt.expectedType {
			t.Fatalf("tests[%d] - tokentype wrong. expected=%q, got=%q Error: %s",
				i, tt.expectedType, tok.Type, l.Error())
		}

		if tok.Literal != tt.expectedLiteral {
			t.Fatalf("tests[%d] - literal wrong. expected=%q, got=%q Error: %s",
				i, tt.expectedLiteral, tok.Literal, l.Error())
		}
	}
}
