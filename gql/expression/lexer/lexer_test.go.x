package lexer

import (
	"fmt"
	"testing"

	"github.com/GFilterExpressionParser/token"
)

func TestSimple(t *testing.T) {
	input := `@filter(le(initial_release_date, "2000"))`

	tests := []struct {
		expectedType    token.TokenType
		expectedLiteral string
	}{
		{token.ATSIGN, "@"},
		{token.FILTER, "filter"},
		{token.LPAREN, "("},
		{token.LE, "le"},
		{token.LPAREN, "("},
		{token.IDENT, "initial_release_date"},
		{token.STRING, "2000"},
		{token.RPAREN, ")"},
		{token.RPAREN, ")"},
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

func TestBooleanExpr(t *testing.T) {
	input := `@filter(allofterms(name, "jones indiana") OR allofterms(name@en, "jurassic park"))`

	tests := []struct {
		expectedType    token.TokenType
		expectedLiteral string
	}{
		{token.ATSIGN, "@"},
		{token.FILTER, "filter"},
		{token.LPAREN, "("},
		{token.ALLOFTERMS, "allofterms"},
		{token.LPAREN, "("},
		{token.IDENT, "name"},
		{token.STRING, "jones indiana"},
		{token.RPAREN, ")"},
		{token.OR, "OR"},
		{token.ALLOFTERMS, "allofterms"},
		{token.LPAREN, "("},
		{token.IDENT, "name"},
		{token.ATSIGN, "@"},
		{token.IDENT, "en"},
		{token.STRING, "jurassic park"},
		{token.RPAREN, ")"},
		{token.RPAREN, ")"},
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

func TestBooleanExprMixedCase(t *testing.T) {
	input := `@filter(allofterms(name, "jones indiana") Or allofterms(name@en, "jurassic park"))`

	tests := []struct {
		expectedType    token.TokenType
		expectedLiteral string
	}{
		{token.ATSIGN, "@"},
		{token.FILTER, "filter"},
		{token.LPAREN, "("},
		{token.ALLOFTERMS, "allofterms"},
		{token.LPAREN, "("},
		{token.IDENT, "name"},
		{token.STRING, "jones indiana"},
		{token.RPAREN, ")"},
		{token.OR, "Or"},
		{token.ALLOFTERMS, "allofterms"},
		{token.LPAREN, "("},
		{token.IDENT, "name"},
		{token.ATSIGN, "@"},
		{token.IDENT, "en"},
		{token.STRING, "jurassic park"},
		{token.RPAREN, ")"},
		{token.RPAREN, ")"},
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
		{token.TRUE, "true"},
		{token.OR, "OR"},
		{token.FALSE, "false"},
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
