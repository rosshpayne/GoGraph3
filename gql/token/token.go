package token

import (
	"strings"
)

type TokenType string
type Keyword string

const (
	ILLEGAL = "ILLEGAL"
	EOF     = "EOF"
)

// not all the following values are relevant to GoGraph's GQL
const (
	IDENT     = "IDENT"
	INT       = "Int"    // 1343456
	FLOAT     = "Float"  // 3.42
	STRING    = "String" // contents between " or """
	RAWSTRING = "RAWSTRING"
	NULL      = "Null"
	// GQL Input Values types
	FUNC    = "func"
	FILTER  = "filter"
	BOOLEAN = "B"
	// Operators
	ASSIGN   = "="
	PLUS     = "+"
	MINUS    = "-"
	BANG     = "!"
	MULTIPLY = "*"
	DIVIDE   = "/"
	// Modifier
	MODIFIER = "m"
	FIRST    = "first"
	// Boolean operators
	AND = "AND"
	OR  = "OR"
	NOT = "NOT"

	AS = "as"

	TRUE  = "true"
	FALSE = "false"

	LPAREN   = "("
	RPAREN   = ")"
	LBRACE   = "{"
	RBRACE   = "}"
	LBRACKET = "["
	RBRACKET = "]"

	ATSIGN = "@"
	DOLLAR = "$"
	EXPAND = "..."
	COLON  = ":"
	// delimiters
	RAWSTRINGDEL = `"""`
	STRINGDEL    = `"`

	BOM = "BOM"

	// predicate
	UID = "uid"

	// Function categories
	TWOARGFUNC    = "F2ARG"
	SINGLEARGFUNC = "F1ARG"
	// Two Arg Funcs
	EQ         = "eq"
	LE         = "le"
	GE         = "ge"
	LT         = "lt"
	GT         = "gt"
	ANYOFTERMS = "anyofterms"
	ALLOFTERMS = "allofterms"
	// Single Arg Func
	HAS   = "has"
	VAL   = "val"
	COUNT = "count"
	//
	AGFUNC = "AGGR"
	AVG    = "avg"
	MIN    = "min"
	MAX    = "max"
	SUM    = "sum"
)

type Pos struct {
	Line int
	Col  int
}

// Token is exposed via token package so lexer can create new instanes of this type as required.
type Token struct {
	Type    TokenType
	Literal string // string value of token - rune, string, int, float, bool
	Loc     Pos    // start position of token
	Illegal bool
}

// These are the actual tokens relevant to GoGraph's GQL
var keywords = map[Keyword]struct {
	Type TokenType
}{
	"and":    {AND},
	"or":     {OR},
	"not":    {NOT},
	"filter": {FILTER},
	"func":   {FUNC},
	"true":   {BOOLEAN},
	"false":  {BOOLEAN},
	// functions that accept <predicate,value>
	"eq":         {TWOARGFUNC},
	"le":         {TWOARGFUNC},
	"ge":         {TWOARGFUNC},
	"lt":         {TWOARGFUNC},
	"gt":         {TWOARGFUNC},
	"anyofterms": {TWOARGFUNC},
	"allofterms": {TWOARGFUNC},
	//functions that accept <predicate> ....
	"count": {SINGLEARGFUNC},
	"has":   {SINGLEARGFUNC},
	"val":   {SINGLEARGFUNC},
	//
	"uid": {UID},
	//
	"avg": {AGFUNC},
	"sum": {AGFUNC},
	"min": {AGFUNC},
	"max": {AGFUNC},
	//
	"first": {FIRST},
	"as":    {AS},
}

func LookupIdent(ident string) TokenType {
	if tok, ok := keywords[Keyword(strings.ToLower(ident))]; ok {
		return tok.Type
	}
	return IDENT
}
