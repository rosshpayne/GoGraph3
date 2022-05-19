package token

import (
	"strings"
)

type TokenType string

const (
	ILLEGAL = "ILLEGAL"
	EOF     = "EOF"
)
const (
	IDENT TokenType = "IDENT"

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

	// Boolean operators

	AND  = "AND"
	OR   = "OR"
	NOT  = "NOT"
	NOOP = "noop"

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
	UID    = "uid"
	UID_IN = "uid_in"

	// Functions
	RFUNC      = "RF"
	EQ         = "eq"
	LE         = "le"
	GE         = "ge"
	LT         = "lt"
	GT         = "gt"
	HAS        = "has"
	ANYOFTERMS = "anyofterms"
	ALLOFTERMS = "allofterms"
	// modifiers

	VAL   = "val"
	COUNT = "count"
	//
	AGFUNC = "AGGR"

	AVG = "avg"
	MIN = "min"
	MAX = "max"
	SUM = "sum"
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

var keywords = map[string]struct {
	Type TokenType
}{
	"and":    {AND},
	"or":     {OR},
	"not":    {NOT},
	"filter": {FILTER},
	"func":   {FUNC},
	"true":   {BOOLEAN},
	"false":  {BOOLEAN},
	"uid":    {UID},
	// suppored functions
	EQ:         {FUNC},
	LE:         {FUNC},
	GE:         {FUNC},
	LT:         {FUNC},
	GT:         {FUNC},
	ANYOFTERMS: {FUNC},
	ALLOFTERMS: {FUNC},
	// supported modifer funcs
	COUNT: {FUNC},
	VAL:   {VAL},
	HAS:   {FUNC},
	AVG:   {FUNC},
	SUM:   {FUNC},
	MIN:   {FUNC},
	MAX:   {FUNC},
	//
	"as": {AS},
}

func LookupIdent(ident string) TokenType {
	if tok, ok := keywords[strings.ToLower(ident)]; ok {
		return tok.Type
	}
	return IDENT
}
