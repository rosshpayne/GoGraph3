package lexer

import (
	"fmt"
	"unicode"
	"unicode/utf8"

	"github.com/GoGraph/gql/expression/token"
)

// Lexer parses an Input string (embedded in token pkg) and returns it as tokens - defined in token package.
type Lexer struct {
	//	Eloc  token.Pos // Loc of illegal char
	input string
	cLoc  int    // Current rune Loc in input
	rLoc  int    // Read rune Loc in input
	ch    rune   // current rune under examination, added to token during lex processings
	del   string // string delimeter
	Line  int
	Col   int // curren col Loc
	err   error
}

func (l *Lexer) CLoc() int {
	return l.cLoc
}

func (l *Lexer) Error() string {
	if l.err != nil {
		return l.err.Error()
	}
	return "NIL"
}

func (l *Lexer) Input() string {
	return l.input
}

func (l *Lexer) Loc() (int, int) {
	return l.Line, l.Col
}
func New(input string) *Lexer {
	l := &Lexer{input: input, Line: 1}
	l.readRune() // prime lexer struct
	return l
}

func (l *Lexer) AtPosition() string {
	return fmt.Sprintf("at line: %d, column: %d", l.Line, l.Col)
}

func (l *Lexer) NextToken() *token.Token {
	var tok *token.Token
	//	fmt.Printf("NextToken: %c\n", l.ch)
	l.skipWhitespace() // scan to next non-whitespace and return its value as a token
	//fmt.Printf("[%c]\n", l.ch)
	switch l.ch {
	case '\ufeff':
		tok = l.newToken(token.BOM, l.ch)
	case '#':
		l.readToEol()
		return l.NextToken()
	case '.': // ... expand sequence
		if l.peekRune() == '.' {
			//ch := l.ch
			l.readRune()
			if l.peekRune() == '.' {
				//ch := l.ch
				l.readRune()
				literal := token.EXPAND
				tok = &token.Token{Type: token.EXPAND, Literal: literal}
			} else {
				tok = l.newToken(token.ILLEGAL, l.ch)
			}
		} else {
			tok = l.newToken(token.ILLEGAL, l.ch)
		}
	case '"':
		if l.peekRune() == '"' {
			//ch := l.ch
			l.readRune()
			if l.peekRune() == '"' {
				l.readRune()
				l.del = token.RAWSTRINGDEL
				tok = l.readString()
			} else {
				tok = l.newToken(token.ILLEGAL, l.ch)
			}
		} else {
			l.del = token.STRINGDEL
			tok = l.readString()
		}
	// case ',':
	// 	tok = l.newToken(token.COMMA, l.ch)
	case '@':
		tok = l.newToken(token.ATSIGN, l.ch)
	case '*':
		tok = l.newToken(token.MULTIPLY, l.ch)
	case '+':
		tok = l.newToken(token.PLUS, l.ch)
	case '-':
		tok = l.newToken(token.MINUS, l.ch)
	case '/':
		tok = l.newToken(token.DIVIDE, l.ch)
	case '{':
		tok = l.newToken(token.LBRACE, l.ch)
	case '}':
		tok = l.newToken(token.RBRACE, l.ch)
	case '(':
		tok = l.newToken(token.LPAREN, l.ch)
	case ')':
		tok = l.newToken(token.RPAREN, l.ch)
	case '[':
		tok = l.newToken(token.LBRACKET, l.ch)
	case ']':
		tok = l.newToken(token.RBRACKET, l.ch)
	case '=':
		tok = l.newToken(token.ASSIGN, l.ch)
	case ':':
		tok = l.newToken(token.COLON, l.ch)
	case 0:
		tok = l.newToken(token.EOF, l.ch)
		tok.Literal = ""
		tok.Type = token.EOF
	default:
		if unicode.IsLetter(l.ch) || l.ch == '_' {
			tok = l.readIdentifier()
			tok.Type = token.LookupIdent(tok.Literal) // IDENT or Keyword
		} else if unicode.IsDigit(l.ch) || l.ch == '-' {
			tok = l.readNumber()
		} else {
			tok = l.newToken(token.ILLEGAL, l.ch)
		}
		return tok
	}
	if tok.Type != "ILLEGAL" {
		l.readRune() // prime l.ch
	}
	return tok
}

func (l *Lexer) skipWhitespace() {
	// Horizontal Tab (U+0009) Space (U+0020)
	// LineTerminator :: New Line (U+000A)
	//  Carriage Return (U+000D) [lookahead ≠ New Line (U+000A)] Carriage Return (U+000D) New Line (U+000A)
	for l.ch == '\u0009' || l.ch == '\u0020' || l.ch == '.' || l.ch == '\u000A' || l.ch == '\u000D' || l.ch == ',' {
		if l.ch == '\n' { // linefeed
			l.Line++
			l.Col = 0
		}
		l.readRune()
	}
}

func (l *Lexer) readRune() {
	// get next byte in string
	if l.rLoc >= len(l.input) {
		l.ch = 0
		l.cLoc++
	} else {
		var size int
		// TODO: check token type. Only comment and string need rune reads all others simple ascii will suffice
		l.ch, size = utf8.DecodeRuneInString(l.input[l.rLoc:])
		l.cLoc = l.rLoc
		l.rLoc += size
		if !(l.ch == '\n' || l.ch == '\r') {
			l.Col++
		}
		//	fmt.Printf("readRune: %c %d %d %d [%s]\n", l.ch, l.cLoc, l.rLoc, size, l.input[:l.rLoc])
	}

}

func (l *Lexer) LexRemaining() string {
	return l.input[l.rLoc:]
}

func (l *Lexer) LexSetPos(r, c int) {
	l.rLoc = r
	l.cLoc = c
}

func (l *Lexer) peekRune() rune {
	if l.rLoc >= len(l.input) {
		return 0
	} else {
		rn, _ := utf8.DecodeRuneInString(l.input[l.rLoc:])
		return rn
	}
}

func (l *Lexer) readIdentifier() *token.Token {
	start := token.Pos{l.Line, l.Col}
	Loc := l.cLoc
	for unicode.IsLetter(l.ch) || l.ch == '.' || l.ch == '_' || unicode.IsDigit(l.ch) {
		l.readRune()
	}
	return &token.Token{Type: token.STRING, Literal: l.input[Loc:l.cLoc], Loc: start}
}

func (l *Lexer) readNumber() *token.Token {
	var tokenT token.TokenType = token.INT
	var illegalT bool
	sLoc := l.cLoc
	start := token.Pos{l.Line, l.Col}
	if l.ch == '-' {
		//l.skipWhitespace()
		l.readRune()
	}
	for unicode.IsDigit(l.ch) {
		l.readRune()
	}
	tokenT = token.INT
	switch l.ch {
	case '.':
		tokenT = token.FLOAT
		l.readRune()
		for unicode.IsDigit(l.ch) {
			l.readRune()
		}
		if l.ch == 'e' || l.ch == 'E' {
			l.readRune()
			if l.ch == '-' || l.ch == '+' {
				l.readRune()
			}
			for unicode.IsDigit(l.ch) {
				l.readRune()
			}
		}

	case 'e', 'E':
		tokenT = token.FLOAT
		l.readRune()
		if l.ch == '-' || l.ch == '+' {
			l.readRune()
		}
		for unicode.IsDigit(l.ch) {
			l.readRune()
		}

	default: // all letters other than e E
		if unicode.IsLetter(l.ch) {
			//l.Eloc = token.Pos{l.Line, l.Col}
			// token is now interpreted as an illegal IDENT
			illegalT = true
			tokenT = token.IDENT
			for !(unicode.IsSpace(l.ch)) {
				l.readRune()
			}
		}
	}
	last := l.cLoc // no digits read after + or -
	if sLoc == l.cLoc {
		tokenT = token.IDENT
		illegalT = true
		last = l.cLoc + 2 // include rune next to + or -
		l.readRune()      // read over + -
	}
	return &token.Token{Type: tokenT, Literal: l.input[sLoc:last], Illegal: illegalT, Loc: start}

}

func (l *Lexer) readString() *token.Token {

	Loc := l.cLoc + 1
	start := token.Pos{l.Line, l.Col}
	//fmt.Println("Loc: ", Loc)
	for {
		l.readRune()
		if l.ch == '"' { // "
			if l.del == token.STRINGDEL {
				break
			} else {
				l.readRune()
				if l.ch == '"' { // "
					if l.del == token.STRINGDEL {
						return l.newToken(token.ILLEGAL, l.ch)
					}
					l.readRune()
					if l.ch == '"' { // "
						break
					}
				}
				return l.newToken(token.ILLEGAL, l.ch)
			}
		}
		if l.del == token.RAWSTRINGDEL && (l.ch == 10) { // linefeed
			l.Line++
			l.Col = 0
		}
	}
	//fmt.Println("l.cLoc: ", l.cLoc)
	var eLoc int
	if l.del == token.RAWSTRINGDEL {
		eLoc = 2
		return &token.Token{Type: token.RAWSTRING, Literal: l.input[Loc : l.cLoc-eLoc], Loc: start}
	}
	return &token.Token{Type: token.STRING, Literal: l.input[Loc : l.cLoc-eLoc], Loc: start}
}

func (l *Lexer) readToEol() {
	for {
		l.readRune()
		if l.ch == '\u000D' || l.ch == '\u000A' {
			//l.skipWhitespace()
			break
		}
	}
}

func (l *Lexer) newToken(tokenType token.TokenType, ch rune, Loc ...token.Pos) *token.Token {
	if len(Loc) > 0 {
		return &token.Token{Type: tokenType, Literal: string(ch), Loc: Loc[0]}
	}
	return &token.Token{Type: tokenType, Literal: string(ch), Loc: token.Pos{l.Line, l.Col}}
}

// func (l *Lexer) GetLoc() *token.Pos {
// 	return &token.Pos{l.Line, l.Col}
// }

// func (l *Lexer) SetELoc() {
// 	l.Eloc = token.Pos{l.Line, l.Col}
// }
// func (l *Lexer) ClearLoc() {
// 	l.Eloc = token.Pos{}
// }
