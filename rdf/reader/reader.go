package reader

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"strings"
	"text/scanner"
	"unicode"

	"github.com/GoGraph/rdf/ds"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/uuid"
)

// transition structs between RDF tuple and dynamodb.AttributeValue

type Decoder struct {
	r     io.Reader
	batch uint // number of tuples in batch
}

func NewRDFloader(f io.Reader) *Decoder {
	return &Decoder{r: f}
}

func syslog(s string) {
	slog.Log("RDFreader", s)
}

type ndShortNm = string

// func openRDFfile(fname string) *os.File {
// 	f, err := os.Open(fname)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	return f
// }
//
// input rdf data - rdf file data. Sent on verify channel to be consumed by verification process
//

//ype RdfNodes

type RDFReader struct {
	//	r      io.Reader
	bs *bufio.Scanner
	//cancel context.CancelFunc
	//Ctx    context.Context // move context to Load()
	ts   scanner.Scanner
	line int
}

func New(f io.Reader) (*RDFReader, uuid.UID) {

	rdf := new(RDFReader)

	//  bufio scanner to split file into lines and text scanner to split lines into tokens
	rdf.bs = bufio.NewScanner(f)
	// use a text scanner to scan the token (an s-p-o i.e. a line in the file) to our specification
	var s scanner.Scanner
	// treat leading `_:` as part of an identifier
	s.IsIdentRune = func(ch rune, i int) bool {
		return ch == '_' && i == 0 || ch == '_' && i == 1 || ch == ':' && i == 1 || unicode.IsLetter(ch) || unicode.IsDigit(ch) && i > 0 || ch == '.' || ch == '-' || ch == '_'
	}
	rdf.ts = s
	//
	// TODO: implement a repo for sname-UID pairs based on file or filegroup
	// setup/find sname-UID cache
	// sortk = sname, attr U = uid
	//   cacheUID = db.GetRDFCacheUID(f)

	return rdf, nil
}

type cur struct {
	obj, subj, pred string
	set             bool
}

var peekRDF cur

// Read read rdf file and bundles common tuples into nodes upto len(n) nodes and passes back via n.
// number of nodes in n is returnd plus error if any.
func (rn RDFReader) Read(n []*ds.Node) (int, bool, error) {

	var (
		rd              float64
		prevSubj        = "__"
		subj, pred, obj string
	)
	syslog(fmt.Sprintf("reader: batch size -=  %d", len(n)))

	v := n[0]
	for ii := 0; ii < len(n); {

		if peekRDF.set {

			pred, subj, obj = peekRDF.pred, peekRDF.subj, peekRDF.obj
			peekRDF.set = false

		} else {

			if rn.bs.Scan() {

				rn.line++

				rn.ts.Init(strings.NewReader(rn.bs.Text()))
				//s.Mode ^= scanner.SkipComments // disable skip comments is enabled by default. Xor will toggle bit 10 to 0 to enable comment display
				subj, pred, obj = "", "", ""

				for i, tok := 0, rn.ts.Scan(); tok != scanner.EOF; tok = rn.ts.Scan() {

					switch i {
					case 0:
						subj = rn.ts.TokenText()[2:]
					case 1:
						pred = rn.ts.TokenText()
					case 2:
						obj = rn.ts.TokenText()
					case 3:
						_ = rn.ts.TokenText()
					default:
						return ii, false, fmt.Errorf("Unexpected token %d at end of line %c ", tok, rn.line)
					}
					i++
				}

			} else {

				if rn.bs.Err() != nil {
					return 0, false, rn.bs.Err()
				}
				ii++
				return ii, true, nil
			}
		}
		//
		// trigger point: change of node short-name bundles up rdf tuples from previous node short-name.
		// provided there is a type specified the bundle of tuples is then sent on channel verify to be processed by verification process.
		// if there is no type log
		//
		rd++
		if math.Mod(rd, 100) == 0 {
			syslog(fmt.Sprintf("spo read:  %s  %s  %s", subj, pred, obj))
		}
		if prevSubj == "__" {
			prevSubj = subj
		}

		switch {

		case len(subj) == 0: // blank line - do nothing.

		case subj == prevSubj:

			switch pred {
			case "__type", "__TYPE":

				v.TyName = obj[1 : len(obj)-1] // remove ""
				v.ID = subj

			case "__id", "__ID":

				id := uuid.UIDb64(obj[1 : len(obj)-1])
				if len(id) != 24 {
					v.PKey = obj[1 : len(obj)-1]
				} else {
					// treat as base64 UUID
					v.UUID = uuid.UIDb64(id).Decode()
				}

			default:

				if obj[0] == '"' {
					obj = obj[1 : len(obj)-1]
				} else if len(obj) > 1 {
					obj = obj[2:]
				}
				line := ds.Line{N: rn.line, Subj: subj, Pred: pred, Obj: obj}
				v.Lines = append(v.Lines, line)

			}
			prevSubj = subj

		case subj != prevSubj:

			ii++
			//
			if ii >= len(n) {
				peekRDF.pred, peekRDF.subj, peekRDF.obj, peekRDF.set = pred, subj, obj, true
				return ii, false, nil
			}
			v = n[ii]
			//
			if pred == "__type" {

				v.TyName = obj[1 : len(obj)-1]
				v.ID = subj
			}

			prevSubj = subj

		}

	}
	return 0, false, nil

}
