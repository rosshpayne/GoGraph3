// build+ dynamodb

// db package defines common and generic database errors. Each occurrance is written to the syslog.
// Errors are not added to the errlog - this is left the application. The aplication can also add any data identifiers
// to help identify the actual data impacted by the error.
package db

import (
	"errors"
	"fmt"

	//"github.com/GoGraph/errlog"
	slog "github.com/GoGraph/syslog"
)

// var (
// 	// error categories - returned from Unwrap()
// 	NoItemFoundErr  = errors.New("no item found")
// 	SystemErr       = errors.New("DB system error")
// 	MarshalingErr   = errors.New("DB marshaling error")
// 	UnmarshalingErr = errors.New("DB unmarshaling error")
// )

const (
	MaxOperRetries   = "MaxOperationRetries"
	MaxUnprocRetries = "MaxUnprocesssedRetries"
	NonRetryOperErr  = "CriticalOperationErr"
)

type UnprocessedErr struct {
	Remaining int
	Total     int
	Retries   int
}

func (u UnprocessedErr) Error() string {
	return fmt.Sprintf("Failed to process %d of %d unprocessed items after %d retries", u.Remaining, u.Total, u.Retries)
}

type DBExprErr struct {
	routine string
	pkey    string
	sortk   string
	err     error // aws dynamo expression error,InvalidParameterError, UnsetParameterError use errors.As
}

func newDBExprErr(rt string, pk string, sk string, err error) error {
	er := &DBExprErr{routine: rt, pkey: pk, sortk: sk, err: err}
	logerr(er)
	return er
}

func (e *DBExprErr) Error() string {
	if len(e.sortk) > 0 {
		return fmt.Sprintf("Expression error in %s [%s, $s]. %s", e.routine, e.pkey, e.sortk, e.err.Error())
	}
	if len(e.pkey) > 0 {
		return fmt.Sprintf("Expression error in %s [%s]. %s", e.routine, e.pkey, e.err.Error())
	}
	return fmt.Sprintf("Expression error in %s. %s", e.routine, e.err.Error())
}

func (e *DBExprErr) Unwrap() error {
	return e.err
}

var ErrItemSizeExceeded = errors.New("Item has reached its maximum allowed size")
var ErrAttributeDoesNotExist = errors.New("An Attribute specified in the update does not exist")
var ErrConditionalCheckFailed = errors.New("Conditional Check Failed Exception")
var NoDataFound = errors.New("No data found")
var UidPredSizeLimitReached = errors.New("uid-predicate item limit reached")

var NodeAttached = errors.New("Node is attached")

type DBSysErr struct {
	routine string
	api     string // DB statement
	err     error  // aws database error
}

func (e *DBSysErr) Unwrap() error {
	return e.err
}

func (e *DBSysErr) Error() string {
	return fmt.Sprintf("DB system error in %s of %s. %s", e.api, e.routine, e.err.Error())
}

func newDBSysErr(rt string, api string, err error) error {

	syserr := &DBSysErr{routine: rt, api: api, err: err}
	slog.LogErr("DBExecute: ", syserr.Error())
	//errlog.Add("DBExecute", syserr)

	return syserr
}

type DBSysErr2 struct {
	routine string
	reason  string // DB statement
	code    string
	data    string
	err     error // aws database error
}

func (e *DBSysErr2) Unwrap() error {
	return e.err
}

func (e *DBSysErr2) Error() string {
	return fmt.Sprintf("Error in %s: %s [%s], %s", e.routine, e.reason, e.code, e.err.Error())
}

func (e *DBSysErr2) ErrorCode() string {
	return e.code
}

func (e *DBSysErr2) ErrorReason() string {
	return e.reason
}

func (e *DBSysErr2) ErrorData() string {
	return e.data
}

func newDBSysErr2(rt string, reason string, code string, data string, err error) error {

	syserr := &DBSysErr2{routine: rt, reason: reason, code: code, data: data, err: err}

	slog.LogErr("DBExecute: ", syserr.Error())
	//errlog.Add("DBExecute", syserr)

	return syserr
}

// var DBNoItemFoundErr = dbNoItemFound()

// func dbNoItemFound() *DBNoItemFound { return &DBNoItemFound{} }

type DBNoItemFound struct {
	routine string
	pkey    string
	sortk   string
	api     string // DB statement
	err     error
}

func (e *DBNoItemFound) Error() string {

	if e.api == "Scan" {
		return fmt.Sprintf("No item found during %s operation in %s [%q]", e.api, e.routine, e.pkey)
	}
	if len(e.sortk) > 0 {
		return fmt.Sprintf("No item found during %s in %s for Pkey %q, Sortk %q", e.api, e.routine, e.pkey, e.sortk)
	}
	return fmt.Sprintf("No item found during %s in %s for Pkey %q", e.api, e.routine, e.pkey)

}

func (e *DBNoItemFound) Unwrap() error {
	return e.err
}

func NewDBNoItemFound(rt string, pk string, sk string, api string) error {

	e := &DBNoItemFound{routine: rt, pkey: pk, sortk: sk, api: api}
	e.err = NoDataFound
	slog.LogErr("DBExecute: ", e.Error())
	//errlog.Add("DBExecute", e)

	return e
}

func newDBNoItemFound(rt string, pk string, sk string, api string) error {

	e := &DBNoItemFound{routine: rt, pkey: pk, sortk: sk, api: api}
	e.err = NoDataFound
	logerr(e)
	return e
}

type DBMarshalingErr struct {
	routine string
	pkey    string
	sortk   string
	api     string // DB statement
	err     error  // aws database error
}

func newDBMarshalingErr(rt string, pk string, sk string, api string, err error) error {
	e := &DBMarshalingErr{routine: rt, pkey: pk, sortk: sk, api: api, err: err}
	slog.LogErr("DBExecute: ", fmt.Sprintf("Error: %s", e.Error()))
	//	errlog.Add("DBExecute", e)
	return e
}

func (e *DBMarshalingErr) Error() string {
	if len(e.sortk) > 0 {
		return fmt.Sprintf("Marshalling error during %s in %s. [%q, %q]. Error: ", e.api, e.routine, e.pkey, e.sortk, e.pkey, e.err.Error())
	}
	return fmt.Sprintf("Marshalling error during %s in %s. [%q]. Error: ", e.api, e.routine, e.pkey, e.err.Error())
}

func (e *DBMarshalingErr) Unwrap() error {
	return e.err
}

type DBUnmarshalErr struct {
	routine string
	pkey    string
	sortk   string
	api     string // DB statement
	err     error  // aws database error
}

func newDBUnmarshalErr(rt string, pk string, sk string, api string, err error) error {
	e := &DBUnmarshalErr{routine: rt, pkey: pk, sortk: sk, api: api, err: err}
	slog.LogErr("DBExecute: ", e.Error())
	//errlog.Add("DBExecute", e)
	return e
}

func (e *DBUnmarshalErr) Error() string {
	if len(e.sortk) > 0 {
		return fmt.Sprintf("Unmarshalling error during %s in %s. [%q, %q]. Error: %s ", e.api, e.routine, e.pkey, e.sortk, e.err.Error())
	}
	return fmt.Sprintf("Unmarshalling error during %s in %s. [%q]. Error: %s ", e.api, e.routine, e.pkey, e.err.Error())
}

func (e *DBUnmarshalErr) Unwrap() error {
	return e.err
}
