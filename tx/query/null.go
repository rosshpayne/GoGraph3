package query

import (
	"database/sql"
)

type NullString struct {
	sql.NullString
}

type NullBool struct {
	sql.NullBool
}

type NullByte struct {
	sql.NullByte
}

type NullFloat64 struct {
	sql.NullFloat64
}

type NullInt16 struct {
	sql.NullInt16
}

type NullInt32 struct {
	sql.NullInt32
}

type NullInt64 struct {
	sql.NullInt64
}

type NullTime struct {
	sql.NullTime
}
