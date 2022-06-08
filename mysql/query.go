package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/GoGraph/db"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/tx/query"
	//_ "github.com/go-sql-driver/mysql"
)

var noDataFoundErr = errors.New("no rows in result set")

type prepStmtT map[string]*sql.Stmt

var prepStmtM prepStmtT

func init() {
	prepStmtM = make(prepStmtT)
}

func crProjection(q *query.QueryHandle) *strings.Builder {

	// generate projection
	var (
		first = true
	)
	var s strings.Builder
	s.WriteString("select ")
	for _, v := range q.GetAttr() {
		if v.IsFetch() {
			if first {
				s.WriteString(v.Name())
				first = false
			} else {
				s.WriteByte(',')
				s.WriteString(v.Name())
			}
		}
	}

	return &s

}

func closePrepStmt(client *sql.DB, q *query.QueryHandle) (err error) {

	if q.PrepStmt() != nil {
		ps := q.PrepStmt().(*sql.Stmt)
		err = ps.Close()
		if err != nil {
			logerr(fmt.Errorf("Failed to close prepared stmt %s, %w", q.Tag, err))
			return err
		}
		syslog(fmt.Sprintf("closed prepared stmt %s", q.Tag))
	}
	return nil
}

// executeQuery handles one stmt per tx.NewQuery*()
// the idea of multiple queries to a tx needs to be considered so tx has []QueryHandle
func executeQuery(ctx context.Context, client *sql.DB, q *query.QueryHandle, opt ...db.Option) error {

	var (
		// options
		oSingleRow = false
		err        error
		row        *sql.Row
		rows       *sql.Rows
		prepStmt   *sql.Stmt
	)

	for _, o := range opt {
		switch strings.ToLower(o.Name) {
		case "singlerow":
			if v, ok := o.Val.(bool); !ok {
				fmt.Errorf(fmt.Sprintf("Expected bool value for prepare option got %T", o.Val))
				return err
			} else {
				oSingleRow = v
			}
		}
	}
	slog.Log("executeQuery", fmt.Sprintf("Enter.. len(opt) %d", len(opt)))
	if q.GetError() != nil {
		return fmt.Errorf(fmt.Sprintf("Cannot execute query because of error %s", q.GetError()))
	}

	// generate SQL statement

	// define projection based on struct passed via Select()
	s := crProjection(q)

	s.WriteString(" from ")
	s.WriteString(string(q.GetTable()))
	s.WriteString(" where ")
	//
	var whereVals []interface{}
	wa := len(q.GetWhereAttrs())
	for i, v := range q.GetWhereAttrs() {
		s.WriteString(v.Name())
		s.WriteString(v.GetOprStr())
		s.WriteByte('?')
		whereVals = append(whereVals, v.Value())
		if wa > 0 && i < wa-1 {
			s.WriteString(" and ")
		}
	}
	//
	if q.HasOrderBy() {
		s.WriteString(q.OrderByString())
	}

	if q.Prepare() {

		slog.Log("executeQuery", fmt.Sprintf("Prepared..."))

		if q.PrepStmt() == nil {

			slog.Log("executeQuery", fmt.Sprintf("PrepStmt is nil"))

			if ctx != nil {
				prepStmt, err = client.PrepareContext(ctx, s.String())
			} else {
				prepStmt, err = client.Prepare(s.String())
			}
			if err != nil {
				return err
			}
			slog.Log("executeQuery", fmt.Sprintf("SetPrepStmt ..."))
			q.SetPrepStmt(prepStmt)

		} else {

			prepStmt = q.PrepStmt().(*sql.Stmt)
			slog.Log("executeQuery", fmt.Sprintf("GetPrepStmt() ...%#v", prepStmt))
		}

		if oSingleRow {
			slog.Log("executeQuery", "oSingleRow..")
			if ctx != nil {
				row = prepStmt.QueryRowContext(ctx, whereVals...)
			} else {
				row = prepStmt.QueryRow(whereVals...)
			}
		} else {
			for _, v := range whereVals {
				slog.Log("executeQuery", fmt.Sprintf("About to execute prepared stmt with value: %T, %#T", v, v))
			}

			if ctx != nil {
				rows, err = prepStmt.QueryContext(ctx, whereVals...)
			} else {
				rows, err = prepStmt.Query(whereVals...)
			}
			slog.Log("executeQuery", "not oSingleRow..")
		}

	} else {

		// non-prepared
		slog.Log("executeQuery", fmt.Sprintf("Not prepared..."))

		if oSingleRow {
			if ctx != nil {
				row = client.QueryRowContext(ctx, s.String(), whereVals...)
			} else {
				row = client.QueryRow(s.String(), whereVals...)
			}
		} else {
			if ctx != nil {
				rows, err = client.QueryContext(ctx, s.String(), whereVals...)
			} else {
				rows, err = client.Query(s.String(), whereVals...)
			}
		}
	}
	if err != nil {
		return err
	}

	if oSingleRow {
		if err = row.Scan(q.Split()...); err != nil {

			if errors.Is(err, sql.ErrNoRows) {
				err = query.NoDataFoundErr
			}
		}
	} else {
		slog.Log("executeQuery", fmt.Sprintf("About to execute rows.Next() "))
		for i := 0; rows.Next(); i++ {
			slog.Log("executeQuery", fmt.Sprintf("rows.Next() i = %d", i))
			// split struct fields into individual bind vars
			if err = rows.Scan(q.Split()...); err != nil {
				slog.Log("executeQuery", fmt.Sprintf("Error rows.Next() "))
				logerr(err)
			}
		}
		if err := rows.Err(); err != nil {
			slog.Log("executeQuery", fmt.Sprintf("Error rows.Next()  2"))
			logerr(err)
		}
	}

	if err != nil {
		return err
	}

	if q.Prepare() {
		slog.Log("executeQuery", fmt.Sprintf("q.Reset()..."))
		q.Reset()
	}
	return nil
}
