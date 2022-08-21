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
				if len(v.Literal()) > 0 {
					s.WriteString(v.Literal() + " " + v.Name()) // for SQL : ,col alias,
				} else {
					s.WriteString(v.Name())
				}
				first = false
			} else {
				s.WriteByte(',')
				if len(v.Literal()) > 0 {
					s.WriteString(v.Literal() + " " + v.Name()) // for SQL : ,col alias,
				} else {
					s.WriteString(v.Name())
				}
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

func sqlOpr(o string) string {

	switch o {
	case "EQ":
		return "="
	case "NE":
		return "!="
	case "LE":
		return "<="
	case "GE":
		return ">="
	case "LT":
		return "<"
	case "GT":
		return ">"
	case "NOT":
		return "not"
	}
	return o
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
	if q.Error() != nil {
		return fmt.Errorf(fmt.Sprintf("Cannot execute query because of error %s", q.Error()))
	}

	// validate query metadata in query.QueryHandle
	err = validateInput(q)
	if err != nil {
		return err
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
		fmt.Println("whereattr: ", v.Name(), v.Literal())
		var found bool
		// search
		for _, vv := range q.GetAttr() {
			if vv.IsFetch() {
				if vv.Name() == v.Name() {
					if len(vv.Literal()) > 0 {
						s.WriteString(vv.Literal())
						found = true
					}
					break
				}
			}
		}
		if !found {
			s.WriteString(v.Name())
		} else {
			found = false
		}

		s.WriteString(sqlOpr(v.GetOprStr()))
		// check value is not an attribute name, in which case don't use "?"
		if col, ok := v.Value().(string); ok {
			// check against attributes in projection
			for _, v := range q.GetAttr() {
				if col == v.Name() {
					s.WriteString(col)
					found = true
					break
				}
			}
		}
		if !found {
			s.WriteByte('?')
			whereVals = append(whereVals, v.Value())
		}
		if wa > 0 && i < wa-1 {
			s.WriteString(" and ")
		}
	}
	//
	if q.HasOrderBy() {
		s.WriteString(q.OrderByString())
	}
	alertlog(fmt.Sprintf("generated sql: [%s]", s.String()))
	if q.Prepare() {

		slog.Log("executeQuery", fmt.Sprintf("Prepared query"))

		if q.PrepStmt() == nil {

			if ctx != nil {
				prepStmt, err = client.PrepareContext(ctx, s.String())
			} else {
				prepStmt, err = client.Prepare(s.String())
			}
			if err != nil {
				return err
			}
			q.SetPrepStmt(prepStmt)

		} else {

			prepStmt = q.PrepStmt().(*sql.Stmt)
		}

		if oSingleRow {
			if ctx != nil {
				row = prepStmt.QueryRowContext(ctx, whereVals...)
			} else {
				row = prepStmt.QueryRow(whereVals...)
			}
		} else {
			if ctx != nil {
				rows, err = prepStmt.QueryContext(ctx, whereVals...)
			} else {
				rows, err = prepStmt.Query(whereVals...)
			}
		}

	} else {

		// non-prepared
		slog.Log("executeQuery", fmt.Sprintf("Non-prepared query"))
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
		for i := 0; rows.Next(); i++ {
			// split struct fields into individual bind vars
			if err = rows.Scan(q.Split()...); err != nil {
				logerr(err)
			}
		}
		if err := rows.Err(); err != nil {
			logerr(err)
		}
	}

	if err != nil {
		return err
	}

	if q.Prepare() {
		q.Reset()
	}
	return nil
}

func validateInput(q *query.QueryHandle) error {
	// validate Keys attributes - TODO implement

	// validate Filter attributes - TODO implement

	// validate Projection attributes- TODO implement

	return nil
}
