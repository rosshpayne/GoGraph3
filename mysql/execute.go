package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"strings"

	"github.com/GoGraph/db"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/uuid"

	_ "github.com/go-sql-driver/mysql"
)

type sqlStmt struct {
	//i      []int //  index in tx package []*mut.Mutations
	sql    string
	params []interface{}
}

// execute handles DML only (see ExecuteQuery).
// if prepare() specified in tx, all unique statements (mutations) are prepared once executed multiple times
func execute(ctx context.Context, client *sql.DB, bs []*mut.Mutations, tag string, cTx *sql.Tx, prepare bool, opt ...db.Option) error {

	// type (
	// 	tblMT  map[string][]*prepT // table name - may have multiple Prep stmts if sql insert/update/delete varies in columns reference say.
	// 	mOprMt map[string]tblMT    // insert, update, delete, Merge?

	// 	xMutT struct {
	// 		stmt   *sql.Stmt     // generated by sql.Prepare()
	// 		sql    string       // non-prepared stmt
	// 		prVals []interface{} // param (mutation) values (generated during sql creation), order matches placeholders in SQL stmt
	// 	}
	// )
	type sqlHashValT string
	var (
		errs       []error
		err        error
		mutM       map[sqlHashValT]*sql.Stmt
		sqlHashVal sqlHashValT
		prepStmt   *sql.Stmt
	)

	add := func(err error) {
		errs = append(errs, err)
	}
	mutM = make(map[sqlHashValT]*sql.Stmt)

	// prepare all mutations if tx.prepare() specified
	// note there is typically multiple instances of the same dml in a tx, each represented as a separate mutation
	// a hash is taken of the dml to identify its uniqueness and so only parse it once. All subsequent identical mutations will
	// use that parsed version stoed in the mutation struct.
	for _, j := range bs {
		for _, m := range *j {

			m := m.(*mut.Mutation)

			sqlstmt := genSqlStmt(m)

			if prepare {
				// designed to have multiple dml stmts per tx.
				h := sha256.New()
				// generate hash of the sql and save to mutation map
				// could use the mutation label instead of hashing but label may not be unique (no constraint) ie. used in multiple mutations
				h.Write([]byte(sqlstmt.sql))
				sqlHashVal = sqlHashValT(fmt.Sprintf("%x", h.Sum(nil)))

				if mhash, ok := mutM[sqlHashVal]; !ok {

					if cTx != nil {
						prepStmt, err = cTx.Prepare(sqlstmt.sql)
					} else {
						prepStmt, err = client.Prepare(sqlstmt.sql)
					}
					if err != nil {
						add(err)
						// save error to mutation
						m.SetError(err)
						continue
					}

					mutM[sqlHashVal] = prepStmt

				} else {
					// get prep stmt from map of hashed sql
					if len(errs) > 0 {
						return errs[0]
					}
					prepStmt = mhash
				}
				m.SetPrepStmt(prepStmt)

			} else {

				m.SetText(sqlstmt.sql)
			}
			m.SetParams(sqlstmt.params)
		}
	}

	if len(errs) > 0 {
		return errs[0]
	}

	// execute the mutations using either the sql or the prepared version
	for _, j := range bs {
		for _, m := range *j {

			m := m.(*mut.Mutation)

			if m.PrepStmt() != nil {

				prepStmt := m.PrepStmt().(*sql.Stmt)

				if ctx != nil {
					_, err = prepStmt.ExecContext(ctx, m.Params()...)
				} else {
					_, err = prepStmt.Exec(m.Params()...)
				}

			} else {

				if cTx != nil {

					if ctx != nil {
						_, err = cTx.ExecContext(ctx, m.SQL(), m.Params()...)
					} else {
						_, err = cTx.Exec(m.SQL(), m.Params()...)
					}
				} else {

					if ctx != nil {
						_, err = client.ExecContext(ctx, m.SQL(), m.Params()...)
					} else {
						_, err = client.Exec(m.SQL(), m.Params()...)
					}
				}
			}
		}
	}

	if err != nil {
		if cTx != nil {
			syslog(fmt.Sprintf("Execute error: %s", err.Error()))
			errR := cTx.Rollback()
			if errR != nil {
				return fmt.Errorf("Rollback failed with error %s. Original error: %w", errR, err)
			}
		}
		return err
	}

	if cTx != nil {
		err = cTx.Commit()
		if err != nil {
			return err
		}
	}

	return nil
}

func genSqlStmt(m *mut.Mutation) sqlStmt {

	sqlstmt := genSQLStatement(m)

	//prep.sql = sqlstmt.sql
	// convert uuid.UID to []byte. TODO: is this necessary?
	for i, v := range sqlstmt.params {
		if v, ok := v.(uuid.UID); ok {
			sqlstmt.params[i] = []byte(v)
		}
	}

	return sqlstmt
}

// pkg db must support mutations, Insert, Update, Merge:
// any other mutations (eg WithOBatchLimit) must be defined outside of DB and passed in (somehow)

// GoGraph SQL Mutations - used to identify "merge SQL". Spanner has no merge sql so GOGraph
// uses a combination of update-insert processing.

type ggMutation struct {
	stmt    []sqlStmt
	isMerge bool
}

func genSQLMerge(m *mut.Mutation, params []interface{}) (string, []interface{}) {

	//INSERT INTO t1 (a,b,c) VALUES (1,2,3)
	// ON DUPLICATE KEY UPDATE c=c+1;

	var sql strings.Builder

	sql.WriteString(`insert into `)
	sql.WriteString(m.GetTable())
	sql.WriteString(` (`)

	for i, col := range m.GetMembers() {

		// does table have a sortk
		if col.Name == "__" {
			// no it doesn't
			continue
		}
		if i != 0 {
			sql.WriteByte(',')
		}
		sql.WriteString(col.Name)
	}
	sql.WriteString(`) values (`)
	for i, set := range m.GetMembers() {
		// does table have a sortk
		if set.Name == "__" {
			// no it doesn't
			continue
		}
		if i != 0 {
			sql.WriteByte(',')
		}
		// check for string type and a SQL keyword surrounded by "_" like "$CURRENT_TIMESTAMP$"
		if s, ok := set.Value.(string); ok {
			if len(s) > 3 && s[0] == '$' && s[len(s)-1] == '$' {
				if s[1:len(s)-1] == "CURRENT_TIMESTAMP" {
					sql.WriteString("SYSDATE()")
				} else {
					sql.WriteString(s[1 : len(s)-1])
				}
				continue
			}
		}
		sql.WriteString("?")
		params = append(params, set.Value)
	}
	sql.WriteByte(')')

	//  ON DUPLICATE KEY UPDATE clause
	var first = true
	for _, col := range m.GetMembers() {
		if col.Name == "__" || col.Opr == mut.IsKey {
			continue
		}
		if first {
			sql.WriteString(" ON DUPLICATE KEY UPDATE ")
			first = false
		} else {
			sql.WriteString(", ")
		}
		sql.WriteString(col.Name)
		sql.WriteString(" = ")
		//
		switch col.Opr {
		case mut.Add:
			sql.WriteString("ifnull(")
			sql.WriteString(col.Name)
			sql.WriteString(",0)")
			sql.WriteByte('+')
		case mut.Inc:
			sql.WriteString("ifnull(")
			sql.WriteString(col.Name)
			sql.WriteString(",0)")
			sql.WriteByte('+')
		case mut.Subtract:
			sql.WriteString("ifnull(")
			sql.WriteString(col.Name)
			sql.WriteString(",0)")
			sql.WriteByte('-')
		}

		// non-Array attributes - set
		if s, ok := col.Value.(string); ok {
			if len(s) > 3 && s[0] == '$' && s[len(s)-1] == '$' {
				if s[1:len(s)-1] == "CURRENT_TIMESTAMP" {
					sql.WriteString("SYSDATE()")
				} else {
					sql.WriteString(s[1 : len(s)-1])
				}
				continue
			}
		}
		sql.WriteString("?") //col.Param)

		params = append(params, col.Value)
	}

	return sql.String(), params
}

func genSQLUpdate(m *mut.Mutation, params []interface{}) (string, []interface{}) {

	var (
		first bool = true
		sql   strings.Builder
	)

	sql.WriteString(`update `)
	sql.WriteString(m.GetTable())
	sql.WriteString(` set `)
	// set clause
	for _, col := range m.GetMembers() {

		if col.Name == "__" || col.Opr == mut.IsKey {
			continue
		}
		if first {
			first = false
		} else {
			sql.WriteByte(',')
		}
		sql.WriteString(col.Name)
		sql.WriteByte('=')

		switch col.Opr {
		case mut.Inc:
			sql.WriteString("ifnull(")
			sql.WriteString(col.Name)
			sql.WriteString(",0)")
			sql.WriteByte('+')
		case mut.Subtract:
			sql.WriteString("ifnull(")
			sql.WriteString(col.Name)
			sql.WriteString(",0)")
			sql.WriteByte('-')
		}

		// non-Array attributes - set
		if s, ok := col.Value.(string); ok {
			if len(s) > 3 && s[0] == '$' && s[len(s)-1] == '$' {
				sql.WriteString(s[1 : len(s)-1])
				continue
			}
		}
		sql.WriteString("?") //col.Param)

		if col.Opr == mut.Remove {
			params = append(params, "NULL")
		} else {
			params = append(params, col.Value)
		}
	}
	// Predicate clause
	first = true
	for _, col := range m.GetMembers() {
		if col.Name == "__" {
			continue
		}
		if col.Opr == mut.IsKey {

			if first {
				sql.WriteString(" where ")
				first = false
			} else {
				sql.WriteString(" and ")
			}
			sql.WriteString(col.Name)
			sql.WriteString(" = ")
			sql.WriteString("?")
			//
			// params[col.Param[1:]] = col.Value
			params = append(params, col.Value)
		}
	}

	return sql.String(), params

}

func genSQLInsertWithValues(m *mut.Mutation, params []interface{}) (string, []interface{}) {

	var sql strings.Builder

	sql.WriteString(`insert into `)
	sql.WriteString(m.GetTable())
	sql.WriteString(` (`)

	for i, col := range m.GetMembers() {
		// does table have a sortk
		if col.Name == "__" {
			// no it doesn't
			continue
		}
		if i != 0 {
			sql.WriteByte(',')
		}
		sql.WriteString(col.Name)
	}
	sql.WriteString(`) values (`)
	for i, set := range m.GetMembers() {
		// does table have a sortk
		if set.Name == "__" {
			// no it doesn't
			continue
		}
		if i != 0 {
			sql.WriteByte(',')
		}
		// check for string type and a SQL keyword surrounded by "_" like "$CURRENT_TIMESTAMP$"
		if s, ok := set.Value.(string); ok {
			if len(s) > 3 && s[0] == '$' && s[len(s)-1] == '$' {
				sql.WriteString(s[1 : len(s)-1])
				continue
			}
		}
		sql.WriteString("?")
		params = append(params, set.Value)
	}
	sql.WriteByte(')')

	return sql.String(), params
}

func genSQLInsertWithSelect(m *mut.Mutation, params []interface{}) (string, []interface{}) {

	var sql strings.Builder

	sql.WriteString(`insert into `)
	sql.WriteString(m.GetTable())
	sql.WriteString(` (`)

	for i, col := range m.GetMembers() {

		if col.Name == "__" {
			// no it doesn't
			continue
		}
		if i != 0 {
			sql.WriteByte(',')
		}
		sql.WriteString(col.Name)
	}
	sql.WriteByte(')')
	sql.WriteString(` values ( `)

	for i, col := range m.GetMembers() {
		if col.Name == "__" {
			// no it doesn't
			continue
		}
		if i != 0 {
			sql.WriteByte(',')
		}
		sql.WriteString("?")

		params = append(params, col.Value)
	}
	sql.WriteByte(')')
	//sql.WriteString(" from dual where NOT EXISTS (select 1 from EOP where PKey=@PKey and SortK=@SortK) ")

	return sql.String(), params
}

func genSQLStatement(m *mut.Mutation) sqlStmt {

	var params []interface{}

	switch m.GetOpr() {

	case mut.Merge:

		m, params := genSQLMerge(m, params)
		return sqlStmt{sql: m, params: params}

	case mut.Update:

		m, params := genSQLUpdate(m, params)
		return sqlStmt{sql: m, params: params}

	case mut.Insert:

		m, params := genSQLInsertWithValues(m, params)
		return sqlStmt{sql: m, params: params}

	case mut.Delete:
	// TODO: implement

	case mut.TruncateTbl:
		return sqlStmt{sql: "truncate " + m.GetTable()}
	// TODO: implement

	default:
		panic(fmt.Errorf("db execute error: opr is not assigned [%v]", m.GetOpr()))
		return sqlStmt{}
	}

	return sqlStmt{}

}

func genSQLparams(m *mut.Mutation) []interface{} {

	// use genSQLStatement to create params (slice of mutation values that match placeholders in SQL stmt)
	sqlstmt := genSQLStatement(m)
	return sqlstmt.params

}
