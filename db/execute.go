//go:build spanner
// +build spanner

package db

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	//"github.com/GoGraph/dbs"
	param "github.com/GoGraph/dygparam"
	//elog "github.com/GoGraph/rdf/errlog"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/util"

	//"google.golang.org/api/spanner/v1"

	"cloud.google.com/go/spanner" //v1.21.0
)

func logerr(e error, panic_ ...bool) {

	if len(panic_) > 0 && panic_[0] {
		slog.Log(logid, e.Error(), true)
		panic(e)
	}
	slog.Log(logid, e.Error())
}

func syslog(s string) {
	slog.Log(logid, s)
}

// func init() {
// 	client, err = dbConn.New()
// 	if err != nil {
// 		syslog(fmt.Sprintf("Cannot create a db Client: %s", err.Error()))
// 		panic(err)
// 	}
// }

// func GetClient() *spanner.Client {
// 	return client
// }

// pkg db must support mutations, Insert, Update, Merge:
// any other mutations (eg WithOBatchLimit) must be defined outside of DB and passed in (somehow)

// GoGraph SQL Mutations - used to identify merge SQL. Spanner has no merge sql so GOGraph
// uses a combination of update-insert processing.
type ggMutation struct {
	stmt    []spanner.Statement
	isMerge bool
}

func genSQLRemove(m *mut.Mutation, params map[string]interface{}) string {

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
		var c string // generatlised column name
		sql.WriteString(col.Name)
		sql.WriteString("= DEFAULT") // NoSQL's remove equivalent in SQL
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
			sql.WriteString(col.Param)
			params[col.Param[1:]] = col.Value
		}
	}

}

func genSQLUpdate(m *mut.Mutation, params map[string]interface{}) string {

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
		var c string // generatlised column name
		sql.WriteString(col.Name)
		sql.WriteByte('=')
		// for array types: set col=ARRAY_CONCAT(col,@param)
		if col.Name[0] == 'L' {
			c = "L*"
		} else {
			c = col.Name
		}

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

		switch c {

		case "Nd", "XF", "Id", "XBl", "L*":
			switch col.Opr {
			case mut.Set:
				sql.WriteString(col.Param)

			default:
				// Array (List) attributes - append/concat to type
				sql.WriteString("ARRAY_CONCAT(")
				sql.WriteString(col.Name)
				sql.WriteByte(',')
				sql.WriteString(col.Param)
				sql.WriteByte(')')
			}
		default:
			// non-Array attributes - set
			if s, ok := col.Value.(string); ok {
				if len(s) > 3 && s[0] == '$' && s[len(s)-1] == '$' {
					sql.WriteString(s[1 : len(s)-1])
					continue
				}
			}
			sql.WriteString(col.Param)
		}

		params[col.Param[1:]] = col.Value
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
			sql.WriteString(col.Param)
			params[col.Param[1:]] = col.Value
		}
	}

	return sql.String()

}

func genSQLInsertWithValues(m *mut.Mutation, params map[string]interface{}) string {

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
		sql.WriteString(set.Param)

		params[set.Param[1:]] = set.Value
	}
	sql.WriteByte(')')

	return sql.String()
}
func genSQLInsertWithSelect(m *mut.Mutation, params map[string]interface{}) string {

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
		sql.WriteString(col.Param)

		params[col.Param[1:]] = col.Value
	}
	sql.WriteByte(')')
	//sql.WriteString(" from dual where NOT EXISTS (select 1 from EOP where PKey=@PKey and SortK=@SortK) ")

	return sql.String()
}

// mut.Mutations=[]dbs.Mutation
func genSQLBulkInsert(m *mut.Mutation, bi mut.Mutations) string {

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
	sql.WriteString(`) values `)

	for i, m := range bi {

		m := m.(*mut.Mutation)
		if i != 0 {
			sql.WriteByte(',')
		}
		sql.WriteByte('(')

		for i, col := range m.GetMembers() {

			// does table have a sortk
			if i != 0 {
				sql.WriteByte(',')
			}
			// check for string type and a SQL keyword surrounded by "_" like "$CURRENT_TIMESTAMP$"
			switch x := col.Value.(type) {

			case float64:
				s64 := strconv.FormatFloat(x, 'E', -1, 64)
				sql.WriteString(s64)
			case int:
				sql.WriteString(strconv.Itoa(x))
			case int64:
				sql.WriteString(strconv.FormatInt(x, 10))
			case string:
				sql.WriteString(fmt.Sprintf("%q", x))
			case bool:
				sql.WriteString(fmt.Sprintf("%v", x))
			case []byte:
				sql.WriteString(fmt.Sprintf("from_base64(%q)", util.UID(x).String()))
			case util.UID:
				sql.WriteString(fmt.Sprintf("from_base64(%q)", x.String()))
			}
		}

		sql.WriteByte(')')

	}

	return sql.String()
}

func genSQLStatement(m *mut.Mutation, opr mut.StdMut) ggMutation {
	var params map[string]interface{}

	// 	type dml struct {
	//   	stmt [2]spanner.Statement
	// 	    merge bool
	// }
	switch opr {

	case mut.Remove:

		params = make(map[string]interface{}) //{"pk": m.GetPK(), "sk": m.GetSK()}

		stmt := spanner.NewStatement(genSQLRemove(m, params))
		stmt.Params = params

		//stmts = make([]spanner.Statement, 1)
		return ggMutation{stmt: []spanner.Statement{stmt}} // stmt

	case mut.Update, mut.Append:

		params = make(map[string]interface{}) //{"pk": m.GetPK(), "sk": m.GetSK()}

		stmt := spanner.NewStatement(genSQLUpdate(m, params))
		stmt.Params = params

		//stmts = make([]spanner.Statement, 1)
		return ggMutation{stmt: []spanner.Statement{stmt}} // stmt

	case mut.Insert:

		params = make(map[string]interface{})

		stmt := spanner.NewStatement(genSQLInsertWithValues(m, params))
		stmt.Params = params
		return ggMutation{stmt: []spanner.Statement{stmt}}

	case mut.Merge:

		params = make(map[string]interface{}) //{"pk": m.GetPK(), "sk": m.GetSK()}

		s1 := spanner.NewStatement(genSQLUpdate(m, params))
		s1.Params = params

		s2 := spanner.NewStatement(genSQLInsertWithSelect(m, params))
		s2.Params = params

		return ggMutation{stmt: []spanner.Statement{s1, s2}, isMerge: true}

	default:
		panic(fmt.Errorf("db execute error: opr is no assigned"))
		return ggMutation{}
	}

}

// mut.Mutations = []dbs.Mutation
func Execute(bs []*mut.Mutations, tag string) error {
	// GoGraph mutations
	var (
		ggms  []ggMutation
		bggms [][]ggMutation
	)

	// generate statements for each mutation
	for _, b := range bs {

		for _, m := range *b {

			y, ok := m.(*mut.Mutation)
			if ok {
				// generate inbuilt "standard" SQL
				if y.GetOpr() == mut.BulkInsert {
					// Pass in complete *b and create single bulk insert stmt
					// b=*mut.Mutations
					stmt := genSQLBulkInsert(y, *b)
					gg := ggMutation{stmt: []spanner.Statement{spanner.Statement{SQL: stmt}}, isMerge: false}
					ggms = append(ggms, gg)

					break

				} else {

					ggms = append(ggms, genSQLStatement(y, y.GetOpr()))
				}

			} else {
				// source SQL from application
				var spnStmts []spanner.Statement

				for _, s := range m.GetStatements() {
					stmt := spanner.Statement{SQL: s.SQL, Params: s.Params}
					spnStmts = append(spnStmts, stmt)
				}
				ggms = append(ggms, ggMutation{spnStmts, false})
			}

		}

		bggms = append(bggms, ggms)
		ggms = nil
	}
	var (
		retryTx bool
		// bundled stmts
		stmts      []spanner.Statement
		mergeRetry []*spanner.Statement
		// batch of bundled statments
		bStmts [][]spanner.Statement
		bRetry [][]*spanner.Statement
	)
	//combine stmts and any merge alternate-SQL into separate slices
	for _, ggms := range bggms {
		retryTx = false
		for _, ggm := range ggms {

			switch ggm.isMerge {
			case true:
				retryTx = true
				stmts = append(stmts, ggm.stmt[0])
				mergeRetry = append(mergeRetry, &ggm.stmt[1])
			default:
				stmts = append(stmts, ggm.stmt...)
				for range ggm.stmt {
					mergeRetry = append(mergeRetry, nil)
				}
			}
		}
		if !retryTx {
			// abort any merge SQL processing by setting slice to nil
			mergeRetry = nil
		}
		// create a batch
		bStmts = append(bStmts, stmts)
		bRetry = append(bRetry, mergeRetry)
		stmts, mergeRetry = nil, nil
	}
	// show SQL
	if param.ShowSQL {
		logStmts(bStmts)
	}

	ctx := context.Background()
	//
	// apply to database using BatchUpdate
	//
	_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		// execute mutatations in single batch
		var retry bool

		for i, stmts := range bStmts {

			retry = false
			for len(stmts) > 0 {
				t0 := time.Now()
				rowcount, err := txn.BatchUpdate(ctx, stmts)
				t1 := time.Now()
				if err != nil {
					// elog.Add - removed as its causes a dependency cycle.
					return err
				}
				mergeRetry := bRetry[i]
				//
				// log every tenth execute
				dur := t1.Sub(t0).String()
				//
				if param.ReducedLog {
					if dot := strings.Index(dur, "."); dur[dot+2] == 57 && (dur[dot+3] == 54 || dur[dot+3] == 55) {
						syslog(fmt.Sprintf("BatchUpdate[%d]: Elapsed: %s, Stmts: %d  rowcount: %v  MergeRetry: %d retry: %v\n", i, dur, len(stmts), rowcount, len(mergeRetry), retry))
					}
				} else {
					syslog(fmt.Sprintf("BatchUpdate[%d]: Elapsed: %s, Stmts: %d  rowcount: %v  MergeRetry: %d retry: %v\n", i, dur, len(stmts), rowcount, len(mergeRetry), retry))
				}
				stmts = nil
				// check status of any merged sql statements
				//apply merge retry stmt if first merge stmt resulted in no rows processed
				if retry {
					break
				}
				for i, r := range mergeRetry {
					if r != nil {
						// retry merge-insert if merge-update processed zero rows.
						if rowcount[i] == 0 {
							retry = true
							break
						}
					}
				}
				if retry {
					// run all merge alternate stmts
					for _, mergeSQL := range mergeRetry {
						if mergeSQL != nil {
							stmts = append(stmts, *mergeSQL)
						}
					}
					// nullify mergeRetry to prevent processing in merge-retry execute
					mergeRetry = nil
				}
			}
		}

		return nil

	})

	return err
}

func logStmts(bStmts [][]spanner.Statement) {
	var (
		s     strings.Builder
		uuids string
	)

	for _, stmts := range bStmts {

		for _, v := range stmts {
			for k, kv := range v.Params {
				switch k {
				//case "Nd", "pk", "opk", "PKey", "cuid", "puid":
				case "pk", "PKey", "Puid":
					switch x := kv.(type) {
					case []byte:
						uuids = util.UID(x).String()
					case [][]uint8:
						for _, x := range x {
							uuids = util.UID(x).String()
						}

					}
				}
			}
		}
		s.WriteByte('[')
		s.WriteString(uuids)
		s.WriteByte(']')
		s.WriteString("Params: ")
		params := s.String()
		s.Reset()
		s.WriteByte('[')
		s.WriteString(uuids)
		s.WriteByte(']')
		s.WriteString("Stmt: ")
		stmt := s.String()
		s.Reset()
		for i, v := range stmts {
			slog.LogF("SQL:", fmt.Sprintf("%s %d sql: %s\n", stmt, i, v.SQL))
			slog.LogF("SQL:", fmt.Sprintf("%s %#v\n", params, v.Params))
		}
	}
}
