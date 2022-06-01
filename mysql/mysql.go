package mysql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/GoGraph/db"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx/mut"
	"github.com/GoGraph/tx/query"

	_ "github.com/go-sql-driver/mysql"
)

const (
	logid = "mysql"
	mysql = "mysql"
)

type MySQL struct {
	options  db.Options
	trunctbl []tbl.Name
	ctx      context.Context
	*sql.DB
}

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

func Init(ctx context.Context) {

	client, err := newMySQL("admin:gjIe8Hl9SFD1g3ahyu6F@tcp(mysql8.cjegagpjwjyi.us-east-1.rds.amazonaws.com:3306)/GoGraph")
	if err != nil {
		logerr(err)
	} else {
		m := MySQL{DB: client, ctx: ctx}
		db.Register("mysql-GoGraph", m)
	}
}

func newMySQL(dsn string) (*sql.DB, error) {

	// Note: first arg must be the same as the name registered by go-sql-driver/mysql using database/sql.Register(). Specifiy explicilty (ie. "mysql")
	// do not use a const or variables which can be changed by the developer.
	// Arg 2, the DSN can vary with the same "mysql" to get multiple sq.DB's that point to different databases.
	mdb, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(fmt.Errorf("Open database error: %w", err)) // TODO: don't panic here...
	}
	//defer mdb.Close() //TODO: when to db.close

	// Open doesn't open a connection. Validate DSN data:
	err = mdb.Ping()
	if err != nil {
		panic(err.Error()) // TODO: proper error handling
	}
	fmt.Println("Successfully pinged database..")
	return mdb, err
}

// Execute manipulates data. See ExecuteQuery()
func (h MySQL) Execute(ctx context.Context, bs []*mut.Mutations, tag string, api db.API, prepare bool, opt ...db.Option) error {

	var (
		err error
		cTx *sql.Tx // client Tx
	)

	// check API
	switch api {
	case db.TransactionAPI:

		if ctx == nil {
			syslog("MySQL transactional API is being used with no context passed in")
			cTx, err = h.Begin()
		} else {
			syslog("MySQL transactional API is being used with a context passed in")
			txo := sql.TxOptions{Isolation: sql.LevelReadCommitted} // TODO: use options argument in .DB(?, options...)
			cTx, err = h.BeginTx(ctx, &txo)
		}
		if err != nil {
			err := fmt.Errorf("Error in BeginTx(): %w", err)
			logerr(err)
			return err
		}
	case db.StdAPI:
		// non-transactional
		syslog("MySQL non-transactional API is being used.")
	case db.BatchAPI:
		syslog(fmt.Sprintf("MySQL does not support a Batch API. Std API will be used instead. Tag: %s", tag))
	default:
		panic(fmt.Errorf("MySQL Execute(): no api specified"))
	}

	err = execute(ctx, h.DB, bs, tag, cTx, prepare, opt...)

	return err

}

// func (h MySQL) Truncate(tbs []tbl.Name) error {
// 	h.trunctbl = append(h.trunctbl, tbs...)
// 	// ctx := context.Background() // TODO Implement
// 	// for _, t := range tbs {
// 	// 	_, err := h.ExecContext(ctx, "truncate "+string(t))
// 	// 	if err != nil {
// 	// 		panic(err)
// 	// 	}
// 	// }
// 	return nil
// }

func (h MySQL) ExecuteQuery(ctx context.Context, q *query.QueryHandle, o ...db.Option) error {

	if ctx == nil {
		ctx = h.ctx
	}
	return executeQuery(ctx, h.DB, q, o...)

}

func (h MySQL) Close(q *query.QueryHandle) error {

	return closePrepStmt(h.DB, q)

}

func (h MySQL) Ctx() context.Context {

	return h.ctx

}

func (h MySQL) CloseTx(bs []*mut.Mutations) {

	for _, j := range bs {
		for _, m := range *j {

			m := m.(*mut.Mutation)

			if m.PrepStmt() != nil {
				m.PrepStmt().(*sql.DB).Close()
			}
		}
	}

}

func (h MySQL) String() string {

	return "mysql [not default]"

}
