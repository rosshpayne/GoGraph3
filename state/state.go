package state

import (
	"errors"
	"fmt"
	"strings"

	//"github.com/GoGraph/db"
	param "github.com/GoGraph/dygparam"
	"github.com/GoGraph/run"
	"github.com/GoGraph/syslog"
	"github.com/GoGraph/tbl"
	"github.com/ros2hp/method-db/tx"
	"github.com/ros2hp/method-db/db"
	"github.com/ros2hp/method-db/mut"
	"github.com/ros2hp/method-db/query"
	"github.com/GoGraph/types"
)

func getState(state string) string {
	var s strings.Builder
	s.WriteString(types.GraphSN())
	s.WriteByte('|')
	if param.Environ == "dev" {
		s.WriteString("dev|")
	}
	s.WriteString(state)

	return s.String()
}

func Set(state string, value interface{}) error {

	syslog.Log("state", fmt.Sprintf("set state data for %s", getState(state)))
	// perform as non-transactional (single)
	stx := tx.NewTx("stateSet").DB("mysql-GoGraph") // was NewSingle()
	// either a dynamodb insert or update will perform a merge like operation ie. an update will insert if item
	// does not exist, or an insert will do an update if the item does exist.
	// however for spanner we need to do a merge to accomplish both inserting and updating of existing item
	// a merge will do an Update (both SQL and dynamo), if no data then an insert.

	m := stx.NewMerge(tbl.State).AddMember("Name", getState(state), mut.IsKey)

	// 	switch value.(type) {
	// 	case int64, int:
	// 		m.AddMember("N", value)
	// 	case string:
	// 		m.AddMember("S", value)
	// 	case binary:
	// 		m.AddMember("B", value)
	// 	}
	m.AddMember("Value", value)
	m.AddMember("Updated", "$CURRENT_TIMESTAMP$")
	m.AddMember("Run", run.GetRunId())

	err := stx.Execute()
	if err != nil {
		return err
	}
	return nil
}

func Get(state string) (string, error) {

	var err error
	type Value struct {
		// N int64
		Value string
		// B []byte
	}
	result := &Value{}

	qtx := tx.NewQuery("State", tbl.State).DB("mysql-gograph", []db.Option{db.Option{Name: "singlerow", Val: true}}...)

	qtx.Select(result).Key("Name", getState(state))

	syslog.Log("state", fmt.Sprintf("get state data for [%s]", getState(state)))

	err = qtx.Execute()
	if err != nil {
		if errors.Is(err, query.NoDataFoundErr) {
			fmt.Println("No rows returned in sate.Get()......")
			return "", err
		}
		// if strings.Contains(strings.ToLower(err.Error()), "no rows in result") {
		// 	fmt.Println("No rows returned in sate.Get()......")
		// 	return "", err
		// }
		// if driverErr, ok := err.(*mySQL.MySQLError); ok { // Now the error number is accessible directly
		// 	fmt.Println(" driverErr.Number ", driverErr.Number)
		// } else {
		// 	fmt.Printf("err type is %T\n", err) //*errors.errorString
		// }
		// if errors.Is(err, query.NoDataFoundErr) {
		// 	syslog.Log("state: ", fmt.Sprintf("No state data found for %s", getState(state)))
		// 	return "", err
		// }
		syslog.Log("state", fmt.Sprintf("Error [%s]", err))
		panic(err)
	}
	fmt.Println("Rows returned in sate.Get().+++++.")
	return result.Value, nil
}
