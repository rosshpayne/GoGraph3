// build+ spanner

package db

import (
	"context"
	"fmt"

	"github.com/GoGraph/db"
	elog "github.com/GoGraph/rdf/errlog"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/util"

	"cloud.google.com/go/spanner" //v1.21.0
)

const (
	batchsize = 100
	logid     = "DB: "
)

var (
	err         error
	rows        int64
	unprocessed [][]byte
	client      *spanner.Client
)

// func init() {
// 	client, _ = dbConn.New()
// }
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

type Unprocessed struct {
	PKey []byte `spanner:"PKey"`
}

func ScanForDPitems(ty string, dpCh chan<- util.UID) {

	var all []util.UID
	slog.Log("DPDB:", fmt.Sprintf("ScanForDPitems for type %q", ty))
	client := db.GetClient()
	stmt := spanner.Statement{SQL: `Select PKey from Block where Ty = @ty and IX = "X"`, Params: map[string]interface{}{"ty": ty}}
	ctx := context.Background()
	iter := client.Single().Query(ctx, stmt)

	err = iter.Do(func(r *spanner.Row) error {
		rows++

		rec := Unprocessed{}
		err := r.ToStruct(&rec)
		if err != nil {
			return err
		}
		all = append(all, util.UID(rec.PKey))

		return nil
	})
	if err != nil {
		elog.Add("DB:", err)
	}
	slog.Log("DPDB:", fmt.Sprintf("Unprocessed records for type %q: %d", ty, len(all)))
	for _, v := range all {
		// blocking enqueue on channel - limited number of handling processors will force a wait on channel
		dpCh <- v
	}
	close(dpCh)

}
