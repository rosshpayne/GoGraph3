package main

import (
	"context"
	"sync"

	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tx"
	"github.com/GoGraph/tx/query"
	"github.com/GoGraph/uuid"
)

//
func ScanParallel(ctx context.Context, parallel int, ty string, sk string, fetchCh chan<- rec, id uuid.UID, restart bool) {

	var (
		err error
		wg  sync.WaitGroup
	)

	p := int(parallel / 2)
	pks := make([][]rec, p, p)

	ptx := tx.NewQueryContext(ctx, "label", tbl.TblName)

	// a parallel scan will be paginated into 1Mb for each thread.
	ptx.Select(&pks).Filter("Ty", ty).Filter("SortK", sk).Limit(100).Paginate(id, restart).Parallel(p)

	wg.Add(p)
	for i, worker := range ptx.Workers() {

		go func(worker *query.QueryHandle, i int) {

			for !worker.EOD() {

				err = ptx.Execute(i)

				if err != nil {
					panic(err)
				}

				for _, v := range pks[i] {
					fetchCh <- v
				}
			}
			wg.Done()
		}(worker, i)
	}

	wg.Wait()
	close(fetchCh)
}
