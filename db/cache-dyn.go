//go:build dynamodb
// +build dynamodb

package db

import (
	"context"
	"fmt"
	"sync"

	"github.com/ros2hp/method-db/log"
	"github.com/ros2hp/method-db/query"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type accessMethod string

const (
	cGetItem accessMethod = "GetItem"
	cQuery   accessMethod = "Query"
	cScan    accessMethod = "Scan"
	cErr     accessMethod = "Err"
)

func init() {
	queryCache = newParCache()
	tabCache = newTabCache()
}

///////////////////// parsed Cache ///////////////////

type qryEntry struct {
	access accessMethod
	pk, sk string
	ch     chan struct{}
}

type queryTag = string

type queryCacheT struct {
	sync.Mutex
	cache map[queryTag]*qryEntry
	//
	entries   []queryTag // lru list
	cacheSize int
}

var queryCache *queryCacheT

func newParCache() *queryCacheT {
	return &queryCacheT{cache: make(map[queryTag]*qryEntry)}
}

func (p *queryCacheT) addEntry(qTag queryTag, e *qryEntry)   {}
func (p *queryCacheT) touchEntry(qTag queryTag, e *qryEntry) {}

///////////////////// table Cache ///////////////////

type tabEntry struct {
	dto *dynamodb.DescribeTableOutput
	ch  chan struct{}
}

type tableName = string

type tabCacheT struct {
	sync.Mutex
	cache map[tableName]*tabEntry
	//
	entries   []tableName // lru list
	cacheSize int
}

var tabCache *tabCacheT

// type libCacheT struct {
// 	sync.Mutex
// 	*queryCacheT
// 	*tabCacheT
// }

// var libCache libCacheT

// func (p *libCache) addNode()
// func (p *libCache) touchNode()

func newTabCache() *tabCacheT {
	return &tabCacheT{cache: make(map[tableName]*tabEntry)}
}

func (p *tabCacheT) addEntry(t tableName, e *tabEntry)   {}
func (p *tabCacheT) touchEntry(t tableName, e *tabEntry) {}

func (p *queryCacheT) fetchQuery(ctx context.Context, dh *DynamodbHandle, q *query.QueryHandle) (*qryEntry, error) {
	var err error
	//logid := "fetchQuery"

	genKey := func() string {
		key := q.GetTable()
		for _, v := range q.GetKeys() {
			key += v
		}
		for _, v := range q.GetKeys() {
			key += q.GetKeyComparOpr(v)
		}
		return key
	}

	mkey := genKey()
	p.Lock()
	e, _ := p.cache[mkey]

	if e == nil {
		e = &qryEntry{ch: make(chan struct{})}
		p.cache[mkey] = e
		p.Unlock()

		d, err := tabCache.fetchTableDesc(ctx, dh, q.GetTable())
		if err != nil {
			return nil, fmt.Errorf("Validation Error for query [tag: %q]: %w", q.Tag, err)
		}
		//err = q.validateQueryInput(q, d)

		err = e.assignAccessMethod(q, d)
		if err != nil {
			p.Lock()
			delete(p.cache, mkey)
			p.Unlock()
			return nil, err
		}
		close(e.ch)
	} else {
		p.Unlock()
		<-e.ch
	}
	if len(e.access) == 0 {
		p.Lock()
		delete(p.cache, mkey)
		p.Unlock()
		return nil, fmt.Errorf("Validation Error: query access not defined")
	}
	return e, err
}

// GetTableKeys returns table keys in pk,sk order

// func GetKeys(ctx context.Context, table string) { // need dh
func GetTableKeys(ctx context.Context, table string, dh *DynamodbHandle) (*tabEntry, error) {

	var err error

	t := tabCache

	t.Lock()

	e, _ := t.cache[table]

	if e == nil {
		e = &tabEntry{ch: make(chan struct{})}
		t.cache[table] = e
		t.Unlock()

		e.dto, err = dbGetTableDesc(ctx, dh, table)
		if err != nil {
			return nil, err
		}
		close(e.ch)
	} else {
		t.Unlock()
		<-e.ch
	}
	if e.dto == nil {
		return nil, fmt.Errorf("fetchTableDesc error: nil entry")
	}
	return e, err
}

func (t *tabCacheT) fetchTableDesc(ctx context.Context, dh *DynamodbHandle, table string) (*tabEntry, error) {
	var err error

	t.Lock()

	e, _ := t.cache[table]

	if e == nil {
		e = &tabEntry{ch: make(chan struct{})}
		t.cache[table] = e
		t.Unlock()

		e.dto, err = dbGetTableDesc(ctx, dh, table)
		if err != nil {
			return nil, err
		}
		close(e.ch)
	} else {
		t.Unlock()
		<-e.ch
	}
	if e.dto == nil {
		return nil, fmt.Errorf("fetchTableDesc error: nil entry")
	}
	return e, err
}

func (pe *qryEntry) assignAccessMethod(q *query.QueryHandle, e *tabEntry) error {

	var (
		err      error
		idxFound bool
	)

	//	logid := "assignAccessMethod"

	qkeys := q.GetKeys()
	log.LogDebug(fmt.Sprintf("assignAccessMethod: tag: %q, qkeys %#v", q.Tag, qkeys))

	if len(qkeys) == 0 {
		pe.access = cScan
		return nil
	}

	if len(qkeys) > 2 {
		return fmt.Errorf("Cannot have more than two keys specified")
	}

	// Validate key(s)
	switch len(q.GetIndex()) {

	case 0:
		// access table
		// check query and table keys match
		for _, vv := range e.dto.Table.KeySchema {
			for _, v := range qkeys {
				if *vv.AttributeName == v {
					if vv.KeyType == types.KeyTypeHash {
						pe.pk = v
					} else {
						pe.sk = v
					}
				}
			}
		}
		log.LogDebug(fmt.Sprintf("assignAccessMethod: case 0 pk,sk: %s %q #Keys: %d compare %d", pe.pk, pe.sk, len(e.dto.Table.KeySchema), ComparOpr(q.GetKeyComparOpr(pe.sk))))

		switch len(e.dto.Table.KeySchema) {

		case 1:
			// no sort key
			if len(pe.pk) > 0 && len(qkeys) == 1 {
				pe.access = cGetItem
				return nil
			}
			if len(qkeys) > 1 {
				err = fmt.Errorf(fmt.Sprintf("Table [%s] has only a partition key. Query definition supplied multiple keys", q.GetTable()))
			}
			if len(pe.pk) == 0 {
				err = fmt.Errorf(fmt.Sprintf("%q is not the partition key for table %q", qkeys[0], q.GetTable()))
			}
			return err

		case 2:
			// pk and sk
			if len(pe.pk) > 0 && len(pe.sk) > 0 && len(qkeys) == 2 {

				if ComparOpr(q.GetKeyComparOpr(pe.sk)) == EQ {
					pe.access = cGetItem
					return nil
				} else {
					pe.access = cQuery
					return nil
				}
			}
			if len(pe.pk) > 0 && len(qkeys) == 1 {
				pe.access = cQuery
				return nil
			}
			if len(pe.pk) == 0 {
				if len(pe.sk) == 0 {
					return fmt.Errorf(fmt.Sprintf("either %q or %q is not a partition or sort key for table %q", qkeys[0], qkeys[1], q.GetTable()))
				} else {
					var qk string
					qk = qkeys[0]
					if qkeys[0] == pe.sk {
						qk = qkeys[1]
					}
					return fmt.Errorf(fmt.Sprintf("%q is not a partition key for table %q", qk, q.GetTable()))
				}
			}
			if len(pe.sk) == 0 {
				var qk string
				qk = qkeys[0]
				if qkeys[0] == pe.pk {
					qk = qkeys[1]
				}
				return fmt.Errorf(fmt.Sprintf("%q is not a sort key for table %q", qk, q.GetTable()))
			}
		}

	default:

		// index keys specified
		for i, v := range e.dto.Table.GlobalSecondaryIndexes {

			if q.GetIndex() == *v.IndexName {
				idxFound = true
				// check keys match
				for _, vv := range e.dto.Table.GlobalSecondaryIndexes[i].KeySchema {
					for _, v := range q.GetKeys() {
						if *vv.AttributeName == v {
							if vv.KeyType == types.KeyTypeHash {
								pe.pk = v
							} else {
								pe.sk = v
							}
						}
					}
				}
				switch len(e.dto.Table.GlobalSecondaryIndexes[i].KeySchema) {

				case 1:
					// no sort key
					if len(pe.pk) > 0 && len(qkeys) == 1 {
						pe.access = cGetItem
						return nil
					}
					if len(qkeys) > 1 {
						return fmt.Errorf(fmt.Sprintf("Table [%s] has only a partition key. Query definition supplied multiple keys", q.GetTable()))
					}
					if len(pe.pk) == 0 {
						return fmt.Errorf(fmt.Sprintf("%q is not the partition key for table %q", qkeys[0], q.GetTable()))
					}

				case 2:
					// pk and sk
					if len(pe.pk) > 0 && len(pe.sk) > 0 && len(qkeys) == 2 {

						if ComparOpr(q.GetKeyComparOpr(pe.sk)) == EQ && len(q.GetIndex()) == 0 {
							pe.access = cGetItem
							return nil
						} else {
							pe.access = cQuery
							return nil
						}
					}
					if len(pe.pk) > 0 && len(qkeys) == 1 {
						pe.access = cQuery
						return nil
					}
					if len(pe.pk) == 0 {
						if len(pe.sk) == 0 {
							return fmt.Errorf(fmt.Sprintf("either %q or %q is not a partition or sort key for table %q", qkeys[0], qkeys[1], q.GetTable()))
						} else {
							var qk string
							qk = qkeys[0]
							if qkeys[0] == pe.sk {
								qk = qkeys[1]
							}
							return fmt.Errorf(fmt.Sprintf("%q is not a partition key for table %q", qk, q.GetTable()))
						}
					}
					if len(pe.sk) == 0 {
						var qk string
						qk = qkeys[0]
						if qkeys[0] == pe.pk {
							qk = qkeys[1]
						}
						return fmt.Errorf(fmt.Sprintf("%q is not a sort key for table %q", qk, q.GetTable()))
					}
				}
			}
		}

		if !idxFound {
			// try
			for i, v := range e.dto.Table.LocalSecondaryIndexes {

				if q.GetIndex() == *v.IndexName {
					idxFound = true
					// check keys match
					for _, vv := range e.dto.Table.LocalSecondaryIndexes[i].KeySchema {
						for _, v := range q.GetKeys() {
							if *vv.AttributeName == v {
								if vv.KeyType == types.KeyTypeHash {
									pe.pk = v
								} else {
									pe.sk = v
								}
							}
						}
					}
					switch len(e.dto.Table.LocalSecondaryIndexes[i].KeySchema) {

					case 1:
						// no sort key
						if len(pe.pk) > 0 && len(qkeys) == 1 {
							pe.access = cGetItem
							return nil
						}
						if len(qkeys) > 1 {
							return fmt.Errorf(fmt.Sprintf("Table [%s] has only a partition key. Query definition supplied multiple keys", q.GetTable()))
						}
						if len(pe.pk) == 0 {
							return fmt.Errorf(fmt.Sprintf("%q is not the partition key for table %q", qkeys[0], q.GetTable()))
						}

					case 2:
						// pk and sk
						if len(pe.pk) > 0 && len(pe.sk) > 0 && len(qkeys) == 2 {

							if ComparOpr(q.GetKeyComparOpr(pe.sk)) == EQ {
								pe.access = cGetItem
								return nil
							} else {
								pe.access = cQuery
								return nil
							}
						}
						if len(pe.pk) > 0 && len(qkeys) == 1 {
							pe.access = cQuery
							return nil
						}
						if len(pe.pk) == 0 {
							if len(pe.sk) == 0 {
								return fmt.Errorf(fmt.Sprintf("either %q or %q is not a partition or sort key for table %q", qkeys[0], qkeys[1], q.GetTable()))
							} else {
								var qk string
								qk = qkeys[0]
								if qkeys[0] == pe.sk {
									qk = qkeys[1]
								}
								return fmt.Errorf(fmt.Sprintf("%q is not a partition key for table %q", qk, q.GetTable()))
							}
						}
						if len(pe.sk) == 0 {
							var qk string
							qk = qkeys[0]
							if qkeys[0] == pe.pk {
								qk = qkeys[1]
							}
							return fmt.Errorf(fmt.Sprintf("%q is not a sort key for table %q", qk, q.GetTable()))
						}
					}
				}
			}
			if !idxFound {
				// index not found error
				return fmt.Errorf("%s index does not exist on table %q", q.GetIndex(), q.GetTable())
			}
		}
	}

	// no Keys defined
	return fmt.Errorf("Inconsitency in assignAccessMethod()")

}

func (pe *qryEntry) GetPK() string {
	return pe.pk
}

func (pe *qryEntry) GetSK() string {
	return pe.sk
}

func dbGetTableDesc(ctx context.Context, dh *DynamodbHandle, tbl string) (*dynamodb.DescribeTableOutput, error) {

	inp := dynamodb.DescribeTableInput{TableName: aws.String(tbl)}
	out, err := dh.DescribeTable(ctx, &inp)
	return out, err
}
