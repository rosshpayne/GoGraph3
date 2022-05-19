package db

//      API                             Supported (Y|N)
//                            Dynamodb           Spanner              MySQL               Comment
//      StdAPI                  Y              Y (SQL)              Y (SQL)              The standard (or default) api's available with the database
//      BatchAPI                Y              Y (batch SQL)        N
//      TransactionalAPI        Y              N (Std behaviour)    Y (begin stmt)
//      ScanAPI                 Y              N                    N
//      OptimAPI                Y

// API sets database properitary api's to use to communicate with the database. See table above for list of supported API values for each database
type API byte

const (
	StdAPI API = iota
	BatchAPI
	TransactionAPI
	ScanAPI
	OptimAPI // run inserts as BatchAPI, update/delets as StdAPI. Inserts and updates must be independent of one another.
)
