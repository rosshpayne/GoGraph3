// +build spanner

package param

var (
	ReducedLog = true // reduced logging. So far only for DB:
	// rdf file: can be modified by rdf.loader "i" argument
	//GraphTable = "DyGraphOD2"
	// sql logging: can be modified by showsql argument
	ShowSQL bool
	DebugOn = false
)

const (
	// Logging

	Logid    = "main:"
	Spanner  = 1
	Dynamodb = 2
	DB       = 1

	// elasticsearch
	// ESenabled = false
	ESindex = "gographidx"
	// number of log entries before updating log table
	//ESlogCommit = 20

	// Batch sizes
	// saving parent-child node to Edge_, EdgeChild_
	MaxMutations = 250
	DBbulkInsert = MaxMutations

	// TimeZone
	TZ = "Australia/Sydney"

	// goroutine concurrency - multipler to apply to number of saveRDF goroutines to determine number of ES load goroutines
	ESgrMultipler = 1

	//SysDebugOn = false
	//
	// Parameters for:  Overflow Blocks - overflow blocks belong to a parent node. It is where the child UIDs and propagated scalar data is stored.
	//                  The overflow block is know as the target of propagation. Each overflow block is identifier by its own UUID.
	//					There are two targets for child data propagation. Either directly inot the the parent uid-pred (edge source). When this area becomes full n
	//                  (as determined by parameter EmbeddedChildNodes) child data is targeted to a selectected overflow block, kown as the target UID..

	// type (ty value in Block table) for overflow blocks
	OVFL = "__ovfl"

	// EmbeddedChildNodes - number of cUIDs (and the assoicated propagated scalar data) stored in the paraent uid-pred attribute e.g. A#G#:S.
	// All uid-preds can be identified by the following sortk: <partitionIdentifier>#G#:<uid-pred-short-name>
	// for a parent with limited amount of scalar data the number of embedded child uids can be relatively large. For a parent
	// node with substantial scalar data this parameter should be corresponding small (< 5) to minimise the space consumed
	// within the parent block. The more space consumed by the embedded child node data the more RCUs required to read the parent Node data,
	// which will be an overhead in circumstances where child data is not required.
	EmbeddedChildNodes = 55 // prod value: 20
	// Overflow block
	//	AvailableOvflBlocks = 1 // prod value: 5

	// MaxOvFlBlocks - max number of overflow blocks. Set to the desired number of concurrent reads on overflow blocks ie. the degree of parallelism required. Prod may have upto 100.
	// As each block resides in its own UUID (PKey) there shoud be little contention when reading them all in parallel. When max is reached the overflow
	// blocks are then reused with new overflow items (Identified by an ID at the end of the sortK e.g. A#G#:S#:N#3, here the id is 3)  being added to each existing block
	// There is no limit on the number of overflow items, hence no limit on the number of child nodes attached to a parent node.
	MaxOvFlBlocks = 5 // prod value : 100

	// OvfwBatchSize - number of uids to an overflow batch. Always fixed at this value.
	// The limit is checked using the database SIZE function during insert of the child data into the overflow block.
	// An overflow block has an unlimited number of batches.
	OvfwBatchSize = 300 // Prod 100 to 500.

	// OBatchThreshold, initial number of batches in an overflow block before creating new Overflow block.
	// Once all overflow blocks have been created (MaxOvFlBlocks), blocks are randomly chosen and each block
	// can have an unlimited number of batches.
	OBatchThreshold = 10 //100

	ElasticSearchOn = true
)

//var LogServices = []string{"DB", "monitor", "grmgr", "gql", "gqlES", "anmgr", "errlog", "rdfuuid", "rdfLoader", "ElasticSearch", "rdfSaveDB", "gqlDB", "TypesDB"}
//var LogServices = []string{"monitor", "grmgr", "gql", "gqlES", "anmgr", "errlog", "rdfuuid", "rdfLoader", "ElasticSearch", "rdfSaveDB", "gqlDB", "TypesDB"}
//var LogServices = []string{"AttachNode", "DB", "rdfLoader", "Tx", "DPDB", "processDP"}

var LogServices = []string{Logid, "DB:", "gql"}
