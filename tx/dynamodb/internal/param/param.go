package param

const (

	// Transaction Size
	// Dynamodb supports upto 25 db operations to a transaction. This imposes a limit on GoGraph when using Dynamodb.
	// For example, there is an imposed limit on scalar propagation of upto 23 attributes. Spanner has no such limitation.
	MaxMutations = 25
	MaxTxBatches = 4
	DBbulkInsert = MaxMutations


	StatsSystemTag string = "__system"
	StatsSaveTag   string = "__save"

	// statistics - sample Durations and Sample set size
	SampleDurDB    = "500ms"
	SampleDurWaits = "500ms"
	MaxSampleSet   = 10000 // maximum number of samples to keep for analysis

	// Dynamodb Batch insert/delete retries
	// unprocessed batch items retries
	MaxUnprocRetries = 10
	// operation error retries
	MaxOperRetries = 5

	// TimeZone
	TZ = "Australia/Sydney"
)

var (
		// statistics monitor
	StatsSystem bool
	)
