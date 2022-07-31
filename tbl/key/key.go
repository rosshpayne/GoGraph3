package key

import ()

type Key struct {
	Name  string // pkey []byte. sortk string
	Value interface{}
}

// populated in MergeMutation2
type MergeKey struct {
	Name   string
	Value  interface{}
	DBtype string // db type from TableKey (not type of Value). Used to validate Value.
}

type TableKey struct {
	Name   string // pkey []byte. sortk string
	DBtype string // "S","N","B"
}
