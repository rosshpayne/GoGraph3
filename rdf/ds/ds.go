package ds

import (
	"github.com/ros2hp/method-db/uuid"
)

type NdShortNm = string

type ErrNodes map[NdShortNm]*Node

// attribute name-dynamo value place holder
type NV struct {
	Sortk string    // dynamodb sortk value  P#:C (for scalar) P#G#:C (for Nd)
	SName NdShortNm //  rdf subject, aka blank-node-name
	Name  string    // predicate name == type attribute name
	DT    string    // GoGraph datatype: N,S,Bl,B, LN,LS,LBL,LB, Nd, SN,SS,SBl, SB
	C     string    // RDF predicate (equiv to GoGraph type attribute name) short name
	Value interface{}
	Ty    string // node type (short name) e.g. P (Person) - used in GSI
	Ix    string // type of index for scalars. x : enter into GSI via p attribute, ft: full text using AWS ElasticSearch service
}

type Line struct {
	N    int    // line number in rdf file
	Subj string // shortName  (blank-node-name) "_a" representing a UUID - conversion takes place just before loading into db
	Pred string // two types of entries: 1) __type 2) Name of attribute in the type.
	Obj  string // typeName  or data (scalar, set/list, shortName for UUID )
}

// channel type
type Node struct {
	ID     NdShortNm // blank-node-id, may not be that short
	PKey   string    // (optional) source from predicate, __ID. User supplied pkey. Not fully implemented as code currently relieds on UUID as PKey. May store PKey as attribute and keep using UUID as solution.
	UUID   uuid.UID  // (optional) source from predicate, __ID. User supplied UUID - typically used for testing to get consistent order in results
	TyName string    // node type name as referenced in GoGraph Type system. getType() returns GoGraph type based on type name
	Lines  []Line
	Err    []error // used by verification process to record any errors
}
