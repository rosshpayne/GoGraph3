package mut

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"

	"github.com/GoGraph/dbs"
	"github.com/GoGraph/tbl"
	"github.com/GoGraph/tbl/key"
	"github.com/GoGraph/uuid"
)

type StdMut byte
type MutOpr byte // aka update-expression in dynamodb (almost)
type Cond byte

//type MutOpr string

const (
	Merge StdMut = iota + 1
	Insert
	Delete
	Update // update performing "set =" operation etc
	TruncateTbl
	//
	Set      MutOpr = iota + 1
	Inc             // set col = col + 1
	Subtract        // set col = col - 1
	Add             // set col = col - <num>
	IsKey           // used in query & update mode. Alternative to registering table/index and its keys. TODO:implement reg throughout.
	Append          // update by appending to array/list attribute
	Remove          // remove attribute
	//
	AttrExists Cond = iota
	AttrNotExists
)

func (s StdMut) String() string {
	switch s {
	case Merge:
		return "merge"
	case Insert:
		return "insert"
	case Update:
		return "update"
	case Delete:
		return "delete"
	}
	return "not-defined"
}

func (s MutOpr) String() string {
	switch s {
	case Set:
		return "Set"
	case Inc:
		return "Inc"
	case Subtract:
		return "Subtract"
	case Add:
		return "Add"
	case IsKey:
		return "IsKey"
	case Append:
		return "Append"
	case Remove:
		return "Remove"
	}
	return "not-defined"
}

// func (c Cond) String() string {
// 	switch c {
// 	case AttrExists:
// 		return "Attribute_Exists"
// 	case AttrNotExists:
// 		return "Attribute_Not_Exists"
// 	}
// 	return "NA"
// }

var (
	err error
)

//
// database API meta structures
//
type Member struct {
	//sortk string
	Name  string // attribute name: when contains "#",":"??
	Param string // used in Spanner implementation. All value placements are identified by "@param"
	Value interface{}
	Array bool   // member can be appended to ie. is an array (List) type in Dynamodb
	Opr   MutOpr // for update stmts only: default is to concat for Array type. When true will overide with set of array.
	//Opr   StdMut // for update of numerics. Add rather than set e.g. set col = col + @v1. Default: set col=@v1
}

func (m *Member) IsKey() bool {
	return m.Opr == IsKey
}

func (m *Member) Set() bool {
	return m.Opr == Set
}

func (m *Member) Inc() bool {
	return m.Opr == Inc
}

func (m *Member) Subtract() bool {
	return m.Opr == Subtract
}

func (m *Member) Add() bool {
	return m.Opr == Add
}

// Examples of condition expression:
// "attribute_not_exists(Price)"    			"attribute_not_exists(%s)"
// "attribute_exists(ProductReviews.OneStar)".  "attribute_exists(%s)"
// "attribute_type(Color, :v_sub)"  			 "attribute_type(%s, %q)" 	  "attribute_type(%s, %g)"
// "begins_with(Pictures.FrontView, :v_sub)"
// "contains(Color, :v_sub)"
// "size(VideoClip) > :v_sub"

type condition struct {
	cond  Cond        // ]ASZ
	attr  string      // =
	value interface{} // size
}

func (c *condition) GetCond() Cond {
	return c.cond
}

func (c *condition) GetAttr() string {
	return c.attr
}

func (c *condition) GetValue() interface{} {
	return c.value
}

type Mutation struct {
	ms []Member
	cd *condition
	// pk   uuid.UID
	// sk   string
	pKey interface{}
	tbl  tbl.Name
	opr  StdMut
	//
	text     string      // alternate representation of a mutation e.g. sql
	prepStmt interface{} // some db's may optional "prepare" mutations before execution Three phase 1) single prepare stmt 2) multiple stmt executions 3) close stmt
	params   []interface{}
	err      error
}

type Mutations []dbs.Mutation //*Mutation

func (im *Mutations) GetMutation(i int) *Mutation {
	return (*im)[i].(*Mutation)
}

func (im *Mutations) NumMutations() int {
	return len(*im)
}

func NewInsert(tab tbl.Name) *Mutation {

	return &Mutation{tbl: tab, opr: Insert}

}

// NewMerge
// This operation is equivalent in a no-SQL Put operation as put will insert if new or update if present.
// However for SQL database it will perform an update, if not present, then insert.
func NewMerge(tab tbl.Name) *Mutation {

	return &Mutation{tbl: tab, opr: Merge}
}

func NewUpdate(tab tbl.Name) *Mutation {

	return &Mutation{tbl: tab, opr: Update}
}

func Truncate(tab tbl.Name) *Mutation {

	return &Mutation{tbl: tab, opr: TruncateTbl}
}

// func NewMutationEventLog(table string, pk  opr interface{}) *Mutation {
// 	return &Mutation{tbl: table, pk: pk, sk: sk, opr: opr}
// }

func (m *Mutation) GetStatements() []dbs.Statement { return nil }

func (m *Mutation) GetMembers() []Member {
	return m.ms
}

func SetMember(i int, ms []Member, v interface{}) {
	ms[i].Value = v
}

// func (m *Mutation) keys() []Member {
// 	var k []Member
// 	for _, v := range m.GetMembers() {
// 		if v.IsKey() {
// 			k = append(k, v)
// 		}
// 	}
// 	return k
// }

func (m *Mutation) GetKeys() []Member {
	var k []Member
	for _, v := range m.GetMembers() {
		if v.IsKey() {
			k = append(k, v)
		}
	}
	return k
}

func (m *Mutation) SetPrepStmt(p interface{}) {
	m.prepStmt = p
}

func (m *Mutation) PrepStmt() interface{} {
	return m.prepStmt
}

func (m *Mutation) SetText(p string) {
	m.text = p
}

func (m *Mutation) Text() string {
	return m.text
}

func (m *Mutation) SetError(e error) {
	m.err = e
}

func (m *Mutation) GetError() error {
	return m.err
}

func (m *Mutation) SQL() string {
	return m.text
}

func (m *Mutation) SetParams(p []interface{}) {
	m.params = p
}

func (m *Mutation) Params() []interface{} {
	return m.params
}

func (m *Mutation) GetOpr() StdMut {
	return m.opr
}

// func (m *Mutation) GetPK() uuid.UID {
// 	return m.pk
// }

// func (m *Mutation) GetSK() string {
// 	return m.sk
// }

func (m *Mutation) GetTable() string {
	return string(m.tbl)
}

func (m *Mutation) getMemberIndex(attr string) int {
	for i, v := range m.ms {
		if v.Name == attr {
			return i
		}
	}
	panic(fmt.Errorf("getMember: member %q not found in mutation members", attr))
	return -1
}

func (m *Mutation) GetMemberValue(attr string) interface{} {
	for _, v := range m.ms {
		if v.Name == attr {
			return v.Value
		}
	}
	panic(fmt.Errorf("GetMemberValue: member %q not found in mutation members", attr))
	return nil
}

func (m *Mutation) SetMemberValue(attr string, v interface{}) {
	i := m.getMemberIndex(attr)
	e := reflect.TypeOf(m.ms[i].Value)
	g := reflect.TypeOf(v)
	if e.Kind() != g.Kind() {
		panic(fmt.Errorf("SetMemberValue for %s expected a type of %q got %a", attr, e, g))
	}
	m.ms[i].Value = g
}

func (im *Mutation) AddMember(attr string, value interface{}, opr ...MutOpr) *Mutation {

	// Parameterised names based on spanner's. Spanner uses a parameter name based on attribute name starting with "@". Params can be ignored in other dbs.
	// For other database, such as MySQL, will need to convert from Spanner's repesentation to relevant database during query formulation in the Execute() phase.
	p := strings.Replace(attr, "#", "_", -1)
	p = strings.Replace(p, ":", "x", -1)
	if p[0] == '0' {
		p = "1" + p
	}
	m := Member{Name: attr, Param: "@" + p, Value: value}

	// assign Set to mut.Opr even for Insert DML where Opr will be ignored.
	m.Opr = Set

	// determine if member is an array type based on its value type. For Dynamobd  arrays are List or Set types.
	// However, there is no way to distinguish between List or Set using the value type,
	// but this is not necessary as GoGraph uses Lists only - as the order of the array data is important and needs to be preserved.
	// For Spanner there is only one array type.
	// Using reflect pkg replaces the use of hardwire attribute names that identify the array types e.g. case "Nd", " ", "Id", "XBl", "L*":
	// (the advantage of hardwiring attribute names is it makes for a generic solution that suits both Dynamodb & Spanner).
	// default behaviour is to append value to end of array
	// TODO: come up with generic solution for both Dynamodb & Spanner - probably not possible so make use of conditional compilation.
	m.Array = IsArray(value)

	// override Opr value with argument value if specified
	if len(opr) > 0 {
		m.Opr = opr[0]
	} else if m.Array {
		// default operation for arrays is append.
		// However, if array attribute does not exist Dynamo generates error: ValidationException: The provided expression refers to an attribute that does not exist in the item
		// in such cases you must Put
		// Conclusion: for the initial load the default is Put - this will overwrite what is in Nd which for the initial load will by a single NULL entry.
		// this is good as it means the the index entries in Nd match those in the scalar propagation atributes.
		// After the initial load the default should be set to Append as all items exist and the associated array attributes exist in those items, so appending will succeed.
		// Alternative solution is to add a update condition that test for attribute_exists(PKey) - fails and uses PUT otherwise Updates.
		m.Opr = Append
	}

	im.ms = append(im.ms, m)

	// Nd attribute is specified only during attach operations. Increment ASZ (Array Size) attribute in this case only.
	// if attr == "Nd" {
	// 	m = Member{Name: "ASZ", Param: "@ASZ", Value: 1, Opr: Inc}
	// 	im.ms = append(im.ms, m)
	// }
	//	}
	return im
}

func (im *Mutation) AddMember2(attr string, value interface{}, opr ...MutOpr) *Mutation {

	// Parameterised names based on spanner's. Spanner uses a parameter name based on attribute name starting with "@". Params can be ignored in other dbs.
	// For other database, such as MySQL, will need to convert from Spanner's repesentation to relevant database during query formulation in the Execute() phase.
	p := strings.Replace(attr, "#", "_", -1)
	p = strings.Replace(p, ":", "x", -1)
	if p[0] == '0' {
		p = "1" + p
	}
	m := Member{Name: attr, Param: "@" + p, Value: value}

	// assign Set to mut.Opr even for Insert DML where Opr will be ignored.
	m.Opr = Set

	// determine if member is an array type based on its value type. For Dynamobd  arrays are List or Set types.
	// However, there is no way to distinguish between List or Set using the value type,
	// but this is not necessary as GoGraph uses Lists only - as the order of the array data is important and needs to be preserved.
	// For Spanner there is only one array type.
	// Using reflect pkg replaces the use of hardwire attribute names that identify the array types e.g. case "Nd", " ", "Id", "XBl", "L*":
	// (the advantage of hardwiring attribute names is it makes for a generic solution that suits both Dynamodb & Spanner).
	// default behaviour is to append value to end of array
	// TODO: come up with generic solution for both Dynamodb & Spanner - probably not possible so make use of conditional compilation.
	m.Array = IsArray(value)

	// override Opr value with argument value if specified
	if len(opr) > 0 {
		m.Opr = opr[0]
	} else if m.Array {
		// default operation for arrays is append.
		// However, if array attribute does not exist Dynamo generates error: ValidationException: The provided expression refers to an attribute that does not exist in the item
		// in such cases you must Put
		// Conclusion: for the initial load the default is Put - this will overwrite what is in Nd which for the initial load will by a single NULL entry.
		// this is good as it means the the index entries in Nd match those in the scalar propagation atributes.
		// After the initial load the default should be set to Append as all items exist and the associated array attributes exist in those items, so appending will succeed.
		// Alternative solution is to add a update condition that test for attribute_exists(PKey) - fails and uses PUT otherwise Updates.
		m.Opr = Append
	}
	im.ms = append(im.ms, m)

	// Nd attribute is specified only during attach operations. Increment ASZ (Array Size) attribute in this case only.
	// if attr == "Nd" {
	// 	m = Member{Name: "ASZ", Param: "@ASZ", Value: 1, Opr: Inc}
	// 	im.ms = append(im.ms, m)
	// }
	//	}
	return im
}

func (im *Mutation) AddCondition(cond Cond, attr string, value ...interface{}) *Mutation { //, opr ...StdMut)

	im.cd = &condition{cond: cond, attr: attr, value: value}

	return im
}

func (im *Mutation) GetCondition() *condition {
	return im.cd
}

// NewMutation is written specifically for GoGraph. Pkg Cache has its own GOGraph version of this function
// Arguments pk,sk need to be replaced somehow, to remove dependency on types. Maybe use Tbl package in
// which tables are registered with known pk and sk (names and datatypes)
func NewMutation(tab tbl.Name, pk uuid.UID, sk string, opr StdMut) *Mutation {

	// not all table are represented in the Key table.
	// Those that are not make use of the IsKey member attribute
	kpk, ksk, _ := tbl.GetKeys(tab)

	mut := &Mutation{tbl: tab, opr: opr}

	// presumes all Primary Keys are a UUID
	// first two elements of mutations must be a PK and SK or a blank SK "__"
	if len(kpk) > 0 {

		mut.AddMember(kpk, []byte(pk), IsKey)
		if len(ksk) > 0 {
			mut.AddMember(ksk, sk, IsKey)
		} else {
			mut.AddMember("__", "")
		}
	}

	return mut
}

func NewMutation2(tab tbl.Name, opr StdMut, keys []key.Key) *Mutation {

	mut := &Mutation{tbl: tab, opr: opr}

	for _, v := range keys {
		mut.AddMember(v.Name, v.Value, IsKey)
	}
	return mut
}

// FindMutation searches the associated batch of mutations based on argument values.
func (bm *Mutations) FindMutation(table tbl.Name, pk uuid.UID, sk string) *Mutation {
	var (
		ok               bool
		sm               *Mutation
		match            int
		pkMatch, skMatch bool
	)
	for _, sm_ := range *bm {

		if sm, ok = sm_.(*Mutation); !ok {
			continue
		}
		if sm.opr == Merge {
			panic(fmt.Errorf("Merge mutation cannot be used with a MergeMuatation method"))
		}
		if sm.tbl != table || !(sm.opr == Insert || sm.opr == Update) {
			continue
		}
		match = 0
		pkMatch, skMatch = false, false

		// cycle thru members of source mutation.
		for _, attr := range sm.ms {

			switch attr.Name {
			case "PKey":
				match++
				if u, ok := attr.Value.(uuid.UID); ok {

					if bytes.Equal(u, pk) {
						pkMatch = true
					}

				} else if u, ok := attr.Value.([]byte); ok {
					if bytes.Equal(u, pk) {
						pkMatch = true
					}
				}

			case "SortK":
				match++
				if vsk, ok := attr.Value.(string); !ok {
					panic(fmt.Errorf("FindMutation iconcistency. Expected string for sortk interface value got %T", attr.Value))
				} else if vsk == sk {
					skMatch = true
				}
			}
			if match == 2 {
				if pkMatch && skMatch {
					return sm
				} else {
					break
				}
			}
		}

	}
	return nil
}

// FindMutation searches the associated batch of mutations based on argument values.
func (bm *Mutations) FindMutation2(table tbl.Name, keys []key.Key) (*Mutation, error) {
	var (
		ok    bool
		sm    *Mutation
		match int
	)
	// TODO: what about active batch???
	for _, sm_ := range *bm {

		if sm, ok = sm_.(*Mutation); !ok {
			continue
		}
		if sm.opr == Merge {
			panic(fmt.Errorf("Merge mutation cannot be used with a MergeMuatation method"))
		}
		if sm.tbl != table || !(sm.opr == Insert || sm.opr == Update) {
			continue
		}
		match = 0

		// merge keys have been validated and are in table key order (partition, sortk)
		for _, k := range keys {

			// cycle thru members of source mutations -
			for _, attr := range sm.ms {

				// evaluate Partition Key and Sortk types (for DYnamodb these are scalar types, number, string, [])
				if k.Name != attr.Name {
					continue
				}

				switch x := k.Value.(type) {

				case int64:
					if av, ok := attr.Value.(int64); !ok {
						return nil, fmt.Errorf("find mutation attribute %q. Expected an int64 type but supplied a %T type", attr.Name, attr.Value)
					} else if x == av {
						match++
					}

				case int:
					if av, ok := attr.Value.(int); !ok {
						return nil, fmt.Errorf("find mutation attribute %q. Expected an int type but supplied a %T type", attr.Name, attr.Value)
					} else if x == av {
						match++
					}

				case float64:
					if av, ok := attr.Value.(float64); !ok {
						return nil, fmt.Errorf("find mutation attribute %q. Expected a float64 type but supplied a %T type", attr.Name, attr.Value)
					} else if x == av {
						match++
					}

				case string:
					if av, ok := attr.Value.(string); !ok {
						return nil, fmt.Errorf("find mutation attribute %q. Expected a string type but suppled a %T type", attr.Name, attr.Value)
					} else if x == av {
						match++
					}

				case []byte:
					if av, ok := attr.Value.([]byte); !ok {
						return nil, fmt.Errorf("find mutation attribute %q. Expected a binary ([]byte type but is a %T type", attr.Name, attr.Value)
					} else if bytes.Equal(x, av) {
						match++
					}

				case uuid.UID:
					if av, ok := attr.Value.(uuid.UID); !ok {
						if av, ok := attr.Value.([]uint8); !ok {
							return nil, fmt.Errorf("find mutation attribute %q. Expected a binary ([]uint8) type but is a %T type", attr.Name, attr.Value)
						} else if bytes.Equal([]byte(x), []byte(av)) {
							match++
						}
					} else if bytes.Equal([]byte(x), []byte(av)) {
						match++
					}
				}
				break
			}
			if match == len(keys) {
				return sm, nil
			}
		}
	}

	if match == len(keys) {
		return sm, nil
	}
	return nil, nil
}
