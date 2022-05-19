package ast

import (
	"strconv"
	"strings"

	"github.com/GoGraph/ds"
	"github.com/GoGraph/gql/expression/token"
)

type FargI interface {
	Name() string
	farg()
}

type InnerFuncI interface {
	innerFunc()
}

// type InnerArgI interface {
// 	innerArg()
// }

type SelectList []*EdgeT

type EdgeT struct {
	Alias   name_
	VarName name_
	Edge    EdgeI
	//f         aggrFunc - now in predicate (edgeT)
}

func (e *EdgeT) AssignName(input string, loc token.Pos) {
	//ValidateName(input, err, Loc)
	e.Alias = name_{Name: input, Loc: loc}
}

func (e *EdgeT) AssignVarName(input string, loc token.Pos) {
	//ValidateName(input, err, Loc)
	e.VarName = name_{Name: input, Loc: loc}
}

type EdgeI interface {
	edge()
	Name() string
}

type ExprI interface {
	expression()
}

// type Arg1 interface {
// 	farg()
// }

type ScalarPred struct {
	name name_
}

func (s ScalarPred) edge() {}
func (s ScalarPred) Name() string {
	return s.name.Name
}
func (s ScalarPred) farg() {}

func (s *ScalarPred) AssignName(input string, loc token.Pos) {
	//ValidateName(input, err, Loc)
	s.name = name_{Name: input, Loc: loc}
}

type UidPred struct {
	Name_ name_
	//Filter *expr.Expression
	Filter ExprI
	Select SelectList
}

func (u *UidPred) AssignName(input string, loc token.Pos) {
	//ValidateName(input, err, Loc)
	u.Name_ = name_{Name: input, Loc: loc}
}

func (p *UidPred) edge() {}
func (p *UidPred) Name() string {
	return p.Name_.Name
}

//func (p *UidPred) innerArg() {}
func (p *UidPred) aggrArg() {}
func (p *UidPred) count_()  {}
func (p *UidPred) farg()    {}

type Variable struct {
	name name_
}

func (u *Variable) AssignName(input string, loc token.Pos) {
	//ValidateName(input, err, Loc)
	u.name = name_{Name: input, Loc: loc}
}

//func (r *Variable) innerArg()  {}
func (r Variable) edge() {}
func (r Variable) farg() {}
func (r Variable) Name() string {
	return r.name.Name
}

//func (r *Variable) innerFunc() {}
func (r *Variable) aggrArg() {}

//type ValFuncT func(v Variable) ValOut

type AggrArg interface {
	aggrArg()
}
type AggrFunc struct {
	FName name_ // count, avg takes either a variable argument or a uid-pred argument
	Arg   AggrArg
}

func (e *AggrFunc) edge() {}

//func (e *AggrFunc) innerFunc() {}

// type HasFunc struct {
// 	name name_
// }

// func (s HasFunc) edge() {}
// func (s HasFunc) Name() string {
// 	return s.name.Name
// }
// func (s HasFunc) farg() {}

// func (s *HasFunc) AssignName(input string, loc token.Pos) {
// 	//ValidateName(input, err, Loc)
// 	s.name = name_{Name: input, Loc: loc}
// }

type CounterI interface {
	count_()
	Name() string
}

type CountFunc struct {
	Arg CounterI // uidPred, UID
}

func (e CountFunc) farg() {}
func (e CountFunc) Name() string {
	return e.Name()
}

type Uid struct {
	Uids []string
}

func (e Uid) edge() {}
func (e Uid) Name() string {
	return ""
}
func (e Uid) count_() {} //??
func (e Uid) farg()   {}

type Uid_IN struct {
	Pred *UidPred
}

func (e Uid_IN) edge() {}
func (e Uid_IN) Name() string {
	return e.Pred.Name()
}
func (e Uid_IN) farg() {}

type Values []interface{} // int,float,string,$var

// =========================  GQLFunc  =============================================

type FuncT func(FargI, interface{}, ds.NVmap, string, int, int) bool

type GQLFunc struct {
	//	name  name_ // for String() purposes
	FName name_ // function name
	F     FuncT
	Farg  FargI // either predicate, count, var
	//	IFarg InnerArgI   // either uidPred, variable
	Value interface{} //  string,int,float,$var, List of string,int,float,$var
}

func (g *GQLFunc) AssignName(input string, loc token.Pos) {
	//ValidateName(input, err, Loc)
	g.FName = name_{Name: input, Loc: loc}
}

func (g *GQLFunc) Name() string {
	return g.FName.Name
}

func (f *GQLFunc) String() string {
	var s strings.Builder
	s.WriteString(f.Name())
	s.WriteByte('(')
	s.WriteString(f.Farg.Name())
	s.WriteByte(')')
	return s.String()
}

func (g *GQLFunc) GetPredicates(pred []string) []string {
	//fmt.Printf("\nin Getpredicates for Farg: %T %s\n", g.Farg, g.Farg.Name())
	s := g.Farg.Name()
	pred = append(pred, s)
	return pred
}

// func (g *GQLFunc) Execute() []db.QResult {
// 	//
// 	return g.F(g.Farg, g.Value, g.nv, g.ty)
// }

// // ============== RootStmt ==============

// type RootStmt struct {
// 	Name name_
// 	Var  Variable
// 	Lang string
// 	// (func: eq(name@en, "Steven Spielberg”))
// 	RootF GQLFunc // generates []uid from GSI data io.Writer Write([]byte) (int, error)
// 	// @filter( has(director.film) )
// 	//Filter *expr.Expression //
// 	Filter ExprI
// 	Select SelectList
// }

// func (r *Stmt) AssignName(input string, loc token.Pos) {
// 	//ValidateName(input, err, Loc)
// 	r.Name = name_{Name: input, Loc: loc}
// }

// func (r *Stmt) AssignVarName(input string, loc token.Pos) {
// 	//ValidateName(input, err, Loc)
// 	r.Var.Name = name_{Name: input, Loc: loc}
// }

// func (r *RootStmt) Execute() {
// 	//
// 	// execute root func
// 	//
// 	uids := r.RootF.F(r.RootF.Farg, r.RootF.Value, r.RootF.nv, r.RootF.ty)

// 	for _, u := range uids {
// 		fmt.Printf("%#v", u)
// 	}
// }

// // ============== QResult ==============

// type QResult struct {
// 	UID   util.UID
// 	SortK string
// 	Ty    string
// }

// func (r *RootStmt) Execute() []QResult {}

// // ============== NameI  ========================

// type NameAssigner interface {
// 	AssignName(name string, Loc *Loc, errS *[]error)
// }

// // ===============  NameValue_  =========================

// // type NameValue_ string

// // func (n NameValue_) String() string {
// // 	return string(n)
// // }

// // func (a NameValue_) Equals(b NameValue_) bool {
// // 	return string(a) == string(b)
// // }

// // func (a NameValue_) EqualString(b string) bool {
// // 	return string(a) == b
// // }

// ===============  name_  =========================

type name_ struct {
	Name string
	Loc  token.Pos
}

func (n name_) String() string {
	return n.Name
}

func (n name_) AtPosition() string {
	if n.Loc.Col == 0 && n.Loc.Line == 0 {
		//panic(fmt.Errorf("Error in AtPosition(), Loc not set"))
		return "Loc not set"
	}
	return "" + strconv.Itoa(n.Loc.Line) + " " + strconv.Itoa(n.Loc.Col) + "] "
}

func (n name_) Exists() bool {
	if len(n.Name) > 0 {
		return true
	}
	return false
}

// // =========== Loc_ =============================

// type Loc struct {
// 	Line   int
// 	Column int
// }

// func (l Loc) String() string {
// 	return "at line: " + strconv.Itoa(l.Line) + " " + "column: " + strconv.Itoa(l.Column)
// 	//return "" + strconv.Itoa(l.Line) + " " + strconv.Itoa(l.Column) + "] "
// }

// type HasName interface {
// 	AssignName(string, Loc_, err *[]error)
// }
