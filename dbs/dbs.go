package dbs

type Mutation interface {
	GetStatements() []Statement
}

type Statement struct {
	SQL    string
	Params map[string]interface{}
}
