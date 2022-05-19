package op

import (
	"github.com/GoGraph/tx/mut"
)

type Operation interface {
	Start() []*mut.Mutation
	End(err error) []*mut.Mutation
}
