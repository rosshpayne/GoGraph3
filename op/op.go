package op

import (
	"github.com/ros2hp/method-db/mut"
)

type Operation interface {
	Start() []*mut.Mutation
	End(err error) []*mut.Mutation
}
