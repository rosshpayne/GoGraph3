package dygerror

import (
	"errors"
)

var NodesAttached = errors.New("Nodes are already attached")
var NodesNotAttached = errors.New("Nodes are not attached")
