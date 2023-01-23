package ds

import (
	"github.com/ros2hp/method-db/uuid"
)

type EdgeChild struct {
	Puid       uuid.UID
	SortK_Cuid string
}

type Edge struct {
	Puid, Cuid uuid.UID
	Sortk      string // attach point ?
	Bid        int
	RespCh     chan bool
}

func NewEdge() *Edge {
	return &Edge{}
}
