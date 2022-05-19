package ds

import (
	"github.com/GoGraph/uuid"
)

type EdgeChild struct {
	Puid       uuid.UID
	SortK_Cuid string
}

type Edge struct {
	Puid, Cuid uuid.UID
	Sortk      string
	Bid        int
	RespCh     chan bool
}

func NewEdge() *Edge {
	return &Edge{}
}
