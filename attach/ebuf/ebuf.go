package ebuf

import (
	"github.com/GoGraph/attach/ds"
)

// ebuf provides a doublebuffer implementation for handling unprocessed edges. One buffer is the write buffer, where unprocessed edgaes are added,
// and one buffer is a readonly buffer supplying unprocessed edges for processing. The concept of doublebuffering is desiged to permite the safe reading of one buffer
// whose contents does not change, while concurrently the other buffer is being populated.
// buffer index
type EdgeBuf []*ds.Edge

// doublebuffer
type edgeDBuf struct {
	w   uint // write buffer index
	buf [2]EdgeBuf
}

func (eb *edgeDBuf) Swap() {
	switch eb.w {
	case 0:
		eb.w = 1
	case 1:
		eb.w = 0
	}
	// clear new write buffer
	eb.buf[eb.w] = nil
}

func (eb *edgeDBuf) Write(e *ds.Edge) {
	eb.buf[eb.w] = append(eb.buf[eb.w], e)
}

// Read returns read buffer
func (eb *edgeDBuf) Read() EdgeBuf {

	return eb.buf[eb.ridx()]

}

// ridx - read buffer index (alternate value to write buffer index)
func (eb *edgeDBuf) ridx() uint {
	// read
	switch eb.w {
	case 0:
		return 1
	default:
		return 0
	}
}

func New() *edgeDBuf {
	return &edgeDBuf{}
}
