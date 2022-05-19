package dbuf

import (
	param "github.com/GoGraph/dygparam"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
)

// Provides a doublebuffer implementation for CW Log Events. Not concurrently safe - as calling function provides the required serialisation

type logEventBuf struct {
	buf []types.InputLogEvent
	idx int
}

// doublebuffer
type evDBuf struct {
	w   uint // write buffer index
	buf [2]*logEventBuf
}

func (eb *evDBuf) Swap() {
	switch eb.w {
	case 0:
		eb.w = 1
	case 1:
		eb.w = 0
	}
	// clear new write buffer
	b := eb.buf[eb.w]
	b.idx = 0
}

func (eb *evDBuf) Write(e *types.InputLogEvent) int {
	b := eb.buf[eb.w]
	b.buf[b.idx] = *e
	b.idx++
	return b.idx
}

// Read returns read buffer
func (eb *evDBuf) Read() []types.InputLogEvent {

	// TODO: should read  swap() then read()?? - no, client code will be less explicit and potentially confusing.
	d := eb.buf[eb.ridx()]
	return d.buf[:d.idx]

}

func (eb *evDBuf) WriteBuf() int {
	return eb.buf[eb.w].idx
}

// ridx - read buffer index (opposite of write buffer index)
func (eb *evDBuf) ridx() uint {
	// read
	switch eb.w {
	case 0:
		return 1
	case 1:
		return 0
	}
	return 0
}

func New() *evDBuf {

	bufA := make([]types.InputLogEvent, param.CWLogLoadSize, param.CWLogLoadSize)
	bufB := make([]types.InputLogEvent, param.CWLogLoadSize, param.CWLogLoadSize)

	return &evDBuf{buf: [2]*logEventBuf{&logEventBuf{buf: bufA}, &logEventBuf{buf: bufB}}}
}
