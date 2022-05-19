package histogram

import (
	"fmt"
	//hdr "github.com/codahale/hdrhistogram"
	hdr "github.com/HdrHistogram/hdrhistogram-go"
	"sort"
)

/*
 s := histogram.NewSet(int(maxVal))
 s.RecordValue("event_name", int64(micros))
 fmt.Printf("Stats:-\n%s", s)
*/

// {{{ Set{}

type Set struct {
	maxval int64
	m      map[string]*hdr.Histogram
}

// }}}

// {{{ NewSet

func NewSet(maxval int64) Set {
	return Set{
		maxval: maxval,
		m:      map[string]*hdr.Histogram{},
	}
}

// }}}
// {{{ s.RecordValue

func (s Set) RecordValue(name string, val int64) {
	if _, exists := s.m[name]; !exists {
		s.m[name] = hdr.New(0, 10000000, 2)
	}

	s.m[name].RecordValue(val)
}

// }}}
// {{{ s.String

func (s Set) String() string {
	names := []string{}
	for name, _ := range s.m {
		names = append(names, name)
	}
	sort.Strings(names)

	str := ""
	for _, name := range names {
		// Anything larger than maxval goes into overflow bucket.
		str += fmt.Sprintf("%-20.20s: %s\n", name, HDR2ASCII(s.m[name], 40, 0, s.maxval))
	}
	return str
}
