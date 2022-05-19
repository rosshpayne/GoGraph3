package histogram

// This module is not efficient; it stores a copy of each value passed into it, and
// recomputes all statistics and buckets every time it needs them.

import (
	"fmt"
	"math"
	"sort"
)

var (
	blockRunes = []rune{'▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'}
	asciiRunes = []rune{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j',
		'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
		'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D',
		'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N',
		'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X'}
)

// ScalarVal is a type wrapper for the kind of numbers we're accummulating into the histogram.
// (It would be nice to use some kind of 'Number' type, that handles ints & floats.)
type ScalarVal int64

// Histogram stores all the values, and defines the range and bucket count. Values outside
// the range end up in underflow/overflow buckets.
type Histogram struct {
	Vals       []ScalarVal
	ValMin     ScalarVal
	ValMax     ScalarVal
	NumBuckets int
	runeset    []rune
}

// Stats reports some statistical properties for the dataset
type Stats struct {
	N            int
	Mean         float64
	Stddev       float64
	Percentile50 ScalarVal
	Percentile90 ScalarVal
}

type scalarValSlice []ScalarVal

func (a scalarValSlice) Len() int           { return len(a) }
func (a scalarValSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a scalarValSlice) Less(i, j int) bool { return a[i] < a[j] }

// UseBlockRunes will use UTF8 block characters instead of ascii letters in the histogram string
func (h *Histogram) UseBlockRunes() {
	h.runeset = blockRunes
}

// Add appends a value into the dataset
func (h *Histogram) Add(v ScalarVal) {
	h.Vals = append(h.Vals, v)
}

// Quantile returns a value from the dataset, below which 'ile'% of the values fall
func (h Histogram) Quantile(quantile int) (ScalarVal, bool) {
	if quantile < 0 || quantile > 99 {
		return 0, false
	}
	sort.Sort(scalarValSlice(h.Vals))
	var fraction = float32(quantile) / 100.0
	index := int(fraction * float32(len(h.Vals)))
	return h.Vals[index], true
}

// Stats computes some basic statistics on the dataset, and returns them
func (h Histogram) Stats() (*Stats, bool) {
	n := len(h.Vals)
	if n == 0 {
		return nil, false
	}

	var mean, diffSquares float64
	for _, v := range h.Vals {
		mean += float64(v)
	}
	mean /= float64(n)
	for _, v := range h.Vals {
		diff := float64(v) - mean
		diffSquares += diff * diff
	}

	stddev := math.Sqrt(diffSquares / float64(n))
	percentile50, _ := h.Quantile(50)
	percentile90, _ := h.Quantile(90)

	return &Stats{
		N:            n,
		Mean:         mean,
		Stddev:       stddev,
		Percentile50: percentile50,
		Percentile90: percentile90,
	}, true
}

// Decide which bucket the given value would fall into.
// Bucket zero is underflow; buckets 1..N are the N defined buckets; and
// bucket N+1 is overflow. (Thus n_buckets=10 will yield 12 buckets)
func (h Histogram) pickBucket(v ScalarVal) int {
	unitVal := 1.0 * float64(v-h.ValMin) / float64(h.ValMax-h.ValMin)
	bucket := int(math.Floor(unitVal*float64(h.NumBuckets))) + 1
	// clip under/overflow
	if bucket < 0 {
		bucket = 0
	}
	if bucket > (h.NumBuckets + 1) {
		bucket = h.NumBuckets + 1
	}
	return bucket
}

// returns an array, one elem per bucket, whose values are the count of values falling into
// the bucket.
func (h Histogram) fillBuckets() []int {
	bkts := make([]int, h.NumBuckets+2)
	for _, v := range h.Vals {
		bkts[h.pickBucket(v)]++
	}
	return bkts
}

func (h Histogram) bucketCountToRune(count int, total int) rune {
	if count == 0 || total == 0 {
		return ' '
	}

	percent := float64(count) / float64(total)
	if percent < 0.005 {
		return '.'
	}

	if h.runeset == nil {
		h.runeset = asciiRunes // Default
	}

	index := int(percent * float64(len(h.runeset)))
	if index >= len(h.runeset) {
		index = len(h.runeset) - 1 // Buckets are [0-9%], [10-19%],...; so at 100%, we overflow
	}

	return h.runeset[index]
}

func (h Histogram) bucketsToString(bkts []int) string {
	total := 0
	for _, v := range bkts {
		total += v
	}

	var runes []rune
	for _, v := range bkts {
		runes = append(runes, h.bucketCountToRune(v, total))
	}

	// bkts (and thus chars) will be (underflow, *chars..., overflow)
	// shift & pop idioms via https://github.com/golang/go/wiki/SliceTricks
	underflow, runes := runes[0], runes[1:]
	overflow, runes := runes[len(runes)-1], runes[:len(runes)-1]
	hist := string(runes)

	if bkts[0] > 0 {
		return fmt.Sprintf("[%c|%s|%c]", underflow, hist, overflow)
	}
	return fmt.Sprintf("[%s|%c]", hist, overflow)
}

// String returns a string representation of the ascii histogram, and the stats
func (h Histogram) String() string {
	histStr := h.bucketsToString(h.fillBuckets())
	stats, _ := h.Stats()
	if stats == nil {
		stats = &Stats{}
	}
	return fmt.Sprintf("%s n=% 6d, mean=% 6d, stddev=% 6d, 50%%ile=% 6d, 90%%ile=% 6d",
		histStr, stats.N, int(stats.Mean),
		int(stats.Stddev), stats.Percentile50, stats.Percentile90)
}
