package histogram

// go test github.com/skypies/util/histogram

import (
	"fmt"
	"testing"

	//	hdr "github.com/codahale/hdrhistogram"
	hdr "github.com/HdrHistogram/hdrhistogram-go"
)

func TestHappypath(t *testing.T) {
	h := Histogram{NumBuckets: 10, ValMin: 0, ValMax: 100}
	vals := []int{4, 4, 4, 2}

	for _, val := range vals {
		h.Add(ScalarVal(val))
	}

	stats, _ := h.Stats()
	if stats.N != len(vals) {
		t.Errorf("Added %d, but found %d\n", len(vals), stats.N)
	}
	expected := 3.5
	if stats.Mean != expected {
		t.Errorf("Expected mean %f, but found %f\n", expected, stats.Mean)
	}
}

func TestHDR2Ascii(t *testing.T) {
	//	oldHist := Histogram{NumBuckets: 20, ValMin: 0, ValMax: 10}

	h := hdr.New(-100000000, 100000000, 5)

	vals := []int64{5909882, 3189743, 10585013, 25458318, 3966783, 3422098, 4231117, 9340752, 3170300, 25994673, 3153782, 3380580, 7610163, 24063752, 3102519, 2591986, 2954410, 9289574, 3921115, 2884490, 3327049, 6709453, 5604132, 6667225, 27533068, 4140877, 3602332, 27568251, 4309190, 9028662, 5588669, 3986636, 8253459, 3196435, 24053134, 4055653, 3816716, 25768547, 2908715, 2671722, 3123276, 2721086, 29713278, 5433067, 4357616, 2487014, 4132033, 20854844, 3402966}
	for _, val := range vals {
		//	oldHist.Add(ScalarVal(val))
		if err := h.RecordValue(val); err != nil {
			t.Errorf("RecordValue error: %v\n", err)
		}
	}
	hist := h
	fmt.Printf("n=% 6d, mean=% 6d, stddev=% 6d, 50%%ile=% 6d, 80%%ile=% 6d",
		hist.TotalCount(),
		int(hist.Mean()),
		int(hist.StdDev()),
		hist.ValueAtQuantile(50),
		hist.ValueAtQuantile(80))
	//	old := oldHist.String()
	//	new := HDR2ASCII(h, 30, 0, 10)

	//	fmt.Printf("* old: %s\n* new: %s\n", old, new)
	//	fmt.Printf("*new: %s\n", new)
	// 	if old != new {
	// 		t.Errorf("strings differ")
	// 	}
}
