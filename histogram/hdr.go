package histogram

import (
	"fmt"
	hdr "github.com/HdrHistogram/hdrhistogram-go"
	//hdr "github.com/codahale/hdrhistogram"
)

// Simple glue to stringfy a hdrhistogram in ascii, built on the old crappy histogram code

func HDR2ASCII(hist *hdr.Histogram, numChars, floor, ceil int) string {
	if hist == nil {
		return "<nil>"
	}

	template := Histogram{NumBuckets: numChars, ValMin: ScalarVal(floor), ValMax: ScalarVal(ceil)}

	// Bucket zero is underflow; buckets 1..N are the N defined buckets; and
	// bucket N+1 is overflow. (Thus n_buckets=10 will yield 12 buckets)
	bkts := make([]int, template.NumBuckets+2)

	for _, bar := range hist.Distribution() {
		// a .Bar can represent a single integer or many; divide up count accordingly
		count := bar.Count
		if numValsSpanned := (bar.To - bar.From + 1); numValsSpanned > 1 {
			count = int64(float64(count) / float64(numValsSpanned))
		}

		for v := bar.From; v <= bar.To; v++ {
			bkt := template.pickBucket(ScalarVal(v))
			bkts[bkt] += int(count)
		}
	}

	//histStr := template.bucketsToString(bkts)

	// return fmt.Sprintf("%s n=% 6d, mean=% 6d, stddev=% 6d, 50%%ile=% 6d, 66%%ile=% 6d, 90%%ile=% 6d",
	// //	histStr,
	// 	hist.TotalCount(),
	// 	int(hist.Mean()),
	// 	int(hist.StdDev()),
	// 	hist.ValueAtQuantile(50),
	// 	hist.ValueAtQuantile(80)
	// 	//hist.ValueAtQuantile(90))
	// 	)
	return fmt.Sprintf("n=% 6d, mean=% 6d, stddev=% 6d, 50%%ile=% 6d, 80%%ile=% 6d",
		hist.TotalCount(),
		int(hist.Mean()),
		int(hist.StdDev()),
		hist.ValueAtQuantile(50),
		hist.ValueAtQuantile(80))
	//)
}
