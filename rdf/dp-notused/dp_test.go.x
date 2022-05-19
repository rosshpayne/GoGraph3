package dp

import (
	"fmt"

	"testing"
)

type eCh chan int

func TestDChannel(t *testing.T) {

	var chs []eCh

	chs = make([]eCh, 10)

	for i := 0; i < 10; i++ {
		chs[i] = make(eCh, 1) // channel with buffer
	}

	for i := 0; i < 10; i++ {
		i := i

		go func(ch eCh) {

			for i := 0; i < 4; i++ {
				ch <- i
			}
			close(ch)

		}(chs[i])
	}
	var x int
	for y := 0; y < 4; y++ {
		for i := 0; i < 10; i++ {
			x = <-chs[i]
			fmt.Printf("%d X=%d\n", i, x)
		}
	}

}
