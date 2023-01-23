package db

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	"github.com/ros2hp/method-db/tx"
	"github.com/ros2hp/method-db/mut"
)

func TestFetchData(t *testing.T) {
	// NOTE: this is considerable slower than FetchDataWithSortK. It would seem
	//. it is faster to user "BeginstWith" key condition on SortK
	t0 := time.Now()

	uid := []byte("5lFOnTStSYWqmi8S6FDFDQ==")
	at, err := FetchData(uid)
	if err != nil {
		panic(err)
	}
	t1 := time.Now()
	fmt.Println()
	fmt.Println("DB Access: ", t1.Sub(t0))

	fmt.Println(at)

}

func TestFetchDataWithSortK(t *testing.T) {
	binaryEqual := func(a, b []byte) bool {
		if len(a) != len(b) {
			return false
		}
		//	for i := 0; i < len(a); i++ {
		for i := range a {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}

	t0 := time.Now()

	//	uid := []byte("5lFOnTStSYWqmi8S6FDFDQ==")
	uid := []byte("k09bI9lSTXuZi2MSkyMm2A==")
	uid2 := []byte("k09bI9lSTXuZi2MSkyMm2A==")
	at, err := FetchData(uid, "A#")
	if err != nil {
		t.Error(err)
	}
	t1 := time.Now()
	fmt.Println("DB Access: ", t1.Sub(t0))
	t0 = time.Now()
	if bytes.Equal(uid, uid2) {
		t1 = time.Now()
		fmt.Println("bytes.Equal: ", t1.TSub(t0))
	}

	t0 = time.Now()
	if binaryEqual(uid, uid2) {
		t1 = time.Now()
		fmt.Println("binaryEqual: ", t1.Sub(t0))
	}

	for _, v := range at {
		if v.SortK == "A#G#:S" {
			pk := v.PKey
			pk_ := make([]byte, 26)
			base64.StdEncoding.Decode(pk_, pk)
			v2 := make([]byte, 1)
			for _, k := range v.XB {
				fmt.Println(string(k))
				t0 = time.Now()
				_, err := base64.StdEncoding.Decode(v2, k)
				t1 := time.Now()
				fmt.Println("base64 decode: ", t1.Sub(t0))
				if err != nil {
					fmt.Println("decode error:", err)
					return
				}

				// check 2nd bit set from rhs
				x := (1 << 1 & v2[0]) >> 1

				if x == 1 {
					fmt.Printf("Child node has been detached: X = %08b\n", v2[0])
				} else {
					fmt.Printf("Child node attached: X = %08b\n", v2[0])
				}
			}

		}
	}

}

func TestAttachNode(t *testing.T) {
	//err := AttachNode([]byte("k09bI9lSTXuZi2MSkyMm2A=="), []byte("5lFOnTStSYWqmi8S6FDFDQ=="), "A#G#:E", "Employee", "Company") // R#<parentType>#<parentUpred>, R#Company#:e, R#Person#:S
	err := AttachNode([]byte("k09bI9lSTXuZi2MSkyMm2A=="), []byte("5lFOnTStSYWqmi8S6FDFDQ=="), "A#G#:S", "Person")
	if err != nil {
		panic(err)
	}
}

func TestDetachChild(t *testing.T) {
	cUID := []byte("bHeALyzDR+qW1cdNWF1sPg==")
	pUID := []byte("k09bI9lSTXuZi2MSkyMm2A==")
	pTy := "Person"
	sortk := "A#G#:S"

	err := DetachChild(cUID, pUID, pTy, sortk)
	if err != nil {
		t.Error(err)
	}
}
