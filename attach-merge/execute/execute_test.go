package execute

import (
	"errors"
	"fmt"
	"testing"
	"time"

	gerr "github.com/GoGraph/dygerror"

	"github.com/GoGraph/cache"
	"github.com/GoGraph/ds"
	"github.com/GoGraph/util"
)

func TestUnmarshalNodeCache(t *testing.T) {
	t0 := time.Now()
	ch := cache.NewCache()

	// TODO fetch data rather than hardwire UID.
	uidb64 := util.UIDb64("Mq2RAESKSdyNSwzqD5d84A==")
	//uidb64 := util.UIDb64("ScNIRTc7T7WYi6uypbs1CA==")
	uid := uidb64.Decode()
	fmt.Printf("uid = [%08b] - %d \n", uid, len(uid))
	// establish node cache
	nc, err := ch.FetchNode(uid) //, "A#")
	if err != nil {
		t.Error(err)
	}

	t1 := time.Now()
	fmt.Println()
	fmt.Println("DB Access: ", t1.Sub(t0))
	var a = ds.ClientNV{ // represents the attributes in a Graph Query
		&ds.NV{Type: "Person", Name: "Age"},
		&ds.NV{Type: "Person", Name: "Name"},
		&ds.NV{Type: "Person", Name: "DOB"},
		&ds.NV{Type: "Person", Name: "Cars"},
		&ds.NV{Type: "Person", Name: "Siblings"},
		&ds.NV{Type: "Person", Name: "Siblings:Name"},
		&ds.NV{Type: "Person", Name: "Siblings:Age"},
		&ds.NV{Type: "Person", Name: "Siblings:DOB"},
		&ds.NV{Type: "Person", Name: "Friend"},
		&ds.NV{Type: "Person", Name: "Friend:Name"},
		&ds.NV{Type: "Person", Name: "Friend:Age"},
		&ds.NV{Type: "Person", Name: "Friend:DOB"},
	}
	//
	// UnmarshalQLMap, populates NV{Value} given NV{Name}
	//
	//err = nc.UnmarshalCache(ty, a) // TODO: get rid of Person argument
	fmt.Println("========================.   UnmarshalCache(a).    ======================= ")
	err = nc.UnmarshalNodeCache(a)
	if err != nil {
		t.Fatal(err)
	}
	//
	// change one of the values - ordinarily this should be saved to cache but we will ignore for time being
	//
	for _, v := range a {
		switch x := v.Value.(type) {
		case int64:
			x++
			v.Value = x
		}
	}
	//
	// for _, v := range a {
	// 	switch x := v.Value.(type) {
	// 	case int64:
	// 		fmt.Printf("%s %d\n", v.Name, x)
	// 	case []int64:
	// 		fmt.Printf("++ %s \n", v.Name)
	// 		for _, r := range x {
	// 			fmt.Printf("%d\n", r)
	// 		}
	// 	case string:
	// 		fmt.Printf("%s %s\n", v.Name, x)
	// 	case []string:
	// 		for _, t := range x {
	// 			fmt.Printf("[]string. %s %s\n", v.Name, t)
	// 		}
	// 	case [][]byte:
	// 		fmt.Printf("%s %s\n", v.Name, x)
	// 	}
	// }
	a.MarshalJSON()

}

func TestUnmarshalValue(t *testing.T) {
	t0 := time.Now()
	ch := cache.NewCache()

	//	uidb64 := util.UIDb64("5lFOnTStSYWqmi8S6FDFDQ==")
	uidb64 := util.UIDb64("5lFOnTStSYWqmi8S6FDFDQ==")
	uid := uidb64.Decode()
	// establish node cache
	nc, err := ch.FetchNode(uid) //, "A#")
	if err != nil {
		t.Error(err)
	}
	t1 := time.Now()
	fmt.Println()
	fmt.Println("DB Access: ", t1.Sub(t0))
	//
	// UnmarshalValue
	//
	//err = nc.UnmarshalValue("Person", "Age", &a) // TODO: get rid of Person argument
	var a int
	err = nc.UnmarshalValue("Age", &a)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("age = ", a+10)

}

func TestUnmarshalMapError(t *testing.T) {

	var expectedErr = "passed in value must be a pointer to struct"
	ch := cache.NewCache()
	uidb64 := util.UIDb64("5lFOnTStSYWqmi8S6FDFDQ==")
	uid := uidb64.Decode()
	at, err := ch.FetchNode(uid) //"A#")
	if err != nil {
		t.Error(err)
	}
	// type output struct {
	// 	age int
	// }
	//var a output

	var a int

	err = at.UnmarshalMap(&a)
	if err.Error() != expectedErr {
		t.Fatalf("Error sould be: %q, got %q", expectedErr, err.Error())
	}
}

func TestUnmarshalMap2(t *testing.T) {

	uidb64 := util.UIDb64("5lFOnTStSYWqmi8S6FDFDQ==")
	uid := uidb64.Decode()
	t0 := time.Now()
	ch := cache.NewCache()
	at, err := ch.FetchNode(uid) // "A#")
	if err != nil {
		t.Fatal(err)
	}
	t1 := time.Now()
	fmt.Println("DB Access: ", t1.Sub(t0))
	type output struct {
		Age             int
		Fix             string
		Name            string
		DOB             string
		Jobs            []string
		SalaryLast3Year []int64 // note: []int generated following error: reflect.AppendSlice: int != int64. See package block as IL is defined as [][]int64
		Siblings        [][]byte
	}
	var a output

	err = at.UnmarshalMap(&a) // TODO: get rid of Person argument
	if err != nil {
		t.Fatalf("%s", err.Error())
	}

	fmt.Printf("%#v\n", a)
	//fmt.Printf("%s\n", a.Siblings[0])
	//fmt.Printf("%s\n", a.Siblings[1])

}

func TestAttachNode(t *testing.T) {

	ch := cache.NewCache()
	cUIDb64 := util.UIDb64("JTX96oaPRyac3OJUGyZX+w==")
	cUID := cUIDb64.Decode2()
	pUIDb64 := util.UIDb64("XZwH0GatSpG5x4PSlc/xdA==")
	pUID := pUIDb64.Decode2()
	sortk := "A#G#:S"
	//
	// }
	var a = ds.ClientNV{ // represents the attributes in a Graph Query
		&ds.NV{Type: "Person", Name: "Age"},
		&ds.NV{Type: "Person", Name: "Name"},
		&ds.NV{Type: "Person", Name: "DOB"},
		&ds.NV{Type: "Person", Name: "Cars"},
		&ds.NV{Type: "Person", Name: "Siblings"},
		&ds.NV{Type: "Person", Name: "Siblings:Name"},
		&ds.NV{Type: "Person", Name: "Siblings:Age"},
	}
	// err = np.UnmarshalCache(a)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// a.MarshalJSON()
	// AttachNode will update cache and db and lock and release pUID entry
	//
	//.  *** AttachNode.  ****
	//
	errS := AttachNode2(cUID, pUID, sortk)
	if errS != nil {
		if len(errS) > 0 {
			for _, v := range errS {
				t.Error(v.Error())
			}
		}
		t.Fatal(fmt.Errorf("Attach node operation failed"))
	}
	// clear cache of all node data TODO: locking strategy
	ch.ClearNodeCache(pUID)
	np, err := ch.FetchNode(pUID) //, "A#")
	defer np.Unlock()
	if err != nil {
		t.Error(err)
	}
	err = np.UnmarshalCache(a)
	if err != nil {
		t.Fatal(err)
	}
	a.MarshalJSON()

}

func TestDetachNode(t *testing.T) {

	ch := cache.NewCache()

	cUIDb64 := util.UIDb64("k09bI9lSTXuZi2MSkyMm2A==")
	cUID := cUIDb64.Decode2()
	pUIDb64 := util.UIDb64("5lFOnTStSYWqmi8S6FDFDQ==")
	pUID := pUIDb64.Decode2()
	sortk := "A#G#:S"

	// np, err := ch.FetchNode(pUID) //, "A#")
	// if err != nil {
	// 	t.Error(err)
	// }
	var a = ds.ClientNV{ // represents the attributes in a Graph Query
		&ds.NV{Type: "Person", Name: "Age"},
		&ds.NV{Type: "Person", Name: "Name"},
		&ds.NV{Type: "Person", Name: "DOB"},
		&ds.NV{Type: "Person", Name: "Cars"},
		&ds.NV{Type: "Person", Name: "Siblings"}, // "G#:S"
		&ds.NV{Type: "Person", Name: "Siblings:Name"},
		&ds.NV{Type: "Person", Name: "Siblings:Age"},
	}
	// err = np.UnmarshalCache(a)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// a.MarshalJSON()

	//  *** DetachNode.  ****

	err := DetachNode(cUID, pUID, sortk)
	if err != nil {
		t.Fatalf("%s", err.Error())
	}
	// clear node cache
	//ch.ClearNodeCache(pUID)
	// must refetch pUID as AttachNode will release LOCK
	np, err2 := ch.FetchNode(pUID) //, "A#")
	if err2 != nil {
		t.Error(err)
	}
	err = np.UnmarshalCache(a) // TODO: XF is in the cache - needs to be cleared or reset.
	if err != nil {
		t.Fatal(err)
	}
	a.MarshalJSON()

}

func TestDetachxNodeNotAttached(t *testing.T) {

	ch := cache.NewCache()

	cUIDb64 := util.UIDb64("k09bI9lSTXuZi2MSkyMm2A==")
	cUID := cUIDb64.Decode2()
	pUIDb64 := util.UIDb64("5lFOnTStSYWqmi8S6FDFDQ==")
	pUID := pUIDb64.Decode2()
	sortk := "A#G#:S"
	//
	//
	//
	np, err := ch.FetchNode(pUID) //, "A#")
	if err != nil {
		t.Error(err)
	}
	var a = ds.ClientNV{ // represents the attributes in a Graph Query
		&ds.NV{Type: "Person", Name: "Age"},
		&ds.NV{Type: "Person", Name: "Name"},
		&ds.NV{Type: "Person", Name: "DOB"},
		&ds.NV{Type: "Person", Name: "Cars"},
		&ds.NV{Type: "Person", Name: "Siblings"}, // "G#:S"
		&ds.NV{Type: "Person", Name: "Siblings:Name"},
		&ds.NV{Type: "Person", Name: "Siblings:Age"},
	}
	err = np.UnmarshalCache(a)
	if err != nil {
		t.Fatal(err)
	}
	a.MarshalJSON()
	// AttachNode will update cache and db and lock and release pUID entry
	//
	//.  *** AttachNode.  ****
	//
	err = DetachNode(cUID, pUID, sortk)
	if err != nil {
		if !errors.Is(err, gerr.NodesNotAttached) {
			t.Fatalf("%s", err.Error())
		}
	}

}

func TestAttachxNodeExisting(t *testing.T) {

	ch := cache.NewCache()

	cUIDb64 := util.UIDb64("k09bI9lSTXuZi2MSkyMm2A==")
	cUID := cUIDb64.Decode2()
	pUIDb64 := util.UIDb64("5lFOnTStSYWqmi8S6FDFDQ==")
	pUID := pUIDb64.Decode2()
	sortk := "A#G#:S"
	//
	//
	//
	np, err := ch.FetchNode(pUID) //, "A#")
	if err != nil {
		t.Error(err)
	}
	var a = ds.ClientNV{ // represents the attributes in a Graph Query
		&ds.NV{Type: "Person", Name: "Age"},
		&ds.NV{Type: "Person", Name: "Name"},
		&ds.NV{Type: "Person", Name: "DOB"},
		&ds.NV{Type: "Person", Name: "Cars"},
		&ds.NV{Type: "Person", Name: "Siblings"},
		&ds.NV{Type: "Person", Name: "Siblings:Name"},
		&ds.NV{Type: "Person", Name: "Siblings:Age"},
	}
	err = np.UnmarshalCache(a)
	if err != nil {
		t.Fatal(err)
	}
	a.MarshalJSON()
	// AttachNode will update cache and db and lock and release pUID entry
	//
	//.  *** AttachNode.  ****
	//
	errS := AttachNode2(cUID, pUID, sortk)
	if len(errS) > 0 {
		for _, e := range errS {
			fmt.Println("error: ", e.Error())
			if !errors.Is(e, gerr.NodesAttached) {
				t.Error(err.Error())
			} else {
				msg := gerr.NodesAttached.Error()
				t.Log(msg)
			}
		}
		t.Fatal()
	}
	time.Sleep(2 * time.Second)
	// must refetch pUID as AttachNode will release LOCK
	np, err = ch.FetchNode(pUID) //, "A#")
	if err != nil {
		t.Error(err)
	}
	err = np.UnmarshalCache(a)
	if err != nil {
		t.Fatal(err)
	}
	a.MarshalJSON()

}

// func TestAppend2Listp(t *testing.T) {
// 	uid := []byte("5lFOnTStSYWqmi8S6FDFDQ==")
// 	attrCty := "A#G#:S"
// 	lty := "LS"

// 	t0 := time.Now()
// 	err := Append2List(lty, uid, attrCty, "Susan Smith")
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	t1 := time.Now()
// 	fmt.Println("DB Access: ", t1.Sub(t0))
// 	t0 = time.Now()
// 	attrCty = "A#G#:S#:A"
// 	lty = "LN"
// 	err = Append2List(lty, uid, attrCty, 234.34)
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	t1 = time.Now()
// 	fmt.Println("DB Access: ", t1.Sub(t0))
// 	t0 = time.Now()

// 	attrCty = "A#G#:S#:A"
// 	lty = "LN"
// 	err = Append2List(lty, uid, attrCty, 667)
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	t1 = time.Now()
// 	fmt.Println("DB Access: ", t1.Sub(t0))
// 	t0 = time.Now()

// 	attrCty = "A#G#:S#:A"
// 	lty = "LN"
// 	v := int32(342)
// 	err = Append2List(lty, uid, attrCty, v)
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	t1 = time.Now()
// 	fmt.Println("DB Access: ", t1.Sub(t0))
// 	t0 = time.Now()
// 	attrCty = "A#G#:S#:A"
// 	lty = "LN"
// 	err = Append2List(lty, uid, attrCty, "123")
// 	if err != nil {
// 		//	t.Error(err)
// 	}
// 	t1 = time.Now()
// 	fmt.Println("DB Access: ", t1.Sub(t0))
// 	t0 = time.Now()

// }
