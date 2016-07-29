package bulkCache

import (
	"fmt"
	"testing"
	"time"
)

func Test_BTreeBulk(t *testing.T) {
	b := NewBTreeBulk(nil)
	n := 10
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key:%d", i)
		val := fmt.Sprintf("value:%d", i)
		b.Add(key, []byte(val), time.Second*time.Duration(i+1))
	}
	t.Log(b.String())
	t.Log("===========ADD DATA==========")

	time.Sleep(time.Second * 1)
	b1 := b.GetAliveInBulk()
	t.Log(b1.String())
	if b1.Len() != n-1 {
		t.Error("eliminate failure after 1 second")
	} else {
		t.Log("eliminate success after 1 second")
	}

	time.Sleep(time.Second * 3)
	b2 := b.GetAliveInBulk()
	t.Log(b2.String())
	if b2.Len() != n-4 {
		t.Error("eliminate failure after 4 second")
	} else {
		t.Log("eliminate success after 4 second")
	}

	time.Sleep(time.Second * time.Duration(n))
	t.Log("eliminate all")
	b3 := b.GetAliveInBulk()
	t.Log(b3)
	if b3.Len() != 0 {
		t.Error("eliminate failure after 14 second")
	} else {
		t.Log("eliminate success after 14 second")
	}
}
