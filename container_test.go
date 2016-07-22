package bulkCache

import (
	"fmt"
	"testing"
	"time"
)

func Test_Container(t *testing.T) {
	c := NewContainer()
	m := 3  //3 bulk
	n := 10 //10 item pre bulk
	for j := 0; j < m; j++ {
		for i := 0; i < n; i++ {
			c.Add(fmt.Sprintf("Video %d", j), fmt.Sprint(i), fmt.Sprintf("Tag %d", i), time.Second*time.Duration(i*j))
		}
	}

	time.Sleep(time.Second * 4)
	t.Log("After 4 second bulks")

	c.Each(func(bulk *Bulk) {
		t.Log(bulk.String())
	})
}
