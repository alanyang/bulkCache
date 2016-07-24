package bulkCache

import (
	"sync/atomic"
)

type (
	Analytics struct {
		Queries  int64
		Memories int64
	}
)

func NewAnalytics() *Analytics {
	return &Analytics{}
}

func (a *Analytics) Add(data []byte) {
	atomic.AddInt64(&a.Memories, int64(len(data)))
}

func (a *Analytics) Get() {
	atomic.AddInt64(&a.Queries, 1)
}

func (a *Analytics) Expired(data []byte) {
	atomic.AddInt64(&a.Memories, -int64(len(data)))
}
