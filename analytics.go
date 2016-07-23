package bulkCache

import (
	"sync/atomic"
)

type (
	Analytics struct {
		Queries  uint64
		Memories uint64
	}
)

func NewAnalytics() *Analytics {
	return &Analytics{}
}

func (a *Analytics) Add(data []byte) {
	atomic.AddUint64(&a.Memories, uint64(len(data)))
}

func (a *Analytics) Get() {
	atomic.AddUint64(&a.Queries, 1)
}
