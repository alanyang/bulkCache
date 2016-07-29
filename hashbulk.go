package bulkCache

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

type (
	HashBulk struct {
		Mut       *sync.RWMutex
		analytics *Analytics
		config    *BulkConfig
		cache     Cached
		stop      bool
	}
)

func NewDefaultHashBulkConfig() *BulkConfig {
	return &BulkConfig{
		MaxItem:      (1 << 16) - 1,
		Eliminate:    time.Duration(time.Millisecond * 800),
		EnabledCache: false,
	}
}

func NewHashBulk(cfg *BulkConfig) *HashBulk {
	if cfg == nil {
		cfg = NewDefaultHashBulkConfig()
	}
	bulk := &HashBulk{
		Mut:       &sync.RWMutex{},
		analytics: NewAnalytics(),
		config:    cfg,
		cache:     Cached{},
	}
	go bulk.Eliminate()
	return bulk
}

func NewHashBulkFromCached(cfg *BulkConfig, cached Cached) *HashBulk {
	b := NewHashBulk(cfg)
	b.cache = cached
	b.Mut = &sync.RWMutex{}
	b.analytics = NewAnalytics()
	return b
}

func (b *HashBulk) Config() *BulkConfig {
	return b.config
}

func (b *HashBulk) Analytics() *Analytics {
	return b.analytics
}

// expired by pre nanosecond
func (b *HashBulk) Add(key string, value []byte, expire time.Duration) error {
	if b.Len() > b.config.MaxItem {
		return errors.New("Bulk is fulled")
	}
	b.Mut.Lock()
	defer b.Analytics().Add(value)
	defer b.Mut.Unlock()
	f := time.Now().Add(expire)
	i := &Item{Data: value, Expire: f}
	b.cache[key] = i
	return nil
}

func (b *HashBulk) Get(key string) *Item {
	b.Mut.RLock()
	defer b.Analytics().Get()
	defer b.Mut.Unlock()
	i, ok := b.cache[key]
	if !ok {
		return nil
	}
	n := time.Now()

	//expired
	if n.After(i.Expire) {
		return nil
	}

	return i
}

func (b *HashBulk) GetAlive() Cached {
	b.Mut.RLock()
	defer b.Mut.RUnlock()
	n := time.Now()
	cached := Cached{}
	es := []string{}
	for k, v := range b.cache {
		if n.Before(v.Expire) {
			cached[k] = v
		} else {
			es = append(es, k)
		}
	}

	for _, e := range es {
		delete(b.cache, e)
	}
	return cached
}

func (b *HashBulk) GetAliveInBulk() Bulk {
	cached := b.GetAlive()
	return &HashBulk{
		config:    b.config,
		Mut:       &sync.RWMutex{},
		analytics: NewAnalytics(),
		cache:     cached,
	}
}

func (b *HashBulk) Len() int {
	return len(b.cache)
}

func (b *HashBulk) Bytes() (n int) {
	for k, i := range b.cache {
		n += len(i.Data) + len(k)
	}
	return
}

func (b *HashBulk) String() string {
	s := []string{"**********Hash BULK**********"}
	for _, v := range b.cache {
		s = append(s, fmt.Sprintf("------[%v]@[%s]------", v.Data, v.Expire.String()))
	}
	return strings.Join(s, "\n")
}

func (b *HashBulk) Eliminate() {
	for !b.stop {
		<-time.After(b.config.Eliminate)
		n := time.Now()
		ks := []string{}
		for k, v := range b.cache {
			if n.After(v.Expire) {
				ks = append(ks, k)
			}
		}
		for _, p := range ks {
			delete(b.cache, p)
		}
	}
}

func (b *HashBulk) Stop() {
	b.stop = true
}
