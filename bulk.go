package bulkCache

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

type (
	Bulk struct {
		Mut       *sync.RWMutex
		Analytics *Analytics
		Config    *BulkConfig
		cache     map[string]*Item
		items     []*Item
	}

	BulkConfig struct {
		MaxItem      int
		Eliminate    time.Duration
		EnabledCache bool
	}

	Item struct {
		Data   interface{}
		Expire time.Time
	}
)

func NewDefaultBulkConfig() *BulkConfig {
	return &BulkConfig{
		MaxItem:      (1 << 16) - 1,
		Eliminate:    time.Duration(time.Second * 3),
		EnabledCache: false,
	}
}

func NewBulk(cfg *BulkConfig) *Bulk {
	if cfg == nil {
		cfg = NewDefaultBulkConfig()
	}
	bulk := &Bulk{
		Mut:       &sync.RWMutex{},
		Analytics: NewAnalytics(),
		Config:    cfg,
		cache:     map[string]*Item{},
		items:     []*Item{},
	}
	go bulk.Eliminate()
	return bulk
}

func NewBulkFromItems(cfg *BulkConfig, its []*Item) *Bulk {
	b := NewBulk(cfg)
	b.items = its
	b.cache = map[string]*Item{}
	b.Mut = &sync.RWMutex{}
	b.Analytics = NewAnalytics()
	return b
}

// expired by pre nanosecond
func (b *Bulk) Add(key string, value interface{}, expire time.Duration) error {
	if b.Len() > b.Config.MaxItem {
		return errors.New("")
	}
	b.Mut.Lock()
	defer b.Mut.Unlock()
	f := time.Now().Add(expire)
	i := &Item{Data: value, Expire: f}
	if key != "" && b.Config.EnabledCache {
		b.cache[key] = i
	}
	b.items = append(b.items, i)
	return nil
}

func (b *Bulk) Get(key string) *Item {
	b.Mut.RLock()
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

func (b *Bulk) GetAlive() (its []*Item) {
	b.Mut.RLock()
	defer b.Mut.RUnlock()
	n := time.Now()
	for _, i := range b.items {
		if n.Before(i.Expire) {
			its = append(its, i)
		}
	}
	return
}

func (b *Bulk) GetAliveInBulk() *Bulk {
	its := b.GetAlive()
	return &Bulk{
		items:     its,
		Config:    b.Config,
		Mut:       &sync.RWMutex{},
		Analytics: NewAnalytics(),
		cache:     b.cache,
	}
}

func (b *Bulk) Len() int {
	return len(b.items)
}

func (b *Bulk) String() string {
	s := []string{"**********BULK**********"}
	for _, i := range b.items {
		s = append(s, fmt.Sprintf("------[%v]@[%s]------", i.Data, i.Expire.String()))
	}
	return strings.Join(s, "\n")
}

func (b *Bulk) Eliminate() {
	for {
		<-time.After(b.Config.Eliminate)
		n := time.Now()
		es := []*Item{}
		for _, i := range b.items {
			if n.After(i.Expire) {
				es = append(es, i)
			}
		}
		for _, e := range es {
			for index, j := range b.items {
				if j == e {
					b.items = append(b.items[:index], b.items[index+1:]...)
					break
				}
			}
		}

		if b.Config.EnabledCache {
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
}
