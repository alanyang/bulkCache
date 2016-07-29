package bulkCache

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/emirpasic/gods/trees/btree"
)

type (
	BTreeBulk struct {
		tree       *btree.Tree
		analytics  *Analytics
		Mut        *sync.RWMutex
		config     *BulkConfig
		timeFormat string
		stop       bool
	}
)

func GenerateTree(cached Cached) *btree.Tree {
	tree := btree.NewWithStringComparator(3)
	for k, v := range cached {
		tree.Put(k, v)
	}
	return tree
}

func NewDefaultBTreeBulkConfig() *BulkConfig {
	return &BulkConfig{
		MaxItem:      (1 << 16) - 1,
		Eliminate:    500,
		EnabledCache: false,
	}
}

func NewBTreeBulk(cfg *BulkConfig) *BTreeBulk {
	if cfg == nil {
		cfg = NewDefaultBTreeBulkConfig()
	}
	return &BTreeBulk{
		tree:       btree.NewWithStringComparator(3),
		analytics:  NewAnalytics(),
		Mut:        &sync.RWMutex{},
		config:     cfg,
		timeFormat: "2006-01-02 15:04:05",
	}
}

func NewBTreeBulkFromCached(cfg *BulkConfig, cached Cached) *BTreeBulk {
	b := NewBTreeBulk(cfg)
	b.tree = GenerateTree(cached)
	b.Mut = &sync.RWMutex{}
	b.analytics = NewAnalytics()
	return b
}

func (b *BTreeBulk) Config() *BulkConfig {
	return b.config
}

func (b *BTreeBulk) Analytics() *Analytics {
	return b.analytics
}

func (b *BTreeBulk) Len() int {
	return b.tree.Size()
}

func (b *BTreeBulk) Bytes() (n int) {
	it := b.tree.Iterator()
	for it.Next() {
		v, _ := it.Value().(*Item)
		k, _ := it.Key().(string)
		n += len(v.Data) + len(k)
	}
	return
}

func (b *BTreeBulk) String() string {
	s := []string{"**********BTree BULK**********"}
	it := b.tree.Iterator()
	for it.Next() {
		key, _ := it.Key().(string)
		val := it.Value()
		s = append(s, fmt.Sprintf("------[%v]@[%s]------", val, strings.Split(key, ":")[0]))
	}
	return strings.Join(s, "\n")
}

func (b *BTreeBulk) Add(key string, value []byte, expire time.Duration) error {
	if b.tree.Size() > b.config.MaxItem && b.config.MaxItem != -1 {
		return errors.New("Bulk is fulled")
	}
	b.Mut.Lock()
	defer b.analytics.Add(value)
	defer b.Mut.Unlock()
	ex := time.Now().Add(expire)
	key = fmt.Sprintf("%s:%s", ex.Format(b.timeFormat), key)
	item := &Item{Data: value, Expire: ex}
	b.tree.Put(key, item)
	return nil
}

func (b *BTreeBulk) GetAlive() Cached {
	b.Mut.RLock()
	defer b.Mut.RUnlock()

	n := time.Now()
	cached := Cached{}
	es := []string{}
	it := b.tree.Iterator()
	for it.Next() {
		key, _ := it.Key().(string)
		val, _ := it.Value().(*Item)
		if n.Before(val.Expire) {
			cached[key] = val
		} else {
			es = append(es, key)
		}
	}
	for _, e := range es {
		b.tree.Remove(e)
	}
	return cached
}

func (b *BTreeBulk) GetAliveInBulk() Bulk {
	cached := b.GetAlive()
	tree := btree.NewWithStringComparator(3)
	for k, v := range cached {
		tree.Put(k, v)
	}
	return &BTreeBulk{
		config:    b.config,
		Mut:       &sync.RWMutex{},
		analytics: NewAnalytics(),
		tree:      tree,
	}
}

func (b *BTreeBulk) Stop() {
	b.stop = true
}

func (b *BTreeBulk) Eliminate() {
	for !b.stop {
		es := []string{}
		it := b.tree.Iterator()
		n := time.Now()
		//low => high
		for it.Next() {
			val, _ := it.Value().(*Item)
			key, _ := it.Key().(string)
			if !n.After(val.Expire) {
				break
			}
			es = append(es, key)
		}

		for _, k := range es {
			b.tree.Remove(k)
		}
	}
}
