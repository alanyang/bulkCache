package bulkCache

import (
	"sync"
	"time"
)

var (
	Default *Container
)

type (
	EachHandler func(*Bulk)
	Container   struct {
		Mut       *sync.RWMutex
		Analytics *Analytics
		bulks     map[string]*Bulk
	}
)

func NewContainer() *Container {
	return &Container{
		Mut:       new(sync.RWMutex),
		Analytics: &Analytics{},
		bulks:     make(map[string]*Bulk),
	}
}

func (c *Container) GetBulk(key string) (bulk *Bulk, ok bool) {
	c.Mut.RLock()
	defer c.Mut.RUnlock()
	bulk, ok = c.bulks[key]
	return
}

func (c *Container) Get(key string) (its []*Item, ok bool) {
	defer c.Analytics.Get()
	b, ok := c.GetBulk(key)
	if !ok {
		return []*Item{}, false
	}
	return b.GetAlive(), true
}

func (c *Container) GetBulkItems(key string) (bulk *Bulk, ok bool) {
	b, ok := c.GetBulk(key)
	if !ok {
		return nil, false
	}
	its := b.GetAlive()
	if len(its) == 0 {
		return nil, false
	}
	return NewBulkFromItems(b.Config, its), true
}

func (c *Container) AddBulk(key string, cfg *BulkConfig) *Bulk {
	c.Mut.Lock()
	defer c.Mut.Unlock()
	b, ok := c.bulks[key]
	if !ok {
		b = NewBulk(cfg)
		c.bulks[key] = b
	}
	return b
}

func (c *Container) Add(key, sub string, value []byte, expire time.Duration) error {
	defer c.Analytics.Add(value)
	var bulk *Bulk
	if !c.Has(key) {
		bulk = c.AddBulk(key, nil)
	} else {
		b, _ := c.GetBulk(key)
		bulk = b
	}
	return bulk.Add(sub, value, expire)
}

func (c *Container) Has(key string) bool {
	c.Mut.RLock()
	defer c.Mut.RUnlock()
	_, ok := c.bulks[key]
	return ok
}

func (c *Container) Remove(key string) {
	c.Mut.Lock()
	defer c.Mut.Unlock()
	bulk, ok := c.GetBulk(key)
	if ok {
		bulk.Stop()
	}
	delete(c.bulks, key)
}

func (c *Container) Flush() {
	c.Mut.Lock()
	defer c.Mut.Unlock()
	c.bulks = map[string]*Bulk{}
}

//just for debug
func (c *Container) Each(handler EachHandler) {
	c.Mut.RLock()
	defer c.Mut.RUnlock()
	for _, b := range c.bulks {
		handler(b.GetAliveInBulk())
	}
}

func (c *Container) master() {
	for {
		<-time.After(time.Second * 3)
		for k, v := range c.bulks {
			if v.Len() == 0 {
				c.Remove(k)
			}
		}
	}
}

func init() {
	Default = NewContainer()
}
