package bulkCache

import (
	"crypto/rand"
	"fmt"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
)

var (
	Default *Container
)

const (
	KeySize = 32

	HashEngine  = "hash"
	BTreeEngine = "btree"
)

type (
	Cached map[string]*Item

	EachHandler func(Bulk)

	Bulk interface {
		Add(string, []byte, time.Duration) error
		GetAlive() Cached
		GetAliveInBulk() Bulk
		Config() *BulkConfig
		Stop()
		Len() int
		Bytes() int
		String() string
		Analytics() *Analytics
	}

	BulkConfig struct {
		MaxItem      int
		Eliminate    time.Duration
		EnabledCache bool
	}

	Container struct {
		Mut       *sync.RWMutex
		Analytics *Analytics
		bulks     map[string]Bulk
		Log       *log.Entry
		Name      string
		Engine    string
	}

	Item struct {
		Data   []byte
		Expire time.Time
	}
)

func GenerateKey() (string, error) {
	b := make([]byte, KeySize)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func NewContainer(name string, engine string) *Container {
	if name == "" {
		name = "Default"
	}
	if engine == "" {
		engine = BTreeEngine
	}
	c := &Container{
		Mut:       new(sync.RWMutex),
		Analytics: &Analytics{},
		Name:      name,
		Engine:    engine,
		bulks:     make(map[string]Bulk),
		Log: log.WithFields(log.Fields{
			"Store Engine": fmt.Sprintf("%s Container", name),
		}),
	}
	go c.master()
	return c
}

func (c *Container) NewBulk(cfg *BulkConfig) Bulk {
	switch c.Engine {
	case HashEngine:
		return NewHashBulk(cfg)
	case BTreeEngine:
		return NewBTreeBulk(cfg)
	}
	return NewBTreeBulk(cfg)
}

func (c *Container) NewBulkFromCached(cfg *BulkConfig, cached Cached) Bulk {
	switch c.Engine {
	case HashEngine:
		return NewHashBulkFromCached(cfg, cached)
	case BTreeEngine:
		return NewBTreeBulkFromCached(cfg, cached)
	}
	return NewBTreeBulkFromCached(cfg, cached)
}

func (c *Container) GetBulk(key string) (Bulk, bool) {
	c.Mut.RLock()
	defer c.Mut.RUnlock()
	bulk, ok := c.bulks[key]
	return bulk, ok
}

func (c *Container) Get(key string) (Cached, bool) {
	defer c.Analytics.Get()
	b, ok := c.GetBulk(key)
	if !ok {
		c.Log.Warning(fmt.Sprintf("Bulk %s is empty", key))
		return nil, false
	}
	return b.GetAlive(), true
}

func (c *Container) GetBulkItems(key string) (bulk Bulk, ok bool) {
	b, ok := c.GetBulk(key)
	if !ok {
		return nil, false
	}
	its := b.GetAlive()
	if len(its) == 0 {
		return nil, false
	}
	return c.NewBulkFromCached(b.Config(), its), true
}

func (c *Container) AddBulk(key string, cfg *BulkConfig) Bulk {
	c.Mut.Lock()
	defer c.Mut.Unlock()
	b, ok := c.bulks[key]
	if !ok {
		b = c.NewBulk(cfg)
		c.bulks[key] = b
	}
	return b
}

func (c *Container) Add(key, sub string, value []byte, expire time.Duration) error {
	defer c.Analytics.Add(value)
	var bulk Bulk
	if !c.Has(key) {
		bulk = c.AddBulk(key, nil)
	} else {
		b, _ := c.GetBulk(key)
		bulk = b
	}
	//padding key
	if sub == "" {
		var err error
		sub, err = GenerateKey()
		if err != nil {
			c.Log.Error(fmt.Sprintf("Generate Key[%d byte] error[%s]", KeySize, err.Error()))
			return err
		}
	}
	if len(sub) > KeySize {
		sub = sub[:KeySize]
	}
	if len(sub) < KeySize {
		sub = sub + string(make([]byte, KeySize-len(sub)))
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
	bulk, ok := c.GetBulk(key)
	if ok {
		bulk.Stop()
	}
	c.Mut.Lock()
	defer c.Mut.Unlock()
	delete(c.bulks, key)
}

func (c *Container) Flush() {
	c.Mut.Lock()
	defer c.Mut.Unlock()
	c.bulks = map[string]Bulk{}
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
	Default = NewContainer("", "")
}
