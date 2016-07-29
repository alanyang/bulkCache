package main

import (
	cache "bulkCache"
	"flag"
)

func main() {
	var (
		http, dage, engine, name string
	)
	flag.StringVar(&http, "http", ":1128", "Http Api Server Port")
	flag.StringVar(&dage, "dage", ":2345", "Dage Api Server Port")
	flag.StringVar(&engine, "engine", cache.BTreeEngine, "Store Engine, btree or hash")
	flag.StringVar(&name, "name", "Default", "Server Name")

	flag.Parse()

	cache.Default = cache.NewContainer(name, engine)

	go cache.HttpApi.Listen(http)

	go cache.DageApi.Listen(dage)

	<-(chan int)(nil)
}
