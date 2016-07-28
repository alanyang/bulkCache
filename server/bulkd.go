package main

import (
	cache "bulkCache"
)

func main() {
	go cache.HttpApi.Listen(":1128")
	go cache.DageApi.Listen(":2345")

	<-(chan int)(nil)
}
