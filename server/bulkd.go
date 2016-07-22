package main

import (
	cache "bulkCache"
)

func main() {
	cache.HttpApi.Listen(":1128")
}
