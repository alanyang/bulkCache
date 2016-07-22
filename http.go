package bulkCache

import (
	"strconv"
	"time"

	"github.com/kataras/iris"
)

var (
	HttpApi *iris.Framework
)

func Delete(ctx *iris.Context) {
	id := ctx.Param("id")
	Default.Remove(id)
	ctx.JSON(200, iris.Map{"status": 0})
}

func Get(ctx *iris.Context) {
	id := ctx.Param("id")
	items, ok := Default.Get(id)
	if !ok {
		ctx.JSON(200, iris.Map{"status": 1})
	}
	its := []interface{}{}
	for _, i := range items {
		d, ok := i.Data.([]byte)
		if ok {
			its = append(its, string(d))
		}
	}
	ctx.JSON(200, iris.Map{"status": 0, "items": its})
}

func Post(ctx *iris.Context) {
	id := ctx.Param("id")
	name := ctx.FormValue("name")
	value := ctx.FormValue("value")
	ex := ctx.FormValue("expire")
	expire, err := strconv.Atoi(string(ex))
	if err != nil {
		ctx.JSON(200, iris.Map{"status": 1})
		return
	}
	Default.Add(id, string(name), value, time.Duration(expire)*time.Second)

	ctx.JSON(200, iris.Map{"status": 0})
}

func init() {
	HttpApi = iris.New()
	p := HttpApi.Party("/bulk")
	{
		p.Get("/:id", Get)
		p.Post("/:id", Post)
		p.Delete("/:id", Delete)
	}
}
