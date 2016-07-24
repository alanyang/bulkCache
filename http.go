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
	ctx.JSON(200, iris.Map{"result": 0})
}

func Get(ctx *iris.Context) {
	id := ctx.Param("id")
	items, ok := Default.Get(id)
	if !ok {
		ctx.JSON(200, iris.Map{"result": 1})
	}
	its := []interface{}{}
	for _, i := range items {
		its = append(its, string(i.Data))
	}
	ctx.JSON(200, iris.Map{"result": 0, "items": its})
}

func Post(ctx *iris.Context) {
	id := ctx.Param("id")
	name := ctx.FormValue("name")
	value := ctx.FormValue("value")
	ex := ctx.FormValue("expire")
	expire, err := strconv.Atoi(string(ex))
	if err != nil {
		ctx.JSON(200, iris.Map{"result": 1})
		return
	}
	Default.Add(id, string(name), value, time.Duration(expire)*time.Second)

	ctx.JSON(200, iris.Map{"result": 0})
}

func Status(ctx *iris.Context) {
	ctx.JSON(200, iris.Map{
		"result": 0,
		"status": map[string]interface{}{
			"memory":  Default.Analytics.Memories,
			"queries": Default.Analytics.Queries,
		},
	})
}

func BulkStatus(ctx *iris.Context) {
	bulk, ok := Default.GetBulk(ctx.Param("id"))
	if !ok {
		ctx.JSON(200, iris.Map{
			"result": 1,
		})
		return
	}
	ctx.JSON(200, iris.Map{
		"result": 0,
		"status": map[string]interface{}{
			"memory":  bulk.Analytics.Memories,
			"queries": bulk.Analytics.Queries,
		},
	})
}

func init() {
	HttpApi = iris.New()
	p := HttpApi.Party("/bulk")
	{
		p.Get("/:id", Get)
		p.Post("/:id", Post)
		p.Delete("/:id", Delete)
	}

	s := HttpApi.Party("/status")
	{
		s.Get("/", Status)
		s.Get("/:id", BulkStatus)
	}
}
