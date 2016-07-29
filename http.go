package bulkCache

import (
	"fmt"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/labstack/echo"
	"github.com/labstack/echo/engine/fasthttp"
)

type (
	Data map[string]interface{}

	EchoHttpServer struct {
		Handler *echo.Echo
		Engine  *fasthttp.Server
		Log     *log.Entry
	}
)

var (
	HttpApi *EchoHttpServer
)

func NewEchoHttpServer() *EchoHttpServer {
	return &EchoHttpServer{
		Handler: echo.New(),
		Log: log.WithFields(log.Fields{
			"Api": "Http power by echo",
		}),
	}
}

func (h *EchoHttpServer) Listen(port string) {
	h.Engine = fasthttp.New(port)
	h.Log.Info("Start http api server on " + port)
	h.Handler.Run(h.Engine)
}

func (h *EchoHttpServer) GetBulkItems(ctx echo.Context) error {
	bulk := ctx.Param("id")
	if bulk == "" {
		return ctx.JSON(200, Data{"result": 1})
	}
	its, ok := Default.Get(bulk)
	if !ok {
		h.Log.Warning(fmt.Sprintf("Bulk %s is empty", bulk))
		return ctx.JSON(200, Data{"result": 1})
	}
	items := []string{}
	bytes := 0
	for _, i := range its {
		bytes += len(i.Data)
		items = append(items, string(i.Data))
	}
	h.Log.Info(fmt.Sprintf("From Bulk %s Get %d bytes data", bulk, bytes))
	return ctx.JSON(200, Data{"result": 0, "items": items})
}

func (h *EchoHttpServer) DeleteBulk(ctx echo.Context) error {
	id := ctx.Param("id")
	Default.Remove(id)
	h.Log.Info(fmt.Sprintf("Deleted Bulk %s", id))
	return ctx.JSON(200, Data{"result": 0})
}

func (h *EchoHttpServer) SetItem(ctx echo.Context) error {
	id := ctx.Param("id")
	name := ctx.FormValue("name")
	value := ctx.FormValue("value")
	ex := ctx.FormValue("expire")
	expire, err := strconv.Atoi(ex)
	if err != nil {
		h.Log.Error(fmt.Sprintf("Invalid expire[%s]", ex))
		return ctx.JSON(200, Data{"result": 1})
	}
	Default.Add(id, name, []byte(value), time.Duration(expire)*time.Second)
	h.Log.Info(fmt.Sprintf("Add %d bytes to %s", len(value), id))
	return ctx.JSON(200, Data{"result": 0})
}

func (h *EchoHttpServer) ContainerStatus(ctx echo.Context) error {
	return ctx.JSON(200, Data{
		"result": 0,
		"status": Data{
			"memory":  Default.Analytics.Memories,
			"queries": Default.Analytics.Queries,
		},
	})
}

func (h *EchoHttpServer) BulkStatus(ctx echo.Context) error {
	bulk, ok := Default.GetBulk(ctx.Param("id"))
	if !ok {
		return ctx.JSON(200, Data{
			"result": 1,
		})
	}
	return ctx.JSON(200, Data{
		"result": 0,
		"status": Data{
			"memory":  bulk.Analytics.Memories,
			"queries": bulk.Analytics.Queries,
		},
	})
}

func init() {
	HttpApi = NewEchoHttpServer()
	api := HttpApi.Handler.Group("/bulk")
	{
		api.GET("/:id", HttpApi.GetBulkItems)
		api.DELETE("/:id", HttpApi.DeleteBulk)
		api.POST("/:id", HttpApi.SetItem)
	}
	status := HttpApi.Handler.Group("/status")
	{
		status.GET("/", HttpApi.ContainerStatus)
		status.GET("/:id", HttpApi.BulkStatus)
	}
}
