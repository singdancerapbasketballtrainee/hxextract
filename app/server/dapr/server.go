package dapr

import (
	"fmt"
	"github.com/dapr/go-sdk/service/common"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"hxextract/api"
	"hxextract/app/config"
	"hxextract/app/dao/pg"
	"hxextract/app/log"
	negt "hxextract/pkg/go-sdk/service/http"
	"net/http"
	"strconv"
	"time"
)

var svc api.NegtServer

// New Server 服务层，该层封装服务级别的接口函数，
// 如http服务对外提供的url,grpc服务对外提供的proto
// New 提供服务的创建方法，在di中进行依赖注入
func New(s api.NegtServer) (srv common.Service, err error) {
	// 创建路由转发
	r := gin.New()
	mux := http.NewServeMux()
	log.GinLog(config.GetLog().GinLogPath, r)
	// 设置路由句柄
	initRoute(r)
	mux.Handle("/", r)
	// 启动服务
	srv = negt.NewServiceWithMux(fmt.Sprintf(":%d", config.GetService().HttpPort), mux)
	svc = s // 给包变量svc赋值为初始化后的service
	return srv, err
}

// initRoute http请求路由设置
func initRoute(r *gin.Engine) {
	r.GET("/readiness", healthCheckHandler)
	r.POST("/export", exportHandler)
	r.GET("/ping", pingHandler)
	r.GET("/cmd", cmdHandler)
}

// cmdHandler 管理命令url
func cmdHandler(c *gin.Context) {
	c.JSON(200, gin.H{})
}

// ping命令
func pingHandler(c *gin.Context) {
	_ = svc.Ping(c)
	// http相关的返回
	c.JSON(200, "pong")
}

func healthCheckHandler(c *gin.Context) {
	log.Log.Debug("service health check")
	if svc.HealthCheck() != nil {
		log.Log.Fatal("service unhealthy")
	}
	c.String(0, "ok")
}

func exportHandler(c *gin.Context) {
	ep, err := getExportParas(c)

	if err != nil {
		log.Log.Error(fmt.Sprintf("export error: %s", err.Error()), zap.String("finname", ep.FinName))
		c.String(400, err.Error())
		return
	}
	err = svc.Export(ep.FinName, ep.QP)
	if err != nil {
		log.Log.Error(fmt.Sprintf("export error: %s", err.Error()), zap.String("finname", ep.FinName))
		c.String(400, err.Error())
	} else {
		log.Log.Info(fmt.Sprintf("export success"), zap.String("finname", ep.FinName))
		c.String(0, "export succeed")
	}

}

func getExportParas(c *gin.Context) (ep pg.ExportParam, err error) {
	if ep.FinName = c.PostForm(pg.FINNAME); ep.FinName == "" {
		err = fmt.Errorf("finname is empty")
		return
	}
	if ep.QP.StartDate, _ = strconv.Atoi(c.PostForm(pg.STARTDATE)); ep.QP.StartDate == 0 {
		y, m, d := time.Now().AddDate(0, 0, -1).Date()
		ep.QP.StartDate = y*10000 + 100*int(m) + d
	}
	if ep.QP.EndDate, _ = strconv.Atoi(c.PostForm(pg.ENDDATE)); ep.QP.EndDate == 0 {
		y, m, d := time.Now().Date()
		ep.QP.EndDate = y*10000 + 100*int(m) + d
	}
	if c.PostForm(pg.TYPE) == "" {
		ep.QP.ProcType = pg.OpRtime
	} else {
		ep.QP.ProcType, _ = strconv.Atoi(c.PostForm(pg.TYPE))
	}
	ep.QP.CodeList = c.PostForm(pg.CODELIST)

	return
}
