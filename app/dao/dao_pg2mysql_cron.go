package dao

import (
	"fmt"
	"go.uber.org/zap"
	"hxextract/app/cron"
	"hxextract/app/dao/pg"
	"hxextract/app/log"
	"strings"
	"time"
)

var cronTab map[int]string

var cronParam = pg.QueryParam{
	ProcType: 2,
}

// 初始化日期，定时任务执行前一天到当天
func cronInit() {
	tToday := time.Now()
	tYesterday := tToday.AddDate(0, 0, -1)
	d, m, y := tToday.Date()
	cronParam.EndDate = d + 100*int(m) + 1000*y
	d, m, y = tYesterday.Date()
	cronParam.StartDate = d + 100*int(m) + 1000*y
}

func (d *dao) pgCronInit() error {
	cronTab = pgDao.GetCronTab()
	spec := "0 * * * * ?" //cron表达式，每分钟触发一次
	return cron.AddFunc(spec, d.exportCron)
}

func (d *dao) exportCron() {
	nowTime := time.Now()
	nowMS := nowTime.Hour()*10000 + nowTime.Minute()*100 + nowTime.Second()

	if _, ok := cronTab[nowMS]; ok {
		taskNames := cronTab[nowMS]
		go d.TasksExport(taskNames)
	}
}

func (d *dao) TasksExport(taskNames string) {
	tasks := strings.Split(taskNames, ",")
	// 去重去空
	tasks = removeDuplicateElement(tasks)
	cronInit()
	for _, taskName := range tasks {
		fins, err := pgDao.GetTaskFins(taskName)
		if err != nil {
			log.Log.Error(fmt.Sprintf("get finnames from task failed: %s", err.Error()),
				zap.String("taskname", taskName))
			continue
		}
		for _, fin := range fins {
			err = d.ExportPgData(fin, cronParam)
			if err != nil {
				log.Log.Error(fmt.Sprintf("export pg data failed: %s", err.Error()),
					zap.String("taskname", taskName),
					zap.String("finname", fin))
			}
		}
	}
}

/*removeDuplicateElement
 * @Description: 去重去空
 * @param stringSlice
 * @return []string
 */
func removeDuplicateElement(stringSlice []string) []string {
	result := make([]string, 0, len(stringSlice))
	temp := map[string]struct{}{}
	for _, item := range stringSlice {
		if item == "" { // 去空
			continue
		}
		if _, ok := temp[item]; !ok { // 去重
			temp[item] = struct{}{}
			result = append(result, item)
		}
	}
	return result
}
