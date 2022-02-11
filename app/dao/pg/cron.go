package pg

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
)

var (
	cronTab = make(map[int]string) // 用于存储所有任务的定时任务时间
)

func (d *pgDao) TaskCronLoad() error {
	querySql := fmt.Sprintf("SELECT distinct taskname,cron from %s.%s where export = 2;",
		d.DB.tables.schemaName, d.DB.tables.taskInfo)
	rows, err := d.DB.baseDB.Raw(querySql).Rows() // 使用原生sql进行查询
	if err != nil {
		return err
	}
	var taskName, cron string
	//逐行处理查询结果
	for rows.Next() {
		err = rows.Scan(&taskName, &cron)
		if err != nil {
			return err
		} else {
			appendCron(taskName, cron)
		}
	}
	return nil
}

func (d *pgDao) GetCronTab() map[int]string {
	return cronTab
}

func (d *pgDao) TaskProc(taskName string) error {
	log.Println("start to handle task:", taskName)

	finNames, err := d.DB.GetTaskFins(taskName)
	if err != nil {
		// TODO log
		return err
	}
	year, month, day := time.Now().Date()
	// go里重定义了month类型
	para := QueryParam{
		OpRtime,
		year*10000 + int(month)*100 + day,
		year*10000 + int(month)*100 + day,
		"",
	}
	for _, finName := range finNames {
		_, err = d.GetRows(finName, para)
		// todo cron 和 mysql dao层交互
		if err != nil {
			// TODO log
			log.Println(err.Error())
		}
	}
	return nil
}

/*appendCron
 * @Description: 将数据库获得的map
 * @param crons
 */
func appendCron(taskName string, cron string) {
	crons := splitCron(cron)
	for _, v := range crons {
		cronTab[v] += taskName + ","
	}
}

/*splitCron
 * @Description: 将数据库中记录的日期字符串转化为整数切片
 * @param crons: 财务文件定时任务格式一般为yy:mm;yy:mm;yy:mm
 * @return cronTab
 */
func splitCron(cron string) []int {
	var crons []int
	times := strings.Split(cron, ";")
	for _, v := range times {
		if v == "" {
			continue
		}
		ti := getTime(v)
		crons = append(crons, ti)
	}
	return crons
}

/*getTime
 * @Description: 将时分转化成整数形式
 * @param ti yy:mm格式的字符串（也支持yy:mm:ss
 * @return T 时间
 */
func getTime(ti string) (T int) {
	hms := strings.Split(ti, ":")
	hour, _ := strconv.Atoi(hms[0])
	minute, second := 0, 0
	//防止瞎写
	if len(hms) == 3 { //时分或时分秒
		minute, _ = strconv.Atoi(hms[1])
		second, _ = strconv.Atoi(hms[2])
	} else if len(hms) == 2 {
		minute, _ = strconv.Atoi(hms[1])
	}
	// 防止有人瞎写
	hour %= 24
	minute %= 60
	second %= 60
	T = hour*10000 + minute*100 + second
	return
}
