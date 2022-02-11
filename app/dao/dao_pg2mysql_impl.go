package dao

import (
	"bytes"
	"database/sql"
	"fmt"
	"go.uber.org/zap"
	"hxextract/app/config"
	"hxextract/app/dao/orm"
	"hxextract/app/dao/pg"
	"hxextract/app/log"
	"reflect"
	"strconv"
	"strings"
	"time"
)

/**
 * @Description: 用于逐行处理sql数据的对象
 */
type colValue struct {
	colNames []string          // 字段名
	colTypes []*sql.ColumnType //字段类型

	// 从pgsql中取出来的数据
	scans  []interface{} //存储values各项地址，用于进行scan操作
	values []sql.RawBytes

	// 组装入库mysql的数据时
	colsScans []interface{} //存储每行的各个字段值，结构[]interface{}进行format
}

//ExportPgData
//  @Description: 从pg导入数据
//  @receiver d
//  @param finName
//  @param param
//  @return error
//
func (d *dao) ExportPgData(finName string, param pg.QueryParam) error {
	log.Log.Info("Start to export data from pg", zap.String("finname", finName), zap.Any("param", param))
	tableName, schemaName, err := pgDao.GetTableInfo(finName)
	if err != nil {
		return err
	}
	db, err := d.DB.getConn(schemaName)
	if err != nil {
		return err
	}
	rows, err := pgDao.GetRows(finName, param)
	if err != nil {
		return err
	}
	sqlList, err := d.rows2sqls(pg.FinanceInfo{SchemaName: schemaName, FinName: finName, TableName: tableName}, rows)
	for _, sqlBytes := range sqlList {
		sqlStr := sqlBytes.String()
		go func() {
			result, unitErr := db.Exec(sqlStr)
			if unitErr != nil {
				log.Log.Error(unitErr.Error())
			} else {
				lastInsertId, _ := result.LastInsertId()
				affectRows, _ := result.RowsAffected()
				log.Log.Info("", zap.Int64("Id", lastInsertId), zap.Int64("affected rows", affectRows))
			}
		}()
	}
	return nil
}

/**
 * @Description: 创建新的单行数据处理对象
 * @param cols
 * @return *colValue
 */
func (d *dao) newValue(cols []string) *colValue {
	length := len(cols)
	scans := make([]interface{}, length)
	values := make([]sql.RawBytes, length)

	// 往mysql入库时不需要market字段，故-1
	colsScans := make([]interface{}, length-1)
	// 用scans存储values的地址，用于scans结构后给Scan()传入values元素的地址
	for i := range values {
		scans[i] = &values[i]
	}
	return &colValue{colNames: cols, scans: scans, values: values, colsScans: colsScans}
}

//
//  rows2sqls
//  @Description: 将从pg请求的结果转化为入库mysql的sql
//  @param fin
//  @param rows
//  @return []*bytes.Buffer
//  @return error
//
func (d *dao) rows2sqls(fin pg.FinanceInfo, rows *sql.Rows) ([]*bytes.Buffer, error) {
	ret := make([]*bytes.Buffer, 0)
	colNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	col := d.newValue(colNames)
	col.colTypes, err = rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	sqlFmtStr, err := d.getFormatStr(colNames, fin.SchemaName)
	if err != nil {
		return nil, err
	}
	// 逐行处理sql
	// 全量执行时数据量较大可能会有数百万行，因此用Buffer缓冲器和fmt来进行string的拼接，以提升性能
	rowCnt := 1
	sqlHead := d.getSqlHead(fin.TableName, colNames)
	querySql := bytes.NewBufferString(sqlHead)
	rowLimit := config.GetMysql().RowLimit
	endFlag := rows.Next() //提过endFlag判断是否还有数据
	// next 在校验下面分支条件中用于判断是否到结尾
	for strValue := ""; rows.Next(); rowCnt++ {
		strValue, err = d.getRowValue(col, sqlFmtStr, rows)
		endFlag = rows.Next()
		if err != nil {
			log.Log.Error(err.Error())
			continue
		}
		querySql.WriteString(strValue)

		// 最后一行或行数到达限制时执行，将该部分数据进行下一步处理
		if !endFlag || rowCnt%rowLimit == 0 {
			querySql.WriteByte(';')
			ret = append(ret, querySql)
			querySql = bytes.NewBufferString(sqlHead)
		} else {
			querySql.WriteByte(',')
		}
	}
	return ret, err
}

/**
 * @Description: 根据字段名组replace into ... （简称sql头
 * @param tableName 表名
 * @param cols 所有字段名
 * @return string 返回replace into `tableName` (col[0],col[1],...,col[i],...,col[n-1])
 */
func (d *dao) getSqlHead(tableName string, cols []string) string {
	sqlHead := new(bytes.Buffer)
	sqlHead.WriteString(fmt.Sprintf("REPLACE INTO `%s`(", tableName))
	lenCols := len(cols)
	for i := 0; i < lenCols-1; i++ {
		if cols[i] == pg.MARKET {
			// 入库mysql时不需要market字段故跳过
			continue
		}
		sqlHead.WriteString(fmt.Sprintf("`%s`,", cols[i]))
	}
	sqlHead.WriteString(fmt.Sprintf("`%s`)VALUES", cols[lenCols-1]))
	return sqlHead.String()
}

/*getFormatStr
 * @Description: 通过字段名查表获取字段类型，组装sql时要需要根据数据类型来判断是否需要引号
 * @param cols
 * @return fmtStr
 */
func (d *dao) getFormatStr(cols []string, schemaName string) (fmtStr string, err error) {
	// TODO 有字段对应不上时报错并返回
	schemaNames := []string{schemaName, "*"}
	var colsWithoutMarket []string
	for _, v := range cols {
		if v != pg.MARKET {
			colsWithoutMarket = append(colsWithoutMarket, v)
		}
	}
	var result []orm.TypeDescribe
	d.DB.defaultOrm.Table("type_describe").Where("field_schema in ? and field_name in ?",
		schemaNames, colsWithoutMarket).Find(&result)
	if d.DB.defaultOrm.Error != nil {
		err = d.DB.defaultOrm.Error
		return
	}
	// sql查询结果和cols中字段名排序不能保持一致，因此通过map来保证其位置对应
	mysqlFieldType := make(map[string]int)
	for _, v := range result {
		mysqlFieldType[v.FieldName] = v.FieldType
	}

	fmtStr = "("
	for _, v := range colsWithoutMarket {
		if mysqlFieldType[v] == orm.TypeSTRING || mysqlFieldType[v] == orm.TypeTIMESTAMP {
			fmtStr += "%q,"
		} else {
			fmtStr += "%s,"
		}
	}
	fmtStr = strings.TrimRight(fmtStr, ",") + ")"
	return
}

/**
 * @Description: 获取单行数据
 * @receiver c
 * @param fmtStr
 * @param rows
 * @return string
 * @return error
 */
func (d *dao) getRowValue(c *colValue, fmtStr string, rows *sql.Rows) (string, error) {
	err := rows.Scan(c.scans...) // 将scans结构（scans中为values的地址
	if err != nil {
		return "", err
	}
	for i, j := 0, 0; i < len(c.values); i++ {
		if c.colNames[i] == pg.MARKET {
			// 入库mysql时不需要市场号，continue跳过j++
			continue
		}
		value := string(c.values[i])
		if value == "" {
			c.colsScans[j] = "NULL"
		} else if c.colTypes[i].ScanType() == reflect.TypeOf(time.Time{}) {
			// 时间类型需要特殊处理
			if c.colNames[i] == pg.RTIME {
				// rtime 需要保留 YYYY-MM-DD hh:ii:ss.micro 的格式
				t, _ := time.Parse(time.RFC3339Nano, value)
				c.colsScans[j] = t.Format("2006-01-02 15:04:05.000000")
			} else {
				// 将时间的字符串转换成YYYYMMDD形式的整数（mysql中该字段为整数型不加引号
				c.colsScans[j] = strconv.Itoa(d.date2Int(value))
			}
		} else {
			c.colsScans[j] = value
		}
		j++
	}
	return fmt.Sprintf(fmtStr, c.colsScans...), err
}

/*date2Int
 * @Description: 对老版财务数据中所取的日期字段做特殊处理
 * @Description: pgsql中取出来的日期为TZ格式的时间，需要转化为int类型，即写为YYYYMMDD的整数，需要进行相应转换
 * @Description: 特殊处理原因：历史遗留问题，日期和时间均为整数型
 * @param date
 * @return int
 */
func (d *dao) date2Int(timeString string) (dateInt int) {
	t, _ := time.Parse(time.RFC3339Nano, timeString)
	dateYear := t.Year()
	dateMonth := int(t.Month())
	dateDay := t.Day()
	dateInt = dateYear*10000 + dateMonth*100 + dateDay
	return
}
