/*
 * @Author: WangHongyin
 * @Date: 2020-09-06 16:44:16
 * @LastEditTime: 2020-09-09 20:21:50
 * @LastEditors: Please set LastEditors
 * @Description: Clickhouse输出组件
 * @FilePath: \collecter\plugin\output_clickhouse.go
 */
package plugin

import (
	"fmt"
	"log"
	"time"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/jmoiron/sqlx"
	"sugon.com/WangHongyin/collecter/logger"
)

type OutputClickhouse struct {
	OutputCommon
	Db *sqlx.DB
}

var OutputClickhouseRegInfo RegisterInfo

func init() {
	OutputClickhouseRegInfo.Plugin = "Output"
	OutputClickhouseRegInfo.Type = "Clickhouse"
	OutputClickhouseRegInfo.SubType = ""
	OutputClickhouseRegInfo.Vendor = "sugon.com"
	OutputClickhouseRegInfo.Desc = "for clickhouse output"
	OutputClickhouseRegInfo.init = OutputClickhouseRouteInit

	OutputRegister(&OutputClickhouseRegInfo)
}

func OutputClickhouseRouteInit(c *YamlCommonData, out interface{}, countptr interface{}, statptr *RouteStatType) interface{} {
	r := new(OutputClickhouse)
	str := fmt.Sprintf("tcp://%s?database=%s&debug=true", c.Output.Clickhouse.Host, c.Output.Clickhouse.Database)
	Db, err := sqlx.Open("clickhouse", str)
	if err != nil {
		logger.Emer("Clickhouse sqlx.Open err : %s", err)
	}

	r.Db = Db
	r.config = c
	r.rc = countptr.(*RouteCount)
	r.reginfo = &OutputClickhouseRegInfo

	return r
}

func (r *OutputClickhouse) Insert(Db *sqlx.DB) {

	insert_sql := "INSERT INTO asset_syslog (id, aseet_syslog_date, asset_ip, asset_id, asset_name, asset_type, asset_category, syslog_level, facility, program, message, pid, hostname)VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"

	tx, _ := Db.Begin()
	stmt, _ := tx.Prepare(insert_sql)
	defer stmt.Close()

	_, err := stmt.Exec(0, time.Now(), "10.22.33.44", 23, "测试机器", "计算", "假发", "Alert", "xxx", "dddd", "dsaad", 1, "gjk")
	if err != nil {
		fmt.Println(err)
		return
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

}
func (r *OutputClickhouse) OutputProcess(msg interface{}, msg_len int) {
	switch msg.(type) {

	case map[string]interface{}:

		r.Insert(r.Db)

	case []map[string]interface{}:

		r.Insert(r.Db)
	}
}
