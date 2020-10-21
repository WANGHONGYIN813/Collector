package plugin

import (
	"bytes"
	"database/sql"
	"encoding/json"

	"sugon.com/WangHongyin/collecter/logger"

	//"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

var dataCount = 0
var byteBuffer = bytes.NewBuffer(nil)
var lockch chan int

type Type_t struct {
	id   int    `db:"id"`
	name string `db:"name"`
}

type SubType_t struct {
	subtype_id   int    `db:"subtype_id"`
	subtype_name string `db:"subtype_name"`
	type_id      int    `db:"type_id"`
}

//var DbSyslog_Udp_InitChan = 100
//var DbSyslogUdplimitChan = make(chan int, DbSyslog_Udp_InitChan)

var DbSyslogUdplimitChan chan int
var Type_map = make(map[interface{}]interface{})
var SubType_map = make(map[interface{}]interface{})

type InputDbSyslog struct {
	InputCommon
	Tcplistener     net.Listener
	Udplistener     *net.UDPConn
	output          FilterInterface
	udp_buffer_max  int
	IpBlackListFlag bool
	IpBlackList     map[string]bool
}

var InputDbSyslogRegInfo RegisterInfo

func (r *InputDbSyslog) exit() {
	defer r.Tcplistener.Close()
}

func init() {

	v := &InputDbSyslogRegInfo

	v.Plugin = "Input"
	v.Type = "DbSyslog"
	v.SubType = ""
	v.Vendor = "sugon.com"
	v.Desc = "for dbsyslog input"
	v.init = RouteInputDbSyslogInit

	InputRegister(v)

	lockch = make(chan int, 1)

}

func queryMulti(DB *sql.DB) {
	type_t := new(Type_t)
	rows, err := DB.Query("select * from type_table ")

	defer func() {
		if rows != nil {
			rows.Close()
		}
	}()

	if err != nil {
		logger.Error("Query type_table Err:%v", err)
		return
	}
	for rows.Next() {
		err = rows.Scan(&type_t.id, &type_t.name)
		if err != nil {
			logger.Error("queryMulti Scan failed,err:%v\n", err)
			return
		}

		s1 := strconv.Itoa(type_t.id)
		Type_map[s1] = type_t.name

	}

	subtype_t := new(SubType_t)
	subrows, err := DB.Query("select * from subtype_table ")

	defer func() {
		if subrows != nil {
			subrows.Close()
		}
	}()

	if err != nil {
		logger.Error("Query subtype_table Err:%v", err)
		return
	}
	for subrows.Next() {
		err = subrows.Scan(&subtype_t.subtype_id, &subtype_t.subtype_name, &subtype_t.type_id)
		if err != nil {
			logger.Error("queryMulti Scan failed,err:%v", err)
			return
		}
		s2 := strconv.Itoa(subtype_t.subtype_id)
		SubType_map[s2] = subtype_t.subtype_name
	}

	//logger.Info("Type_map:", Type_map)
	//logger.Info(Type_map["22"])
	//logger.Info("SubType_map", SubType_map)
}

func RouteInputDbSyslogInit(c *YamlCommonData, out interface{}, countptr interface{}, statptr *RouteStatType) interface{} {
	DB := GetMysqlConn()
	queryMulti(DB)

	r := new(InputDbSyslog)

	r.config = c
	r.rc = countptr.(*RouteInputCounts)
	r.reginfo = &InputDbSyslogRegInfo

	r.output = out.(FilterInterface)

	r.switcher = (*int32)(statptr)

	tc := c.Input.DbSyslog

	if len(tc.Ipblacklist) != 0 {
		r.IpBlackListFlag = true

		r.IpBlackList = make(map[string]bool)

		for _, v := range tc.Ipblacklist {
			r.IpBlackList[v] = true
		}
	}

	r.udp_buffer_max = SysConfigGet().Udp_buffer_max

	var DbSyslog_Udp_InitChan = r.InputCommon.config.Input.DbSyslog.Udp_Limit
	DbSyslogUdplimitChan = make(chan int, DbSyslog_Udp_InitChan)

	createFile(r.InputCommon.config.Output.File.Path)
	return r
}

func isExist(path string) bool {
	_, err := os.Stat(path) //os.Stat获取文件信息
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}

func createFile(filePath string) error {
	if !isExist(filePath) {
		err := os.MkdirAll(filePath, os.ModePerm)
		return err
	}
	return nil
}

func (r *InputDbSyslog) DbSyslogOutputDataInit(remote_ip interface{}, o map[string]interface{}) {

	var v Beat_t

	v.Hostname = Hostname
	v.Hostip = LocalIP

	o["beat"] = v

	if r.config.Input.DbSyslog.Fields != nil {
		o["fields"] = r.config.Input.DbSyslog.Fields
	}

	if r.config.Input.DbSyslog.Type != "" {
		o["Type"] = r.config.Input.DbSyslog.Type
	}

	o["_RemoteIp"] = remote_ip

	o["@timestamp"] = time.Now().UTC().Format("2006-01-02T15:04:05.999Z")

	Hostmassagebuffer := Hostmassage

	if Hostmassagebuffer != nil {
		logger.Info("BUFFER:", Hostmassagebuffer)
		for _, value := range Hostmassagebuffer {
			logger.Info("assetIp", value["assetIp"])
			logger.Info("remote_ip", remote_ip)

			Ipstring := remote_ip.(net.IP).String()

			logger.Info("Ipstring:", Ipstring)

			if value["assetIp"] == Ipstring {

				logger.Info("o:", o)

				o["assetId"] = value["assetId"]
				o["assetType"] = value["assetType"]
				o["assetCategory"] = value["assetCategory"]
				o["assetName"] = value["assetName"]

				//logger.Info("o:", o)

			}
		}
	}

}

func (r *InputDbSyslog) datahander(b *bytes.Buffer, totalbyte int) ([][]byte, int, int) {

	flag := byte('\n')

	var arr [][]byte

	OriLines := 0

	for i := 0; ; i++ {

		a, err := b.ReadBytes(flag)

		this_len := len(a)

		if err != nil {
			if totalbyte > 0 {
				OriLines += 1

				b := a[:totalbyte]

				arr = append(arr, b)
			}
			break
		}

		totalbyte -= this_len

		OriLines += 1

		msg_len := len(a)

		if msg_len == 1 && a[0] == '\n' {
			atomic.AddInt64(&r.rc.InvalidCount, 1)
			continue
		}

		arr = append(arr, a[:msg_len-1])
	}

	atomic.AddInt64(&r.rc.InputCount, int64(OriLines))

	return arr, OriLines, len(arr)
}

func (r *InputDbSyslog) readConn(conn net.Conn) ([][]byte, int, int) {

	b := bytes.NewBuffer(nil)

	sum := 0

	for {
		data := make([]byte, buffer_max)

		readnumber, err := conn.Read(data)
		if err != nil {
			//logger.Trace("Connect Close")
			logger.Debug("Connect Close")
			break
		}

		//logger.Debug("Read Data:", string(data))

		sum += readnumber
		_, _ = b.Write(data[0:readnumber])
	}

	//logger.Debug("Read Data Done:", b.String())

	arr, orilen, arrlen := r.datahander(b, sum)

	atomic.AddInt64(&r.rc.OutputCount, int64(arrlen))

	return arr, orilen, arrlen
}

//处理一条数据
func DbSyslogMessageFormatParse(msg string) string {

	s := strings.SplitAfterN(msg, ": ", 2)
	if len(s) == 2 {
		return s[1]
	} else {
		return ""
	}

}

func remove(arr []string, datamap map[string]interface{}) map[string]interface{} {
	for _, v := range arr {
		delete(datamap, v)
	}
	return datamap
}

func (r *InputDbSyslog) DbSyslogFormatParse(arr [][]byte, remote_ip interface{}, arrlen int) ([]map[string]interface{}, int) {

	var c []map[string]interface{}

	for _, v := range arr {

		s := DbSyslogMessageFormatParse(string(v))
		//logger.Info("message:", s)

		if len(s) == 0 {
			arrlen = arrlen - 1
			atomic.AddInt64(&r.rc.InvalidCount, 1)
			continue
		}

		//json转map(这里的json为需要的数据)
		var datamap = make(map[string]interface{})
		err := json.Unmarshal([]byte(s), &datamap)
		if err != nil {
			arrlen = arrlen - 1
			//logger.Error("Err message:", err, "Err data:", s)
			atomic.AddInt64(&r.rc.InvalidCount, 1)
			continue
		}

		//datamap["type"]是float类型
		//logger.Debug(datamap["type"])
		//logger.Debug(datamap["protocol"])

		//float转string

		s1 := strconv.FormatFloat(datamap["type"].(float64), 'f', -1, 64)
		s2 := strconv.FormatFloat(datamap["protocol"].(float64), 'f', -1, 64)
		datamap["type_name"] = Type_map[s1]
		datamap["protocol_name"] = SubType_map[s2]

		r.DbSyslogOutputDataInit(remote_ip, datamap)

		if len(r.config.Input.DbSyslog.Remove_Fields) != 0 {
			//删除字段
			datamap = remove(r.config.Input.DbSyslog.Remove_Fields, datamap)
		}

		c = append(c, datamap)
	}

	return c, arrlen
}

func (r *InputDbSyslog) DbSyslogTcpDataHandler(conn net.Conn) {

	defer conn.Close()

	t := time.Now()

	remoteaddr := conn.RemoteAddr().String()
	st := strings.Split(remoteaddr, ":")
	remote_ip := st[0]

	logger.Info("Recv Data From : ", remote_ip)

	arr, orilen, arrlen := r.readConn(conn)

	logger.Debug("DbSyslog TCP Input Lines : %d ,ffective Lines : %d . Cost %s", orilen, arrlen, time.Now().Sub(t))

	if arrlen == 0 {
		return
	}

	if r.InputCommon.config.Input.DbSyslog.Mode == "performance" {

		defer PanicHandler()

		controlch := make(chan int, 1)
		d := arr
		bulknum := r.config.Output.Elasticsearch.Bulk
		msglen := len(d)
		bg := msglen / bulknum    //求整
		yushu := msglen % bulknum //求余
		if yushu != 0 {
			bg = bg + 1
		}
		var j int
		var l int
		for i := 0; i < bg; i++ {
			controlch <- 1
			l = i
			if (i+1)*bulknum > msglen {
				j = msglen
			} else {
				j = (i + 1) * bulknum
			}

			//splitepool <- 1
			go func() {

				defer PanicHandler()

				t := time.Now()
				datacount := 0
				min := l * bulknum
				max := j
				<-controlch
				buf := bytes.NewBuffer(nil)

				for _, value := range d[min:max] {

					s := DbSyslogMessageFormatParse(string(value))
					//logger.Info("message:", s)

					if len(s) == 0 {
						arrlen = arrlen - 1
						atomic.AddInt64(&r.rc.InvalidCount, 1)
						continue
					}

					//json转map(这里的json为需要的数据)
					var datamap = make(map[string]interface{})
					err := json.Unmarshal([]byte(s), &datamap)
					if err != nil {
						arrlen = arrlen - 1
						//logger.Error("Err message:", err, "Err data:", s)
						atomic.AddInt64(&r.rc.InvalidCount, 1)
						continue
					}

					//float转string

					s1 := strconv.FormatFloat(datamap["type"].(float64), 'f', -1, 64)
					s2 := strconv.FormatFloat(datamap["protocol"].(float64), 'f', -1, 64)
					datamap["type_name"] = Type_map[s1]
					datamap["protocol_name"] = SubType_map[s2]

					r.DbSyslogOutputDataInit(remote_ip, datamap)

					if len(r.config.Input.DbSyslog.Remove_Fields) != 0 {
						//删除字段
						datamap = remove(r.config.Input.DbSyslog.Remove_Fields, datamap)
					}

					//ret为map[string]interface{}
					if datamap != nil {
						datacount++
						b := Json_Marshal(datamap)
						buf.Write(bulk_flag_byte)
						buf.Write(b)
						buf.Write(return_flag)
					}

				}

				c := r.config.Output.Elasticsearch
				h := c.EsUrl
				es_target_num := len(h)
				index := rand.Intn(es_target_num)

				oldindexname := c.Index
				var newindexname string

				if c.Index_by_day {

					tag := "-" + time.Now().Format("2006-01-02")

					newindexname = c.Index + tag

				}

				oldurl := h[index]
				url := strings.Replace(oldurl, oldindexname, newindexname, 1)

				res, err := http.Post(url, "application/json", strings.NewReader(buf.String()))
				if err != nil {

					atomic.AddInt64(&r.rc.ErrCount, int64(datacount))

				} else {

					atomic.AddInt64(&r.rc.OutputCount, int64(datacount))

					logger.Debug("Inputdata %d : Filite And To Es Total Cost %s", datacount, time.Now().Sub(t))
				}

				defer res.Body.Close()

				//<-splitepool

			}()

		}

		return
	}

	//数据处理
	c, arrlen_t := r.DbSyslogFormatParse(arr, remote_ip, arrlen)

	logger.Debug("DbSyslog TCP Input Lines : %d ,Output Lines : %d . Cost %s", orilen, arrlen_t, time.Now().Sub(t))

	//logger.Error("in", len(c), arrlen_t)

	r.output.FilterProcess(c, arrlen_t, nil)
}

//DbSyslogUDPDataHandler UDP处理过程
func (r *InputDbSyslog) DbSyslogUDPDataHandler(conn *net.UDPConn) {

	defer PanicHandler()

	t := time.Now()

	data := make([]byte, r.udp_buffer_max)

	//logger.Info("Read From UDP")
	read, remoteAddr, err := conn.ReadFromUDP(data)
	if err != nil {
		logger.Error("UDP Read err:", err)
		<-DbSyslogUdplimitChan
		return
	}

	if read == 0 {
		<-DbSyslogUdplimitChan
		return
	}

	//logger.Debug("Read Data", string(data[0:read]))

	atomic.AddInt64(&r.rc.OutputCount, 1)

	var arr [][]byte
	arr = append(arr, data[0:read])
	if r.config.Input.DbSyslog.Mode == "performance" {

		for _, v := range arr {
			lockch <- 1
			_, err := byteBuffer.Write(v)
			if err != nil {
				logger.Error("Writr byteBuffer Err :", err)
				continue
			}
			dataCount = dataCount + 1
			//logger.Info("dataCount:", dataCount)
			if dataCount >= r.config.Output.File.LineCount {
				//写文件
				dataCount = 0
				fileBuffer := make([]byte, buffer_max)
				_, readErr := byteBuffer.Read(fileBuffer)
				if readErr != nil {
					<-DbSyslogUdplimitChan
					lockch <- 1
					return
				}

				timeUnix := strconv.Itoa(int(time.Now().Unix()))

				f, err := os.Create(r.config.Output.File.Path + timeUnix + ".log")
				defer f.Close()
				if err != nil {
					logger.Error("Creat %s Fail : ", r.config.Output.File.Path+timeUnix, err)
				} else {
					_, writeErr := f.Write(fileBuffer)
					if writeErr != nil {
						logger.Error("Write %s Fail :", r.config.Output.File.Path+timeUnix, writeErr)
					}
					command := "mv " + r.config.Output.File.Path + timeUnix + ".log" + " " + r.config.Output.File.Path + timeUnix + ".sgn"
					cmd := exec.Command("/bin/bash", "-c", command)
					_, cmdErr := cmd.Output()
					if cmdErr != nil {
						logger.Error("Rename .log Err :", cmdErr)
					}
				}

			}
			<-lockch
			<-DbSyslogUdplimitChan
		}

		return
	}
	<-DbSyslogUdplimitChan

	c, arrlen_t := r.DbSyslogFormatParse(arr, remoteAddr.IP, 1)

	logger.Debug("DbSyslog UDP Input Lines : %d ,Output Lines : %d . Cost %s", 1, arrlen_t, time.Now().Sub(t))

	r.output.FilterProcess(c, arrlen_t, nil)
}

func (r *InputDbSyslog) SyslogTcpListen() {

	defer r.Tcplistener.Close()

	logger.Alert("DbSyslog Input TCP Start")

	for atomic.LoadInt32(r.switcher) > 0 {

		conn, err := r.Tcplistener.Accept()
		if err != nil {
			logger.Warn(err)
			continue
		}

		go r.DbSyslogTcpDataHandler(conn)
	}

	logger.Alert("DbSyslog TCP Instance %s Close Listen", r.config.Input.DbSyslog.Addr)

	if atomic.LoadInt32(r.switcher) == int32(RouteStat_Off) {

		logger.Alert("DbSyslog TCP Instance %s Stop", r.config.Input.DbSyslog.Addr)
	}
}

//SyslogUDPListen UDP通信监听
func (r *InputDbSyslog) SyslogUDPListen() {

	logger.Alert("DbSyslog Input UDP Start")

	defer r.Udplistener.Close()

	for atomic.LoadInt32(r.switcher) > 0 {

		DbSyslogUdplimitChan <- 1
		go r.DbSyslogUDPDataHandler(r.Udplistener)
	}

	logger.Alert("DbSyslog UDP Instance %s Close Listen", r.config.Input.DbSyslog.Addr)

	if atomic.LoadInt32(r.switcher) == int32(RouteStat_Off) {

		logger.Alert("DbSyslog UDP Instance %s Stop", r.config.Input.DbSyslog.Addr)
	}

}

//InputProcess 输入函数
func (r *InputDbSyslog) InputProcess(input interface{}) error {

	ipaddr := input.(string)

	listener, err := net.Listen("tcp", ipaddr)
	if err != nil {
		logger.Alert(err)
		return err
	}

	r.Tcplistener = listener

	go r.SyslogTcpListen()

	udpAddr, err := net.ResolveUDPAddr("udp", ipaddr)
	if err != nil {
		logger.Alert(err)
		return err
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		logger.Alert(err)
		return err
	}

	r.Udplistener = conn

	go r.SyslogUDPListen()

	return nil
}

//MainLoop 入口函数
func (r *InputDbSyslog) MainLoop() error {

	if atomic.LoadInt32(r.switcher) == int32(RouteStat_Ready) {
		logger.Alert("DbSyslog Input Instance %s Start", r.config.Input.DbSyslog.Addr)

		ret := r.InputProcess(r.config.Input.DbSyslog.Addr)
		if ret != nil {
			return ret
		}

		atomic.StoreInt32(r.switcher, int32(RouteStat_On))
	}

	return nil
}
