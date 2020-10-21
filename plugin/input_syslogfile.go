package plugin

import (
	"sugon.com/WangHongyin/collecter/logger"

	//"bufio"
	"bytes"
	"io/ioutil"
	"os"

	//"os/exec"
	//"path/filepath"
	"encoding/json"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type InputSyslogFile struct {
	InputCommon
	Read_interval int
	Handle_mode   string
	Bath_path     string
	Dir_arr       []string
	output        FilterInterface
}

var InputSyslogFileRegInfo RegisterInfo

var changFileNamech chan int

//var readxdrpool chan int

func (r *InputSyslogFile) exit() {

}

func init() {
	v := &InputSyslogFileRegInfo

	v.Plugin = "Input"
	v.Type = "SyslogFile"
	v.SubType = ""
	v.Vendor = "sugon.com"
	v.Desc = "for syslogfile input"
	v.init = RouteInputSyslogFileInit

	InputRegister(v)
	changFileNamech = make(chan int, 1)
	//readxdrpool = make(chan int, *readxdrpoolnum)

	//logger.Info("Register InputSyslogfiLE")

}

func RouteInputSyslogFileInit(c *YamlCommonData, out interface{}, countptr interface{}, statptr *RouteStatType) interface{} {
	r := new(InputSyslogFile)

	r.config = c
	r.rc = countptr.(*RouteInputCounts)
	r.reginfo = &InputSyslogFileRegInfo

	r.output = out.(FilterInterface)
	r.switcher = (*int32)(statptr)

	// r.Bath_path = c.Input.XdrFile.Bath_path
	// r.Dir_arr = c.Input.XdrFile.Dir_arr
	// r.Read_interval = c.Input.XdrFile.Read_interval

	logger.Info("InputSyslogFile path : ", r.Bath_path)

	return r
}

func (r *InputSyslogFile) datahander(b *bytes.Buffer, totalbyte int) ([][]byte, int, int) {

	flag := byte('\n')

	var arr [][]byte

	OriLines := 0

	for i := 0; ; i++ {

		a, err := b.ReadBytes(flag) //读取缓冲区第一个分隔符前面的内容以及分隔符并返回，缓冲区会清空读取的内容。如果没有发现分隔符，则返回读取的内容并返回错误io.EOF

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

func (r *InputSyslogFile) GetSyslogdata(filepath string, suffix string) {
	t := time.Now()

	//遍历filepath目录下.sgn文件,后缀名改为.txt
	filelist, _ := WalkDirAndTrans(filepath, suffix)
	<-changFileNamech
	if len(filelist) == 0 {
		return
	}

	rangech := make(chan int, 1) // 控制并发速度（循环的value值需传入后续处理过程中）

	var filename string
	for _, v := range filelist {
		rangech <- 1
		filename = v
		//这里也可并发执行
		//readxdrpool <- 1
		go func() {

			defer PanicHandler()

			sgn_path := filename
			<-rangech

			sum := 0
			b := bytes.NewBuffer(nil)

			syslogdata, readSyslogErr := ioutil.ReadFile(sgn_path)
			if readSyslogErr != nil {
				logger.Error("Read SyslogData Err", readSyslogErr)
				return
			}

			sum = sum + len(syslogdata)
			_, _ = b.Write(syslogdata)

			remove_sgn_err := os.Remove(sgn_path)
			if remove_sgn_err != nil {
				logger.Error("Delete %s Fail:", sgn_path, remove_sgn_err)

			}

			arr, orilen, arrlen := r.datahander(b, sum)

			logger.Debug("DbSyslog File Input Lines : %d ,Effective Lines : %d . Cost %s", orilen, arrlen, time.Now().Sub(t))

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

						//r.DbSyslogOutputDataInit(remote_ip, datamap)

						if len(r.config.Input.SyslogFile.Remove_Fields) != 0 {
							//删除字段
							datamap = remove(r.config.Input.SyslogFile.Remove_Fields, datamap)
						}
						datamap["@timestamp"] = time.Now().UTC().Format("2006-01-02T15:04:05.999Z")
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

						logger.Debug("Inputdata %d : Bulk to ES fail")

					} else {

						atomic.AddInt64(&r.rc.OutputCount, int64(datacount))

						logger.Debug("Inputdata %d : Filite And To Es Total Cost %s", datacount, time.Now().Sub(t))
					}

					defer res.Body.Close()

					//<-splitepool

				}()

			}

			return
			//<-readxdrpool
		}()

	}

}

func (r *InputSyslogFile) InputProcess() error {
	go func() {

		for atomic.LoadInt32(r.switcher) > 0 {
			changFileNamech <- 1 //控制多次循环读取相同的文件
			r.GetSyslogdata(r.config.Input.SyslogFile.Dir_path, r.config.Input.SyslogFile.File_suffix)
		}
		logger.Alert("SyslogFile Input Instance %s Close ", r.config.Input.SyslogFile.Dir_path)

		if atomic.LoadInt32(r.switcher) == int32(RouteStat_Off) {

			logger.Alert("SyslogFile Input Instance %s Stop", r.config.Input.SyslogFile.Dir_path)
		}
	}()

	return nil
}

func (r *InputSyslogFile) MainLoop() error {

	if atomic.LoadInt32(r.switcher) == int32(RouteStat_Ready) {
		logger.Alert("SyslogInput Input Start")

		ret := r.InputProcess()
		if ret != nil {
			return ret
		}

		atomic.StoreInt32(r.switcher, int32(RouteStat_On))
	}
	return nil
}
