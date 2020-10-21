package plugin

import (
	"fmt"
	"reflect"
	"strconv"
	"sync/atomic"
	"time"

	g "github.com/soniah/gosnmp"
	"sugon.com/WangHongyin/collecter/logger"
)

var Snmpwalk_interval_default int

type SnmpwalkCollect struct {
	InputCommon
	CurrentName      string
	MacAddrFlag      bool
	CurrentNameIndex int
	Ret              map[string]interface{}
}

type InputSnmpwalk struct {
	InputCommon
	output   FilterInterface
	MibOids  map[string]string
	interval int
}

var InputSnmpwalkRegInfo RegisterInfo

func init() {

	v := &InputSnmpwalkRegInfo

	v.Plugin = "Input"
	v.Type = "Snmpwalk"
	v.SubType = ""
	v.Vendor = "sugon.com"
	v.Desc = "for snmpwalk input"
	v.init = RouteInputSnmpwalkInit

	InputRegister(v)

	Snmpwalk_interval_default = SysConfigGet().Snmpwalk_interval_default
}

func RouteInputSnmpwalkInit(c *YamlCommonData, out interface{}, countptr interface{}, statptr *RouteStatType) interface{} {
	r := new(InputSnmpwalk)

	r.config = c
	r.rc = countptr.(*RouteInputCounts)
	r.reginfo = &InputSnmpwalkRegInfo

	r.output = out.(FilterInterface)

	sc := r.config.Input.Snmpwalk

	r.switcher = (*int32)(statptr)

	if sc.Interval == 0 {
		r.interval = Snmpwalk_interval_default
	} else {
		r.interval = sc.Interval
	}

	return r
}

func (r *InputSnmpwalk) exit() {
}

func (r *InputSnmpwalk) SnmpwalkOutputDataInit(ip string) map[string]interface{} {

	var v Beat_t

	o := make(map[string]interface{})

	v.Hostname = Hostname
	v.Hostip = LocalIP

	o["beat"] = v

	o["Source"] = ip

	o["CollectTime"] = time.Now().Local().Format("2006-01-02T15:04:05")
	o["@timestamp"] = time.Now().UTC().Format("2006-01-02T15:04:05.999Z")

	if r.config.Input.Snmpwalk.Fields != nil {
		o["fields"] = r.config.Input.Snmpwalk.Fields
	}

	if r.config.Input.Snmpwalk.Type != "" {
		o["Type"] = r.config.Input.Snmpwalk.Type
	}

	return o
}

var mibs_cmd_fmt = "snmptranslate -On -IR %s "
var IDENTIFIER_get_cmd = "snmptranslate -Ta %s | cut -d \":\" -f3"

func MibsToOidsMap(Oidnamelist []string) (map[string]string, error) {

	m := make(map[string]string)

	for _, v := range Oidnamelist {

		cmd := fmt.Sprintf(mibs_cmd_fmt, v)

		ret, err := ExecShellCommand(cmd)
		if err != nil {
			return nil, err
		}
		m[v] = ret
	}

	return m, nil
}
func (r *SnmpwalkCollect) fn(pdu g.SnmpPDU) error {

	var i int
	var s string

	string_flag := false

	switch pdu.Type {
	case g.OctetString:
		string_flag = true

		if r.MacAddrFlag {
			s = GetIPaddr(pdu.Value.([]byte))

		} else {
			switch pdu.Value.(type) {
			case string:

				s = pdu.Value.(string)
			case []uint8:

				p := pdu.Value.([]byte)
				s = string(p)
			}

		}

	case g.ObjectIdentifier:
		string_flag = true

		s = pdu.Value.(string)

		cmd := fmt.Sprintf(IDENTIFIER_get_cmd, s)

		ret, err := ExecShellCommand(cmd)
		if err != nil {
			fmt.Println("Err : ", err)
		}

		s = ret

	case g.Integer:
		i = pdu.Value.(int)

	case g.Counter32:
		i = int(pdu.Value.(uint))
	case g.IPAddress:
		string_flag = true
		s = pdu.Value.(string)
	}

	if string_flag {

		if r.Ret[r.CurrentName] != nil {

			var a []string

			if r.CurrentNameIndex == 1 {

				a = append(a, r.Ret[r.CurrentName].(string))
			} else {
				a = append(a, r.Ret[r.CurrentName].([]string)...)
			}

			a = append(a, s)

			r.Ret[r.CurrentName] = a

			r.CurrentNameIndex += 1

		} else {
			r.Ret[r.CurrentName] = s
			r.CurrentNameIndex = 1
		}

	} else {

		if r.Ret[r.CurrentName] != nil {

			var a []int

			if r.CurrentNameIndex == 1 {

				a = append(a, r.Ret[r.CurrentName].(int))
			} else {
				a = append(a, r.Ret[r.CurrentName].([]int)...)
			}

			a = append(a, i)

			r.Ret[r.CurrentName] = a

			r.CurrentNameIndex += 1

		} else {
			r.Ret[r.CurrentName] = i
			r.CurrentNameIndex = 1
		}
	}

	return nil
}

func NewSnmpwalkCollect(c *YamlCommonData) *SnmpwalkCollect {
	r := new(SnmpwalkCollect)

	r.config = c

	return r
}

func (r *InputSnmpwalk) SnmpWalkDataHandler(ip string) int {

	g.Default.Target = ip

	err := g.Default.Connect()
	if err != nil {
		atomic.AddInt64(&r.rc.ErrCount, 1)
		logger.Error("Connect() err: %v", err)
		return 0
	}
	defer g.Default.Conn.Close()

	data := NewSnmpwalkCollect(r.config)

	data.Ret = make(map[string]interface{})

	for k, v := range r.MibOids {

		data.CurrentName = k

		if k == "ifPhysAddress" {
			data.MacAddrFlag = true
		}

		err := g.Default.BulkWalk(v, data.fn)
		if err != nil {
			fmt.Printf("Walk() err: %v", err)
			atomic.AddInt64(&r.rc.ErrCount, 1)
			return 0
		}

		data.MacAddrFlag = false
	}

	atomic.AddInt64(&r.rc.InputCount, 1)

	//fmt.Println("data.Ret:", data.Ret)

	Hostmassagebuffer := Hostmassage

	alarmmessage := make(map[string]interface{})

	if Hostmassagebuffer != nil {
		for _, v := range Hostmassagebuffer {
			if v["assetIp"] == ip {
				data.Ret["assetId"] = v["assetId"]
				data.Ret["assetType"] = v["assetType"]
				data.Ret["assetCategory"] = v["assetCategory"]
				data.Ret["assetName"] = v["assetName"]
			}
		}
	}

	data.Ret["Cpu_percent"] = 100 - data.Ret["ssCpuIdle"].(int)                                                                                                                                                                          // cpu利用率
	data.Ret["Memory_percent"] = ((data.Ret["memTotalReal"].(int) - data.Ret["memTotalFree"].(int) - data.Ret["memShared"].(int) - data.Ret["memCached"].(int) + data.Ret["memAvailSwap"].(int)) * 100) / data.Ret["memTotalReal"].(int) //内存利用率

	fmt.Println("Memory_percent:", data.Ret["Memory_percent"]) //test

	var hrStorageUsedSum int64
	t1 := reflect.ValueOf(data.Ret["hrStorageUsed"])
	for i := 0; i < t1.Len(); i++ {
		hrStorageUsedSum = hrStorageUsedSum + t1.Index(i).Int()
	}
	fmt.Println("hrStorageUsedSum:", hrStorageUsedSum) //test

	var hrStorageSizeSum int64
	t2 := reflect.ValueOf(data.Ret["hrStorageSize"])
	for i := 0; i < t2.Len(); i++ {
		hrStorageSizeSum = hrStorageSizeSum + t2.Index(i).Int()
	}
	fmt.Println("hrStorageSizeSum:", hrStorageSizeSum) //test

	data.Ret["Disk_percent"] = (hrStorageUsedSum * 100) / hrStorageSizeSum // 磁盘利用率

	Thresholdmassagebuffer := Thresholdmassage

	if Thresholdmassagebuffer != nil {
		for _, v := range Thresholdmassagebuffer {
			if v["alarmType"] == "cpu" {
				if v["state"] != "off" {

					if v["judgeCondition"] == ">" {
						IntthresholdValue, _ := strconv.Atoi(v["thresholdValue"].(string))
						if data.Ret["Cpu_percent"].(int) > IntthresholdValue {
							alarmmessage["Cpu_percent"] = data.Ret["Cpu_percent"]
							alarmmessage["Cpu_level"] = v["alarmLevel"]
							alarmmessage["Cpu_alarmname"] = v["alarmName"]
							alarmmessage["Cpu_alarmdesc"] = v["alarmDescribe"]
							alarmmessage["Cpu_ThresholdValue"] = v["thresholdValue"]
						}
					}
				}
			}
			if v["alarmType"] == "mem" {
				if v["stat"] != "off" {
					if v["judgeCondition"] == ">" {
						IntthresholdValue, _ := strconv.Atoi(v["thresholdValue"].(string))
						if data.Ret["Memory_percent"].(int) > IntthresholdValue {
							alarmmessage["Memory_level"] = v["alarmLevel"]
							alarmmessage["Memory_alarmname"] = v["alarmName"]
							alarmmessage["Memory_alarmdesc"] = v["alarmDescribe"]
							alarmmessage["Memory_percent"] = data.Ret["Memory_percent"]
							alarmmessage["Memory_ThresholdValue"] = v["thresholdValue"]
						}
					}
				}
			}
			if v["alarmType"] == "disk" {
				if v["stat"] != "off" {
					if v["judgeCondition"] == ">" {
						IntthresholdValue, _ := strconv.ParseInt(v["thresholdValue"].(string), 10, 64)
						if data.Ret["Disk_percent"].(int64) > IntthresholdValue {
							alarmmessage["Disk_level"] = v["alarmLevel"]
							alarmmessage["Disk_alarmname"] = v["alarmName"]
							alarmmessage["Disk_alarmdesc"] = v["alarmDescribe"]
							alarmmessage["Disk_percent"] = data.Ret["Disk_percent"]
							alarmmessage["Disk_ThresholdValue"] = v["thresholdValue"]
						}
					}
				}
			}

		}
	}

	data.Ret["data_type"] = "normal"

	r.output.FilterProcess(data.Ret, 1, r.SnmpwalkOutputDataInit(ip))

	if len(alarmmessage) != 0 {

		if Hostmassagebuffer != nil {
			for _, v := range Hostmassagebuffer {
				if v["assetIp"] == ip {
					alarmmessage["assetId"] = v["assetId"]
					alarmmessage["assetType"] = v["assetType"]
					alarmmessage["assetCategory"] = v["assetCategory"]
					alarmmessage["assetName"] = v["assetName"]
				}
			}
		}

		alarmmessage["data_type"] = "alarm"
		r.output.FilterProcess(alarmmessage, 1, r.SnmpwalkOutputDataInit(ip))
	}

	return 1
}

func (r *InputSnmpwalk) SnmpWalkMainloop() {

	//Hostlist := r.config.Input.Snmpwalk.Host //修改，动态配置端口

	var Hostlist []string

	for atomic.LoadInt32(r.switcher) > 0 {
		Hostlist = HostList

		// if Hostmassage != nil {
		// 	for _, v := range Hostmassage {
		// 		host := v["assetIp"].(string)

		// 		for _, value := range Hostlist {
		// 			if value == v["assetIp"] {
		// 				break
		// 			}

		// 		}

		// 		Hostlist = append(Hostlist, host)
		// 	}
		// }
		if Hostlist == nil {
			continue
		}

		t := time.Now()

		sum := 0

		for _, v := range Hostlist {

			sum += r.SnmpWalkDataHandler(v)
		}

		atomic.AddInt64(&r.rc.OutputCount, int64(sum))

		logger.Info("Snmpwalk Output Lines : %d . Cost %s", sum, time.Now().Sub(t))

		if atomic.LoadInt32(r.switcher) == 0 {
			break
		}

		time.Sleep(time.Duration(r.interval) * time.Second)

		if r == nil {
			break
		}
	}

	logger.Alert("Snmpwalk Input Instance", Hostlist, " Stop")
}


func (r *InputSnmpwalk) InputProcess() error {

	m, err := MibsToOidsMap(r.config.Input.Snmpwalk.Oidnamelist)
	if err != nil {
		logger.Error("MibsToOidsMap Err : ", err)
		return err
	}

	r.MibOids = m

	go r.SnmpWalkMainloop()

	return nil
}

func (r *InputSnmpwalk) MainLoop() error {

	if atomic.LoadInt32(r.switcher) == int32(RouteStat_Ready) {
		logger.Alert("Snmpwalk Input Instance ", r.config.Input.Snmpwalk.Host, " Start")

		ret := r.InputProcess()
		if ret != nil {
			return ret
		}

		atomic.StoreInt32(r.switcher, int32(RouteStat_On))
	}

	return nil
}
