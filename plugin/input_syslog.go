package plugin

import (
	"bytes"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"sugon.com/WangHongyin/collecter/logger"
)

var Syslog_Udp_InitChan = 100
var SyslogUdplimitChan = make(chan int, Syslog_Udp_InitChan)

type InputSyslog struct {
	InputCommon
	Tcplistener     net.Listener
	Udplistener     *net.UDPConn
	output          FilterInterface
	udp_buffer_max  int
	IpBlackListFlag bool
	IpBlackList     map[string]bool
}

type SyslogBaseInfo struct {
	Facility  string
	Level     string
	Timestamp string
	Hostname  string
	Program   string
	Pid       string
	Message   string
}

var FacilityList = []string{
	"kernel messages",
	"user-level messages",
	"mail system",
	"system daemons",
	"security/authorization messages",
	"messages generated internally by syslogd",
	"line printer subsystem",
	"network news subsystem",
	"UUCP subsystem",
	"clock daemon",
	"security/authorization messages",
	"FTP daemon",
	"NTP subsystem",
	"log audit",
	"log alert",
	"clock daemon",
}

var LevelList = []string{
	"Emergency",
	"Alert",
	"Critical",
	"Error",
	"Warning",
	"Notice",
	"Informational",
	"Debug",
}

var InputSyslogRegInfo RegisterInfo

func (r *InputSyslog) exit() {
	defer r.Tcplistener.Close()
}

func init() {

	v := &InputSyslogRegInfo

	v.Plugin = "Input"
	v.Type = "Syslog"
	v.SubType = ""
	v.Vendor = "sugon.com"
	v.Desc = "for syslog input"
	v.init = RouteInputSyslogInit

	InputRegister(v)

}

func RouteInputSyslogInit(c *YamlCommonData, out interface{}, countptr interface{}, statptr *RouteStatType) interface{} {
	r := new(InputSyslog)

	r.config = c
	r.rc = countptr.(*RouteInputCounts)
	r.reginfo = &InputSyslogRegInfo

	r.output = out.(FilterInterface)

	r.switcher = (*int32)(statptr)

	tc := c.Input.Syslog

	if len(tc.Ipblacklist) != 0 {
		r.IpBlackListFlag = true

		r.IpBlackList = make(map[string]bool)

		for _, v := range tc.Ipblacklist {
			r.IpBlackList[v] = true
		}
	}

	r.udp_buffer_max = SysConfigGet().Udp_buffer_max

	return r
}

func (r *InputSyslog) SyslogOutputDataInit(remote_ip interface{}, o map[string]interface{}) {

	var v Beat_t

	v.Hostname = Hostname
	v.Hostip = LocalIP

	o["beat"] = v

	if r.config.Input.Syslog.Fields != nil {
		o["fields"] = r.config.Input.Syslog.Fields
	}

	if r.config.Input.Syslog.Type != "" {
		o["Type"] = r.config.Input.Syslog.Type
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

				logger.Info("o:", o)

			}
		}
	}

}

func (r *InputSyslog) datahander(b *bytes.Buffer, totalbyte int) ([][]byte, int, int) {

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

func (r *InputSyslog) readConn(conn net.Conn) ([][]byte, int, int) {

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

func SyslogMessageFormatParse(msg string) SyslogBaseInfo {

	var s SyslogBaseInfo
	var p int
	var tag string

	index := strings.Index(msg, ": ")
	if index == -1 {
		return s
	}

	syslog_head := msg[0:index]

	index = strings.LastIndex(syslog_head, " ")

	tag = syslog_head[index:]

	pre := syslog_head[0:index]

	index = strings.LastIndex(pre, " ")

	s.Hostname = pre[index+1:]

	pre = pre[0:index]

	index = strings.Index(pre, ">")

	if index == -1 {
		return s
	}

	pp := pre[1:index]

	//logger.Debug("pp : ", pp)

	p, _ = strconv.Atoi(pp)

	//logger.Debug("p", p)

	s.Timestamp = pre[index+1:]

	//logger.Debug("s.Timestamp: ", s.Timestamp)

	FacilityList_Index := p / 8

	if FacilityList_Index > len(FacilityList) {
		s.Facility = strconv.Itoa(FacilityList_Index)
	} else {

		s.Facility = FacilityList[p/8]
	}

	//logger.Debug("s.Facility", s.Facility)

	s.Level = LevelList[p%8]

	//logger.Debug("s.Level: ", s.Level)

	if strings.Index(tag, "[") == -1 {
		s.Program = tag[1:]
	} else {
		a := strings.Split(tag, "[")
		s.Program = a[0][1:]
		b := strings.Split(a[1], "]")
		s.Pid = b[0]
	}

	index = strings.Index(msg, ": ")
	s.Message = msg[index+2:]

	return s
}

func (r *InputSyslog) SyslogFormatParse(arr [][]byte, remote_ip interface{}) []map[string]interface{} {

	var c []map[string]interface{}

	for _, v := range arr {

		s := SyslogMessageFormatParse(string(v))

		if len(s.Message) == 0 {
			atomic.AddInt64(&r.rc.InvalidCount, 1)
			continue
		}

		m := StructToMap(s)

		//对m["Program"]字段进行判断，数据分发
		if r.InputCommon.config.Input.Syslog.Distribute != nil {
			for key, value := range r.InputCommon.config.Input.Syslog.Distribute {
				//logger.Error(key, m["Program"])
				if key == m["Program"] {
					//分发,地址为value;
					conn, err := net.Dial("udp", value)
					if err != nil {
						logger.Error("Distribute Attack Message Err:", err)
						continue
					}
					defer conn.Close()
					//mjson, _ := json.Marshal(m)
					conn.Write(v)
					logger.Alert("Message:", string(v))
					break
				}
			}
		}

		r.SyslogOutputDataInit(remote_ip, m)

		c = append(c, m)
	}

	return c
}

func (r *InputSyslog) SyslogTcpDataHandler(conn net.Conn) {

	defer conn.Close()

	t := time.Now()

	remoteaddr := conn.RemoteAddr().String()
	st := strings.Split(remoteaddr, ":")
	remote_ip := st[0]

	logger.Info("Recv Data From : ", remote_ip)

	arr, orilen, arrlen := r.readConn(conn)

	if arrlen == 0 {
		return
	}

	c := r.SyslogFormatParse(arr, remote_ip)

	logger.Debug("Syslog TCP Input Lines : %d ,Output Lines : %d . Cost %s", orilen, arrlen, time.Now().Sub(t))

	r.output.FilterProcess(c, arrlen, nil)
}

func (r *InputSyslog) SyslogUdpDataHandler(conn *net.UDPConn) {

	defer PanicHandler()

	t := time.Now()

	data := make([]byte, r.udp_buffer_max)

	read, remoteAddr, err := conn.ReadFromUDP(data)
	if err != nil {
		logger.Error("UDP Read err:", err)
		return
	}

	if read == 0 {
		return
	}

	//logger.Debug("Read Data", string(data[0:read]))

	atomic.AddInt64(&r.rc.OutputCount, 1)

	<-SyslogUdplimitChan

	var arr [][]byte
	arr = append(arr, data[0:read])

	c := r.SyslogFormatParse(arr, remoteAddr.IP)

	logger.Debug("Syslog UDP Input Lines : %d ,Output Lines : %d . Cost %s", 1, 1, time.Now().Sub(t))

	r.output.FilterProcess(c, 1, nil)
}

func (r *InputSyslog) SyslogTcpListen() {

	defer r.Tcplistener.Close()

	logger.Alert("Syslog Input TCP Start")

	for atomic.LoadInt32(r.switcher) > 0 {

		conn, err := r.Tcplistener.Accept()
		if err != nil {
			logger.Warn(err)
			continue
		}

		go r.SyslogTcpDataHandler(conn)
	}

	logger.Alert("Syslog TCP Instance %s Close Listen", r.config.Input.Syslog.Addr)

	if atomic.LoadInt32(r.switcher) == int32(RouteStat_Off) {

		logger.Alert("Syslog TCP Instance %s Stop", r.config.Input.Syslog.Addr)
	}
}

func (r *InputSyslog) UdpListen() {

	logger.Alert("Syslog Input UDP Start")

	defer r.Udplistener.Close()

	for atomic.LoadInt32(r.switcher) > 0 {

		SyslogUdplimitChan <- 1
		go r.SyslogUdpDataHandler(r.Udplistener)
	}

	logger.Alert("Syslog UDP Instance %s Close Listen", r.config.Input.Syslog.Addr)

	if atomic.LoadInt32(r.switcher) == int32(RouteStat_Off) {

		logger.Alert("Syslog UDP Instance %s Stop", r.config.Input.Syslog.Addr)
	}

}

func (r *InputSyslog) InputProcess(input interface{}) error {

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

	go r.UdpListen()

	return nil
}

func (r *InputSyslog) MainLoop() error {

	if atomic.LoadInt32(r.switcher) == int32(RouteStat_Ready) {
		logger.Alert("Syslog Input Instance %s Start", r.config.Input.Syslog.Addr)

		ret := r.InputProcess(r.config.Input.Syslog.Addr)
		if ret != nil {
			return ret
		}

		atomic.StoreInt32(r.switcher, int32(RouteStat_On))
	}

	return nil
}
