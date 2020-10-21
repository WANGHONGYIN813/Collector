package plugin

import (
	"bytes"
	//"fmt"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"sugon.com/WangHongyin/collecter/logger"
)

var buffer_max int

//var tcpconnpool chan int

type InputTcp struct {
	InputCommon
	listener        net.Listener
	output          FilterInterface
	IpBlackListFlag bool
	Lines_merge     bool
	IpBlackList     map[string]bool
	DataHandler     func(conn net.Conn)
}

var InputTcpRegInfo RegisterInfo

func (r *InputTcp) exit() {
	defer r.listener.Close()
}

func init() {

	v := &InputTcpRegInfo

	v.Plugin = "Input"
	v.Type = "Tcp"
	v.SubType = ""
	v.Vendor = "sugon.com"
	v.Desc = "for tcp input"
	v.init = RouteInputTcpInit

	InputRegister(v)

	buffer_max = SysConfigGet().Tcp_buffer_max

	//tcpconnpool = make(chan int, *tcpconnpoolnum)
}

func RouteInputTcpInit(c *YamlCommonData, out interface{}, countptr interface{}, statptr *RouteStatType) interface{} {
	r := new(InputTcp)

	r.config = c
	r.rc = countptr.(*RouteInputCounts)
	r.reginfo = &InputTcpRegInfo

	r.Lines_merge = false

	if c.Input.Tcp.Lines_merge {
		r.Lines_merge = true
	}

	r.output = out.(FilterInterface)

	r.switcher = (*int32)(statptr)

	tc := c.Input.Tcp

	if len(tc.Ipblacklist) != 0 {
		r.IpBlackListFlag = true

		r.IpBlackList = make(map[string]bool)

		for _, v := range tc.Ipblacklist {
			r.IpBlackList[v] = true
		}
	}

	if tc.Performance_mode {
		r.DataHandler = r.TcpDataHandler_PerformanceMode
	} else {
		r.DataHandler = r.TcpDataHandler
	}

	logger.Info("Tcp Buffer : ", buffer_max)

	return r
}

func (r *InputTcp) TcpOutputDataInit() map[string]interface{} {

	o := make(map[string]interface{})

	if r.config.Input.Tcp.Beat_info {

		var v Beat_t
		v.Hostname = Hostname
		v.Hostip = LocalIP

		o["beat"] = v
	}

	if r.config.Input.Tcp.Fields != nil {
		o["fields"] = r.config.Input.Tcp.Fields
	}

	if r.config.Input.Tcp.Source_info {
		o["Source"] = r.config.Input.Tcp.Addr
	}

	if r.config.Input.Tcp.Type != "" {
		o["Type"] = r.config.Input.Tcp.Type
	}

	return o
}

func (r *InputTcp) datahander(b *bytes.Buffer, totalbyte int) ([][]byte, int, int) {

	flag := byte('\n')

	var arr [][]byte

	OriLines := 0

	if r.Lines_merge {

		arr = append(arr, b.Bytes())

		atomic.AddInt64(&r.rc.InputCount, 1)
		return arr, 1, len(arr)
	}

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

func (r *InputTcp) readConn(conn net.Conn) ([][]byte, int, int) {

	b := bytes.NewBuffer(nil)

	sum := 0

	for {
		data := make([]byte, buffer_max) //初始化一个元素个数为buffer_max切片,元素初始值为0

		readnumber, err := conn.Read(data) //从连接中读数据到data数组中
		if err != nil {
			logger.Trace("Connect Close")
			break
		}

		//logger.Info("Read data is ", string(data))

		sum += readnumber                  //该次连接中读取到的字节数
		_, _ = b.Write(data[0:readnumber]) //写入缓冲区
	}

	arr, orilen, arrlen := r.datahander(b, sum) //tcp conn关闭后进行数据处理

	atomic.AddInt64(&r.rc.OutputCount, int64(arrlen))

	return arr, orilen, arrlen
}

func (r *InputTcp) readConn_PerformanceMode(conn net.Conn) ([]byte, int) {

	b := bytes.NewBuffer(nil)

	for {
		data := make([]byte, buffer_max)

		readnumber, err := conn.Read(data)
		if err != nil {
			logger.Trace("Connect Close")
			break
		}

		//logger.Info("Get Data:", string(data))

		_, _ = b.Write(data[0:readnumber])
	}

	atomic.AddInt64(&r.rc.OutputCount, 1)

	return b.Bytes(), 1
}

func (r *InputTcp) TcpDataHandler_PerformanceMode(conn net.Conn) {

	defer PanicHandler()

	defer conn.Close()

	t := time.Now()

	arr, _ := r.readConn_PerformanceMode(conn)

	if len(arr) == 0 {
		return
	}

	logger.Debug("Tcp-P Cost %s", time.Now().Sub(t))

	r.output.FilterProcess(arr, -1, nil)

	logger.Debug("Tcp-P Total Cost %s", time.Now().Sub(t))

	//<-tcpconnpool

}

func (r *InputTcp) TcpDataHandler(conn net.Conn) {

	defer PanicHandler()

	defer conn.Close()
	t := time.Now()

	remoteaddr := conn.RemoteAddr().String()
	st := strings.Split(remoteaddr, ":")
	remote_ip := st[0]

	logger.Debug("Recv Data From : ", remote_ip)

	if r.IpBlackListFlag {
		if r.IpBlackList[remote_ip] {
			atomic.AddInt64(&r.rc.InputFilterNumbers, 1) //过滤加入黑名单的IP
			logger.Trace("Match IPBlackList :%s", remote_ip)
			return
		}
	}

	arr, orilen, arrlen := r.readConn(conn) //从连接里读数据

	if arrlen == 0 {
		return
	}

	tags := r.TcpOutputDataInit()

	tags["_RemoteIp"] = remote_ip
	tags["@timestamp"] = time.Now().UTC().Format("2006-01-02T15:04:05.999Z")

	logger.Debug("Tcp InLine : %d  OutLine : %d Cost %s", orilen, arrlen, time.Now().Sub(t))

	r.output.FilterProcess(arr, arrlen, tags) //arr是一个二维数组（字节数组的数组）

	logger.Debug("Input Tcp Total Cost %s", time.Now().Sub(t))

	//<-tcpconnpool
}

func (r *InputTcp) TcpListen() {

	defer r.listener.Close()

	for atomic.LoadInt32(r.switcher) > 0 {

		//tcpconnpool <- 1

		//logger.Alert("Current  TcpConn Routine Pool Left :", *tcpconnpoolnum-len(tcpconnpool))

		conn, err := r.listener.Accept() //等待TCP连接

		logger.Alert("Get %s Tcp  Conn Time : %s", r.InputCommon.config.configfilename, time.Now().UTC().Format("2006-01-02T15:04:05.999Z"))

		if err != nil {
			logger.Warn(err)
			continue //继续下一次循环
		}

		go r.DataHandler(conn)
	}

	logger.Alert("Tcp Input Instance %s Close Listen", r.config.Input.Tcp.Addr)

	if atomic.LoadInt32(r.switcher) == int32(RouteStat_Off) {

		logger.Alert("Tcp Input Instance %s Stop", r.config.Input.Tcp.Addr)
	}

}

func (r *InputTcp) InputProcess(input interface{}) error {

	ipaddr := input.(string)

	listener, err := net.Listen("tcp", ipaddr)
	if err != nil {
		logger.Alert(err)
		return err
	}

	r.listener = listener

	go r.TcpListen()

	return nil
}

func (r *InputTcp) MainLoop() error {

	if atomic.LoadInt32(r.switcher) == int32(RouteStat_Ready) {
		logger.Alert("Tcp Input Instance %s Start", r.config.Input.Tcp.Addr)

		ret := r.InputProcess(r.config.Input.Tcp.Addr)
		if ret != nil {
			return ret
		}

		atomic.StoreInt32(r.switcher, int32(RouteStat_On))
	}

	return nil
}
