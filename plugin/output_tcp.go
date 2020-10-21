package plugin

import (
	"bytes"
	"net"
	"sync/atomic"
	"time"

	"sugon.com/WangHongyin/collecter/logger"
)

type OutputTcpCount struct {
	OutputAddr string
	InputLines int
	TcpSend    int
}

type OutputTcp struct {
	OutputCommon
	Count       OutputTcpCount
	msg         *[]interface{}
	bufferlines [][2]int
}

var OutputTcpRegInfo RegisterInfo

func init() {
	OutputTcpRegInfo.Plugin = "Output"
	OutputTcpRegInfo.Type = "Tcp"
	OutputTcpRegInfo.SubType = ""
	OutputTcpRegInfo.Vendor = "sugon.com"
	OutputTcpRegInfo.Desc = "for tcp output"
	OutputTcpRegInfo.init = OutputTcpRouteInit

	OutputRegister(&OutputTcpRegInfo)
}

func OutputTcpRouteInit(c *YamlCommonData, out interface{}, countptr interface{}, statptr *RouteStatType) interface{} {

	r := new(OutputTcp)

	r.config = c
	r.rc = countptr.(*RouteCount)
	r.reginfo = &OutputTcpRegInfo

	return r
}

func (r *OutputTcp) OutputProcessTcpSender(Ipv4Addr string, msg []interface{}, p chan<- int) {
	//t := time.Now()

	message_send_target := len(msg)

	con, err := net.Dial("tcp", Ipv4Addr)
	if err != nil {
		logger.Error("Con error:", err)
		atomic.AddInt64(&r.rc.ErrCount, int64(message_send_target))
		p <- 0
		return
	}
	defer con.Close()

	buf := bytes.NewBuffer(nil)

	for _, v := range msg {

		b := Json_Marshal(v)

		buf.Write(b)
		buf.Write(return_flag)
	}

	_, err = con.Write(buf.Bytes())
	if err != nil {
		logger.Error("Tcp Write Err:", err)
		atomic.AddInt64(&r.rc.ErrCount, int64(message_send_target))
		message_send_target = 0
	}

	//logger.Info("TcpSend :", Ipv4Addr+"\tLines:", message_send_target, "\t", time.Now().Sub(t))

	p <- message_send_target
}

func (r *OutputTcp) OutputProcessTcpDataPrepare(Ipv4Addr string, p chan<- int) {

	data := *r.msg

	ch := make(chan int)

	num := 0

	for k, v := range r.bufferlines {

		num = k

		go r.OutputProcessTcpSender(Ipv4Addr, data[v[0]:v[1]], ch)
	}

	sum := 0
	for i := 0; i <= num; i++ {
		deal_lines := <-ch
		sum += deal_lines
	}

	p <- sum
}

func (r *OutputTcp) OutputProcess(msg interface{}, msg_num int) {
	t := time.Now()

	d := msg.([]interface{})

	r.msg = &d

	r.Count.InputLines = msg_num

	atomic.AddInt64(&r.rc.InputCount, int64(r.Count.InputLines))

	tcp := r.config.Output.Tcp

	block := 0

	if tcp.Block != 0 {
		block = tcp.Block
	} else {
		block = r.Count.InputLines
	}

	r.bufferlines = DataBulkBufferInit(r.Count.InputLines, block)

	ch := make(chan int)

	num := 0
	for k, v := range tcp.Host {

		num = k
		go r.OutputProcessTcpDataPrepare(v, ch)
	}

	sum := 0
	for i := 0; i <= num; i++ {
		deal_lines := <-ch
		sum += deal_lines
	}

	r.Count.TcpSend = sum

	atomic.AddInt64(&r.rc.OutputCount, int64(sum))

	logger.Debug("Output Tcp lines : %d : %s", r.Count.TcpSend, time.Now().Sub(t))
}
