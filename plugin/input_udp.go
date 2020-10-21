package plugin

import (
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"sugon.com/WangHongyin/collecter/logger"
)

var Udp_InitChan = 1000

var InputUdplimitChan = make(chan int, Udp_InitChan)

var udp_buffer_max int

type InputUdp struct {
	InputCommon
	listener *net.UDPConn
	output   FilterInterface
}

var InputUdpRegInfo RegisterInfo

func (r *InputUdp) exit() {
	defer r.listener.Close()
}

func init() {

	v := &InputUdpRegInfo

	v.Plugin = "Input"
	v.Type = "Udp"
	v.SubType = ""
	v.Vendor = "sugon.com"
	v.Desc = "for udp input"
	v.init = RouteInputUdpInit

	InputRegister(v)

	udp_buffer_max = SysConfigGet().Udp_buffer_max

}

func RouteInputUdpInit(c *YamlCommonData, out interface{}, countptr interface{}, statptr *RouteStatType) interface{} {
	r := new(InputUdp)

	r.config = c
	r.rc = countptr.(*RouteInputCounts)
	r.reginfo = &InputUdpRegInfo

	r.output = out.(FilterInterface)

	r.switcher = (*int32)(statptr)

	logger.Info("Udp Buffer : ", udp_buffer_max)

	return r
}

func (r *InputUdp) UdpDataHandler(conn *net.UDPConn) {

	t := time.Now()

	data := make([]byte, udp_buffer_max)

	read, remoteAddr, err := conn.ReadFromUDP(data)
	if err != nil {
		fmt.Println("UDP Read err:", err)
		return
	}
	atomic.AddInt64(&r.rc.OutputCount, 1)

	<-InputUdplimitChan

	logger.Debug("Udp-P Cost %s", time.Now().Sub(t))

	tags := make(map[string]interface{})

	tags["@timestamp"] = time.Now().UTC().Format("2006-01-02T15:04:05.999Z")
	tags["_RemoteIp"] = remoteAddr.IP

	r.output.FilterProcess(data[0:read], 1, tags)

	logger.Debug("Udp-P Total Cost %s", time.Now().Sub(t))
}

func (r *InputUdp) UdpListen() {

	defer r.listener.Close()

	for atomic.LoadInt32(r.switcher) > 0 {

		InputUdplimitChan <- 1
		go r.UdpDataHandler(r.listener)
	}

	logger.Alert("Udp Input Instance %s Close Listen", r.config.Input.Udp.Addr)

	if atomic.LoadInt32(r.switcher) == int32(RouteStat_Off) {

		logger.Alert("Udp Input Instance %s Stop", r.config.Input.Udp.Addr)
	}

}

func (r *InputUdp) InputProcess(input interface{}) error {

	ipaddr := input.(string)

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

	r.listener = conn

	go r.UdpListen()

	return nil
}

func (r *InputUdp) MainLoop() error {

	if atomic.LoadInt32(r.switcher) == int32(RouteStat_Ready) {
		logger.Alert("Udp Input Instance %s Start", r.config.Input.Udp.Addr)

		ret := r.InputProcess(r.config.Input.Udp.Addr)
		if ret != nil {
			return ret
		}

		atomic.StoreInt32(r.switcher, int32(RouteStat_On))
	}

	return nil
}
