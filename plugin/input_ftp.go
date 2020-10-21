package plugin

import (
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"sugon.com/WangHongyin/collecter/logger"
	server "sugon.com/WangHongyin/collecter/plugin/ftpserver"
)

type InputFtp struct {
	InputCommon
	ip        string
	port      int
	ftpserver *server.Server
	output    FilterInterface
}

var InputFtpRegInfo RegisterInfo

func init() {
	v := &InputFtpRegInfo

	v.Plugin = "Input"
	v.Type = "Ftp"
	v.SubType = ""
	v.Vendor = "sugon.com"
	v.Desc = "for ftp input"
	v.init = RouteInputFtpInit

	InputRegister(v)
}

func InputFtpExit() {
	//server.Ftp_Server_Exit()
}

func (r *InputFtp) exit() {
	r.ftpserver.Shutdown()
}

func RouteInputFtpInit(c *YamlCommonData, out interface{}, countptr interface{}, statptr *RouteStatType) interface{} {
	r := new(InputFtp)

	r.config = c
	r.rc = countptr.(*RouteInputCounts)
	r.reginfo = &FilterSplitRegInfo

	r.output = out.(FilterInterface)

	r.switcher = (*int32)(statptr)

	split_item := strings.Split(r.config.Input.Ftp.Addr, ":")
	if split_item != nil {
		if split_item[0] != "" {
			r.ip = split_item[0]
		} else {
			r.ip = "0.0.0.0"
		}
		if split_item[1] != "" {
			r.port, _ = strconv.Atoi(split_item[1])
		} else {
			r.port = SysConfigGet().Ftp_default_port
		}

		if r.config.Input.Ftp.Active_port != 0 {
			active_port := r.config.Input.Ftp.Active_port

			logger.Info("FTP Active Mode Port :", active_port)
			server.Setup_Local_FTP_Data_Port(active_port)

		}
	}

	return r
}

func (r *InputFtp) FtpOutputDataInit(name string, filelen int64) map[string]interface{} {

	var v Beat_t

	o := make(map[string]interface{})

	v.Hostname = Hostname
	v.Hostip = LocalIP

	o["beat"] = v

	if r.config.Input.Ftp.Fields != nil {
		o["fields"] = r.config.Input.Ftp.Fields
	}

	if r.config.Input.Ftp.Type != "" {
		o["Type"] = r.config.Input.Ftp.Type
	}

	o["@timestamp"] = time.Now().UTC().Format("2006-01-02T15:04:05.999Z")

	o["Source"] = r.config.Input.Ftp.Addr

	o["Filename"] = name
	o["Filelen"] = filelen

	return o
}

func (r *InputFtp) FtpDataHandler(name string, b []byte, num int64) error {

	if b == nil {
		atomic.AddInt64(&r.rc.InvalidCount, 1)
		return nil
	}

	name = filepath.Base(name)

	logger.Info("Target File :", name)

	tags := r.FtpOutputDataInit(name, num)

	go r.output.FilterProcess(b, 1, tags)

	atomic.AddInt64(&r.rc.OutputCount, 1)

	return nil
}

func (r *InputFtp) FtpMainLoop() {

	server := r.ftpserver

	for atomic.LoadInt32(r.switcher) == int32(RouteStat_On) {

		server.ServeOnce()
		atomic.AddInt64(&r.rc.InputCount, 1)
	}

	server.Shutdown()

	logger.Alert("Ftp Input Instance %s : Close Listen", r.config.Input.Ftp.Addr)
}

func (r *InputFtp) InputProcess() error {

	f := r.config.Input.Ftp

	factory := &server.FlowDriverFactory{
		FunCallBack: r.FtpDataHandler,
	}

	opts := &server.ServerOpts{
		Factory:  factory,
		Port:     r.port,
		Hostname: r.ip,
		Auth:     &server.SimpleAuth{Name: f.User, Password: f.Pass},
	}

	logger.Info("Starting ftp server on %v:%v", opts.Hostname, opts.Port)

	r.ftpserver = server.NewServer(opts)

	err := r.ftpserver.Listen()
	if err != nil {
		logger.Error("Error starting server:", err)
		return err
	}

	go r.FtpMainLoop()

	return nil
}

func (r *InputFtp) MainLoop() error {

	if atomic.LoadInt32(r.switcher) == int32(RouteStat_Ready) {
		logger.Alert("Ftp Input Instance %s Start", r.config.Input.Ftp.Addr)

		ret := r.InputProcess()
		if ret != nil {
			return ret
		}

		atomic.StoreInt32(r.switcher, int32(RouteStat_On))
	}

	return nil
}
