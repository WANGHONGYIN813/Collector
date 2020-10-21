package plugin

import (
	"os"
	"strings"
	"sync/atomic"
	"time"

	"sugon.com/WangHongyin/collecter/logger"
)

type FileState struct {
	Fd        *os.File
	Filename  string
	Offset    int64
	CurInode  string
	cmd       string
	Read_mode string
}

type InputFile struct {
	InputCommon
	FileMap   map[string]*FileState
	Inode_cmd string
	Read_mode string
	Interval  int
	output    FilterInterface
}

var inode_cmd string = "stat -c %i "

var InputFileRegInfo RegisterInfo

func (r *InputFile) exit() {
}

func init() {

	v := &InputFileRegInfo

	v.Plugin = "Input"
	v.Type = "File"
	v.SubType = ""
	v.Vendor = "sugon.com"
	v.Desc = "for file input. it aims to replace filebeat"
	v.init = RouteInputFileInit

	InputRegister(v)
}

func RouteInputFileInit(c *YamlCommonData, out interface{}, countptr interface{}, statptr *RouteStatType) interface{} {
	r := new(InputFile)

	r.config = c
	r.rc = countptr.(*RouteInputCounts)
	r.reginfo = &InputFileRegInfo

	r.output = out.(FilterInterface)

	r.switcher = (*int32)(statptr)

	tc := c.Input.File

	logger.Info("File Read Mode:", tc.Read_mode)

	r.Read_mode = tc.Read_mode
	r.Interval = tc.Interval

	r.FileMap = make(map[string]*FileState)

	if r.Interval <= 0 {
		r.Interval = SysConfigGet().File_listen_interval
	}

	return r
}

func NewFileState(filename string, read_mode string) (*FileState, error) {

	r := new(FileState)

	r.Filename = filename

	r.cmd = inode_cmd + filename

	r.Read_mode = read_mode

	err := r.FileStateInit()

	return r, err
}

func (this *FileState) FileStateInit() error {

	var err error

	this.Fd, err = os.Open(this.Filename)
	if err != nil {
		logger.Error("Open File Error:", err)
		return err
	}

	this.CurInode, err = Exec_Shell(this.cmd)

	if this.Read_mode == "end" {
		this.Offset, err = this.Fd.Seek(0, os.SEEK_END)
	} else if this.Read_mode == "set" {
		this.Offset, err = this.Fd.Seek(0, os.SEEK_SET)
	} else {
		this.Offset, err = this.Fd.Seek(0, os.SEEK_CUR)
	}

	if err != nil {
		logger.Error("File Seek Error:", err)
		return err
	}

	if this.Offset > 0 {
		this.Offset -= 1
	}

	logger.Info(this.Filename, " Start Offset :", this.Offset)

	return err
}

func (this *FileState) FileInodeCheck() error {

	var err error

	inode, _ := Exec_Shell(this.cmd)

	if inode != this.CurInode {
		logger.Alert("Inode Change, New Inode:", inode)

		this.Fd.Close()

		this.Offset = 0

		this.CurInode = inode

		this.Fd, err = os.Open(this.Filename)
		if err != nil {
			logger.Error("Open File Error:", err)
			return err
		}
	}

	return nil
}

func (r *FileState) ReadNewDate() []byte {

	end, _ := r.Fd.Seek(0, os.SEEK_END)

	if end > 0 {
		end -= 1
	}

	if end > r.Offset {

		logger.Debug(r.Filename, " CurOffset :", r.Offset, "Old Offset :", end)
		r.Fd.Seek(0, os.SEEK_CUR)

		buf := make([]byte, end-r.Offset)

		r.Fd.ReadAt(buf, r.Offset)

		r.Offset = end

		return buf

	} else if r.Offset > end {

		logger.Debug(r.Filename, " CurOffset :", r.Offset, "New Offset :", end)
		r.Fd.Seek(0, os.SEEK_SET)

		buf := make([]byte, end)

		r.Fd.ReadAt(buf, 0)

		r.Offset = end

		return buf
	}

	return nil
}

func (r *InputFile) FileOutputDataInit(filename string) map[string]interface{} {

	o := make(map[string]interface{})

	fc := r.config.Input.File

	o["@timestamp"] = time.Now().UTC().Format("2006-01-02T15:04:05.999Z")

	o["_Source"] = filename

	if fc.Beat_info {

		var v Beat_t
		v.Hostname = Hostname
		v.Hostip = LocalIP

		o["beat"] = v
	}

	if fc.Fields != nil {
		o["fields"] = r.config.Input.File.Fields
	}

	if fc.Type != "" {
		o["_Type"] = r.config.Input.File.Type
	}

	date := Search_date_from_string(filename)

	if date == "" {
		date = time.Now().Format("2006-01-02")
	}

	o["_Date"] = date

	if len(fc.Indexvar) != 0 {

		var tmp []string

		for _, v := range fc.Indexvar {

			if _, ok := o[v]; ok {
				tmp = append(tmp, o[v].(string))

			}
		}

		o["Indexvar"] = strings.Join(tmp, "_")
	}

	return o
}

func (r *InputFile) FileDataHandler(fs *FileState) {

	err := fs.FileInodeCheck()

	if err == nil {

		logger.Debug("Scan File %s", fs.Filename)

		ret := fs.ReadNewDate()

		datalen := len(ret)

		if datalen != 0 {

			tags := r.FileOutputDataInit(fs.Filename)

			arr, arrlen := ByteSliceIntoString(ret)

			logger.Debug("File %s Input : %d", fs.Filename, arrlen)

			r.output.FilterProcess(arr, arrlen, tags)

			atomic.AddInt64(&r.rc.InputCount, int64(arrlen))
			atomic.AddInt64(&r.rc.OutputCount, int64(arrlen))
		}
	}
}

func (r *InputFile) InputFileProcess(files_list []string) {

	for _, filename := range files_list {

		if _, ok := r.FileMap[filename]; ok {

			go r.FileDataHandler(r.FileMap[filename])

		} else {

			fs, err := NewFileState(filename, r.Read_mode)
			if err != nil {
				logger.Alert("File %s Instance Init Err:", filename, err)
				continue
			}

			logger.Alert("Start Input File %s", fs.Filename)

			r.FileMap[filename] = fs

			go r.FileDataHandler(fs)
		}

	}

}

func (r *InputFile) InputProcess() error {

	for atomic.LoadInt32(r.switcher) > 0 {

		for _, files := range r.config.Input.File.Paths {

			files_list, err := GetFilesList(files)
			if err != nil {
				logger.Alert("Path %s Init Err:", files, err)
				continue
			}

			if len(files_list) == 0 {
				logger.Alert("Path %s Empty", files)
				continue
			}

			go r.InputFileProcess(files_list)
		}

		time.Sleep(time.Duration(r.Interval) * time.Second)
	}

	logger.Alert("File Input Instance %s Close ", r.config.Input.File.Paths)

	if atomic.LoadInt32(r.switcher) == int32(RouteStat_Off) {

		logger.Alert("File Input Instance %s Stop", r.config.Input.File.Paths)
	}

	return nil
}

func (r *InputFile) MainLoop() error {

	if atomic.LoadInt32(r.switcher) == int32(RouteStat_Ready) {
		logger.Alert("File Input Start")

		ret := r.InputProcess()
		if ret != nil {
			return ret
		}

		atomic.StoreInt32(r.switcher, int32(RouteStat_On))
	}

	return nil
}
