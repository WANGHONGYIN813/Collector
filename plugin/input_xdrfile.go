package plugin

import (
	"bufio"
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"sugon.com/WangHongyin/collecter/logger"
)

type InputXdrFile struct {
	InputCommon
	Read_interval int
	Handle_mode   string
	Bath_path     string
	Dir_arr       []string
	output        FilterInterface
}

var InputXdrFileRegInfo RegisterInfo

var readxdrpool chan int

func (r *InputXdrFile) exit() {

}

func init() {
	v := &InputXdrFileRegInfo

	v.Plugin = "Input"
	v.Type = "XdrFile"
	v.SubType = ""
	v.Vendor = "sugon.com"
	v.Desc = "for xdrfile input"
	v.init = RouteInputXdrFileInit

	InputRegister(v)

	readxdrpool = make(chan int, *readxdrpoolnum)

	//logger.Info("Register InputXDRfiLE")

}

func RouteInputXdrFileInit(c *YamlCommonData, out interface{}, countptr interface{}, statptr *RouteStatType) interface{} {
	r := new(InputXdrFile)

	r.config = c
	r.rc = countptr.(*RouteInputCounts)
	r.reginfo = &InputXdrFileRegInfo

	r.output = out.(FilterInterface)
	r.switcher = (*int32)(statptr)

	r.Bath_path = c.Input.XdrFile.Bath_path
	r.Dir_arr = c.Input.XdrFile.Dir_arr
	r.Read_interval = c.Input.XdrFile.Read_interval

	logger.Info("InputXdrFile path : ", r.Bath_path)

	return r
}

//获取zip文件中有效日志条数
func get_file_lines(sgn_path string) int {

	f, err := os.Open(sgn_path)
	if err != nil {
		logger.Error("Open .sgn File Fail :", err)
		return 0
	}

	defer f.Close()

	scanner := bufio.NewScanner(f)

	var line int

	for scanner.Scan() {
		//fmt.Println(scanner.Text())
		if strings.Contains(scanner.Text(), "RecordNumber") {

			//fmt.Println(scanner.Text())
			splitearr := strings.Split(scanner.Text(), ": ")
			//fmt.Println(splitearr[1])

			line, _ = strconv.Atoi(splitearr[1])
			break
		}

	}
	return line

	// cammand := "cat " + sgn_path + ` | grep RecordNumber | awk -F ": " '{print $2}'`
	// cmd := exec.Command("/bin/bash", "-c", cammand)
	// linenum, err := cmd.Output()
	// if err != nil {
	// 	logger.Error("Read .sgn file %s fail : ", sgn_path, err)
	// 	return 0
	// }
	// logger.Debug("linenum:", string(linenum))
	// linenumstr := strings.Replace(string(linenum), "\n", "", -1)
	// linenumint, errs := strconv.Atoi(linenumstr)
	// if errs != nil {
	// 	logger.Debug("strconv err:", errs)
	// 	return 0
	// }
	// logger.Debug(".sgn lines : ", linenumint)
	// return linenumint

}

func read_zcat_file(zip_path string, lines int) []byte {

	//zcat ./103_20200606040112_01_001_001.zip | head -n 32198 | tail -n +2
	cammand := "zcat " + zip_path + " | head -n " + strconv.Itoa(lines+1) + " | tail -n +2"
	cmd := exec.Command("/bin/bash", "-c", cammand)
	bytesarr, err := cmd.Output()
	if err != nil {
		logger.Error("Read zip file %s fail : ", zip_path, err)
		return nil
	}

	return bytesarr

	//执行 cmd 并获得结果
}

func WalkDirAndTrans(dirPth, suffix string) (files []string, err error) {
	files = make([]string, 0, 30)
	suffix = strings.ToUpper(suffix)

	err = filepath.Walk(dirPth, func(filename string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if fi.IsDir() {
			return nil
		}

		if strings.HasSuffix(strings.ToUpper(fi.Name()), suffix) {
			txtfilename := strings.Replace(filename, ".sgn", ".txt", -1)
			cammand := "mv " + filename + " " + txtfilename
			cmd := exec.Command("/bin/bash", "-c", cammand)
			_, err := cmd.Output()
			if err != nil {
				//logger.Error("Change %s to %s fail : ", filename, txtfilename, err)
				return nil
			}

			files = append(files, txtfilename)
		}

		return nil
	})

	return files, err
}

func (r *InputXdrFile) datahander(b *bytes.Buffer, totalbyte int) ([][]byte, int, int) {

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

func (r *InputXdrFile) GetXdrdata(filepath string, datatype string) {
	t := time.Now()

	tags := make(map[string]interface{})
	tags["@timestamp"] = time.Now().UTC().Format("2006-01-02T15:04:05.999Z")
	tags["Type"] = datatype
	//遍历filepath目录下.sgn文件, 得到.sgn文件中RecordNumber的值,对应的为.txt文件中要取的行数,zcat .tar文件读出所需数据message(用bytes.NewBuffer装message),删除所读的.sgn .tar文件
	filelist, _ := WalkDirAndTrans(filepath, ".sgn")
	// if err != nil {
	// 	logger.Error("Get Xdr Filelist Err : ", err)
	// 	return
	// }

	if len(filelist) == 0 {
		//logger.Error("file list is null")
		return
	}

	//logger.Alert(".sgn filelist:", filelist)
	//b := bytes.NewBuffer(nil)
	//sum := 0
	rangech := make(chan int, 1)

	// for k, value := range filelist {
	// 	filelist[k] = strings.Replace(value, ".sgn", ".txt", 1)
	// }

	var filename string
	for _, v := range filelist {
		rangech <- 1
		filename = v
		//这里也可并发执行
		readxdrpool <- 1
		go func() {

			defer PanicHandler()

			sgn_path := filename
			<-rangech
			//logger.Alert(".sgn file:", sgn_path)
			sum := 0
			b := bytes.NewBuffer(nil)
			lines := get_file_lines(sgn_path)
			zip_path := strings.Replace(sgn_path, ".txt", ".zip", 1)
			bytearr := read_zcat_file(zip_path, lines) //arr为字节数组，即从.zip读到的数据
			sum = sum + len(bytearr)
			_, _ = b.Write(bytearr)

			remove_sgn_err := os.Remove(sgn_path)
			if remove_sgn_err != nil {
				//logger.Error("Delete %s Fail:", sgn_path, remove_sgn_err)
			}

			remove_zip_err := os.Remove(zip_path)
			if remove_zip_err != nil {
				//logger.Error("Delete %s Fail:", zip_path, remove_zip_err)
			}

			arr, orilen, arrlen := r.datahander(b, sum)
			atomic.AddInt64(&r.rc.OutputCount, int64(arrlen))

			logger.Debug("Input Xdr_%s Input Line : %d, Output Line : %d  Total Cost %s", datatype, orilen, arrlen, time.Now().Sub(t))

			r.output.FilterProcess(arr, arrlen, tags)
			<-readxdrpool
		}()

		// 读单个文件不并发:
		// sgn_path := v
		// //获取对应的zip文件中有效日志行数
		// lines := get_file_lines(sgn_path)
		// zip_path := strings.Replace(sgn_path, ".sgn", ".zip", 1)
		// arr := read_zcat_file(zip_path, lines) //arr为字节数组，即从.zip读到的数据
		// sum = sum + len(arr)
		// _, _ = b.Write(arr)

		// //删除读过的文件
		// remove_sgn_err := os.Remove(sgn_path)
		// if remove_sgn_err != nil {
		// 	logger.Error("Delete %s Fail", sgn_path)
		// }

		// remove_zip_err := os.Remove(zip_path)
		// if remove_zip_err != nil {
		// 	logger.Error("Delete %s Fail", zip_path)
		// }
	}
	//<-readxdrpool
	//将对bytes.NewBuffer进行操作，按"/n"转成[][]byte
	// arr, orilen, arrlen := r.datahander(b, sum)
	// atomic.AddInt64(&r.rc.OutputCount, int64(arrlen))

	// logger.Debug("Input Xdr_%s Input Line : %d, Output Line : %d  Total Cost %s", datatype, orilen, arrlen, time.Now().Sub(t))

	// r.output.FilterProcess(arr, arrlen, tags)
}

func (r *InputXdrFile) InputProcess() error {
	go func() {

		for atomic.LoadInt32(r.switcher) > 0 {

			for _, v := range r.Dir_arr {
				datatype := strings.ToLower(v)
				filepath := r.Bath_path + "/" + v
				//readxdrpool <- 1
				r.GetXdrdata(filepath, datatype)
			}

			//time.Sleep(time.Duration(r.Read_interval) * time.Second)
		}

		logger.Alert("XdrFile Input Instance %s Close ", r.config.Input.XdrFile.Bath_path)

		if atomic.LoadInt32(r.switcher) == int32(RouteStat_Off) {

			logger.Alert("XdrFile Input Instance %s Stop", r.config.Input.XdrFile.Bath_path)
		}
	}()

	return nil
}

func (r *InputXdrFile) MainLoop() error {

	if atomic.LoadInt32(r.switcher) == int32(RouteStat_Ready) {
		logger.Alert("XdrFile Input Start")

		ret := r.InputProcess()
		if ret != nil {
			return ret
		}

		atomic.StoreInt32(r.switcher, int32(RouteStat_On))
	}

	return nil
}
