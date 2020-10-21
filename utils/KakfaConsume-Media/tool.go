package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

var save_path = flag.String("save_dir", "save_dir", "save dir")
var save_all_flag = flag.Bool("save_all", false, "save all file, include .m3u8 txt")

var Download_flag = flag.String("D", "", "Download: Download m3u8 url or media url")

var merge_flag = flag.Bool("m", false, "merge all the ts to one mp4")
var clear_flag = flag.Bool("c", false, "clear ts after merge")
var parallel_num = flag.Int("p", 10, "thread Pool max number")
var Merge_File_flag = flag.String("M", "", "Merge: specify dir for Merge all ts file to one mp4")
var Targe_File_Suffix = flag.String("file_suffix", "ts", "Merge File Suffix. m3u8 is ts")
var Merge_File_Name = flag.String("file_name", "", "Merge File Output name, must end with .mp4, or .flv . default auto select")

var Extract_flag = flag.Bool("e", false, "extract frames when download ts")
var Extract_File_flag = flag.String("E", "", "Extract: specify dir or file for Extract frames")
var Extract_File_Suffix = flag.String("extract_file_suffix", "mp4", "extract File Suffix")
var Extract_I_Frame_mode = flag.Bool("extract_if_mode", false, "extract I frames mode. otherwise extract all frames")
var Extract_Size = flag.String("extract_size", "", "specify img save size, like : 160x90")
var Extract_Scale = flag.Int("extract_scale", 0, "specify scale img(10-90), like : 90, means width resize to 90%, and height resize :90%")

/***************************************************/

func PathExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return false
}

func WalkDir(dirPth, suffix string) (files []string, err error) {
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
			files = append(files, filename)
		}

		return nil
	})

	return files, err
}

func FileList_Find(dir string, file_suffix string) ([]string, error) {

	fmt.Println("WalkDir :", dir)

	f, err := os.Stat(dir)
	if err != nil {
		fmt.Println("Open Dir ", dir, " Err:", err)
		return nil, err
	}

	if !f.IsDir() {
		fmt.Println(dir, "is not dir")
		return nil, err
	}

	if file_suffix == "" {
		file_suffix = ".ts"
	}

	filelist, err := WalkDir(dir, "."+file_suffix)
	if err != nil {
		fmt.Println("WalkDir Err:", err)
		return nil, err
	}

	return filelist, err
}

/***************************************************/

type AppMode int

const (
	AppMode_M3U8_Download  AppMode = 0
	AppMode_MEDIA_Download AppMode = 1
	AppMode_Merge          AppMode = 2
	AppMode_Extract        AppMode = 3
)

var appmode AppMode

func init() {

	flag.Parse()

	if *Download_flag != "" {

		if PathExists(*save_path) == false {
			err := os.Mkdir(*save_path, os.ModePerm)
			if err != nil {
				fmt.Println("Failed to create the save dir :", save_path, err)
				os.Exit(0)
			}
		}

		if path.Ext(*Download_flag) == ".m3u8" {

			appmode = AppMode_M3U8_Download
		} else {
			appmode = AppMode_MEDIA_Download
		}

	} else if *Merge_File_flag != "" {

		if PathExists(*Merge_File_flag) == false {
			fmt.Printf("Merge Dir %s do not exist\n", *Merge_File_flag)
			os.Exit(0)
		}

		appmode = AppMode_Merge

	} else if *Extract_File_flag != "" {

		if PathExists(*Extract_File_flag) == false {
			fmt.Printf("Extract File %s do not exist\n", *Extract_File_flag)
			os.Exit(0)
		}

		if PathExists(*save_path) == false {

			err := os.Mkdir(*save_path, os.ModePerm)
			if err != nil {
				fmt.Println("Failed to create the save dir :", save_path, err)
				os.Exit(0)
			}

		}

		appmode = AppMode_Extract
	} else {
		fmt.Println("use -h for help")
		os.Exit(0)
	}
}

/***********************************************************************/

type GoroutinePool_t struct {
	Routine_ret chan map[string]bool
	Routine_cnt chan int
}

type TaskObj struct {
	M3u8_Url       string
	M3u8_Url_Ori   string
	Url_Prefix     string
	Url_Prefix_Ori string
	Ts_list        []string
	Ts_list_Done   []string
	output_name    string
	routine_pool   *GoroutinePool_t
	extract_flag   bool
	if_mode        bool
	frame_size     string
	frame_scale    int
	save_dir       string
}

var ret_buffer_max_size = 200

func RoutinePoolInit(pool_cnt int) *GoroutinePool_t {
	d := new(GoroutinePool_t)

	d.Routine_ret = make(chan map[string]bool, ret_buffer_max_size)
	d.Routine_cnt = make(chan int, pool_cnt)

	fmt.Println("Thread Pool : ", pool_cnt)

	return d
}

func TaskObjInit(url string, parallel_num int, output_name string, extract_flag bool, if_mode bool, frame_size string, frame_scale int, save_dir string) *TaskObj {

	r := new(TaskObj)

	if url != "" {
		r.M3u8_Url = url
		r.M3u8_Url_Ori = url

		r.Url_Prefix, _ = filepath.Split(url)
		r.Url_Prefix_Ori = r.Url_Prefix

		r.routine_pool = RoutinePoolInit(parallel_num)
	}

	r.extract_flag = extract_flag
	r.if_mode = if_mode
	r.frame_size = frame_size

	if frame_scale != 0 {
		r.frame_scale = frame_scale / 10
	}

	r.save_dir = save_dir

	r.output_name = output_name

	return r
}

func (r *TaskObj) Media_Source_Get() {

	url_paths, name := filepath.Split(r.M3u8_Url)

	r.Url_Prefix = url_paths

	r.Ts_list = append(r.Ts_list, name)

}

func (r *TaskObj) M3u8_Source_Get() error {

	url_paths, _ := filepath.Split(r.M3u8_Url)

	resp, err := http.Get(r.M3u8_Url)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	src := string(body)

	fmt.Println("Get ", r.M3u8_Url, "context:\n", src)

	b := bufio.NewScanner(strings.NewReader(src))
	b.Split(bufio.ScanLines)

	i := 1

	for b.Scan() {
		line := b.Text()

		if line == "" {
			continue
		}

		if !strings.HasPrefix(line, "#") {
			tsname := line

			//fmt.Println("Line", i, ":", tsname)
			i += 1

			if path.Ext(tsname) == ".m3u8" {

				r.M3u8_Url = url_paths + tsname

				url_paths, _ = filepath.Split(r.M3u8_Url)

				r.Url_Prefix = url_paths

				err = r.M3u8_Source_Get()

			} else {

				r.Ts_list = append(r.Ts_list, tsname)

			}
		}
	}

	return err
}

func (r *TaskObj) RoutinePoolRecycler(num int, p chan<- int) {

	g := r.routine_pool

	i := 0

	ch := make(chan error)
	ch_extract := make(chan int)

	if r.extract_flag {

		go func(tar_len int, p chan<- int) {
			i := 0
			for {
				err := <-ch
				if err != nil {
					fmt.Println("Err:", err)
				}

				i += 1
				if i == tar_len {
					p <- 1
					return
				}
			}
		}(num, ch_extract)
	}

	for ret := range g.Routine_ret {
		//fmt.Printf("Current Routine Pool Left : %d, Total Routine : %d\n", *parallel_num-len(g.Routine_cnt), i)
		i += 1

		for k, v := range ret {
			if v {
				r.Ts_list_Done = append(r.Ts_list_Done, k)
			}

			if r.extract_flag {

				filename := strings.TrimSuffix(filepath.Base(k), path.Ext(k))

				output_dir := filepath.Join(r.save_dir, filename)

				os.Mkdir(output_dir, os.ModePerm)

				go r.TsList_Extract(filepath.Join(r.save_dir, k), output_dir, ch)
			}
		}

		if i == num {

			if r.extract_flag {
				<-ch_extract
			}

			p <- num

			return
		}
	}
}

func (r *TaskObj) TsList_Download() error {

	//fmt.Println("Ts List     :", r.Ts_list)
	fmt.Println("Ts List Len :", len(r.Ts_list))

	t := time.Now()

	ch := make(chan int)

	go r.RoutinePoolRecycler(len(r.Ts_list), ch)

	for _, ts := range r.Ts_list {

		r.routine_pool.Routine_cnt <- 1

		go r.TS_Download(ts)
	}

	_ = <-ch

	//fmt.Println("Download Ts_list :   ", r.Ts_list_Done)
	fmt.Println("Download Ts_list Len :", len(r.Ts_list_Done))

	fmt.Println("Download Cost : ", time.Now().Sub(t))

	return nil
}

var client = &http.Client{}

func (r *TaskObj) TS_Download(name string) error {

	ts_url := r.Url_Prefix + name

	fmt.Println("Downloading: ", ts_url)

	ret := make(map[string]bool)

	request, err := http.NewRequest("GET", ts_url, nil)
	if err != nil {
		fmt.Println(ts_url, "http.NewRequest failed : ", err)
		ret[name] = false
		r.routine_pool.Routine_ret <- ret
		<-r.routine_pool.Routine_cnt
		return err
	}

	resp, _ := client.Do(request)

	defer resp.Body.Close()

	if resp.StatusCode != 200 {

		fmt.Println(ts_url, "http Request Failed, StatusCode: ", resp.StatusCode)
		ret[name] = false
		r.routine_pool.Routine_ret <- ret
		<-r.routine_pool.Routine_cnt
		return err
	}

	fmt.Println(name, ":", resp.ContentLength, "Bytes")

	body, err := ioutil.ReadAll(resp.Body)

	save_name := filepath.Join(r.save_dir, name)

	err = ioutil.WriteFile(save_name, body, 0644)
	if err != nil {
		fmt.Println("save ts :", name, "failed:", err)
	}

	if err != nil {
		ret[name] = false
	} else {
		ret[name] = true
	}

	r.routine_pool.Routine_ret <- ret
	<-r.routine_pool.Routine_cnt

	return nil
}

func (r *TaskObj) TsList_Merge_Main(dir string, s string) error {

	r.Ts_list_Done, _ = FileList_Find(dir, s)

	for k, f := range r.Ts_list_Done {

		_, name := filepath.Split(f)

		r.Ts_list_Done[k] = name
	}

	r.TsList_Merge()

	return nil
}

func (r *TaskObj) TsList_Merge() {

	tslist := r.Ts_list_Done

	if len(tslist) == 0 {
		return
	}

	t := time.Now()

	sort.Strings(r.Ts_list_Done)

	//fmt.Println("Merge TS List     :", tslist)
	fmt.Println("Merge TS List Len :", len(tslist))

	outname := ""

	if r.output_name == "" {

		reg := regexp.MustCompile(`[A-z]+`)

		n := reg.FindAllString(tslist[0], -1)

		if len(n) == 1 {
			outname = "default_output.mp4"
		} else {
			outname = n[0] + ".mp4"
		}
	} else {
		outname = r.output_name
	}

	outname = filepath.Join(r.save_dir, outname)

	b := new(bytes.Buffer)

	for _, f := range tslist {
		b.WriteString(fmt.Sprintf("file %s\n", f))
	}

	ts_list_file := outname + ".filelist"

	if ioutil.WriteFile(ts_list_file, b.Bytes(), 0644) != nil {
		fmt.Println("Open ", ts_list_file, "Fail. Merge Fail")
		return
	}

	cmd_args := fmt.Sprintf("ffmpeg -nostdin -loglevel quiet -y -f concat -i %s -acodec copy -vcodec copy %s", ts_list_file, outname)
	cmd := exec.Command("/bin/bash", "-c", cmd_args)

	_, err := cmd.Output()
	if err != nil {
		fmt.Printf("Execute Shell:%s failed with error:%s", cmd_args, err.Error())
		return
	}

	os.Remove(ts_list_file)

	fmt.Println("Output File :", outname)

	fmt.Println("Merge Cost : ", time.Now().Sub(t))

	fmt.Println("Clear TS List")

}

func (r *TaskObj) TsList_Clear() {

	fmt.Println("Clear TS List")

	for _, f := range r.Ts_list_Done {
		err := os.Remove(filepath.Join(r.save_dir, f))
		if err != nil {
			fmt.Println("RM Err:", err)
		}
	}
}

func (r *TaskObj) TsList_Extract_Main(dir string, s string) {

	r.Ts_list_Done, _ = FileList_Find(dir, s)

	if len(r.Ts_list_Done) == 0 {
		fmt.Println("Empty Dir")
		return
	}

	ch := make(chan error)

	for _, f := range r.Ts_list_Done {

		filename := strings.TrimSuffix(filepath.Base(f), path.Ext(f))

		output_dir := filepath.Join(r.save_dir, filename)

		fmt.Println("Output Dir :", output_dir)

		err := os.Mkdir(output_dir, os.ModePerm)
		if err != nil {
			//fmt.Printf("Create Output Dir %s Err: %s\n", output_dir, err)
		}

		go r.TsList_Extract(f, output_dir, ch)
	}

	num := 0
	tar_len := len(r.Ts_list_Done)

	for {
		err := <-ch
		if err != nil {
			fmt.Println("Err:", err)
		}

		num += 1

		if num == tar_len {
			break
		}
	}

}

func (r *TaskObj) TsList_Extract(file string, save_dir string, p chan<- error) {

	vf := ""

	cmd_args := "ffmpeg -nostdin -loglevel quiet -y "

	if r.if_mode {

		if r.frame_scale != 0 {

			vf = fmt.Sprintf("-vf \"select='eq(pict_type\\,I)',scale=iw*0.%d:ih*0.%d\" -vsync 2 ", r.frame_scale, r.frame_scale)

		} else {
			vf = fmt.Sprintf("-vf \"select='eq(pict_type\\,I)'\" -vsync 2 ")
		}

	} else {

		if r.frame_scale != 0 {

			vf = fmt.Sprintf("-vf \"scale=iw*0.%d:ih*0.%d\" ", r.frame_scale, r.frame_scale)
		}
	}

	input := fmt.Sprintf("-i %s ", file)

	size := ""

	if r.frame_size != "" {
		size = fmt.Sprintf("-s %s ", r.frame_size)
	}

	output := fmt.Sprintf("-f image2 %s", filepath.Join(save_dir, "frame_%08d.jpeg"))

	cmd_args += input
	cmd_args += vf
	cmd_args += size
	cmd_args += output

	fmt.Println("cmd_args :", cmd_args)

	cmd := exec.Command("/bin/bash", "-c", cmd_args)
	_, err := cmd.Output()
	if err != nil {
		ret := fmt.Sprintf("File %s Execute Shell:%s failed with error : %s\n", file, cmd_args, err.Error())
		p <- errors.New(ret)
		return
	}

	p <- nil

	return
}

/***********************************************************************/

func main() {

	r := TaskObjInit(*Download_flag, *parallel_num, *Merge_File_Name, *Extract_flag, *Extract_I_Frame_mode, *Extract_Size, *Extract_Scale, *save_path)

	t := time.Now()

	switch appmode {

	case AppMode_M3U8_Download:

		err := r.M3u8_Source_Get()
		if err != nil {
			fmt.Println("Get M3u8 Err:", err)
			break
		}

		r.TsList_Download()

		if *merge_flag {
			r.TsList_Merge()
		}

		if *clear_flag {
			r.TsList_Clear()
		}

	case AppMode_MEDIA_Download:

		r.Media_Source_Get()

		r.TsList_Download()

		if *clear_flag {
			r.TsList_Clear()
		}

	case AppMode_Merge:

		r.TsList_Merge_Main(*Merge_File_flag, *Targe_File_Suffix)

		if *clear_flag {
			r.TsList_Clear()
		}

	case AppMode_Extract:

		r.TsList_Extract_Main(*Extract_File_flag, *Extract_File_Suffix)

	default:
		fmt.Println("use -h for help")
	}

	fmt.Println("Total Cost : ", time.Now().Sub(t))

}
