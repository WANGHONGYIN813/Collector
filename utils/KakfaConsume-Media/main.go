package main

import (
	"bufio"
	//"encoding/json"
	"./media"
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
)

var url = flag.String("u", "", "m3u8 url")
var extract = flag.String("extract", "", "extract frame")
var save_path = flag.String("save", "save_dir", "save dir")
var download_flag = flag.Bool("d", false, "download ts")
var merge_flag = flag.Bool("m", false, "merge all the ts to one mp4")
var clear_flag = flag.Bool("c", false, "clear ts after merge")

/*
//extract I frame
ffmpeg -nostdin -i $file -vf select='eq(pict_type\,I)' -f image2 frame_%05d.jpeg


resolution=160x90
//extract I frame with specific size
ffmpeg -nostdin -i $file -vf select='eq(pict_type\,I)' -s $resolution -f image2 frame_%05d.jpeg

//extract all frame
ffmpeg -nostdin -loglevel quiet -y -i $file $tardir/frame_%05d.jpeg
*/

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

func init() {

	flag.Parse()

	if *download_flag {

		if PathExists(*save_path) == false {
			err := os.Mkdir(*save_path, os.ModePerm)
			if err != nil {
				fmt.Println("Failed to create the save dir :", save_path, err)
				os.Exit(0)
			}
		}

	}
}

func M3u8Get(m3u8_url string, rets []string) ([]string, error) {

	url_paths, _ := filepath.Split(m3u8_url)

	resp, err := http.Get(m3u8_url)
	if err != nil {
		return rets, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return rets, err
	}

	src := string(body)

	ch := make(chan map[string]bool)

	num := 0

	fmt.Println("Get ", m3u8_url, "context:\n", src)

	sReader := strings.NewReader(src)

	bReader := bufio.NewScanner(sReader)
	bReader.Split(bufio.ScanLines)

	i := 1

	for bReader.Scan() {
		line := bReader.Text()

		if line == "" {
			break
		}

		if !strings.HasPrefix(line, "#") {
			tsFileName := line

			fmt.Println("Line", i, ":", tsFileName)
			i += 1

			if path.Ext(tsFileName) == ".m3u8" {

				sub_m3u8 := url_paths + tsFileName

				rets, err = M3u8Get(sub_m3u8, rets)
				if err != nil {
					return rets, err
				}

			} else {

				if *download_flag == false {
					continue
				}

				tsFileUrl := fmt.Sprintf("%s%s", url_paths, tsFileName)

				num += 1
				go downloadTS(tsFileUrl, ch)
			}
		}
	}

	for i := 0; i < num; i++ {
		err := <-ch
		for k, v := range err {
			if v {
				rets = append(rets, k)
			}
		}
	}

	sort.Strings(rets)

	return rets, err
}

func ExtractFrame(file string) {

}

func downloadTS(tsFileUrl string, e chan<- map[string]bool) error {

	fmt.Println("Downloading: ", tsFileUrl)

	ret := make(map[string]bool)

	_, name := filepath.Split(tsFileUrl)

	resp, err := http.Get(tsFileUrl)
	if err != nil {
		fmt.Println("download ts ", tsFileUrl, "failed,", err)
		ret[name] = false
		e <- ret
		return err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)

	fmt.Println(name, ":", len(body), "Bytes")

	save_name := filepath.Join(*save_path, name)

	err = ioutil.WriteFile(save_name, body, 0644)
	if err != nil {
		fmt.Println("save ts :", name, "failed:", err)
	}

	if err != nil {
		ret[name] = false
	} else {
		ret[name] = true
	}

	e <- ret

	return nil
}

func OrderTs(r []string) {

	if len(r) == 0 {
		return
	}

	fmt.Println("Merge TS List:", r)

	reg := regexp.MustCompile(`[A-z]+`)

	n := reg.FindAllString(r[0], -1)

	outname := ""

	if len(n) == 1 {
		outname = "default_output.mp4"
	} else {
		outname = n[0] + ".mp4"
	}

	outname = filepath.Join(*save_path, outname)

	for i, f := range r {
		r[i] = filepath.Join(*save_path, f)
	}

	ts_list := strings.Join(r, "|")

	cmd_args := fmt.Sprintf("ffmpeg -nostdin -loglevel quiet -y -i \"concat:%s\" -acodec copy -vcodec copy %s", ts_list, outname)

	cmd := exec.Command("/bin/bash", "-c", cmd_args)

	_, err := cmd.Output()
	if err != nil {
		fmt.Printf("Execute Shell:%s failed with error:%s", cmd_args, err.Error())
		return
	}

	fmt.Println("Output File :", outname)

	if *clear_flag {
		fmt.Println("Clear TS List")

		for _, f := range r {
			err := os.Remove(f)
			if err != nil {
				fmt.Println("RM Err:", err)
			}
		}
	}
}

func main() {

	if *url == "" {
		fmt.Printf("no url")
		return
	}

	var rets []string

	rets, err := M3u8Get(*url, rets)
	if err != nil {
		fmt.Println("Err:", err)
	}

	if *merge_flag {
		OrderTs(rets)
	}
}
