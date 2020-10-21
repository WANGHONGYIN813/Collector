package send

import (
	"bytes"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"github.com/json-iterator/go"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"path/filepath"
)

type SType int

const (
	S_Nothing SType = 0
	S_Local   SType = 1
	S_Hbase   SType = 2
	S_HDFS    SType = 3
)

type StroageMethod_t struct {
	Httpfspath   string
	Hbasepath    string
	Hbase_colnum string
	Localpath    string
	Username     string
	stype        SType
}

var sm StroageMethod_t

var hbase_insert_url string

func init() {

	if Gconfig.Hbase.Host != "" {

		sm.stype = S_Hbase

		sm.Hbasepath = Gconfig.Hbase.Host + "/" + Gconfig.Hbase.Table + "/"

		fmt.Println("Hbase Path: ", sm.Hbasepath)

		sm.Hbase_colnum = Gconfig.Hbase.Column

		hbase_insert_url = sm.Hbasepath + "insert"

	} else if Gconfig.Httpfs.Host != "" {

		dir := path.Join(Gconfig.Httpfs.Home, Gconfig.Httpfs.Dir)

		sm.Httpfspath = Gconfig.Httpfs.Host + "/webhdfs/v1" + dir + "/"

		sm.stype = S_HDFS

		if Gconfig.Httpfs.User != "" {
			sm.Username = Gconfig.Httpfs.User
		} else {
			sm.Username = "hadoop"
		}

		fmt.Println("Httpfs Path: ", sm.Httpfspath)

	} else if Gconfig.Stroage_dir != "" {
		sm.Localpath = Gconfig.Stroage_dir

		if PathExists(sm.Localpath) == false {

			err := os.Mkdir(sm.Localpath, os.ModePerm)
			if err != nil {
				sm.Localpath = ""
			}
		}

		sm.stype = S_Local

		fmt.Println("Localhost Save Path: ", sm.Localpath)
	} else {

		sm.stype = S_Nothing

		fmt.Println("Do not Save")
	}

}

func createFromData(fileName string, data []byte) error {

	url1 := sm.Httpfspath + path.Join(Gconfig.Httpfs.Dir, fileName)

	url := url1 + "?op=CREATE&data=true&user.name=" + sm.Username

	req, err := http.NewRequest("PUT", url, bytes.NewReader(data))
	if err != nil {
		fmt.Println(fmt.Errorf("url(%v) NewRequest: %v", url, err))
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	rep, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println(fmt.Errorf("url(%v) DefaultClient.Do: %v", url, err))
		return err
	}

	if rep.StatusCode != http.StatusCreated {
		fmt.Println(fmt.Errorf("url(%v) rep not ok, req(%v)", url, rep))
		return err
	}

	_, err = ioutil.ReadAll(rep.Body)
	if err != nil {
		fmt.Println(fmt.Errorf("url(%v) rep body read failed: %v", url, err))
		return err
	}

	fmt.Printf("%s URL : %s, Datalen : %d\n", fileName, url1, len(data))
	return nil
}

type Cell struct {
	XMLName xml.Name `xml:"CellSet"`
	Cellset []Row_t  `xml:"Row"`
}

type Row_t struct {
	XMLName xml.Name `xml:"Row"`
	Key     string   `xml:"key,attr"`
	Cell    Cell_t   `xml:"Cell"`
}

type Cell_t struct {
	XMLName xml.Name `xml:"Cell"`
	Column  string   `xml:"column,attr"`
	Value   string   `xml:",innerxml"`
}

func hbase_insert_xml_data(http_host string, id string, column string, input []byte) error {
	bs := Cell{}

	key := base64.StdEncoding.EncodeToString([]byte(id))

	colunm_name := base64.StdEncoding.EncodeToString([]byte(column))

	value := base64.StdEncoding.EncodeToString(input)

	cell := Cell_t{Column: colunm_name, Value: value}

	row := Row_t{Key: key, Cell: cell}

	bs.Cellset = append(bs.Cellset, row)

	data, _ := xml.MarshalIndent(&bs, "", "")

	outdata := string(data)

	var send_uri = http_host + id

	fmt.Printf("Hbase : File : %s ,Uri : %s\n", id, send_uri)
	fmt.Println("send data ", outdata)
	return nil

	err, _ := HbaseHttpPutXml(send_uri, outdata)
	if err != nil {
		return err
	}

	//fmt.Printf("Hbase : File : %s ,Uri : %s\n", id, send_uri)
	return nil
}

func FileStroageHandler(file string, cur_path string, tag string, p chan<- int) error {

	ori := path.Join(cur_path, file)

	switch sm.stype {

	case S_Nothing:

		os.RemoveAll(ori)
		p <- 1
		return nil

	case S_Local:

		tar := path.Join(sm.Localpath, file)

		err := os.Rename(ori, tar)
		if err != nil {
			p <- 0
			fmt.Println("Rename Err :", err)
			return err
		}

	case S_HDFS:

		data, err := ioutil.ReadFile(ori)
		if err != nil {
			p <- 0
			fmt.Println("Read file failed", err)
			return err
		}

		err = createFromData(file, data)
		if err != nil {
			p <- 0
			fmt.Println(fmt.Errorf("create hdfs file failed:%v", err))
			return err
		}

	case S_Hbase:

		buf, err := ioutil.ReadFile(ori)
		if err != nil {
			p <- 0
			fmt.Println("Read file failed", err)
			return err
		}

		buf1 := buf[0 : len(buf)-1]

		sub_colnum := sm.Hbase_colnum + ":" + tag

		err = hbase_insert_xml_data(sm.Hbasepath, file, sub_colnum, buf1)
		if err != nil {
			p <- 0
			fmt.Println(fmt.Errorf("create hbase file failed:%v", err))
			return err
		}
	}

	p <- 1

	return nil
}

type Hbase_Json_Data_t struct {
	Row []Json_Row_t `json:"Row"`
}

type Json_Row_t struct {
	Key  string        `json:"key"`
	Cell []Json_Cell_t `json:"Cell"`
}

type Json_Cell_t struct {
	Column string `json:"column"`
	Value  string `json:"$"`
}

func make_hbase_batch_json_data(id string, column string, input []byte, p chan<- Json_Row_t) {

	key := base64.StdEncoding.EncodeToString([]byte(id))

	colunm_name := base64.StdEncoding.EncodeToString([]byte(column))

	value := base64.StdEncoding.EncodeToString(input)

	cell := Json_Cell_t{Column: colunm_name, Value: value}

	row := Json_Row_t{Key: key}

	row.Cell = append(row.Cell, cell)

	p <- row
}

func BatchFileStroageHandler(file_buffer_map map[string][]byte, tag string) error {

	if sm.stype != S_Hbase {
		return nil
	}

	num := 0

	sub_colnum := sm.Hbase_colnum + ":" + tag

	ch := make(chan Json_Row_t)

	hbase_data := Hbase_Json_Data_t{}

	for file, buf := range file_buffer_map {

		go make_hbase_batch_json_data(file, sub_colnum, buf, ch)

		num += 1
	}

	for i := 0; i < num; i++ {
		row := <-ch
		hbase_data.Row = append(hbase_data.Row, row)
	}

	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	data, _ := json.MarshalToString(hbase_data)

	err := HbaseHttpPutJson(hbase_insert_url, data)

	return err
}

func ZipFileStroageHandler(file_path string) error {

	if sm.stype == S_Local {

		tar := path.Join(sm.Localpath, filepath.Base(file_path))

		err := os.Rename(file_path, tar)
		if err != nil {
			fmt.Println("file rename Error!: ", err)
			return err
		}
	} else {

		err := os.Remove(file_path)
		if err != nil {
			fmt.Println("file remove Error!: ", err)
			return err
		}
	}

	return nil
}
