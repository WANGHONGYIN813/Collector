package plugin

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/structs"
	_ "github.com/go-sql-driver/mysql"
	jsoniter "github.com/json-iterator/go"
	"github.com/oschwald/geoip2-golang"
	"golang.org/x/text/encoding/simplifiedchinese"
	"sugon.com/WangHongyin/collecter/logger"
)

type Charset string

const (
	UTF8    = Charset("UTF-8")
	GB18030 = Charset("GB18030")
)

func GetLocalIP() string {

	addrs, err := net.InterfaceAddrs()

	if err != nil {
		fmt.Println(err)
		return ""
	}

	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return (ipnet.IP.String())
			}
		}
	}

	return ""
}

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

func NullLoop() {
	for {
		runtime.Gosched()
	}
}

func DataBulkBufferInit(lines int, bulk int) [][2]int {

	var bufferlines [][2]int

	for i := 0; i < lines; {

		var buf [2]int

		end := i + bulk

		if end >= lines {
			end = lines
		}

		buf[0] = i
		buf[1] = end

		bufferlines = append(bufferlines, buf)

		i = end
	}

	return bufferlines
}

func SearchItemName_for_NotZero(m interface{}) string {
	s := structs.New(m)

	for _, f := range s.Fields() {

		if f.IsExported() {
			if f.IsZero() == false {
				return f.Name()
			}
		}
	}

	return ""
}

func SearchItemNameList_for_NotZero(m interface{}) []string {
	s := structs.New(m)
	var arr []string

	for _, f := range s.Fields() {

		if f.IsExported() {
			if f.IsZero() == false {
				arr = append(arr, f.Name())
			}
		}
	}

	return arr
}

/*********************************************/

func SearchConfigs(path string) ([]string, error) {

	path += "/*.yaml"
	filelist, err := filepath.Glob(path)
	if err != nil {
		return nil, err
	}

	return filelist, err
}

func SearchAllConfigs(pathname string, file_flag string) ([]string, error) {

	var s []string

	rd, err := ioutil.ReadDir(pathname)
	if err != nil {
		fmt.Println("read dir fail:", err)
		return s, err
	}
	for _, fi := range rd {
		if fi.IsDir() {
			fullDir := pathname + "/" + fi.Name()
			s, err = SearchAllConfigs(fullDir, file_flag)
			if err != nil {
				fmt.Println("read dir fail:", err)
				return s, err
			}
		} else {

			ext := path.Ext(fi.Name())

			if ext == file_flag {
				fullName := pathname + "/" + fi.Name()
				s = append(s, fullName)
			}
		}
	}
	return s, nil
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

/******************************************/

func ExecShellCommand(s string) (string, error) {
	cmd := exec.Command("/bin/bash", "-c", s)

	var out bytes.Buffer
	cmd.Stdout = &out

	err := cmd.Run()
	if err != nil {
		fmt.Printf("Run Command \"%s\" Error : %s\n", s, err)
		return "", err
	}

	ret := out.Bytes()

	retlen := len(ret)
	if retlen != 0 {
		r1 := ret[:retlen-1]
		return string(r1), nil
	}

	return "", err
}

func GetIPaddr(ip []byte) string {
	if len(ip) > 0 {
		return fmt.Sprintf("%x:%x:%x:%x:%x:%x", ip[0], ip[1], ip[2], ip[3], ip[4], ip[5])
	} else {
		return ""
	}
}

func StructToMap(obj interface{}) map[string]interface{} {
	obj1 := reflect.TypeOf(obj)
	obj2 := reflect.ValueOf(obj)

	var data = make(map[string]interface{})
	for i := 0; i < obj1.NumField(); i++ {
		data[obj1.Field(i).Name] = obj2.Field(i).Interface()
	}
	return data
}

func SplitStringParse(s string) (string, string) {

	a := strings.Split(s, ",")

	tar1 := a[0]
	m := a[1]

	var tar2 string

	if strings.Contains(m, "\"") {

		i := strings.Index(m, "\"")
		tar := m[i+1:]
		i2 := strings.Index(tar, "\"")
		tar2 = tar[:i2]

	} else if strings.Contains(m, "'") {

		i := strings.Index(m, "'")
		tar := m[i+1:]
		i2 := strings.Index(tar, "'")
		tar2 = tar[:i2]

	} else {
		tar2 = strings.Replace(m, " ", "", -1)
	}

	return tar1, tar2
}

type GeoBaseInfo struct {
	ContinentCode     string
	ContinentID       uint
	CountryIsoCode    string
	CountryID         uint
	CityName          string
	CityID            uint
	ProvinceID        uint
	Province_iso_code string
	Province_name     string

	Latitude  float64
	Longitude float64
}

func GetGeoInfo(ip string) (GeoBaseInfo, error) {

	var g GeoBaseInfo

	if Geo_db == nil {
		return g, nil
	}

	arr := strings.Split(ip, ":")
	ip = arr[0]

	record, err := Geo_db.City(net.ParseIP(ip))
	if err != nil {
		return g, err
	}

	if record.Continent.Code != "" {
		g.ContinentCode = record.Continent.Code
		g.ContinentID = record.Continent.GeoNameID
	}

	if record.City.GeoNameID != 0 {

		g.CityID = record.City.GeoNameID

		if record.City.Names != nil {
			if record.City.Names != nil {
				g.CityName = record.City.Names["en"]
			}
		}
	}

	if len(record.Subdivisions) != 0 {
		g.ProvinceID = record.Subdivisions[0].GeoNameID
		g.Province_iso_code = record.Subdivisions[0].IsoCode
		g.Province_name = record.Subdivisions[0].Names["en"]
	}

	g.CountryIsoCode = record.Country.IsoCode
	g.CountryID = record.Country.GeoNameID

	if record.Location.Latitude == 0 {
		g.Latitude = 0.01
	} else {
		g.Latitude = record.Location.Latitude
	}

	if record.Location.Longitude == 0 {
		g.Longitude = 0.01
	} else {
		g.Longitude = record.Location.Longitude
	}

	return g, nil
}

func UpdateGeoData(geo_config [][]string, ori map[string]interface{}) error {

	var err error

	if geo_config != nil {
		for _, v := range geo_config {

			var name string

			if v[1] != "" {
				name = v[1]
			} else {
				name = v[0] + "_Geo"
			}

			if _, ok := ori[v[0]]; ok {

				ori[name], err = GetGeoInfo(ori[v[0]].(string))
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

var Geo_db *geoip2.Reader

func Utils_init() {

	var err error

	Geo_path := SysConfigGet().Mmdb_path

	Geo_db, err = geoip2.Open(Geo_path)
	if err != nil {
		logger.Warn("Open Geo DB %s error : %s\n", Geo_path, err)
	}

}

func Add_fields_Gen(add_field map[string]string, ori []interface{}) {

	if len(add_field) == 0 {
		return
	}

	for _, i := range ori {

		data := i.(map[string]interface{})

		for k, v := range add_field {
			if _, ok := data[v]; ok {
				data[k] = data[v]
			}
		}
	}
}

func Add_fields_Calculation_Check(add_fields_cal map[string][]string) error {

	if len(add_fields_cal) == 0 {
		return nil
	}

	for _, v := range add_fields_cal {
		if len(v) < 2 {
			logger.Error("add_fields_cal Format Error : Format + Data\n")
			return errors.New("add_fields_cal Format Error : Format + Data")
		}
	}

	return nil
}

func Get_String_With_Fmt_And_Arrlist(format string, arrlist []int) string {

	var l []string

	start := 0

	for _, sub_val := range arrlist {

		i := strings.Index(format, "d")

		if i == -1 {
			logger.Alert("Format error, use default format")
			return ""
		}

		sub_format := format[0 : i+1]

		start = i + 1

		format = format[start:]

		s := fmt.Sprintf(sub_format, sub_val)

		l = append(l, s)
	}

	return strings.Join(l, "")
}

func Add_fields_Calculation(add_fields_cal map[string][]string, ori_data map[string]interface{}) {

	for k, v := range add_fields_cal {

		format := v[0]

		var arrlist []int

		for i := 1; i < len(v); i++ {

			item_name := v[i]

			if val, ok := ori_data[item_name]; ok {

				switch val.(type) {

				case string:
					val_int, _ := strconv.Atoi(val.(string))

					arrlist = append(arrlist, val_int)

				case int:
					arrlist = append(arrlist, val.(int))
				}

			} else {
				logger.Error("Add_fields_Calculation Error : No Item:", item_name)
				return
			}

		}

		ori_data[k] = Get_String_With_Fmt_And_Arrlist(format, arrlist)
	}
}

var Json_Marshal_fd = jsoniter.ConfigCompatibleWithStandardLibrary

func Json_Marshal(data interface{}) []byte {

	bf := bytes.NewBuffer([]byte{})
	jsonEncoder := Json_Marshal_fd.NewEncoder(bf)
	jsonEncoder.SetEscapeHTML(false)
	jsonEncoder.Encode(data)

	return bf.Bytes()
}

func Show_Json_Marshal(data interface{}) {

	b := Json_Marshal(data)

	var out bytes.Buffer
	json.Indent(&out, b, "", "\t")

	fmt.Println(out.String())
}

func GetUTCTimeStampClass(data int64) string {

	if data > 1000000000000000000 {

		n := time.Unix(0, data).UTC().Format("2006-01-02T15:04:05.999Z")
		return n
	} else if data > 100000000000000 {

		n := time.Unix(0, data*1000).UTC().Format("2006-01-02T15:04:05.999Z")
		return n
	} else if data > 10000000000 {

		n := time.Unix(0, data*1000000).UTC().Format("2006-01-02T15:04:05.999Z")
		return n
	} else {
		n := time.Unix(data, 0).UTC().Format("2006-01-02T15:04:05.999Z")
		return n
	}
}

func Exec_Shell(s string) (string, error) {
	cmd := exec.Command("/bin/sh", "-c", s)

	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()

	if err != nil {
		return "", err
	}

	return string(out.Bytes()[0 : len(out.Bytes())-1]), err
}

func ByteSlice(b []byte) ([][]byte, int) {

	var arr [][]byte

	nl := 0

	for {
		i := bytes.IndexByte(b, '\n')

		if i == -1 {
			if len(b[0:]) != 0 {
				arr = append(arr, b[0:])
			} else {
				nl += 1
			}
			break
		}

		if len(b[0:i]) != 0 {
			arr = append(arr, b[0:i])
		} else {
			nl += 1
		}

		b = b[i+1:]
	}

	return arr, len(arr)
}

func ByteSliceIntoString(b []byte) ([]string, int) {

	var o []string

	arr, arrlen := ByteSlice(b)

	for _, v := range arr {
		o = append(o, string(v))
	}

	return o, arrlen
}

func GetFilesList(paths string) ([]string, error) {

	var files_list []string

	_, err := os.Stat(paths)
	if err == nil {
		files_list = append(files_list, paths)
		return files_list, err
	}

	fileName, desc := filepath.Split(paths)

	if fileName == "" {
		fileName = "."
	}

	pattern := path.Ext(desc)

	if pattern == "" {
		_, err := os.Open(paths)
		if err == nil {
			files_list = append(files_list, paths)
			return files_list, nil
		}

		return files_list, errors.New("No Pattern in " + paths)
	}

	_, err = os.Open(fileName)
	if err != nil {
		return files_list, err
	}

	cmd := pattern

	gen_index := strings.IndexRune(pattern, '*')
	if gen_index != -1 {
		cmd = string([]byte(pattern[gen_index+1:]))
	}

	cmd = "\\" + cmd

	reg, err := regexp.Compile(cmd)
	if err != nil {
		return files_list, err
	}

	filepath.Walk(fileName,
		func(path string, f os.FileInfo, err error) error {
			if err != nil {
				fmt.Println(err)
				return err
			}
			if f.IsDir() {
				return nil
			}
			ck_name := f.Name()

			matched := reg.MatchString(ck_name)
			if matched {
				files_list = append(files_list, path)
			}
			return nil
		})

	return files_list, err
}

func Search_date_from_string(data string) string {

	ori := []byte(data)

	i := strings.Index(data, "20")

	if i == -1 {
		return ""
	}

	data_part := ori[i:]

	if len(data_part) < 10 {
		return ""
	}

	if data_part[4] != '-' && data_part[4] != '_' {
		return ""
	}

	target := data_part[0:10]

	return string(target)
}

func EnvGet(env string) string {
	out := os.Getenv(env)

	if out != "" {
		tmp := strings.Replace(out, " ", "", -1)
		logger.Info("Env: %s --- %s", env, tmp)
		return tmp
	}
	return ""
}

func EnvCheck(env string) string {
	if len(env) <= 1 {
		return ""
	}

	tmp := strings.Replace(env, " ", "", -1)

	env_byte := []byte(tmp)

	if env_byte[0] == '$' {
		out := string(env_byte[1:])
		return EnvGet(out)
	}

	return tmp
}

func SiceCheck(string_data string) []string {

	out := strings.Split(string_data, ",")

	return out
}

func EnvSliceVarCheck(string_var []string) []string {

	var _list []string
	for _, s := range string_var {
		ret := EnvCheck(s)
		_list = append(_list, SiceCheck(ret)...)
	}

	return _list
}
func ConvertByte2String(byte []byte, charset Charset) string {

	var str string
	switch charset {
	case GB18030:
		var decodeBytes, _ = simplifiedchinese.GB18030.NewDecoder().Bytes(byte)
		str = string(decodeBytes)
	case UTF8:
		fallthrough // 若此case被执行,dedault也会执行
	default:
		str = string(byte)
	}

	return str
}

func Transform_to_UTF8(transform_to_utf8 []string, ori_data map[string]interface{}, namelist map[int]string) {

	if len(transform_to_utf8) == 0 {
		return
	}
	if len(transform_to_utf8) == 1 && transform_to_utf8[0] == "All" {
		//全字段转码
		for key, value := range ori_data {
			valuestr, _ := value.(string)
			arr := []byte(valuestr)
			str := ConvertByte2String(arr, GB18030)
			ori_data[key] = str
			logger.Debug(key, valuestr, str)
		}
		return
	} else if len(transform_to_utf8) == 1 && strings.LastIndex(transform_to_utf8[0], "-") != -1 {
		//指定字段标号范围转码
		a := strings.Split(transform_to_utf8[0], "-")
		startnum, _ := strconv.Atoi(a[0])
		endnum, _ := strconv.Atoi(a[1])
		for i := startnum; i < endnum+1; i++ {
			fieldname := namelist[i]
			fieldvalue := ori_data[fieldname]
			fieldvaluestr, _ := fieldvalue.(string)
			arr := []byte(fieldvaluestr)
			str := ConvertByte2String(arr, GB18030)
			ori_data[fieldname] = str
			logger.Debug(fieldname, fieldvaluestr, str)
		}
		return
	}

	_, err := strconv.Atoi(transform_to_utf8[0])
	if err != nil {
		//指定字段名转码
		for _, value := range transform_to_utf8 {
			v := ori_data[value]
			vstr, _ := v.(string)
			arr := []byte(vstr)
			str := ConvertByte2String(arr, GB18030)
			ori_data[value] = str
			logger.Debug(value, vstr, str)
		}
		return
	} else {
		//指定字段标号转码
		for _, fieldnum := range transform_to_utf8 {
			fieldnumint, _ := strconv.Atoi(fieldnum)
			fieldname := namelist[fieldnumint]
			fieldvalue := ori_data[fieldname]
			fieldvaluestr, _ := fieldvalue.(string)
			arr := []byte(fieldvaluestr)
			str := ConvertByte2String(arr, GB18030)
			ori_data[fieldname] = str
			logger.Debug(fieldname, fieldvaluestr, str)
		}
		return
	}

}

func Transform_to_UTF8_JSON(transform_to_utf8 []string, oridata map[string]interface{}) {
	logger.Debug("Transform To UTF-8")
	if len(transform_to_utf8) == 0 {
		return
	}
	for _, value := range transform_to_utf8 {
		fieldvalue := oridata[value]
		fieldvaluestr, _ := fieldvalue.(string)
		arr := []byte(fieldvaluestr)
		//logger.Debug(value, fieldvalue, fieldvaluestr, arr)
		str := ConvertByte2String(arr, UTF8)
		//logger.Debug(str)
		oridata[value] = str
	}
}

func GetMysqlConn() *sql.DB {
	Mysql_config := SysConfigGet().Mysql_config
	logger.Info("Mysql_config", Mysql_config)
	username := Mysql_config["username"]
	password := Mysql_config["password"]
	network := Mysql_config["network"]
	server := Mysql_config["server"]
	database := Mysql_config["database"]
	port := Mysql_config["port"]

	dsn := fmt.Sprintf("%s:%s@%s(%s:%d)/%s", username, password, network, server, port, database)

	logger.Info("dsn", dsn)
	DB, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Printf("Open mysql failed,err:%v\n", err)
		return nil
	}
	//最大连接周期，超过时间的连接就close
	DB.SetConnMaxLifetime(100 * time.Second)
	//设置最大连接数
	DB.SetMaxOpenConns(100)
	//设置闲置连接数
	DB.SetMaxIdleConns(16)
	return DB

}

func GetDayGap(now time.Time, before time.Time) int {
	now = now.UTC().Truncate(24 * time.Hour)
	before = before.UTC().Truncate(24 * time.Hour)
	sub := now.Sub(before)
	return int(sub.Hours() / 24)
}
