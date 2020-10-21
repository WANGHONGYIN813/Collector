package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/fatih/structs"
	"github.com/json-iterator/go"
	yaml "gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

var config_file = flag.String("f", "", "config file")
var tlv_bin_file = flag.String("b", "", "tlv bin file")
var debug_flag = flag.Bool("D", false, "debug flag")
var json_flag = flag.Bool("j", false, "output json flag")

type st_tlv_data struct {
	Message_type int
	Message_len  int
	Name         string
	Value        interface{}
}

type header_t struct {
	Major_ver byte
	Minor_ver byte
	length    int
}

type st_x10_log_head struct {
	Head       header_t
	Msg_type   int
	Proto_type int
	Tlvs       []*st_tlv_data
}

func Read_Byte_To_Int(buf []byte, offset int, num int) int {
	s := hex.EncodeToString(buf[offset : num+offset])
	n, _ := strconv.ParseInt(s, 16, 0)
	return int(n)
}

func Read_Byte_To_Long(buf []byte, offset int) int64 {

	first_part := hex.EncodeToString(buf[offset : offset+4])
	second_part := hex.EncodeToString(buf[offset+4 : offset+8])

	s := second_part + first_part
	n, _ := strconv.ParseInt(s, 16, 0)
	return n
}

type TLV_Message_t struct {
	Msg_type   int        `yaml:msg_type`
	Proto_type int        `yaml:proto_type`
	Tlv_config [][]string `yaml:tlv_config`
}

type TLV_Common_Message_t struct {
	Tlv_config [][]string `yaml:tlv_config`
}

var TLV_Message_Parse_Map map[int]map[int][2]string
var TLV_Common_Map map[int][2]string

type YamlCommonData struct {
	Tlv        TLV_Message_t        `yaml:tlv`
	Tlv_common TLV_Common_Message_t `yaml:tlv_common`
}

func ParseYamlBaseConfig(yamlfile string) (YamlCommonData, error) {

	var config YamlCommonData

	yamlFile, err := ioutil.ReadFile(yamlfile)
	if err != nil {
		fmt.Printf("yamlFile.Get %s err #%v \n", yamlfile, err)
		return config, err
	}

	_ = yaml.Unmarshal(yamlFile, &config)

	return config, nil
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

func ConfigWalk(target_path string) ([]string, error) {

	var outputlist []string

	f, err := os.Stat(target_path)
	if err != nil {
		return outputlist, err
	}

	if f.IsDir() {

		outputlist, err = WalkDir(target_path, ".yaml")
		if err != nil {
			fmt.Println("Open err:", target_path, err)
			return outputlist, err
		}
		if len(outputlist) == 0 {
			fmt.Println("No Yaml Configs in ", target_path)
			return outputlist, errors.New("No Yaml Configs")
		}
	} else {
		outputlist = append(outputlist, target_path)
	}

	return outputlist, nil
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

func Tlv_Config_Parse(Tlv_config [][]string) map[int][2]string {

	tlv_config_map := make(map[int][2]string)

	for _, v := range Tlv_config {

		tag_name := v[0]
		tag_key, _ := strconv.Atoi(v[1])
		tag_type := "i"

		if len(v) == 3 {
			if v[2] == "s" || v[2] == "string" {
				tag_type = "s"
			} else if v[2] == "l" || v[2] == "long" {
				tag_type = "l"
			}
		}

		tlv_config_map[tag_key] = [2]string{tag_name, tag_type}
	}

	return tlv_config_map
}

func Tlv_Map_Config_Init(configfilelist []string) {

	TLV_Message_Parse_Map = make(map[int]map[int][2]string)

	for _, v := range configfilelist {

		config, _ := ParseYamlBaseConfig(v)

		if *debug_flag {
			fmt.Println("Load Config File", v)
		}

		li := SearchItemNameList_for_NotZero(config)

		for _, __v := range li {

			if __v == "Tlv" {

				tlv_config_map := Tlv_Config_Parse(config.Tlv.Tlv_config)

				TLV_Message_Parse_Map[config.Tlv.Proto_type] = tlv_config_map

			}
			if __v == "Tlv_common" {

				TLV_Common_Map = Tlv_Config_Parse(config.Tlv_common.Tlv_config)

			}
		}

	}

}

func init() {

	flag.Parse()

	if *config_file == "" || *tlv_bin_file == "" {
		fmt.Println("-h for help")
		os.Exit(1)
	}

	f, err := ConfigWalk(*config_file)
	if err != nil {
		os.Exit(1)
	}

	Tlv_Map_Config_Init(f)
}

/*****************************************************/

func tlv_message_parse(buf []byte) map[string]interface{} {

	tlv_message := new(st_x10_log_head)

	tlv_message.Head.Major_ver = buf[0]
	tlv_message.Head.Minor_ver = buf[1]

	tlv_message.Head.length = Read_Byte_To_Int(buf, 2, 2)

	tlv_message.Msg_type = Read_Byte_To_Int(buf, 4, 1)
	tlv_message.Proto_type = Read_Byte_To_Int(buf, 5, 1)

	var output = make(map[string]interface{})
	tlv_body := buf[6:]

	n := len(tlv_body)

	var tlv_config_map map[int][2]string

	if _, ok := TLV_Message_Parse_Map[tlv_message.Proto_type]; ok {
		tlv_config_map = TLV_Message_Parse_Map[tlv_message.Proto_type]
	} else {
		fmt.Printf("Can not Find Config Map For Proto_type %+v\n", tlv_message)
	}

	output["Log_type"] = tlv_message.Msg_type
	output["Proto_type"] = tlv_message.Proto_type

	for i := 0; i < n; {

		var tlv_tag st_tlv_data

		tag_offset := i
		len_offset := i + 2
		val_offset := i + 4

		tlv_tag.Message_type = Read_Byte_To_Int(tlv_body, tag_offset, 2)
		tlv_tag.Message_len = Read_Byte_To_Int(tlv_body, len_offset, 2)

		tag_type := ""

		com_Name := ""
		s_Name := ""

		if tlv_config_map != nil {

			if TLV_Common_Map != nil {
				com_Name = TLV_Common_Map[tlv_tag.Message_type][0]
			}

			s_Name = tlv_config_map[tlv_tag.Message_type][0]

			if s_Name == "" {
				tlv_tag.Name = com_Name
				tag_type = TLV_Common_Map[tlv_tag.Message_type][1]
			} else if com_Name == "" {
				tlv_tag.Name = s_Name
				tag_type = tlv_config_map[tlv_tag.Message_type][1]
			} else {

				if tlv_tag.Message_len == 8 {
					tlv_tag.Name = com_Name
					tag_type = TLV_Common_Map[tlv_tag.Message_type][1]
				} else {
					tlv_tag.Name = s_Name
					tag_type = tlv_config_map[tlv_tag.Message_type][1]
				}
			}

			if tag_type == "s" {

				ret := tlv_body[val_offset : val_offset+tlv_tag.Message_len]

				if ret[0] != 0 {
					tlv_tag.Value = string(ret)
				} else {
					tlv_tag.Value = ""
				}

			} else if tag_type == "i" {

				tlv_tag.Value = Read_Byte_To_Int(tlv_body, val_offset, tlv_tag.Message_len)

			} else if tag_type == "l" {

				tlv_tag.Value = Read_Byte_To_Long(tlv_body, val_offset)
			}
		} else {
			tlv_tag.Value = string(tlv_body[val_offset : val_offset+tlv_tag.Message_len])
		}

		if *debug_flag {
			fmt.Printf("tlv_tag : %+v\n", tlv_tag)
		}

		output[tlv_tag.Name] = tlv_tag.Value

		i += 4 + tlv_tag.Message_len

	}

	return output
}

/***********************************************************************/

var Json_Marshal_fd = jsoniter.ConfigCompatibleWithStandardLibrary

func Json_Marshal(data interface{}) []byte {

	bf := bytes.NewBuffer([]byte{})
	jsonEncoder := Json_Marshal_fd.NewEncoder(bf)
	jsonEncoder.SetEscapeHTML(false)
	jsonEncoder.Encode(data)

	return bf.Bytes()
}

func main() {

	buf, err := ioutil.ReadFile(*tlv_bin_file)
	if err != nil {
		os.Exit(1)
	}

	ret := tlv_message_parse(buf)

	if !*json_flag {
		b := Json_Marshal(ret)

		var out bytes.Buffer
		json.Indent(&out, b, "", "\t")
		fmt.Println(out.String())

	} else {
		fmt.Printf("%+v\n", ret)
	}

}
