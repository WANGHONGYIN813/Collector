package plugin

import (
	"encoding/hex"
	"errors"
	"reflect"
	"strconv"
	"sync/atomic"
	"time"

	"sugon.com/WangHongyin/collecter/logger"
)

/**************************************/

var FilterTlvRegInfo RegisterInfo

func init() {

	v := &FilterTlvRegInfo

	v.Plugin = "Filter"
	v.Type = "United_tlv"
	v.SubType = ""
	v.Vendor = "sugon.com"
	v.Desc = "for tlv format messge"
	v.init = Filter_Tlv_Init

	FilterRegister(v)
}

/**************************************/

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

/**************************************/

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

/**************************************/

type Tlv_Config_Contenter struct {
	TLV_Map map[int][2]string
	DocType string
}

type FilterTlv struct {
	FilterCommon
	output        OutputInterface
	TLV_Parse_Map map[int]map[int]Tlv_Config_Contenter
}

func Filter_Tlv_Init(c *YamlCommonData, out interface{}, countptr interface{}, statptr *RouteStatType) interface{} {

	r := new(FilterTlv)

	r.config = c
	r.rc = countptr.(*RouteCount)
	r.reginfo = &FilterTlvRegInfo

	r.output = out.(OutputInterface)

	err := r.Tlv_Map_Config_Init(c.Filter.United_tlv)

	if err != nil {
		return nil
	}

	return r
}

func (r *FilterTlv) Tlv_Config_Parse(config []Tlv_Config_Format_t) map[int][2]string {

	tlv_config_map := make(map[int][2]string)

	for _, v := range config {

		tag_name := v.TagName
		tag_key := v.TagKey
		tag_type := v.TagType

		tlv_config_map[tag_key] = [2]string{tag_name, tag_type}
	}

	return tlv_config_map
}

func (r *FilterTlv) Tlv_Map_Config_Init(united_tlv_config []map[string]map[string]interface{}) error {

	var United_Tlv United_TlvModule_t

	for _, u := range united_tlv_config {

		if _, ok := u["tlv"]; !ok {
			continue
		}

		v := u["tlv"]

		var tmp TlvModule_t

		if _, ok := v["Doc_type"]; ok {
			tmp.Doc_type = v["doc_type"].(string)
		}

		if _, ok := v["log_type"]; ok {
			tmp.Log_type = v["log_type"].(int)
		} else {
			logger.Error("log_type Do not exist : %+v\n!", v)
			return errors.New("log_type Do not exist")
		}

		if _, ok := v["proto_type"]; ok {
			tmp.Proto_type = v["proto_type"].(int)
		} else {
			logger.Error("proto_type Do not exist!:%+v\n", v)
			return errors.New("proto_type Do not exist")
		}

		if _, ok := v["tlv_config"]; ok {
			for _, __u := range v["tlv_config"].([]interface{}) {

				__v := __u.([]interface{})

				var tt Tlv_Config_Format_t

				tt.TagName = __v[0].(string)
				tt.TagKey = __v[1].(int)

				if len(__v) == 3 {
					tt.TagType = __v[2].(string)
				} else {
					tt.TagType = "i"
				}

				tmp.Tlv_config = append(tmp.Tlv_config, tt)
			}
		} else {
			logger.Error("tlv_config Do not exist!")
			return errors.New("tlv_config Do not exist")
		}

		United_Tlv.Tlv = append(United_Tlv.Tlv, tmp)
	}

	r.TLV_Parse_Map = make(map[int]map[int]Tlv_Config_Contenter)

	for _, v := range United_Tlv.Tlv {

		tlv_config_map := r.Tlv_Config_Parse(v.Tlv_config)

		var Tlv_Contenter Tlv_Config_Contenter

		Tlv_Contenter.TLV_Map = tlv_config_map

		if v.Doc_type != "" {
			Tlv_Contenter.DocType = v.Doc_type
		}

		if _, ok := r.TLV_Parse_Map[v.Log_type]; !ok {
			r.TLV_Parse_Map[v.Log_type] = make(map[int]Tlv_Config_Contenter)
		}

		r.TLV_Parse_Map[v.Log_type][v.Proto_type] = Tlv_Contenter
	}

	return nil
}

func (r *FilterTlv) FilterProcess(msg interface{}, msg_num int, addtag interface{}) {

	t := time.Now()

	atomic.AddInt64(&r.rc.InputCount, int64(msg_num))

	num := 0
	var arr []interface{}

	ch := make(chan interface{})

	switch msg.(type) {

	case [][]byte:

		/*
			d := msg.([][]byte)

			for _, v := range d {
				num += 1

				if addtag != nil {
					tags := addtag.(map[string]interface{})
					go r.TlvProcess(v, tags, ch)
				} else {
					go r.TlvProcess(v, nil, ch)

				}
			}
		*/

	case []byte:

		d := msg.([]byte)

		num = 1

		if addtag != nil {
			tags := addtag.(map[string]interface{})

			go r.TlvProcess(d, tags, ch)
		} else {
			go r.TlvProcess(d, nil, ch)
		}

	default:
		logger.Info("Err Type : ", reflect.TypeOf(msg))
		logger.Info("Data is ", msg)
		return
	}

	for i := 0; i < num; i++ {
		data := <-ch
		if data != nil {
			arr = append(arr, data)
		}
	}

	arrlen := len(arr)
	if arrlen == 0 {
		atomic.AddInt64(&r.rc.InvalidCount, int64(msg_num))
		return
	}

	atomic.AddInt64(&r.rc.OutputCount, int64(arrlen))

	logger.Debug("Filer Tlv Numbers : %d , Cost %s", arrlen, time.Now().Sub(t))

	r.output.OutputProcess(arr, arrlen)

}

func (r *FilterTlv) TlvProcess(buf []byte, tags map[string]interface{}, p chan<- interface{}) {

	var output = make(map[string]interface{})

	tlv_message := new(st_x10_log_head)

	tlv_message.Head.Major_ver = buf[0]
	tlv_message.Head.Minor_ver = buf[1]

	tlv_message.Head.length = Read_Byte_To_Int(buf, 2, 2)

	tlv_message.Msg_type = Read_Byte_To_Int(buf, 4, 1)
	tlv_message.Proto_type = Read_Byte_To_Int(buf, 5, 1)

	tlv_body := buf[6:]
	n := len(tlv_body)

	var tlv_config_map map[int][2]string

	doctype := ""

	if _, ok := r.TLV_Parse_Map[tlv_message.Msg_type]; ok {

		if _, ok := r.TLV_Parse_Map[tlv_message.Msg_type][tlv_message.Proto_type]; ok {

			tlv_config_map = r.TLV_Parse_Map[tlv_message.Msg_type][tlv_message.Proto_type].TLV_Map

			doctype = r.TLV_Parse_Map[tlv_message.Msg_type][tlv_message.Proto_type].DocType

		} else {
			logger.Warn("Can not Find Config Map For Proto_type %+v\n", tlv_message)
			p <- nil
			return
		}
	} else {
		logger.Warn("Can not Find Config Map For Msg_type %+v\n", tlv_message)
		p <- nil
		return
	}

	if doctype != "" {
		output["DocType"] = doctype
	}

	output["LogType"] = tlv_message.Msg_type
	output["ProtoType"] = tlv_message.Proto_type

	for i := 0; i < n; {

		len_offset := i + 2
		val_offset := i + 4

		Message_type := Read_Byte_To_Int(tlv_body, i, 2)
		Message_len := Read_Byte_To_Int(tlv_body, len_offset, 2)

		var Value interface{}

		Name := tlv_config_map[Message_type][0]
		tag_type := tlv_config_map[Message_type][1]

		switch tag_type {

		case "s":

			ret := tlv_body[val_offset : val_offset+Message_len]

			if ret[0] != 0 {
				Value = string(ret)
			} else {
				Value = ""
			}

		case "i":

			Value = Read_Byte_To_Int(tlv_body, val_offset, Message_len)

		case "l":

			Value = Read_Byte_To_Long(tlv_body, val_offset)
		}

		//logger.Info("name : ", Name, "tag_type : ", tag_type, "Value :", Value)

		output[Name] = Value

		i += 4 + Message_len

	}

	p <- output

}
