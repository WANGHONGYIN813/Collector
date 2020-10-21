package plugin

import (
	"reflect"
	"sync/atomic"
	"time"

	jsoniter "github.com/json-iterator/go"
	"sugon.com/WangHongyin/collecter/logger"
)

type FilterJson struct {
	FilterCommon
	output OutputInterface
}

var FilterJsonRegInfo RegisterInfo

func init() {

	v := &FilterJsonRegInfo

	v.Plugin = "Filter"
	v.Type = "Json"
	v.SubType = ""
	v.Vendor = "sugon.com"
	v.Desc = "for json format messge"
	v.init = FilterJsonInit

	FilterRegister(v)
}

func FilterJsonInit(c *YamlCommonData, out interface{}, countptr interface{}, statptr *RouteStatType) interface{} {

	r := new(FilterJson)

	r.config = c
	r.rc = countptr.(*RouteCount)
	r.reginfo = &FilterJsonRegInfo

	r.output = out.(OutputInterface)

	return r
}

func (r *FilterJson) FilterProcess(msg interface{}, msg_num int, addtag interface{}) {

	t := time.Now()

	atomic.AddInt64(&r.rc.InputCount, int64(msg_num))

	num := 0
	var arr []interface{}
	ch := make(chan []interface{})

	switch msg.(type) {

	case [][]byte:

		d := msg.([][]byte)

		for _, v := range d {
			num += 1

			if addtag != nil {
				tags := addtag.(map[string]interface{})
				go r.JsonProcess(v, tags, ch)
			} else {
				go r.JsonProcess(v, nil, ch)

			}
		}

	case []byte:

		d := msg.([]byte)

		num = 1

		if addtag != nil {
			tags := addtag.(map[string]interface{})
			go r.JsonProcess(d, tags, ch)
		} else {
			go r.JsonProcess(d, nil, ch)

		}

	case []map[string]interface{}:

		d := msg.([]map[string]interface{})

		for _, v := range d {
			num += 1

			go r.MapDataProcess(v, addtag, ch)
		}

	default:
		logger.Info("Err Type : ", reflect.TypeOf(msg))
		logger.Info("Data is ", msg)
		return
	} //switch end

	for i := 0; i < num; i++ {
		data := <-ch
		if data != nil {
			arr = append(arr, data...)
		}
	}

	arrlen := len(arr)
	if arrlen == 0 {
		atomic.AddInt64(&r.rc.InvalidCount, int64(msg_num))
		return
	}

	atomic.AddInt64(&r.rc.OutputCount, int64(arrlen))

	logger.Debug("Filer Json Numbers : %d , Cost %s", arrlen, time.Now().Sub(t))

	r.output.OutputProcess(arr, arrlen)

}

func (r *FilterJson) JsonProcess(msg []byte, tags map[string]interface{}, p chan<- []interface{}) {

	var tmp interface{}

	c := r.config.Filter.Json

	var json = jsoniter.ConfigCompatibleWithStandardLibrary

	err := json.Unmarshal(msg, &tmp) //反序列化
	if err != nil {
		logger.Error("Json Unmarshal err is ", err)
		logger.Error("Message is ", string(msg))
		//atomic.AddInt64(&r.rc.InvalidCount, 1)
		p <- nil
		return
	}

	var arr []interface{}

	switch tmp.(type) {

	case map[string]interface{}:

		m := tmp.(map[string]interface{})

		if tags != nil {
			for k, v := range tags {
				m[k] = v
			}
		}

		UpdateGeoData(c.Geoip, m)

		//Transform_to_UTF8_JSON(c.Transform_to_utf8, m)

		arr = append(arr, tmp)

	case []interface{}:

		a := tmp.([]interface{})

		for _, v := range a {

			data := v.(map[string]interface{})

			if tags != nil {
				for k1, v1 := range tags {
					data[k1] = v1
				}
			}

			UpdateGeoData(c.Geoip, data)

			//Transform_to_UTF8_JSON(c.Transform_to_utf8, data)

			arr = append(arr, data)
		}
	default:
		logger.Error("Filter Json : inval data :", tmp)
		atomic.AddInt64(&r.rc.InvalidCount, 1)
		p <- nil
		return
	}

	Add_Fields(r.config.Filter.Json.Add_fields, arr)

	Remove_Fields(r.config.Filter.Json.Remove_fields, arr)

	p <- arr
}

func (r *FilterJson) MapDataProcess(d map[string]interface{}, addtags interface{}, p chan<- []interface{}) {

	var arr []interface{}
	var tmp map[string]interface{}

	c := r.config.Filter.Json
	var json = jsoniter.ConfigCompatibleWithStandardLibrary

	ori := make(map[string]interface{})

	for k, v := range d {

		if k == "Message" {

			err := json.Unmarshal([]byte(v.(string)), &tmp)

			if err != nil {
				logger.Error("err is ", err)
				logger.Error("Message Item is :", v)
				atomic.AddInt64(&r.rc.InvalidCount, 1)
				p <- nil
				return
			}

			for mk, mv := range tmp {
				ori[mk] = mv
			}
		} else {
			ori[k] = v
		}

		if addtags != nil {

			tags := addtags.(map[string]interface{})

			for k1, v1 := range tags {
				ori[k1] = v1
			}
		}

		UpdateGeoData(c.Geoip, ori)

		//Transform_to_UTF8_JSON(c.Transform_to_utf8, ori)

	}

	arr = append(arr, ori)

	Add_Fields(r.config.Filter.Json.Add_fields, arr)

	Remove_Fields(r.config.Filter.Json.Remove_fields, arr)

	p <- arr
}
