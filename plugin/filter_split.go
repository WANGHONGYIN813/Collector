package plugin

import (
	"bytes"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"sugon.com/WangHongyin/collecter/logger"
)

type FilterSplit struct {
	FilterCommon
	output OutputInterface
}

var FilterSplitRegInfo RegisterInfo

var splitepool chan int

func init() {

	v := &FilterSplitRegInfo

	v.Plugin = "Filter"
	v.Type = "Mutate"
	v.SubType = ""
	v.Vendor = "sugon.com"
	v.Desc = "for split message"
	v.init = FilterSplitInit

	FilterRegister(v)

	splitepool = make(chan int, *splitepoolnum)
}

func FilterSplitInit(c *YamlCommonData, out interface{}, countptr interface{}, statptr *RouteStatType) interface{} {

	r := new(FilterSplit)

	r.config = c
	r.rc = countptr.(*RouteCount)
	r.reginfo = &FilterSplitRegInfo

	r.output = out.(OutputInterface)

	return r
}

func (r *FilterSplit) FilterProcess(msg interface{}, msg_num int, addtag interface{}) {

	//var splitepool chan int

	//splitepool = make(chan int, *splitepoolnum)

	t := time.Now()

	atomic.AddInt64(&r.rc.InputCount, int64(msg_num))

	filter := r.config.Filter

	sf := filter.Mutate.SplitItem.SplitFlag
	namelist := filter.Mutate.Split_fields
	convertlist := filter.Mutate.Convert
	removelist := filter.Mutate.Remove_fields

	num := 0

	ch := make(chan map[string]interface{})

	switch msg.(type) {
	case [][]byte:

		d := msg.([][]byte)

		if r.config.Filter.Mutate.Direct_operation == true {

			controlch := make(chan int, 1)

			bulknum := r.config.Output.Elasticsearch.Bulk
			//logger.Debug("bulknum:", bulknum)
			msglen := len(d)
			//logger.Debug("meglen:", msglen)
			bg := msglen / bulknum //求整
			//logger.Debug("bg", bg)
			yushu := msglen % bulknum //求余
			//logger.Debug("yushu", yushu)
			if yushu != 0 {
				bg = bg + 1
			}
			var j int
			var l int
			for i := 0; i < bg; i++ {
				controlch <- 1
				l = i
				//logger.Debug("i+1:", i+1)

				if (i+1)*bulknum > msglen {
					j = msglen
				} else {
					j = (i + 1) * bulknum
				}

				//logger.Debug("j:", j)
				//logger.Debug("i*bulknum", i*bulknum)
				splitepool <- 1
				go func() {

					defer PanicHandler()

					t := time.Now()
					datacount := 0
					min := l * bulknum
					max := j
					<-controlch
					buf := bytes.NewBuffer(nil)

					tags := addtag.(map[string]interface{})

					//logger.Debug(i*bulknum, j)
					//logger.Debug(min, max)
					for _, value := range d[min:max] {
						ret := r.SplitAndStorageProcess(tags, value, sf, namelist, convertlist, removelist)
						if ret != nil {
							datacount++
							b := Json_Marshal(ret)
							buf.Write(bulk_flag_byte)
							buf.Write(b)
							buf.Write(return_flag)
						}

					}

					c := r.config.Output.Elasticsearch
					h := c.EsUrl
					es_target_num := len(h)
					index := rand.Intn(es_target_num)

					oldindexname := c.Index
					var newindexname string

					if c.Index_by_day {

						tag := "-" + time.Now().Format("2006-01-02")

						newindexname = c.Index + tag

					}

					oldurl := h[index]
					url := strings.Replace(oldurl, oldindexname, newindexname, 1)

					_, err := http.Post(url, "application/json", strings.NewReader(buf.String()))
					if err != nil {

						atomic.AddInt64(&r.rc.ErrCount, int64(datacount))
						logger.Error("Bulk ERROR :", err)

					} else {

						atomic.AddInt64(&r.rc.OutputCount, int64(datacount))

						logger.Debug("Inputdata %d : Filite And To Es Total Cost %s", datacount, time.Now().Sub(t))

					}

					<-splitepool

				}()

			}

			return

		} else {

			for _, v := range d {
				num += 1

				//logger.Alert("Current  Splite Routine Pool Left :", *splitepoolnum-len(splitepool))

				//splitepool <- 1

				if addtag != nil {
					tags := addtag.(map[string]interface{})
					go r.SplitProcess(tags, v, sf, namelist, convertlist, removelist, ch)
				} else {
					go r.SplitProcess(nil, v, sf, namelist, convertlist, removelist, ch)
				}
			}
		}

	case []byte:
		d := msg.([]byte)
		num += 1

		//splitepool <- 1

		if addtag != nil {
			tags := addtag.(map[string]interface{})
			go r.SplitProcess(tags, d, sf, namelist, convertlist, removelist, ch)
		} else {
			go r.SplitProcess(nil, d, sf, namelist, convertlist, removelist, ch)
		}

	}

	var arr []interface{}

	for i := 0; i < num; i++ {
		data := <-ch
		if data != nil {
			arr = append(arr, data)
		}
	}

	arrlen := len(arr)

	if arrlen == 0 {
		return
	}

	atomic.AddInt64(&r.rc.OutputCount, int64(arrlen))

	logger.Debug("FilerSplit Lines : %d , Cost %s", arrlen, time.Now().Sub(t))

	r.output.OutputProcess(arr, arrlen)
}

func (r *FilterSplit) SplitProcess(tags map[string]interface{}, msg []byte, sf string, name map[int]string, convert [][]string, remove []string, p chan<- map[string]interface{}) {

	OriData := make(map[string]interface{})

	split_item := strings.Split(string(msg), sf)

	slen := len(split_item)

	if slen != len(name) {
		atomic.AddInt64(&r.rc.InvalidCount, 1)
		logger.Error("Err Message %s, Targen len = %d, Current Len = %d\n", string(msg), len(name), len(split_item))
		p <- nil
		return
	}

	if tags != nil {
		for k, v := range tags {
			OriData[k] = v
		}
	}

	for k, v := range split_item {
		if name[k] == "" {
			continue
		}
		OriData[name[k]] = v
	}

	for _, v := range convert {
		if OriData[v[0]] != nil {
			OriData[v[0]], _ = strconv.Atoi(OriData[v[0]].(string))
		}
	}

	UpdateGeoData(r.config.Filter.Mutate.Geoip, OriData)

	Add_Fields(r.config.Filter.Mutate.Add_fields, OriData)

	Transform_Fields(r.config.Filter.Mutate.Transform_fields, OriData)

	Add_fields_Calculation(r.config.Filter.Mutate.Add_fields_cal, OriData)

	Transform_to_UTF8(r.config.Filter.Mutate.Transform_to_utf8, OriData, name)

	for _, v := range remove {
		delete(OriData, v)
	}

	p <- OriData
}

func (r *FilterSplit) SplitAndStorageProcess(tags map[string]interface{}, msg []byte, sf string, name map[int]string, convert [][]string, remove []string) map[string]interface{} {

	OriData := make(map[string]interface{})

	split_item := strings.Split(string(msg), sf)

	slen := len(split_item)

	if slen != len(name) {
		atomic.AddInt64(&r.rc.InvalidCount, 1)
		logger.Error("Err Message %s, Targen len = %d, Current Len = %d\n", string(msg), len(name), len(split_item))

		return nil
	}

	if tags != nil {
		for k, v := range tags {
			OriData[k] = v
		}
	}

	for k, v := range split_item {
		if name[k] == "" {
			continue
		}
		OriData[name[k]] = v
	}

	for _, v := range convert {
		if OriData[v[0]] != nil {
			OriData[v[0]], _ = strconv.Atoi(OriData[v[0]].(string))
		}
	}

	UpdateGeoData(r.config.Filter.Mutate.Geoip, OriData)

	Add_Fields(r.config.Filter.Mutate.Add_fields, OriData)

	Transform_Fields(r.config.Filter.Mutate.Transform_fields, OriData)

	Add_fields_Calculation(r.config.Filter.Mutate.Add_fields_cal, OriData)

	Transform_to_UTF8(r.config.Filter.Mutate.Transform_to_utf8, OriData, name)

	for _, v := range remove {
		delete(OriData, v)
	}

	return OriData

}
