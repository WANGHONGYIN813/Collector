package plugin

import (
	"errors"
	"strings"
	"sync/atomic"
	"time"

	"sugon.com/WangHongyin/collecter/logger"
)

type FilterMutateMap struct {
	FilterCommon
	output OutputInterface
}

var FilterMutateMapRegInfo RegisterInfo

func init() {

	v := &FilterMutateMapRegInfo

	v.Plugin = "Filter"
	v.Type = "Mutatemap"
	v.SubType = ""
	v.Vendor = "sugon.com"
	v.Desc = "for Mutate_map split message"
	v.init = FilterMutateMapInit

	FilterRegister(v)
}

func FilterMutateMapInit(c *YamlCommonData, out interface{}, countptr interface{}, statptr *RouteStatType) interface{} {

	r := new(FilterMutateMap)

	r.config = c
	r.rc = countptr.(*RouteCount)
	r.reginfo = &FilterMutateMapRegInfo

	r.output = out.(OutputInterface)

	return r
}

func (r *FilterMutateMap) FilterProcess(msg interface{}, msg_num int, addtag interface{}) {

	t := time.Now()

	switch msg.(type) {

	case []map[string]interface{}:

		break

	default:
		logger.Error("Msg Type Not support!")
		atomic.AddInt64(&r.rc.InvalidCount, int64(msg_num))
		return
	}

	d := msg.([]map[string]interface{})

	atomic.AddInt64(&r.rc.InputCount, int64(msg_num))

	m := r.config.Filter.Mutatemap

	num := 0

	ch := make(chan map[string]interface{})

	for _, v := range d {
		num += 1

		go r.MutateMapProcess(v, m.SplitItem, m.SplitMap_Item, m.Remove_fields, addtag, ch)
	}

	var arr []interface{}

	for i := 0; i < num; i++ {
		data := <-ch
		if data != nil {
			//logger.Debug("%+v\n", data)
			arr = append(arr, data)
		}
	}

	arrlen := len(arr)

	if arrlen == 0 {
		return
	}

	atomic.AddInt64(&r.rc.OutputCount, int64(arrlen))

	logger.Debug("Mutatemap Lines : %d , Cost %s", arrlen, time.Now().Sub(t))

	r.output.OutputProcess(arr, arrlen)

}

func (r *FilterMutateMap) MutateMapProcess(ori map[string]interface{}, split SplitItem_t, sub_split SplitItem_t, remove []string, tags interface{}, p chan<- map[string]interface{}) {

	err := MesssageParse(ori, ori[split.SplitMsg].(string), split.SplitFlag, sub_split.SplitFlag)
	if err != nil {
		atomic.AddInt64(&r.rc.InvalidCount, 1)
		p <- nil
		return
	}

	if tags != nil {
		t := tags.(map[string]interface{})
		for k, v := range t {
			ori[k] = v
		}
	}

	for _, v := range remove {
		delete(ori, v)
	}

	m := r.config.Filter.Mutatemap

	UpdateGeoData(m.Geoip, ori)

	p <- ori
}

func MesssageParse(ori map[string]interface{}, m string, split_flag string, sub_split_flag string) error {

	newm := strings.Replace(m, split_flag, "\t", -1)

	var ss string

	for {
		if newm == "" {
			break
		}

		if strings.Contains(newm, "\"") {

			l := 0
			i := strings.Index(newm, "\"")
			l += i
			ss += newm[0:i]
			tar := newm[i+1:]

			l += 1
			i2 := strings.Index(tar, "\"")

			if i2 == -1 {
				return errors.New("Format error")
			}

			tar2 := tar[:i2]
			l += i2
			l += 1

			tmp := strings.Replace(tar2, "\t", " ", -1)
			ss += tmp
			newm = newm[l:]

		} else {
			if newm != "" {
				ss += newm
			}

			break
		}
	}

	item := strings.Split(ss, "\t")

	for _, v := range item {
		sub_item := strings.Split(v, sub_split_flag)

		if len(sub_item) == 2 {
			ori[sub_item[0]] = sub_item[1]
		} else {
			ori["Content"] = v
		}
	}

	return nil
}
