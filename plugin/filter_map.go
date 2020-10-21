package plugin

import (
	"sync/atomic"
	"time"

	"sugon.com/WangHongyin/collecter/logger"
)

type FilterMap struct {
	FilterCommon
	output OutputInterface
}

var FilterMapRegInfo RegisterInfo

func init() {

	v := &FilterMapRegInfo

	v.Plugin = "Filter"
	v.Type = "Map"
	v.SubType = ""
	v.Vendor = "sugon.com"
	v.Desc = "for map[string]interface{} format "
	v.init = FilterMapInit

	FilterRegister(v)
}

func FilterMapInit(c *YamlCommonData, out interface{}, countptr interface{}, statptr *RouteStatType) interface{} {

	r := new(FilterMap)

	r.config = c
	r.rc = countptr.(*RouteCount)
	r.reginfo = &FilterMapRegInfo

	r.output = out.(OutputInterface)

	return r
}

func (r *FilterMap) FilterProcess(msg interface{}, num int, addtag interface{}) {

	t := time.Now()

	d := msg.(map[string]interface{})

	var arr []interface{}

	atomic.AddInt64(&r.rc.InputCount, int64(num))

	tags := addtag.(map[string]interface{})

	for k, v := range tags {
		d[k] = v
	}

	arr = append(arr, d)

	lenarr := len(arr)

	atomic.AddInt64(&r.rc.OutputCount, int64(lenarr))

	logger.Debug("Filer Map Numbers Cost %s", time.Now().Sub(t))

	r.output.OutputProcess(arr, lenarr)

}
