package plugin

import (
	"fmt"
	//"reflect"
	"sync/atomic"

	"sugon.com/WangHongyin/collecter/logger"
)

var OutputRegisterInfoList []*RegisterInfo

/***************************************************/

func OutputRegister(r *RegisterInfo) {
	OutputRegisterInfoList = append(OutputRegisterInfoList, r)
}

func OutputPluginProbe(ftype string) (*RegisterInfo, error) {

	if ftype != "" {
		for _, v := range OutputRegisterInfoList {
			if ftype == v.Type {
				return v, nil
			}
		}
	}

	logger.Info("No Output plugin match. Use default output")

	return &OutputDefaultInfo, nil
}

/**************** Default(Console) Output ************************/

type OutputDefault struct {
	OutputCommon
}

var OutputDefaultInfo RegisterInfo

func init() {
	v := &OutputDefaultInfo

	v.Plugin = "Output"
	v.Type = "Console"
	v.SubType = ""
	v.Vendor = "sugon.com"
	v.Desc = "for Console stdout output"
	v.init = OutputDefaultInit

	OutputRegister(v)
}

func OutputDefaultInit(c *YamlCommonData, out interface{}, countptr interface{}, statptr *RouteStatType) interface{} {

	r := new(OutputDefault)

	r.reginfo = &OutputDefaultInfo

	r.config = c
	r.rc = countptr.(*RouteCount)

	return r
}

func (r *OutputDefault) OutputProcess(msg interface{}, num int) {

	rc := r.config.Output.Console

	atomic.AddInt64(&r.rc.InputCount, int64(num))

	if rc.Quietmode {

		atomic.AddInt64(&r.rc.OutputCount, int64(num))
		return
	}

	//fmt.Println(reflect.TypeOf(msg))

	switch msg.(type) {

	case []byte:
		fmt.Println(string(msg.([]byte)))

	default:
		Show_Json_Marshal(msg)
	}

	atomic.AddInt64(&r.rc.OutputCount, int64(num))
}

/**************************************************************/
