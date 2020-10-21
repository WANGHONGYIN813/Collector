package plugin

import "sugon.com/WangHongyin/collecter/logger"

var FilterRegisterInfoList []*RegisterInfo

/***************************************************/

func FilterRegister(r *RegisterInfo) {
	FilterRegisterInfoList = append(FilterRegisterInfoList, r)
}

func FilterPluginProbe(ftype string) (*RegisterInfo, error) {

	if ftype != "" {
		for _, v := range FilterRegisterInfoList {
			if ftype == v.Type {
				return v, nil
			}
		}
	}

	logger.Info("No Filter plugin match. Use empty filter")

	return &FilterDefaultInfo, nil
}

/**************************************************/

/**************** Default Filter ************************/

type FilterDefault struct {
	FilterCommon
	output OutputInterface
}

var FilterDefaultInfo RegisterInfo

func init() {
	v := &FilterDefaultInfo
	v.init = FilterDefaultInit
}

func FilterDefaultInit(c *YamlCommonData, out interface{}, countptr interface{}, statptr *RouteStatType) interface{} {

	r := new(FilterDefault)

	r.output = out.(OutputInterface)

	return r
}

func (r *FilterDefault) FilterProcess(msg interface{}, num int, addtag interface{}) {

	if num == -1 {
		r.output.OutputProcess(msg, 1)
		return
	}

	var arr []interface{}

	switch msg.(type) {

	case []string:

		d := msg.([]string)

		for _, v := range d {
			arr = append(arr, r.MsgProcess(v, addtag))
		}

	case [][]byte:

		d := msg.([][]byte)

		for _, v := range d {
			arr = append(arr, r.MsgProcess(v, addtag))
		}

	case []byte:

		d := msg.([]byte)

		arr = append(arr, r.MsgProcess(d, addtag))

	case []map[string]interface{}:

		d := msg.([]map[string]interface{})

		OriData := make(map[string]interface{})

		for _, v := range d {

			for k1, v1 := range v {
				OriData[k1] = v1

				if addtag != nil {
					tags := addtag.(map[string]interface{})

					for k2, v2 := range tags {
						OriData[k2] = v2
					}
				}
			}

			arr = append(arr, OriData)
		}
	}
	//logger.Error(len(arr), num)
	r.output.OutputProcess(arr, num) //arr是一个map数组，num表示数组里有多少map

	return
}

/**************************************************************/

func (r *FilterDefault) MsgProcess(b interface{}, addtag interface{}) map[string]interface{} {

	OriData := make(map[string]interface{})
	OriData["Message"] = b //b是字节数组，即一行字符串

	if addtag != nil {
		tags := addtag.(map[string]interface{})
		for k, v := range tags {
			OriData[k] = v
		}
	}

	return OriData
}
