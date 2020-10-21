package plugin

import (
	"bytes"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	jsoniter "github.com/json-iterator/go"
	"sugon.com/WangHongyin/collecter/logger"
)

type OutputEs struct {
	OutputCommon
	Bulk                int
	msg                 *[]interface{}
	buffer              []*bytes.Buffer
	bufferlines         []int
	OuputFunc           func([][2]int, interface{}) int
	es_setting_url_list map[string]bool
}

//var esconnpool chan int

var es_setting_url_list map[string]bool
var es_mappings_url_list map[string]bool

var sendmode string

var OutputEsRegInfo RegisterInfo

type ES_Error_Item_t struct {
	Index map[string]interface{} `json:"index"`
}

type ES_Error_t struct {
	Took   int               `json:"took"`
	Errors bool              `json:"errors"`
	Items  []ES_Error_Item_t `json:"items"`
}

func init() {

	OutputEsRegInfo.Plugin = "Output"
	OutputEsRegInfo.Type = "Elasticsearch"
	OutputEsRegInfo.SubType = ""
	OutputEsRegInfo.Vendor = "sugon.com"
	OutputEsRegInfo.Desc = "for elasticsearch output"
	OutputEsRegInfo.init = OutputEsRouteInit

	OutputRegister(&OutputEsRegInfo)

	es_setting_url_list = make(map[string]bool)
	es_mappings_url_list = make(map[string]bool)
	//esconnpool = make(chan int, *esconnpoolnum)
}

func OutputEsRouteInit(c *YamlCommonData, out interface{}, countptr interface{}, statptr *RouteStatType) interface{} {

	if len(c.Output.Elasticsearch.Host) != 0 {

		output_es_init(c)
	}

	r := new(OutputEs)

	r.config = c
	r.rc = countptr.(*RouteCount)
	r.reginfo = &OutputEsRegInfo

	r.Bulk = c.Output.Elasticsearch.Bulk

	esconfig := c.Output.Elasticsearch

	switch esconfig.esmode {
	case EsMode_COPY:
		r.OuputFunc = r.OutputProcessEsCopy
	case EsMode_RANDOM:
		r.OuputFunc = r.OutputProcessEsRandom
	case EsMode_POLLING:
		r.OuputFunc = r.OutputProcessEsPolling
	}

	sendmode = r.config.Output.Elasticsearch.Sendmode

	r.es_setting_url_list = make(map[string]bool)
	return r
}

func output_es_init(config *YamlCommonData) {

	config.Output.Elasticsearch.enable = true

	var err error

	if config.Output.Elasticsearch.Mapping_load != "" {

		config.Output.Elasticsearch.Mapping_string, err = MappingFileParse(config.Output.Elasticsearch.Mapping_load)

		if err != nil {
			log.Println("Init mapping err:", err)
		}

		if config.Output.Elasticsearch.Index_by_day {

			if config.Output.Elasticsearch.Index != "" {

				if config.Output.Elasticsearch.Cron_desc == "" {

					config.Output.Elasticsearch.Cron_desc = SysConfig.Sys_cron

				}

				if config.Output.Elasticsearch.esmode == EsMode_COPY {

					for _, v := range config.Output.Elasticsearch.Host {

						base_url := "http://" + v + "/" + config.Output.Elasticsearch.Index

						go LoadMapping_CronJob_Start(config.Output.Elasticsearch.Cron_desc, base_url, config.Output.Elasticsearch.Mapping_string)

					}

				} else {

					base_url := "http://" + config.Output.Elasticsearch.Host[0] + "/" + config.Output.Elasticsearch.Index

					go LoadMapping_CronJob_Start(config.Output.Elasticsearch.Cron_desc, base_url, config.Output.Elasticsearch.Mapping_string)

				}

			}

		}

	}

}

func __EsSettingsSetup(settings_index string, msg string) error {

	logger.Info("Setup Index:", settings_index)

	payload := strings.NewReader(msg)

	req, err := http.NewRequest("PUT", settings_index, payload)
	if err != nil {
		logger.Error(err)
		return err
	}

	req.Header.Add("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		logger.Error(err)
		return err
	}

	defer res.Body.Close()

	return err
}

func EsSettingsSetup(settings_index string) {

	logger.Info("Setup Settings For New Index:", settings_index)

	msg := `{"index":{"number_of_replicas":0}}`

	__EsSettingsSetup(settings_index, msg)

}

func EsMappingsSetup(base_url string, key string, mappings string) {

	if es_mappings_url_list[key] {

		return
	} else {
		logger.Info("Setup Mappings For Index:", base_url)
		logger.Info("Key is ", key)

		es_mappings_url_list[key] = true

		err := EsMappingSender(base_url, mappings)
		if err != nil {
			logger.Error(err)
		}
	}

}

type EsReturn struct {
	Took   int
	Errors bool
}

func (r *OutputEs) HttpPostSender(ori_url string, indexname string, msg string, num int) int {

	url := ori_url

	//t := time.Now()

	c := r.config.Output.Elasticsearch

	if c.Index_by_day {

		tag := "-" + time.Now().Format("2006-01-02")

		targetindex := "/" + indexname

		newindex := "/" + indexname + tag

		url = strings.Replace(ori_url, targetindex, newindex, 1)

	}

	res, err := http.Post(url, "application/json", strings.NewReader(msg)) //批量入库

	// httpclient := &http.Client{
	// 	Timeout: 3 * time.Second,
	// }

	// res, err := httpclient.Post(url, "application/json", strings.NewReader(msg))

	if err != nil {
		logger.Error(err.Error())
		atomic.AddInt64(&r.rc.ErrCount, int64(num))
		return 0
	}
	defer res.Body.Close()

	body, _ := ioutil.ReadAll(res.Body)

	EsReturnMap := make(map[string]interface{})

	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	json.Unmarshal(body, &EsReturnMap)

	if EsReturnMap != nil {
		//?????
		if EsReturnMap["status"] != nil {
			logger.Error("Erro status: URL %s Fail : %s\n", url, string(body))
			atomic.AddInt64(&r.rc.ErrCount, int64(num))

		} else if EsReturnMap["errors"] == true {

			//logger.Error("HttpPostSender:Send URL %s Error : %s\n", url, string(body))
			logger.Error("HttpPostSender:Send URL %s Error:\n", url)

			count := 0

			if _, ok := EsReturnMap["items"]; ok {

				var _r ES_Error_t
				json.Unmarshal(body, &_r)

				for _, _v := range _r.Items {

					//logger.Error("v is ", _v)

					data := _v.Index

					if data["status"] == 400 {
						logger.Error("status 400 : Item:%s\n", _v)
						count += 1

					} else {
						if _, ok := data["error"]; ok {
							logger.Error("Error Item:", _v)
							//logger.Debug("Error Messafge:", msg) //查看错误数据
							count += 1
						}
					}

				}

				atomic.AddInt64(&r.rc.ErrCount, int64(count))
			}
			return 0
		}
	}

	return num
}

func (r *OutputEs) EsIndexSettup(index string) error {

	if r.es_setting_url_list[index] {

		return nil
	}

	r.es_setting_url_list[index] = true

	c := r.config.Output.Elasticsearch

	es_setting_url := strings.Replace(index, "/doc/_bulk", "", 1)

	err := __EsSettingsSetup(es_setting_url, c.Mapping_string)

	return err
}

type BulkVarBuffer struct {
	Url    string
	Buffer *bytes.Buffer
	Number *int
}

func (r *OutputEs) OutputDataBulkHandler(send_url string, d []interface{}, p chan<- int) {

	lines := len(d)

	ec := r.config.Output.Elasticsearch
	ret := 0

	if ec.indexvarflag {

		var var_name string

		BulkMap := make(map[string]BulkVarBuffer)

		for _, v := range d {

			sub_item := v.(map[string]interface{})

			if v, ok := sub_item[ec.Index_var]; ok {

				var_name = v.(string)

				if var_name != "" {
					var_name = strings.Trim(var_name, " ")
					var_name = strings.ToLower(var_name)
				} else {
					var_name = "default"
				}

				if ec.Index_var_remove {
					delete(sub_item, ec.Index_var)
				}

			} else {
				var_name = "default"

			}

			var buf *bytes.Buffer
			var Number *int

			if _, ok := BulkMap[var_name]; ok {

				buf = BulkMap[var_name].Buffer

				Number = BulkMap[var_name].Number

			} else {

				new_send_url := strings.Replace(send_url, "IndexVar", var_name, 1)

				buf = bytes.NewBuffer(nil)

				Number = new(int)

				BulkMap[var_name] = BulkVarBuffer{Url: new_send_url, Buffer: buf, Number: Number}
			}

			data := Json_Marshal(v)

			buf.Write(bulk_flag_byte)
			buf.Write(data)
			buf.Write(return_flag)

			*Number += 1
		}

		index_name := ec.Index_prefix + var_name

		for _, v := range BulkMap {

			logger.Debug("Var URL :", v.Url)

			if ec.Mapping_load_enable {

				if r.config.Output.Elasticsearch.Index_by_day {
					tag := "-" + time.Now().Format("2006-01-02")
					f := strings.Replace(v.Url, "/doc/_bulk", tag+"/doc/_bulk", 1)
					err := r.EsIndexSettup(f)
					if err != nil {
						p <- ret
						continue
					}
				} else {
					err := r.EsIndexSettup(v.Url)
					if err != nil {
						p <- ret
						continue
					}
				}
			}

			ret = r.HttpPostSender(v.Url, index_name, v.Buffer.String(), *v.Number)
		}

	} else {

		buf := bytes.NewBuffer(nil)

		for _, v := range d {

			b := Json_Marshal(v)

			if ec.Id != "" {

				data := v.(map[string]interface{})

				if _, ok := data[ec.Id]; ok {

					s := make(map[string]interface{})
					s1 := make(map[string]interface{})

					s1["_id"] = data[ec.Id]
					s["index"] = s1

					b1 := Json_Marshal(s)

					buf.Write(b1)
					buf.Write(return_flag)
				} else {
					buf.Write(bulk_flag_byte)
				}
			} else {

				buf.Write(bulk_flag_byte) // bulk_flag_byte = []byte(bulk_flag); var bulk_flag = "{\"index\":{}}\n"
			}

			buf.Write(b)
			buf.Write(return_flag) // return_flag = []byte("\n")
		}

		ret = r.HttpPostSender(send_url, ec.Index, buf.String(), lines)
	}
	//<-esconnpool

	p <- ret
}

func (r *OutputEs) OutputProcessEsCopy(bluklist [][2]int, msg interface{}) int {

	ch := make(chan int)

	num := 0

	d := msg.([]interface{})

	es := r.config.Output.Elasticsearch

	for _, v := range es.EsUrl {

		for _, v1 := range bluklist {

			num += 1

			//logger.Alert("Current  EsConn Routine Pool Left :", *esconnpoolnum-len(esconnpool))

			//esconnpool <- 1

			go r.OutputDataBulkHandler(v, d[v1[0]:v1[1]], ch)
		}
	}

	sum := 0
	for i := 0; i < num; i++ {
		deal_lines := <-ch
		sum += deal_lines
	}

	return sum
}

func (r *OutputEs) OutputProcessEsRandom(bluklist [][2]int, msg interface{}) int {

	ch := make(chan int)

	num := 0

	d := msg.([]interface{})

	es := r.config.Output.Elasticsearch

	es_target_num := len(es.EsUrl)

	for _, v1 := range bluklist {

		num += 1

		index := rand.Intn(es_target_num)

		//logger.Alert("Current  EsConn Routine Pool Left :", *esconnpoolnum-len(esconnpool))

		//esconnpool <- 1

		go r.OutputDataBulkHandler(es.EsUrl[index], d[v1[0]:v1[1]], ch)
	}

	sum := 0
	for i := 0; i < num; i++ {
		deal_lines := <-ch
		sum += deal_lines
	}

	return sum
}

func (r *OutputEs) OutputProcessEsPolling(bluklist [][2]int, msg interface{}) int {

	ch := make(chan int)

	num := 0

	d := msg.([]interface{})

	es := r.config.Output.Elasticsearch

	es_target_num := len(es.EsUrl)

	for k, v1 := range bluklist {

		num += 1

		index := k % es_target_num

		//logger.Alert("Current  EsConn Routine Pool Left :", *esconnpoolnum-len(esconnpool))

		//esconnpool <- 1
		//logger.Error(len(d), v1[0], v1[1])
		go r.OutputDataBulkHandler(es.EsUrl[index], d[v1[0]:v1[1]], ch) //  http://0.0.0.0:9200/output.Elasticsearch.Index/doc/_bulk,批量入库的数据
	}

	sum := 0
	for i := 0; i < num; i++ {
		deal_lines := <-ch
		sum += deal_lines
	}

	return sum
}

func (r *OutputEs) OutputProcess(msg interface{}, msg_len int) {

	t := time.Now()

	atomic.AddInt64(&r.rc.InputCount, int64(msg_len))

	bluklist := DataBulkBufferInit(msg_len, r.Bulk) //r.Bulk为批量入库数据条数,bluklist为一个二维数组,内层数组记录批量入库i-j条

	ret := r.OuputFunc(bluklist, msg)

	atomic.AddInt64(&r.rc.OutputCount, int64(ret))

	logger.Debug("Output Es %s Lines : %d ,Cost :  %s", sendmode, ret, time.Now().Sub(t))
}
