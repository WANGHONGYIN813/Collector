package plugin

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"runtime"
	"strconv"

	"sugon.com/WangHongyin/collecter/logger"
)

var bind_ip_port = "0.0.0.0:"

var Hostmassage []map[string]interface{}
var Thresholdmassage []map[string]interface{}

var HostList []string

func init() {
	bind_ip_port += SysConfigGet().Web_port
}

func HttpSendError(w http.ResponseWriter, e error) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	fmt.Fprintln(w, e)
}

func HttpSendString(w http.ResponseWriter, msg string) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	fmt.Fprint(w, msg)
}

func HttpSendByte(w http.ResponseWriter, msg interface{}) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json;charset=UTF-8")
	j := Json_Marshal(msg)
	fmt.Fprint(w, string(j))
}

func WebClearCountHanler(w http.ResponseWriter, r *http.Request) {
	RouteCountClear()
	HttpSendString(w, "Done\n")
}

func WebQueryCountHanler(w http.ResponseWriter, r *http.Request) {
	data := RouteCountGet()
	HttpSendByte(w, data)
}

func WebQueryInfoHanler(w http.ResponseWriter, r *http.Request) {
	data := RouteCountInfoGet()
	HttpSendByte(w, data)
}

func WebQueryConfigHanler(w http.ResponseWriter, r *http.Request) {
	data := RouteConfigGet()
	HttpSendByte(w, data)
}

func WebQueryStatHanler(w http.ResponseWriter, r *http.Request) {
	data := RouteStatGet()
	HttpSendByte(w, data)
}

func WebStatControlHanler(w http.ResponseWriter, r *http.Request) {

	u, _ := url.Parse(r.URL.String())
	v, _ := url.ParseQuery(u.RawQuery)

	index := v["index"]
	control := v["control"]

	if len(index) == 0 || len(control) == 0 {
		HttpSendString(w, "Parameter error. example : curl 0.0.0.0:8001/control_stat?index=1&control=off\n")
		return
	}

	i, _ := strconv.Atoi(index[0])

	err := RouteStatSet(i, control[0])
	if err != nil {
		HttpSendError(w, err)
		return
	}

	HttpSendString(w, "Done\n")
}

func WebQueryTableHanler(w http.ResponseWriter, r *http.Request) {
	data := RouteTableGet()
	HttpSendByte(w, data)
}

func WebConfigLoadHanler(w http.ResponseWriter, r *http.Request) {

	defer r.Body.Close()

	con, _ := ioutil.ReadAll(r.Body)

	err := RouteConfigLoad(string(con))

	if err != nil {
		HttpSendError(w, err)
		return
	}

	HttpSendString(w, "Done\n")
}

func WebVersionHanler(w http.ResponseWriter, r *http.Request) {

	s := VersionInfoGet()

	s += "\n"

	HttpSendString(w, s)
}

func WebClearAllcountHanler(w http.ResponseWriter, r *http.Request) {
	RouteCountClear()
	RouteCountTodayClear()
	HttpSendString(w, "Done\n")
}

//删除当天统计
func WebClearTodaycountHanler(w http.ResponseWriter, r *http.Request) {
	RouteCountTodayClear()
	HttpSendString(w, "Done\n")
}

func WebQueryTodayinfoHandler(w http.ResponseWriter, r *http.Request) {
	data := RouteCountInfoTodayGet()
	HttpSendByte(w, data)
}

func WebQuerySelecteddayInfoHandler(w http.ResponseWriter, r *http.Request) {
	Url := r.URL.String()
	u, _ := url.Parse(Url)
	urlmap, _ := url.ParseQuery(u.RawQuery)

	if _, ok := urlmap["date"]; ok {
		date := urlmap["date"]
		if _, ok := EverydayCount[date[0]]; ok {
			data := EverydayCount[date[0]]
			HttpSendByte(w, data)
		} else {
			HttpSendString(w, "NOT EXIT TODAY COUNT\n")
		}
	} else {
		data := RouteCountInfoGet()
		HttpSendByte(w, data)
	}
}

func WebKafkaconsumingerrortime(w http.ResponseWriter, r *http.Request) {

	data := kafkaConsumingErrorTimes
	HttpSendByte(w, data)
}

func SetSnmpHost(w http.ResponseWriter, r *http.Request) {
	// Url := r.URL.String()
	// u, _ := url.Parse(Url)
	// urlmap, _ := url.ParseQuery(u.RawQuery)

	// if _, ok := urlmap["host"]; ok {
	// 	hostmassage := urlmap["host"][0]
	// 	err := json.Unmarshal([]byte(hostmassage), &Hostmassage)
	// 	if err != nil {
	// 		logger.Error("hostmassage Unmarshal error :", err)
	// 	}
	// 	HttpSendString(w, "Done\n")
	// } else {
	// 	HttpSendString(w, "Param Err\n")
	// }

	defer r.Body.Close()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return
	}
	//fmt.Println("body:", body)
	errs := json.Unmarshal([]byte(body), &Hostmassage)
	if errs != nil {
		HttpSendString(w, "hostmassage Unmarshal error\n")
		logger.Error("hostmassage Unmarshal error :", errs)
	}
	var Hostlist []string
	for _, v := range Hostmassage {

		host := v["assetIp"].(string)

		Hostlist = append(Hostlist, host)
	}

	HostList = Hostlist

	logger.Alert("HostMessage:", Hostmassage)

	HttpSendString(w, "Done\n")

}

func SetThreshold(w http.ResponseWriter, r *http.Request) {

	// Url := r.URL.String()
	// u, _ := url.Parse(Url)
	// urlmap, _ := url.ParseQuery(u.RawQuery)

	// if _, ok := urlmap["threshold"]; ok {
	// 	thresholdmassage := urlmap["threshold"][0]
	// 	err := json.Unmarshal([]byte(thresholdmassage), &Thresholdmassage)
	// 	if err != nil {
	// 		logger.Error("thresholdmassage Unmarshal error :", err)
	// 	}
	// 	HttpSendString(w, "Done\n")
	// } else {
	// 	HttpSendString(w, "Param Err\n")
	// }

	defer r.Body.Close()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return
	}
	//fmt.Println("body:", body)
	errs := json.Unmarshal([]byte(body), &Thresholdmassage)
	if errs != nil {
		logger.Error("thresholdmassage Unmarshal error :", errs)
		HttpSendString(w, "thresholdmassage Unmarshal error\n")
	}

	logger.Alert("Thresholdmassage:", Thresholdmassage)

	HttpSendString(w, "Done\n")

}

func WebQueryGoroutineHanler(w http.ResponseWriter, r *http.Request) {

	t := runtime.NumGoroutine()

	HttpSendByte(w, t)

}
func HttpServer() {

	EverydayCount = make(map[string][]RouteOpsCountInfo)

	go GetTodayBeforedawnCount()

	http.HandleFunc("/ggc", WebQueryGoroutineHanler)

	http.HandleFunc("/show_table", WebQueryTableHanler)

	http.HandleFunc("/show_info", WebQueryInfoHanler)

	http.HandleFunc("/show_count", WebQueryCountHanler)
	//http.HandleFunc("/clear_count", WebClearCountHanler)
	http.HandleFunc("/sc", WebQueryCountHanler)
	//http.HandleFunc("/cc", WebClearCountHanler)

	http.HandleFunc("/show_stat", WebQueryStatHanler)
	http.HandleFunc("/control_stat", WebStatControlHanler)

	http.HandleFunc("/show_config", WebQueryConfigHanler)
	http.HandleFunc("/load_config", WebConfigLoadHanler)

	http.HandleFunc("/version", WebVersionHanler)

	http.HandleFunc("/clear_allcount", WebClearAllcountHanler)
	http.HandleFunc("/ca", WebClearAllcountHanler)

	//重置今日的统计
	http.HandleFunc("/clear_todaycount", WebClearTodaycountHanler)
	http.HandleFunc("/ct", WebClearTodaycountHanler)

	//查看今日的数据统计
	http.HandleFunc("/show_today_info", WebQueryTodayinfoHandler)
	http.HandleFunc("/st", WebQueryTodayinfoHandler)

	//查看每天的数据统计
	http.HandleFunc("/show_selected_day_info", WebQuerySelecteddayInfoHandler)
	http.HandleFunc("/ssd", WebQuerySelecteddayInfoHandler)

	//kafka消费异常统计
	http.HandleFunc("/kafka_consume_error_time", WebKafkaconsumingerrortime)

	http.HandleFunc("/snmp_host", SetSnmpHost)
	http.HandleFunc("/threshold", SetThreshold)

	fmt.Println("Start Http Server On ", bind_ip_port)

	if err := http.ListenAndServe(bind_ip_port, nil); err != nil {
		logger.Error("ListenAndServe: ", err)
	}
}
