package send

import (
	"bytes"
	"github.com/json-iterator/go"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type Kafka_Message_Type int

const (
	Kafka_Message_Type_LOG_T  Kafka_Message_Type = 0
	Kafka_Message_Type_FILE_T Kafka_Message_Type = 1
)

const MFlag = 0x20190323

type Message_header_t struct {
	Message_Flag     int
	Message_boby_len int
}

var bulk_flag = "{\"index\":{}}\n"
var bulk_flag_byte []byte
var return_flag []byte

var es_setting_url_list map[string]bool

func init() {

	bulk_flag_byte = []byte(bulk_flag)
	return_flag = []byte("\n")

	go SignalListen()

	es_setting_url_list = make(map[string]bool)
}

var FileRevertIndexKeyNameList = []string{
	"OriginalFileID",
	"Filetype",
	"FileName",
	"Capturetime",
	"Proto",
	"User_IP",
	"User_port",
	"Server_IP",
	"Server_port",
	"Url",
	"Host",
	"Request",
	"Encoding",
	"Charset",
	"Referer",
	"Filesize",
}

var FileRevertIndexIntKeyList = []string{
	"Filesize",
}

func ReadAll(filePth string) ([]byte, error) {
	f, err := os.Open(filePth)
	if err != nil {
		return nil, err
	}

	return ioutil.ReadAll(f)
}

func PathExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return false
}

func ExitFunc() {

	if PathExists(Gconfig.Buffer_dir) {
		os.RemoveAll(Gconfig.Buffer_dir)
		log.Println("Clear Buffer Done")
	}

	log.Println("Exit")
	os.Exit(0)
}

func SignalListen() {

	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	for s := range c {
		log.Println("Get Signal: ", s)
		ExitFunc()
	}
}

func EsSettingsSetup(settings_index string) {

	msg := `{"index":{"number_of_replicas":0}}`

	payload := strings.NewReader(msg)

	req, _ := http.NewRequest("PUT", settings_index, payload)

	req.Header.Add("Content-Type", "application/json")

	res, _ := http.DefaultClient.Do(req)

	defer res.Body.Close()
}

func EsSettingsRegister(index string) {

	es_setting_url := index + "/_settings"

	if es_setting_url_list[es_setting_url] {

		return
	} else {

		es_setting_url_list[es_setting_url] = true

		EsSettingsSetup(es_setting_url)
	}
}

func HttpPostSender(url string, msg string, es_host string) error {

	res, err := http.Post(url, "application/json", strings.NewReader(msg))
	if err != nil {
		log.Println("Http Send err :", err)
		return err
	}
	defer res.Body.Close()

	//log.Printf("Url : %s Send Done\n", url)

	go EsSettingsRegister(es_host)

	return nil
}

func EsSendBluk(host string, indexname string, data []interface{}) error {

	var send_url string
	var es_host string

	if Gconfig.Es.Index_by_day {

		tag := "-" + time.Now().Format("2006-01-02")
		es_host = Gconfig.Es.index_url + tag

		send_url = es_host + "/doc/_bulk"

	} else {

		es_host = Gconfig.Es.index_url

		send_url = es_host + "/doc/_bulk"
	}

	buf := bytes.NewBuffer(nil)

	for _, v := range data {
		b, _ := json.Marshal(v)
		buf.Write(bulk_flag_byte)
		buf.Write(b)
		buf.Write(return_flag)
	}

	return HttpPostSender(send_url, buf.String(), es_host)
}

func EsSend(host string, indexname string, data interface{}) error {

	var send_url string
	var es_host string

	if Gconfig.Es.Index_by_day {

		tag := "-" + time.Now().Format("2006-01-02")

		es_host = "http://" + host + "/" + indexname + tag

		send_url = es_host + "/doc"

	} else {

		es_host = "http://" + host + "/" + indexname

		send_url = es_host + "/doc"
	}

	d := data.(map[string]interface{})

	b, _ := json.Marshal(d)

	return HttpPostSender(send_url, string(b), es_host)
}

func HbaseHttpPutXml(send_uri string, msg string) (error, string) {

	client := &http.Client{}

	req, err := http.NewRequest("PUT", send_uri, strings.NewReader(msg))
	if err != nil {
		log.Println(err)
		return err, ""
	}

	req.Header.Set("Accept", "text/xml")
	req.Header.Set("Content-Type", "text/xml")
	req.Close = true

	resp, err := client.Do(req)
	if err != nil {
		log.Println(err)
		return err, ""
	}

	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)

	return nil, string(body)
}

func HbaseHttpPutJson(send_uri string, msg string) error {

	client := &http.Client{}

	req, err := http.NewRequest("PUT", send_uri, strings.NewReader(msg))
	if err != nil {
		log.Println(err)
		return err
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Close = true

	resp, err := client.Do(req)
	if err != nil {
		log.Println(err)
		return err
	}

	defer resp.Body.Close()

	//body, _ := ioutil.ReadAll(resp.Body)

	return nil
}

func GetBulkSplit(sum int, bulk int) [][2]int {

	var bufferlines [][2]int

	for i := 0; i < sum; {

		var buf [2]int

		end := i + bulk

		if end >= sum {
			end = sum
		}

		buf[0] = i
		buf[1] = end

		bufferlines = append(bufferlines, buf)

		i = end
	}

	return bufferlines
}
