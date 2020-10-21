package plugin

import (
	"sugon.com/WangHongyin/collecter/cron"
	"sugon.com/WangHongyin/collecter/logger"

	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
)

type LoadMapping_CronJob struct {
	cron_desc string
	key       string
	base_url  string
	mappings  string
}

func (this LoadMapping_CronJob) Run() {

	next_day_index := this.base_url + "-" + time.Now().AddDate(0, 0, 1).Format("2006-01-02")

	log.Println("Load Mappings For Index:", next_day_index)
	log.Println("mappings is ", this.mappings)

	err := EsMappingSender(next_day_index, this.mappings)

	if err != nil {
		logger.Error("EsMappingSender Error : ", err)
	}
}

func LoadMapping_CronJob_Start(desc string, base_url string, mappings string) {

	c := cron.New()

	var t LoadMapping_CronJob

	t.cron_desc = desc
	t.base_url = base_url
	t.mappings = mappings

	c.AddJob(desc, t)

	c.Start()

	log.Println("Start Cron Task, Desc:", desc)
}

func MappingFileParse(file_path string) (string, error) {

	f, err := os.Open(file_path)
	if err != nil {
		log.Println("Mapping File Err:", err)
		return "", err
	}

	buffer, _ := ioutil.ReadAll(f)

	config_string := string(buffer)

	send_data := strings.Replace(config_string, " ", "", -1)

	send_data = strings.Replace(send_data, "\n", "", -1)

	send_data = strings.Replace(send_data, "\t", "", -1)

	return send_data, nil
}

func EsIndexCheck(index string) (bool, error) {

	resp, err := http.Get(index)
	if err != nil {
		log.Println("Check URL err:", err)
		return false, err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}

	m := make(map[string]interface{})

	var json = jsoniter.ConfigCompatibleWithStandardLibrary

	err = json.Unmarshal(body, &m)

	if m != nil {
		errret := m["error"]

		if errret == nil {
			return true, nil
		} else {
			return false, nil
		}
	}
	return false, nil
}

func EsMappingSender(index string, config_json_string string) error {
	//e是布尔类型
	e, err := EsIndexCheck(index)
	if err != nil {
		return err
	}
	if e {
		ret := index + " url already exists"
		return errors.New(ret)
	}

	payload := strings.NewReader(config_json_string)

	req, err := http.NewRequest("PUT", index, payload)
	if err != nil {
		log.Println("PUT Mapping err:", err)
		return err
	}

	req.Header.Add("Content-Type", "application/json")

	res, _ := http.DefaultClient.Do(req)

	defer res.Body.Close()

	body, _ := ioutil.ReadAll(res.Body)

	m := make(map[string]interface{})

	var json = jsoniter.ConfigCompatibleWithStandardLibrary

	err = json.Unmarshal(body, &m)

	if m != nil {
		acknowledged := m["acknowledged"]

		if acknowledged == nil {
			return errors.New(string(body))
		}

		if acknowledged != true {
			ret := index + " Setup Fail :" + string(body)
			return errors.New(ret)
		}
	} else {
		ret := index + " Return None :" + string(body)
		return errors.New(ret)
	}

	return nil
}
