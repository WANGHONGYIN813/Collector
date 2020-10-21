package plugin

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

var LocalIP string
var Hostname string

var bulk_flag = "{\"index\":{}}\n"
var bulk_flag_byte []byte
var return_flag []byte

func ComDataInit() {
	bulk_flag_byte = []byte(bulk_flag)
	return_flag = []byte("\n")
}

type Fields_t struct {
	Input_type string `json:"input_type"`
	Tag        string `json:"tag"`
}

type Beat_t struct {
	Hostname string `json:"hostname"`
	Hostip   string `json:"hostip"`
}

func init() {

	ComDataInit()

	LocalIP = GetLocalIP()
	Hostname, _ = os.Hostname()

	go SignalListen()
}

func ExitFunc() {

	InputFtpExit()

	fmt.Println("Exit")
	os.Exit(0)
}

func SignalListen() {

	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	for s := range c {
		fmt.Println("Get Signal: ", s)
		ExitFunc()
	}

}

func Add_Fields(add_field map[string]string, oridata interface{}) {

	if len(add_field) == 0 {
		return
	}

	switch oridata.(type) {

	case []interface{}:
		d1 := oridata.([]interface{})

		for _, i := range d1 {
			d2 := i.(map[string]interface{})

			__addfields(add_field, d2)
		}

	case map[string]interface{}:
		d := oridata.(map[string]interface{})
		__addfields(add_field, d)
	}

}

func __addfields(add_field map[string]string, data map[string]interface{}) {

	for k, v := range add_field {
		if _, ok := data[v]; ok {

			dd := data[v]

			if k == "@timestamp" {
				switch dd.(type) {
				case float64:
					data[k] = GetUTCTimeStampClass(int64(dd.(float64)))
				case int64:
					data[k] = GetUTCTimeStampClass(dd.(int64))
				case int:
					data[k] = GetUTCTimeStampClass(int64(dd.(int)))
				}
			} else {
				data[k] = dd
			}
		}
	}
}

func Transform_Fields(Transform_fields map[string][]interface{}, oridata interface{}) {

	if len(Transform_fields) == 0 {
		return
	}

	switch oridata.(type) {

	case []interface{}:
		d1 := oridata.([]interface{})

		for _, i := range d1 {
			d2 := i.(map[string]interface{})

			__transform_fields(Transform_fields, d2)
		}

	case map[string]interface{}:
		d := oridata.(map[string]interface{})
		__transform_fields(Transform_fields, d)
	}

}

func __transform_fields(transform_fields map[string][]interface{}, data map[string]interface{}) {

	for k, v := range transform_fields {

		if _, ok := data[k]; ok {

			if data[k] == v[0] {

				data[k] = v[1]
			}

		}
	}
}

func Remove_Fields(Remove_fields []string, oridata interface{}) {

	if len(Remove_fields) == 0 {
		return
	}

	switch oridata.(type) {

	case []interface{}:
		d1 := oridata.([]interface{})

		for _, i := range d1 {
			d2 := i.(map[string]interface{})

			__remove_fields(Remove_fields, d2)
		}

	case map[string]interface{}:
		d := oridata.(map[string]interface{})
		__remove_fields(Remove_fields, d)
	}

}

func __remove_fields(Remove_fields []string, data map[string]interface{}) {

	for _, key := range Remove_fields {

		if _, ok := data[key]; ok {

			delete(data, key)
		}
	}
}
