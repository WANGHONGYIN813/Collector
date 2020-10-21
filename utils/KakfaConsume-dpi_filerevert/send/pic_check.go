package send

import (
	"bytes"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	//"time"
	"log"
)

var commit_path = ""

func init() {

	if Gconfig.Pic_check.Check_addr != "" {
		Gconfig.Pic_check.Enable = true
		commit_path = Gconfig.Pic_check.Check_addr

		log.Println("Pic Check Addr : ", commit_path)

		log.Println("Pic Check Type: ", Gconfig.Pic_check.File_type)

		Gconfig.Pic_check.CheckMap = make(map[string]bool)

		for _, v := range Gconfig.Pic_check.File_type {

			o := "." + v
			Gconfig.Pic_check.CheckMap[o] = true
		}

	} else {
		Gconfig.Pic_check.Enable = false
	}

	if Gconfig.Pic_check.Bulk_num != 0 {
		log.Println("Pic check bulk num: ", Gconfig.Pic_check.Bulk_num)
	}
}

func HttpCommitFile_FromBuffer(buf []byte, filename string, tag string) (string, error) {

	bodyBuffer := &bytes.Buffer{}

	bodyWriter := multipart.NewWriter(bodyBuffer)

	fileWriter, _ := bodyWriter.CreateFormFile(tag, filename)
	fileWriter.Write(buf)

	contentType := bodyWriter.FormDataContentType()
	bodyWriter.Close()

	resp, err := http.Post(commit_path, contentType, bodyBuffer)
	if err != nil {
		log.Println("Http Post err : ", err)
		return "", err
	}

	resp_body, err := ioutil.ReadAll(resp.Body)

	resp.Body.Close()

	return string(resp_body), err
}

func http_post_mutil_file(filelist map[string][]byte, url string, tag string) (string, error) {

	bodyBuf := &bytes.Buffer{}
	bodyWriter := multipart.NewWriter(bodyBuf)

	num := 0

	for k, v := range filelist {

		num += 1

		fileWriter, _ := bodyWriter.CreateFormFile(tag, k)
		fileWriter.Write(v)
	}

	contentType := bodyWriter.FormDataContentType()
	bodyWriter.Close()

	return HttpPostFile(url, contentType, bodyBuf)
}

func http_post_mutil_file_bulk_split(filelist map[string][]byte, bulk_item [][2]int, url string, tag string, p chan<- string) error {

	bulk := Gconfig.Pic_check.Bulk_num

	bodyBuf := &bytes.Buffer{}
	bodyWriter := multipart.NewWriter(bodyBuf)

	num := 0
	__i := 0

	for k, v := range filelist {

		index := __i / bulk

		__i += 1

		batch := bulk_item[index]

		start := batch[0]
		end := batch[1]

		bulk_num := end - start

		fileWriter, _ := bodyWriter.CreateFormFile(tag, k)
		fileWriter.Write(v)

		num += 1

		if num == bulk_num {

			num = 0
			contentType := bodyWriter.FormDataContentType()
			bodyWriter.Close()

			ret, err := HttpPostFile(url, contentType, bodyBuf)
			if err != nil {
				log.Println("HttpPost Err:", err)
				p <- ""
				continue
			}

			p <- ret

			bodyBuf.Reset()
		}
	}

	close(p)

	return nil
}

func HttpPostFile(url string, content_type string, buffer *bytes.Buffer) (string, error) {

	client := &http.Client{}

	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return "", err
	}

	req.Header.Set("Content-Type", content_type)
	req.Close = true

	req.Body = ioutil.NopCloser(buffer)

	data, err := client.Do(req)
	if err != nil {
		return "", err
	}

	bytes, err := ioutil.ReadAll(data.Body)
	if err != nil {
		return "", err
	}

	defer data.Body.Close()

	return string(bytes), nil
}

func IntegrationTag(ret_tag []string, pic_file_xdr []interface{}, host string, indexname string) error {

	p := make(map[string]interface{})

	for _, v := range pic_file_xdr {

		ori := v.(map[string]interface{})

		p[ori["FileName"].(string)] = ori

	}

	for _, v := range ret_tag {

		tag := make(map[string]string)

		err := json.Unmarshal([]byte(v), &tag)
		if err != nil {
			//log.Println("Get Tag Err", err)

		} else {

			for k1, v1 := range tag {
				if p[k1] == nil {
					continue
				}

				o := p[k1].(map[string]interface{})

				switch v1 {
				case "porn":
					o["PicCheck"] = "porn"
				case "suspected":
					o["PicCheck"] = "suspected"
				case "normal":
					o["PicCheck"] = "normal"
				default:
					//o["PicCheck"] = "unkown"
					delete(p, k1)
				}

			}
		}
	}

	var data []interface{}

	for _, v := range p {
		data = append(data, v)
	}

	return EsSendBluk(host, indexname, data)
}

func Pic_batch_check_and_send(file_buffer_list map[string][]byte, pic_file_xdr []interface{}, host string, indexname string, name string) error {

	if Gconfig.Pic_check.Bulk_num == 0 {

		for k, v := range file_buffer_list {

			ret, _ := HttpCommitFile_FromBuffer(v, k, "file")

			log.Println(k, "tag :", ret)
		}

	} else if Gconfig.Pic_check.Bulk_num == -1 {

		ret, _ := http_post_mutil_file(file_buffer_list, commit_path, "Imagefile")

		log.Println("ret is ", ret)
	} else {

		sum := len(file_buffer_list)

		bulk_item := GetBulkSplit(sum, Gconfig.Pic_check.Bulk_num)

		ch := make(chan string, len(bulk_item))

		go http_post_mutil_file_bulk_split(file_buffer_list, bulk_item, commit_path, "Imagefile", ch)

		var retlist []string
		for ret := range ch {
			if ret != "" {
				retlist = append(retlist, ret)
			}
		}

		log.Printf("%s : Send Number %d\n", name, sum)

		if Gconfig.Es.enable {
			IntegrationTag(retlist, pic_file_xdr, host, indexname)
		} else {
			log.Println(name, "Ret: ", retlist)
		}
	}

	return nil
}
