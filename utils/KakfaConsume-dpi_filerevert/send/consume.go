package send

import (
	zip "./zip"
	"bufio"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

var save_path = ""

func init() {

	if Gconfig.Buffer_dir != "" {

		if PathExists(Gconfig.Buffer_dir) == false {
			err := os.MkdirAll(Gconfig.Buffer_dir, os.ModePerm)
			if err != nil {
				log.Println("Failed to create the path", err)

				Gconfig.Buffer_dir = ".buffer"
				log.Println("User ", Gconfig.Buffer_dir)

				if PathExists(Gconfig.Buffer_dir) == false {
					err := os.Mkdir(Gconfig.Buffer_dir, os.ModePerm)
					if err != nil {
						log.Println("Failed to create the path", err)
					}
				}
			}
		}

		save_path = Gconfig.Buffer_dir + "/"
	}

}

func FileSave(name string, contents []byte, num float64) (int, error) {

	fileObj, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Println("Failed to open ", name, err.Error())
		return 0, err
	}
	defer fileObj.Close()

	n := 0
	if n, err = fileObj.Write(contents); err != nil {
		log.Println("Write err :", err)
		return 0, err
	}

	return n, nil
}

func read_file_by_line(indexfile string) ([]map[string]interface{}, error) {

	f, err := os.Open(indexfile)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	rd := bufio.NewReader(f)

	var filelist []map[string]interface{}

	for {
		line, err := rd.ReadString('\n')

		if err != nil || io.EOF == err {
			break
		}

		filerevert_map := make(map[string]interface{})

		split_item := strings.Split(line, "\t")

		tarlen := len(FileRevertIndexKeyNameList)

		for k, v := range split_item {

			if k == tarlen {
				break
			}

			filerevert_map[FileRevertIndexKeyNameList[k]] = v
		}

		for _, v := range FileRevertIndexIntKeyList {
			val := filerevert_map[v]
			if val != nil {
				filerevert_map[v], _ = strconv.Atoi(val.(string))
			}
		}

		filerevert_map["@timestamp"] = time.Now().UTC().Format("2006-01-02T15:04:05.999Z")

		filelist = append(filelist, filerevert_map)
	}

	return filelist, nil
}

func txt_file_batch_dispath(file_buffer_list map[string][]byte, txt_file_xdr []interface{}) {

	if Gconfig.Es.enable {
		go EsSendBluk(Gconfig.Es.Host, Gconfig.Es.Index, txt_file_xdr)
	}

	BatchFileStroageHandler(file_buffer_list, "TXT")
}

func pic_check_file_batch_dispath(file_buffer_list map[string][]byte, pic_file_xdr []interface{}, name string) {

	go BatchFileStroageHandler(file_buffer_list, "PIC")

	Pic_batch_check_and_send(file_buffer_list, pic_file_xdr, Gconfig.Es.Host, Gconfig.Es.Index, name)
}

func pic_no_check_file_batch_dispath(file_buffer_list map[string][]byte, pic_file_xdr []interface{}, name string) {

	go BatchFileStroageHandler(file_buffer_list, "PIC")

	if Gconfig.Es.enable {
		EsSendBluk(Gconfig.Es.Host, Gconfig.Es.Index, pic_file_xdr)
	}
}

func file_batch_dispath(name string, filelist []map[string]interface{}, stroage_dir string) (int, error) {

	filenamelist := make(map[string]bool)
	//存文件数据,key为文件说明的FileName字段
	txt_file_buffer := make(map[string][]byte)
	pic_file_check_buffer := make(map[string][]byte)
	pic_file_no_check_buffer := make(map[string][]byte)

	//存文件说明
	var txt_xdr_list []interface{}
	var pic_check_xdr_list []interface{}
	var pic_no_check_xdr_list []interface{}

	//进行去重处理

	sum := 0

	for _, v := range filelist {

		if v == nil {
			continue
		}

		if _, ok := v["FileName"]; !ok {
			continue
		}

		filename := v["FileName"].(string)

		Filetype := v["Filetype"].(string)

		//去重
		if filenamelist[filename] {

			continue
		} else {

			filenamelist[filename] = true
			sum += 1

			buf, err := ioutil.ReadFile(stroage_dir + filename)
			if err != nil {
				//fmt.Printf("Open %s Err %s\n", stroage_dir+filename, err)
				continue
			}

			if Filetype == "TXT" {

				txt_file_buffer[filename] = buf[0 : len(buf)-1]
				txt_xdr_list = append(txt_xdr_list, v)
			} else {

				if Gconfig.Pic_check.Enable {

					suffix := strings.ToLower(path.Ext(filename))

					if Gconfig.Pic_check.CheckMap[suffix] {

						pic_file_check_buffer[filename] = buf[0 : len(buf)-1]
						pic_check_xdr_list = append(pic_check_xdr_list, v)

						continue
					}

				}

				pic_file_no_check_buffer[filename] = buf[0 : len(buf)-1]
				pic_no_check_xdr_list = append(pic_no_check_xdr_list, v)
			}
		}
	}

	if len(txt_xdr_list) != 0 {
		go txt_file_batch_dispath(txt_file_buffer, txt_xdr_list)
	}

	if len(pic_check_xdr_list) != 0 {
		go pic_check_file_batch_dispath(pic_file_check_buffer, pic_check_xdr_list, name)
	}

	if len(pic_no_check_xdr_list) != 0 {
		go pic_no_check_file_batch_dispath(pic_file_no_check_buffer, pic_no_check_xdr_list, name)
	}

	os.RemoveAll(stroage_dir)

	return sum, nil
}

func FileHandler(name string, contents string, num float64, p chan<- error) error {

	//fmt.Printf("Receive %s\n", name)

	fileSuffix := path.Ext(name)

	pure_file_name := strings.TrimSuffix(name, fileSuffix)

	if fileSuffix != ".zip" {
		p <- errors.New("Not Zip file")
		return errors.New("Not Zip file")
	}

	decodeBytes, err := base64.StdEncoding.DecodeString(contents)
	if err != nil {
		log.Println(err)
		p <- err
		return err
	}

	save_filename_with_path := save_path + name

	//n, err := FileSave(save_filename_with_path, decodeBytes, num)
	_, err = FileSave(save_filename_with_path, decodeBytes, num)
	if err != nil {
		log.Println(err)
		p <- err
		return err
	}

	//fmt.Printf("Receive %s Done , %d B\n", name, n)

	save_dir := save_path + pure_file_name + "/"

	//由于没有对内存解压的接口，因此先留存本地，解压后再删除。后期优化
	err = zip.DeCompress(save_filename_with_path, save_dir)
	if err != nil {
		log.Println("Decompress err :", err)
		p <- err
		return err
	}

	err = ZipFileStroageHandler(save_filename_with_path)
	if err != nil {
		log.Println("file remove Error!: ", err)
		p <- err
		return err
	}

	index_file := save_dir + pure_file_name + ".index"

	filelistmap, err := read_file_by_line(index_file) //读.index文件,返回一个map数组
	if err != nil {
		log.Println("Read Index File err : ", err)
		p <- err
		return err
	}
	//
	sum, err := file_batch_dispath(name, filelistmap, save_dir)
	if err != nil {
		log.Printf("%s : Line %d Done\n", name, len(filelistmap))
		p <- err
		return nil
	}

	log.Printf("%s : File Number %d, Not-repeated Numer %d\n", name, len(filelistmap), sum)

	p <- nil
	return nil
}

func (r *InputKafka) MessageProcess(m interface{}, pid int32) {

	msg := m.(*sarama.ConsumerMessage) //这一步类型转换

	d := msg.Value

	message_len := len(d)

	if message_len <= 0 {
		return
	}

	var v []map[string]interface{} //数组

	err := json.Unmarshal(d, &v)
	if err != nil {
		log.Println(err)
		return
	}

	ch := make(chan error)
	num := 0

	for k, r := range v {

		filename := r["Filename"].(string)
		fileconents := r["Message"].(string)
		filelen := r["Filelen"].(float64)

		num = k

		go FileHandler(filename, fileconents, filelen, ch)
	}

	for i := 0; i <= num; i++ {
		rets := <-ch

		if rets != nil {
			r.routine_pool.Routine_ret <- fmt.Sprintln(err)
			break
		}
	}

	r.routine_pool.Routine_ret <- ""

	<-r.routine_pool.Routine_cnt

}

var ret_buffer_max_size = 100

type GoroutinePool_t struct {
	Routine_ret chan string
	Routine_cnt chan int
}

type InputKafka struct {
	listener     sarama.Consumer
	pconsumer    sarama.PartitionConsumer
	routine_pool *GoroutinePool_t
}

func RoutinePoolInit(ret_buffer_size int, pool_cnt int) *GoroutinePool_t {
	d := new(GoroutinePool_t)

	d.Routine_ret = make(chan string, ret_buffer_size)
	d.Routine_cnt = make(chan int, pool_cnt)

	log.Println("Input Kafka Thread Pool Number : ", pool_cnt)

	return d
}

func RouteInputKafkaInit(l sarama.Consumer, p sarama.PartitionConsumer) *InputKafka {

	r := new(InputKafka)

	r.listener = l
	r.pconsumer = p
	r.routine_pool = RoutinePoolInit(ret_buffer_max_size, *parallel_num)

	return r
}

func (r *InputKafka) KafkaRoutinePoolRecycler() {

	g := r.routine_pool

	for _ = range g.Routine_ret {
		log.Println("Current Routine Pool Left :", *parallel_num-len(g.Routine_cnt))
	}
}

func (r *InputKafka) KafkaListen(pconsumer sarama.PartitionConsumer, listener sarama.Consumer, pid int32) {

	defer listener.Close()
	defer pconsumer.Close()

	go r.KafkaRoutinePoolRecycler()

	log.Printf("Kafka Consumer Part %d Start\n", pid)

	for {

		select {

		case msg := <-pconsumer.Messages():

			if msg != nil {

				r.routine_pool.Routine_cnt <- 1

				go r.MessageProcess(msg, pid)
			}

		case err := <-pconsumer.Errors():
			log.Println("Part ", pid, "Consumer.Errors:", err)
			return
		}

	}

}

func ConsumeMailoop(ipaddr []string, topic string, partition int32) error {

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Version = sarama.V0_11_0_0
	consumer, e := sarama.NewConsumer(ipaddr, config)
	if e != nil {
		log.Println("error(part)", partition, " get consumer: ", e)
		return e
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
	if err != nil {
		log.Printf("error get partition %d consumer : %s\n", partition, err)
		return err
	}
	defer partitionConsumer.Close()

	log.Println("Start MainLoop")

	r := RouteInputKafkaInit(consumer, partitionConsumer)

	r.KafkaListen(partitionConsumer, consumer, partition)

	return nil
}

func ConsumeEntry() error {

	split_item := strings.Split(Gconfig.Kafka.Partition, "-")

	if len(split_item) > 1 {

		start, _ := strconv.ParseInt(split_item[0], 10, 32)
		end, _ := strconv.ParseInt(split_item[1], 10, 32)

		for i := start; i < end; i++ {

			log.Println("Start Sub Partition ", i)

			go ConsumeMailoop(Gconfig.Kafka.Addr, Gconfig.Kafka.Topic, int32(i))
		}

		log.Println("Start Main Partition ", int32(end))

		err := ConsumeMailoop(Gconfig.Kafka.Addr, Gconfig.Kafka.Topic, int32(end))
		if err != nil {
			return err
		}

	} else {

		p, _ := strconv.ParseInt(Gconfig.Kafka.Partition, 10, 32)

		log.Println("Start Main Partition ", p)

		err := ConsumeMailoop(Gconfig.Kafka.Addr, Gconfig.Kafka.Topic, int32(p))
		if err != nil {
			return err
		}
	}

	return nil
}
