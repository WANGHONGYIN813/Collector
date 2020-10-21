package plugin

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"sugon.com/WangHongyin/collecter/logger"
)

var ret_buffer_max_size = 100

type GoroutinePool_t struct {
	Routine_ret chan string
	Routine_cnt chan int
}

type InputKafka struct {
	InputCommon
	output       FilterInterface
	listener     sarama.Consumer
	pconsumer    sarama.PartitionConsumer
	routine_pool *GoroutinePool_t
}

var InputKafkaRegInfo RegisterInfo

func init() {
	v := &InputKafkaRegInfo

	v.Plugin = "Input"
	v.Type = "Kafka"
	v.SubType = ""
	v.Vendor = "sugon.com"
	v.Desc = "for kafka input"
	v.init = RouteInputKafkaInit

	InputRegister(v)
}

func (r *InputKafka) exit() {
	r.pconsumer.Close()
	r.listener.Close()
}

func RoutinePoolInit(ret_buffer_size int, pool_cnt int) *GoroutinePool_t {
	d := new(GoroutinePool_t)

	d.Routine_ret = make(chan string, ret_buffer_size)
	d.Routine_cnt = make(chan int, pool_cnt)

	logger.Info("Input Kafka Thread Pool Number : ", pool_cnt)

	return d
}

func RouteInputKafkaInit(c *YamlCommonData, out interface{}, countptr interface{}, statptr *RouteStatType) interface{} {
	r := new(InputKafka)

	r.config = c
	r.rc = countptr.(*RouteInputCounts)
	r.reginfo = &FilterSplitRegInfo

	r.output = out.(FilterInterface)

	r.switcher = (*int32)(statptr)

	kconfig := c.Input.Kafka

	pool_num := 0

	if kconfig.Thread_pool_max_num != 0 {
		pool_num = kconfig.Thread_pool_max_num
	} else if SysConfigGet().Parallel_pool_max_num != 0 {
		pool_num = SysConfigGet().Parallel_pool_max_num
	}

	r.routine_pool = RoutinePoolInit(ret_buffer_max_size, pool_num)

	return r
}

func (r *InputKafka) KafkaOutputDataInit() map[string]interface{} {

	o := make(map[string]interface{})

	if r.config.Input.Kafka.Fields != nil {
		o["fields"] = r.config.Input.Kafka.Fields
	}

	/*
		o["Topic"] = r.config.Input.Kafka.Topic
		o["Partition"] = r.config.Input.Kafka.Group
	*/

	if r.config.Input.Kafka.Type != "" {
		o["Type"] = r.config.Input.Kafka.Type
	}

	o["@timestamp"] = time.Now().UTC().Format("2006-01-02T15:04:05.999Z")

	//o["Source"] = r.config.Input.Kafka.Addr

	return o
}

func (r *InputKafka) datahander(b []byte) ([][]byte, int, int) {

	var arr [][]byte

	l := 0
	nl := 0

	for {

		i := bytes.IndexByte(b, '\n')

		if i == -1 {
			if len(b[0:]) != 0 {
				l += 1
				arr = append(arr, b[0:])
			} else {
				nl += 1
			}
			break
		}

		l += 1

		if len(b[0:i]) != 0 {
			arr = append(arr, b[0:i])
		} else {
			nl += 1
		}

		b = b[i+1:]
	}

	arrlen := len(arr)

	atomic.AddInt64(&r.rc.InputCount, int64(l))

	return arr, l, arrlen
}

func (r *InputKafka) DataHandler(msg *sarama.ConsumerMessage, pid int32) {

	t := time.Now()

	arr, orilen, arrlen := r.datahander(msg.Value)

	if arrlen == 0 {
		atomic.AddInt64(&r.rc.InvalidCount, int64(orilen))
		return
	}

	if DebugModeGet() {
		for _, v := range arr {
			fmt.Println(string(v))
		}
	}

	tags := r.KafkaOutputDataInit()

	logger.Debug("Input Kafka InLines : %d , OutLines : %d . Cost %s", orilen, arrlen, time.Now().Sub(t))

	atomic.AddInt64(&r.rc.OutputCount, int64(arrlen))

	r.output.FilterProcess(arr, arrlen, tags)

	r.routine_pool.Routine_ret <- fmt.Sprintf("Input Part %d : Total Cost %s", pid, time.Now().Sub(t))

	<-r.routine_pool.Routine_cnt
}

func (r *InputKafka) KafkaRoutinePoolRecycler() {

	g := r.routine_pool

	for ret := range g.Routine_ret {
		logger.Debug(ret, " Current Routine Pool Left :", SysConfigGet().Parallel_pool_max_num-len(g.Routine_cnt))
	}
}

func (r *InputKafka) KafkaListen(pconsumer sarama.PartitionConsumer, listener sarama.Consumer, pid int32) {

	defer listener.Close()
	defer pconsumer.Close()

	go r.KafkaRoutinePoolRecycler()

	for atomic.LoadInt32(r.switcher) > 0 {

		select {

		case msg := <-pconsumer.Messages():

			if msg != nil {

				r.routine_pool.Routine_cnt <- 1

				go r.DataHandler(msg, pid)
			}

		case err := <-pconsumer.Errors():
			errtime := time.Now().Local().Format("2006-01-02T15:04:05")
			kafkaConsumingErrorTimes = append(kafkaConsumingErrorTimes, errtime)
			logger.Error("Consumer.Errors:", err) //在这里退出
			atomic.AddInt64(&r.rc.ErrCount, 1)
			logger.Alert("Kafka Input Instance %s , Partition : %d : Get Error", r.config.Input.Kafka.Addr, pid)

			time.Sleep(time.Second * 600)
			//r.KafkaListen(pconsumer, listener, pid)
			//return
		}

	}

	logger.Alert("Kafka Input Instance %s , Partition : %d : Close Listen", r.config.Input.Kafka.Addr, pid)
}

func (r *InputKafka) KafkaConsumeMainEntry(ipaddr []string, topic string, partition int32, offset int64, p chan<- error) error {

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Version = sarama.V0_11_0_0

	consumer, err := sarama.NewConsumer(ipaddr, config)
	if err != nil {
		logger.Error("Error get consumer:", err)
		p <- err
		return err
	}

	partitionConsumer, err := consumer.ConsumePartition(topic, partition, offset)
	if err != nil {
		logger.Error("Error get tpoic %s partition %d consumer :%s", topic, partition, err)
		p <- err
		return err
	}

	p <- nil

	go r.KafkaListen(partitionConsumer, consumer, partition)

	return nil
}

func (r *InputKafka) InputProcess() error {

	k := r.config.Input.Kafka

	split_item := strings.Split(k.Partition, "-")

	if len(split_item) > 1 {

		start, _ := strconv.ParseInt(split_item[0], 10, 32)
		end, _ := strconv.ParseInt(split_item[1], 10, 32)

		ch := make(chan error)

		for i := start; i <= end; i++ {

			logger.Info("Start Sub Partition ", i)

			go r.KafkaConsumeMainEntry(k.Addr, k.Topic, int32(i), k.Offset, ch)
		}
		//有一个分区消费失败，则退出
		for i := start; i <= end; i++ {
			rets := <-ch

			if rets != nil {
				return rets
			}
		}

	} else {

		p, _ := strconv.ParseInt(r.config.Input.Kafka.Partition, 10, 32)

		logger.Info("Start Specific Partition ", p)

		ch := make(chan error)

		r.KafkaConsumeMainEntry(k.Addr, k.Topic, int32(p), k.Offset, ch)

		rets := <-ch
		if rets != nil {
			return rets
		}
	}

	return nil
}

func (r *InputKafka) MainLoop() error {

	if atomic.LoadInt32(r.switcher) == int32(RouteStat_Ready) {
		logger.Alert("Kafka Input Instance %s Start", r.config.Input.Kafka.Addr)

		ret := r.InputProcess()
		if ret != nil {
			return ret
		}

		atomic.StoreInt32(r.switcher, int32(RouteStat_On))
	}

	return nil
}
