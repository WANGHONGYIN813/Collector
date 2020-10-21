package plugin

import (

	//"bytes"
	//"encoding/gob"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"sugon.com/WangHongyin/collecter/logger"
)

type OutputKakfa struct {
	OutputCommon
	MaxMessageBytes    int
	Max_message_line   int
	Kakfa_output_slice int
	Pro                sarama.SyncProducer
	AsyncPro           sarama.AsyncProducer
	OuputFunc          func(string, interface{}, int) (int, error)
}

var OutputKakfaRegInfo RegisterInfo

func init() {

	v := &OutputKakfaRegInfo

	v.Plugin = "Output"
	v.Type = "Kafka"
	v.SubType = ""
	v.Vendor = "sugon.com"
	v.Desc = "for kafka output"
	v.init = OutputKakfaRouteInit

	OutputRegister(v)
}

func OutputKakfaRouteInit(c *YamlCommonData, out interface{}, countptr interface{}, statptr *RouteStatType) interface{} {

	r := new(OutputKakfa)

	r.config = c
	r.rc = countptr.(*RouteCount)
	r.reginfo = &OutputKakfaRegInfo

	if r.config.Output.Kafka.Maxmessagebytes != 0 {
		r.MaxMessageBytes = r.config.Output.Kafka.Maxmessagebytes
		logger.Info("config.Producer.MaxMessageByte is ", r.MaxMessageBytes)
	}

	r.Kakfa_output_slice = SysConfigGet().Kakfa_output_slice
	r.Max_message_line = SysConfigGet().Kafka_output_max_message_line

	if r.config.Output.Kafka.Max_message_line != 0 {
		r.Max_message_line = r.config.Output.Kafka.Max_message_line
	}

	r.Max_message_line = r.config.Output.Kafka.Max_message_line

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Timeout = 3 * time.Second
	config.Producer.RequiredAcks = sarama.NoResponse
	//config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner

	config.Version = sarama.V0_11_0_2

	config.Producer.MaxMessageBytes = r.MaxMessageBytes

	k := r.config.Output.Kafka

	var err error
	//是否是同步模式
	if k.Syncmode {

		r.Pro, err = sarama.NewSyncProducer(k.Host, config)
		if err != nil {
			logger.Emer("Kafka sarama.NewSyncProducer err : %s", err)
		}

		r.OuputFunc = r.KafkaSyncProducer

	} else {
		logger.Info("Start Make Async Producer")

		r.AsyncPro, err = sarama.NewAsyncProducer(k.Host, config)
		if err != nil {
			logger.Emer("Kafka sarama.NewAsyncProducer err : %s", err)
		}

		r.OuputFunc = r.KafkaAsyncProducer

		go r.KafkaAsyncProducerGoroutine()
	}

	return r
}

//func (r *OutputKakfa) KafkaSyncProducer(topic string, data []interface{}) (int, error) {
func (r *OutputKakfa) KafkaSyncProducer(topic string, data interface{}, message_send_target int) (int, error) {

	//t := time.Now()

	b := Json_Marshal(data)

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(b),
	}

	_, _, err := r.Pro.SendMessage(msg)
	if err != nil {
		logger.Error("Kafka Send message err : %s", err)
		atomic.AddInt64(&r.rc.ErrCount, int64(message_send_target))
		return 0, err
	}

	//logger.Debug("Produce %d Cost : %s\n", message_send_target, time.Now().Sub(t))

	return message_send_target, nil
}

func (r *OutputKakfa) KafkaAsyncProducerGoroutine() {

	if r.AsyncPro == nil {
		return
	}

	logger.Info("Kafka Start Goroutine")

	for {
		select {
		//case suc := <-r.AsyncPro.Successes():
		case <-r.AsyncPro.Successes():
			//logger.Info("Successes : %+v\n", suc)

		case fail := <-r.AsyncPro.Errors():
			if fail != nil {
				logger.Error("Kafak AsyncPro Get Err: ", fail.Err)
			}
		}
	}
}

func (r *OutputKakfa) KafkaAsyncProducerSender(topic string, data []byte) {

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(data),
	}

	r.AsyncPro.Input() <- msg
}

func (r *OutputKakfa) KafkaAsyncProducer(topic string, data interface{}, message_send_target int) (int, error) {

	if r.AsyncPro == nil {
		return 0, nil
	}
	switch data.(type) {

	case []byte:

		output := data.([]byte)

		r.KafkaAsyncProducerSender(topic, output)

	case []interface{}:

		output := data.([]interface{})

		out_len := len(output)

		if r.Max_message_line < out_len {

			dd := DataBulkBufferInit(out_len, r.Kakfa_output_slice)

			for _, v := range dd {
				b := Json_Marshal(output[v[0]:v[1]])
				r.KafkaAsyncProducerSender(topic, b)
			}
		} else {

			b := Json_Marshal(data)
			r.KafkaAsyncProducerSender(topic, b)
		}

	default:

		b := Json_Marshal(data)
		r.KafkaAsyncProducerSender(topic, b)

		//logger.Info("Aync Produce %d , Cost : %s", message_send_target, time.Now().Sub(t1))
	}

	return message_send_target, nil
}

func (r *OutputKakfa) OutputProcess(msg interface{}, msg_len int) {
	t := time.Now()

	atomic.AddInt64(&r.rc.InputCount, int64(msg_len))

	k := r.config.Output.Kafka

	sum, _ := r.OuputFunc(k.Topic, msg, msg_len)

	atomic.AddInt64(&r.rc.OutputCount, int64(sum))

	logger.Debug("Output Kakfa lines : %d : %s", sum, time.Now().Sub(t))
}
