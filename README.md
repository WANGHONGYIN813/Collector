




[1] 运行环境：

	1) 操作系统： 

	Centos 7.5 


	2）Go 环境：

	<1>, go version go1.11.5 linux/amd64

	<2>, 特殊依赖包: 

		github.com/json-iterator/go

		github.com/Shopify/sarama

		gopkg.in/yaml.v2

		github.com/soniah/gosnmp

		crypto/tls

		mime/multipart

		archive/zip

	<3>, Kakfa Topic : 

		dpi_log_protocol
		dpi_log_port
		dpi_log_flow
		dpi_log_app
		topic_mdr_payment
		topic_mdr_takeaway
		topic_mdr_travel
		topic_mdr_email
		topic_dpi_xdr_com
		topic_dpi_xdr_ftp
		topic_dpi_xdr_rtsp
		topic_dpi_xdr_http
		topic_dpi_xdr_radius
		topic_dpi_xdr_https
		topic_dpi_xdr_dns
		topic_dpi_xdr_sip
		topic_dpi_xdr_email
		topic_syslog_av
		topic_syslog_apt
		topic_syslog_ips

		每个topic 参数： --replication-factor 2 --partitions 6

		每个topic 的 message.max.bytes=100000000


	<4>, hbase Table : 

		1. 表结构：

		{NAME => 'context', BLOOMFILTER => 'ROW', VERSIONS => '1', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING => 'NONE', TTL => 'F
OREVER', COMPRESSION => 'NONE', MIN_VERSIONS => '0', BLOCKCACHE => 'true', BLOCKSIZE => '65536', REPLICATION_SCOPE => '0'} 

		创建方法： create 'dpi_filerevert', 'context'

		2. Hbase Restful Server 端口：9900


[2] 使用方法 ：

	<1>, 编译(依赖包齐全条件下) :

		sh build.sh
			
		会生成data_collecter.bin ， KakfaConsume-gen.bin  ， KakfaConsume-dpi_filerevert.bin

		其中， data_collecter.bin 用于数据采集，对接DPI、DDOS等设备；
		
		KakfaConsume-gen.bin 用于消费kakfa，完成通用类型数据进行处理；

		KakfaConsume-dpi_filerevert.bin 用于消费 DPI 文件还原数据，属于 KakfaConsume-gen.bin 的补充；

		KakfaConsume-gen.bin 是由 data_collecter.bin copy而来，但软件承担功能不一样，所运行物理节点也不在一处，故发布两个独立程序。

	<2>, 启动方式 :

		1）， 下载 Data_Collecter_Prepare_Data_Install.tar  后，解压压缩包：

			tar zxvf Data_Collecter_Prepare_Data_Install.tar 

			执行 sh Data_Collecter_Prepare_Data_Install.sh 安装必要的数据(需要输入root密码)


		2)

		1. 采集引擎启动：

			./data_collecter.bin -f release_config_produce

		2. 数据处理系统启动：

			./KakfaConsume-gen.bin -f release_config

		3. 文件还原数据处理启动：

 			./KakfaConsume-dpi_filerevert.bin -f utils/KakfaConsume-dpi_filerevert/config.yaml


		data_collecter.bin 的输出对象为 kafka， KakfaConsume-gen.bin 的输出对象为es，KakfaConsume-dpi_filerevert.bin 的输出对象为es 和 hbase；

		release_config 为消费者的全部配置文件， release_config_produce 为生产者的全部配置文件；

		当前配置文件中es 的输出地址为10.0.24.42:9200,如果环境变化，可使用如下命令： 

		find . -name "*.yaml" | xargs sed -r -i 's/(10.0.24.42:9200)/10.0.24.60:9201/'
		
		实现批量替换配置文件中的es index 地址

		同样， 当前kakfa 配置地址为10.0.24.42:9092， 如果发生变化，可以使用如上命令实现批量内容替换

		修改配置文件后， 重启软件即可生效





