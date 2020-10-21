#!/bin/sh

echo "Start to Collect DPI Consumer Logs"

data_collecter -F  /root/dpi_frs.log  -t dpi_consumer_frs -e 10.0.24.60:9201  &

data_collecter -F  /tmp/gbeat-log/dpi_xdr_appid_consumer_configs_in_use_v1_3-app.log  -t dpi_consumer_xdr -e 10.0.24.60:9201  &

data_collecter -F  /tmp/gbeat-log/dpi_mdr_associated_config-app.log  -t dpi_consumer_mdr_associated -e 10.0.24.60:9201  &

data_collecter -F  /tmp/gbeat-log/dpi_mdr_consumer_configs-app.log  -t dpi_consumer_mdr -e 10.0.24.60:9201  &

data_collecter -F  /tmp/gbeat-log/syslog_consumer-app.log  -t syslog_consumer  -e 10.0.24.60:9201  &

data_collecter -F  /tmp/gbeat-log/dpi_mdr_im_consumer_qq.yaml-app.log  -t dpi_consumer_mdr_qq  -e 10.0.24.60:9201  &

echo "DPI Consumer Logs Done"


echo "Start to Collect DPI Product Logs"

data_collecter -F /tmp/gbeat-log/dpi_xdr_produce_configs-app.log -t dpi_product_xdr -e 10.0.24.60:9201 &

data_collecter -F /tmp/gbeat-log/dpi_mdr_produce_config_associated-app.log -t dpi_product_mdr -e 10.0.24.60:9201 &

data_collecter -F /tmp/gbeat-log/dpi_mdr_im_produce.yaml-app.log -t dpi_product_mdr_qq -e 10.0.24.60:9201 &

data_collecter -F /tmp/gbeat-log/syslog_produce_configs-app.log -t syslog_product -e 10.0.24.60:9201 &


echo "DPI Product Logs Done"



echo "Start to Collect Web Dev Logs"

data_collecter -F /opt/dawning/NSA_JAVA/DawningNSA_jar/log/spring-boot-log-info.log -e 10.0.24.60:9201 -t webdev  &

data_collecter -F /opt/dawning/NSA_JAVA/DawningUser_jar/log/spring-boot-log-info.log -e 10.0.24.60:9201 -t webuser  &


echo "Web Dev Logs Done"




echo "Start to Collect URL Plugin Logs"

data_collecter -F /root/logs/ybzt.log -e 10.0.24.60:9201 -t url_plugin  &


echo "URL Plugin Logs Done"




echo "Start to Collect person_portrait_ws Logs"


data_collecter -F /root/v1/Offline_Analysis/Person_Portrait_Analysis/2019_10_30_web_server_log_8012 -t person_portrait_ws -e 10.0.24.60:9201  &


echo "person_portrait_ws Done"



echo "Start to Collect Kafka Logs"

data_collecter -F /home/kafka/kafka_2.12-2.0.0/logs/server.log -t kafka_server -e 10.0.24.60:9201  &
data_collecter -F /home/kafka/kafka_2.12-2.0.0/logs/controller.log -t kafka_controller -e 10.0.24.60:9201  &
data_collecter -F /home/kafka/kafka_2.12-2.0.0/logs/controller.log -t kafka_state-change -e 10.0.24.60:9201  &


echo "Kafka Logs Done"


echo "Start to Collect Hadoop Logs"

data_collecter -F /home/hadoop/hadoop/logs/hadoop-hadoop-datanode-hadoop1.log -t hadoop_datanode -e 10.0.24.60:9201  &
data_collecter -F /home/hadoop/hadoop/logs/hadoop-hadoop-namenode-hadoop1.log -t hadoop_namenode -e 10.0.24.60:9201  &

echo "Hadoop Logs Done"


echo "Start to Collect AI Logs"

data_collecter -F /root/pic.out -t ai_pic -e 10.0.24.60:9201  &


echo "AI Logs Done"



