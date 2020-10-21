#!/bin/sh

BinPath=/root/data_collecter_product
ConfigPath=$BinPath/hanyang_project_config


BinName=data_collecter_v5
RunPath=$BinPath/$BinName

FR_ConfigPath=$ConfigPath/dpi_filerevert_config


log_path=$BinPath/logs

cp mappings/*  /opt/

echo "mappings CP Done"


sysconfigPath=/opt/sysconfig

cp -r sysconfig  /opt/

if [ -d $sysconfigPath ];then
	echo "Clear $sysconfigPath"
	rm -rf $sysconfigPath
fi

cp -r sysconfig  $sysconfigPath

echo "sysconfig CP Done"


GEO=/opt/Geo_db/GeoLite2-City.mmdb

if [ ! -f $GEO ];then
echo "$GEO DO NOT EXIST!, Please Fix it"
exit
fi


if [ -d $log_path ];then
	echo "Clear Log Dir $log_path"
	rm -rf $log_path
fi

mkdir $log_path


echo "$RunPath Start"

$RunPath  -f $ConfigPath/dpi_xdr_es_configs/ -w 8001 &
$RunPath -f $ConfigPath/dpi_mdr_es_configs/ -w 8002 &
$RunPath -f $ConfigPath/dpi_log_es_configs/ -w 8003 &

$RunPath -f $ConfigPath/syslog_sunyainfo_configs/ -w 8004 &


echo "$BinName Start Done"


FR_BinName=dpi_filerevert_consumer
FR_RunPath=$BinPath/$FR_BinName

echo "Start File revert Bins"

$RunPath -f $FR_ConfigPath/fr_product_config.yaml  -w 8020 &

echo "FR Product Done"

$FR_RunPath -p 5 -f $FR_ConfigPath/fr_consumer_config.yaml 2> $log_path/dpi_frs.log  &

echo "FR Consumer Done"




processname=$BinName
 
cmd="ps aux |grep $processname | grep -v grep | awk '{print \$2}' |xargs  kill"
 
echo $cmd > kill_$BinName.sh



