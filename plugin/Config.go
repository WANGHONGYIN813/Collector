package plugin

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	yaml "gopkg.in/yaml.v2"
	"sugon.com/WangHongyin/collecter/logger"
)

/************** Global Config ****************************/

var configfile = flag.String("f", "", "config File path Or dir path")
var web_port = flag.String("w", "", "specified web port")

var log_file_path = flag.String("l", "log_config.json", "log config file path")
var sysconfigfile = flag.String("s", "sysconfig.yaml", "system config file path")
var sysconfig_dir = flag.String("d", "", "sysconfig_dir")
var sys_cron = flag.String("c", "", "specified sys cron")
var parallel_num = flag.Int("p", 0, "Parallel Thread Pool Max Number. default 100")

var InputFilePaths = flag.String("F", "", "Target input Files : /tmp/.log")
var InputFileReadMode = flag.String("r", "", "read mode : default : from start")
var InputFileEsHost = flag.String("e", "", "es host for Input File data to write")
var InputFileType = flag.String("t", "", "_Type item for Input File data")
var InputFileIndex = flag.String("I", "", "index name for Input File data")
var InputFileInterval = flag.Int("g", 30, "time Interval for Input File scan")

var DebugMode = flag.Bool("D", false, "Debug mode")
var version_info = flag.Bool("v", false, "version info")

var crontime = flag.String("h", "0 55 23 * * ?", "when get today count")

var splitech = flag.Int("C", 0, "splite channel")

var esconnpoolnum = flag.Int("E", 50, "Es Conn Pool")
var tcpconnpoolnum = flag.Int("T", 10, "Tcp Conn Pool")
var splitepoolnum = flag.Int("S", 20, "Splite Pool")
var readxdrpoolnum = flag.Int("X", 30, "Read Xdr To Es Pool")

var softname string = "gbeat"
var version string = "V2.0_20191204"
var version_desc string = "by handw@sugon.com"

/********************************************************/

/************ input ********************************/

type IType int

const (
	InputStdout_t IType = 1
	InputFile_t   IType = 2
	InputTcp_t    IType = 3
)

type InputConfigFile_t struct {
	Paths         []string          `yaml:"paths" json:",omitempty"`
	Read_mode     string            `yaml:"read_mode" json:",omitempty"`
	Fields        map[string]string `yaml:"fields" json:",omitempty"`
	Interval      int               `yaml:"interval" json:",omitempty"`
	Beat_info     bool              `yaml:"beat_info" json:",omitempty"`
	Source_info   bool              `yaml:"source_info" json:",omitempty"`
	Exclude_lines []int             `yaml:"exclude_lines" json:",omitempty"`
	Type          string            `yaml:"type" json:",omitempty"`
	Indexvar      []string          `yaml:"indexvar" json:",omitempty"`
}

type InputConfigUdp_t struct {
	Addr             string            `yaml:"addr" json:",omitempty"`
	Fields           map[string]string `yaml:"fields" json:",omitempty"`
	Beat_info        bool              `yaml:"beat_info" json:",omitempty"`
	Source_info      bool              `yaml:"source_info" json:",omitempty"`
	Type             string            `yaml:"type" json:",omitempty"`
	Performance_mode bool              `yaml:"performance_mode" json:",omitempty"`
}

type InputConfigTcp_t struct {
	Addr             string            `yaml:"addr" json:",omitempty"`
	Fields           map[string]string `yaml:"fields" json:",omitempty"`
	Beat_info        bool              `yaml:"beat_info" json:",omitempty"`
	Source_info      bool              `yaml:"source_info" json:",omitempty"`
	Ipblacklist      []string          `yaml:"ipblacklist" json:",omitempty"`
	Type             string            `yaml:"type" json:",omitempty"`
	Lines_merge      bool              `yaml:"lines_merge" json:",omitempty"`
	Performance_mode bool              `yaml:"performance_mode" json:",omitempty"`
}

type InputConfigSyslog_t struct {
	Addr        string            `yaml:"addr" json:",omitempty"`
	Fields      map[string]string `yaml:"fields" json:",omitempty"`
	Ipblacklist []string          `yaml:"ipblacklist" json:",omitempty"`
	Type        string            `yaml:"type" json:",omitempty"`
	Distribute  map[string]string `yaml:"distribute" json:",omitempty"`
}

type InputConfigDbSyslog_t struct {
	Addr          string            `yaml:"addr" json:",omitempty"`
	Fields        map[string]string `yaml:"fields" json:",omitempty"`
	Ipblacklist   []string          `yaml:"ipblacklist" json:",omitempty"`
	Type          string            `yaml:"type" json:",omitempty"`
	Remove_Fields []string          `yaml:"remove_fields" json:",omitempty"`
	Mode          string            `yaml:"mode" json:",omitempty"`
	Udp_Limit     int               `yaml:"udplimit" json:",omitempty"`
}

type InputConfigKafka_t struct {
	Addr                []string          `yaml:"addr" json:",omitempty"`
	Topic               string            `yaml:"topic" json:",omitempty"`
	Group               int32             `yaml:"group" json:",omitempty"`
	Partition           string            `yaml:"partition" json:",omitempty"`
	Thread_pool_max_num int               `yaml:"thread_pool_max_num" json:",omitempty"`
	Offset              int64             `yaml:"offset" json:",omitempty"`
	Type                string            `yaml:"type" json:",omitempty"`
	Fields              map[string]string `yaml:"fields" json:",omitempty"`
}

type InputConfigSnmpwalk_t struct {
	Host        []string          `yaml:"host" json:",omitempty"`
	Type        string            `yaml:"type" json:",omitempty"`
	Interval    int               `yaml:"interval" json:",omitempty"`
	Oidnamelist []string          `yaml:"oidnamelist" json:",omitempty"`
	Fields      map[string]string `yaml:"fields" json:",omitempty"`
}

type InputConfigFtp_t struct {
	Addr        string `yaml:"addr" json:",omitempty"`
	User        string `yaml:"user" json:",omitempty"`
	Pass        string `yaml:"pass" json:",omitempty"`
	Active_port int    `yaml:"active_port" json:",omitempty"`
	//Passive_port int               `yaml:"passive_port" json:",omitempty"`
	Type   string            `yaml:"type" json:",omitempty"`
	Fields map[string]string `yaml:"fields" json:",omitempty"`
}

type InputConfigXdrFile_t struct {
	Read_interval int      `yaml:"read_interval" json:",omitempty"`
	Handle_mode   string   `yaml:"handle_mode" json:",omitempty"`
	Bath_path     string   `yaml:"base_path" json:",omitempty"`
	Dir_arr       []string `yaml:"dir_arr" json:",omitempty"`
}

type InputSyslogFile_t struct {
	Dir_path      string   `yaml:"dirpath" json:",omitempty"`
	File_suffix   string   `yaml:"filesuffix" json:",omitempty"`
	Remove_Fields []string `yaml:"remove_fields" json:",omitempty"`
}

type ConfigInputModule_t struct {
	File       InputConfigFile_t     `yaml:"file"`
	Tcp        InputConfigTcp_t      `yaml:"tcp"`
	Udp        InputConfigUdp_t      `yaml:"udp"`
	Syslog     InputConfigSyslog_t   `yaml:"syslog"`
	Kafka      InputConfigKafka_t    `yaml:"kafka"`
	Snmpwalk   InputConfigSnmpwalk_t `yaml:"snmpwalk"`
	Ftp        InputConfigFtp_t      `yaml:"ftp"`
	XdrFile    InputConfigXdrFile_t  `yaml:"xdrfile"`
	DbSyslog   InputConfigDbSyslog_t `yaml:"dbsyslog"`
	SyslogFile InputSyslogFile_t     `yaml:"syslogfile"`
}

/************ filter  ********************************/

type SplitItem_t struct {
	SplitMsg  string `json:",omitempty"`
	SplitFlag string `json:",omitempty"`
}

type MutateModule_t struct {
	Split               string                   `yaml:"split" json:",omitempty"`
	SplitItem           SplitItem_t              `json:",omitempty"`
	Split_fields        map[int]string           `yaml:"split_fields" json:",omitempty"`
	Convert             [][]string               `yaml:"convert" json:",omitempty"`
	Add_fields          map[string]string        `yaml:"add_field" json:",omitempty"`
	Add_fields_cal      map[string][]string      `yaml:"add_fields_cal" json:",omitempty"`
	Remove_fields       []string                 `yaml:"remove_fields" json:",omitempty"`
	Transform_fields    map[string][]interface{} `yaml:"transform_fields" json:",omitempty"`
	Geoip               [][]string               `yaml:"geoip" json:",omitempty"`
	Transform_to_utf8   []string                 `yaml:"transform_to_utf8" json:",omitempty"`
	add_fields_cal_flag bool
	Direct_operation    bool `yaml:"direct_operation" json:",omitempty"`
}

type MutateMapModule_t struct {
	Split               string              `yaml:"split" json:",omitempty"`
	Split_map           string              `yaml:"split_map" json:",omitempty"`
	SplitItem           SplitItem_t         `json:",omitempty"`
	SplitMap_Item       SplitItem_t         `json:",omitempty"`
	Split_fields        map[int]string      `yaml:"split_fields" json:",omitempty"`
	Convert             [][]string          `yaml:"convert" json:",omitempty"`
	Add_fields          map[string]string   `yaml:"add_field" json:",omitempty"`
	Add_fields_cal      map[string][]string `yaml:"add_fields_cal" json:",omitempty"`
	Remove_fields       []string            `yaml:"remove_fields" json:",omitempty"`
	Geoip               [][]string          `yaml:"geoip" json:",omitempty"`
	add_fields_cal_flag bool
}

type JsonModule_t struct {
	Source            string            `yaml:"source" json:",omitempty"`
	Add_fields        map[string]string `yaml:"add_field" json:",omitempty"`
	Remove_fields     []string          `yaml:"remove_fields" json:",omitempty"`
	Geoip             [][]string        `yaml:"geoip" json:",omitempty"`
	Transform_to_utf8 []string          `yaml:"transform_to_utf8" json:",omitempty"`
}

type MapModule_t struct {
	Source        string            `yaml:"source" json:",omitempty"`
	Add_fields    map[string]string `yaml:"add_field" json:",omitempty"`
	Remove_fields []string          `yaml:"remove_fields" json:",omitempty"`
}

type ByteModule_t struct {
	Source        string            `yaml:"source" json:",omitempty"`
	Add_fields    map[string]string `yaml:"add_field" json:",omitempty"`
	Remove_fields []string          `yaml:"remove_fields" json:",omitempty"`
}

type Tlv_Config_Format_t struct {
	TagName string
	TagKey  int
	TagType string
}

type TlvModule_t struct {
	Doc_type   string
	Log_type   int
	Proto_type int
	Tlv_config []Tlv_Config_Format_t
}

type United_TlvModule_t struct {
	Tlv []TlvModule_t
}

type ConfigFilterModule_t struct {
	Mutate     MutateModule_t                      `yaml:"mutate" json:",omitempty"`
	Mutatemap  MutateMapModule_t                   `yaml:"mutatemap" json:",omitempty"`
	Json       JsonModule_t                        `yaml:"json" json:",omitempty"`
	United_tlv []map[string]map[string]interface{} `yaml:"united_tlv" json:",omitempty"`
	Map        MapModule_t                         `yaml:"map" json:",omitempty"`
	Byte       ByteModule_t                        `yaml:"byte" json:",omitempty"`
}

/************ output ********************************/

type EsMode int

const (
	EsMode_COPY    EsMode = 0
	EsMode_POLLING EsMode = 1
	EsMode_RANDOM  EsMode = 2
)

type OutputElasticsearch_t struct {
	Index               string   `yaml:"index" json:",omitempty"`
	Index_prefix        string   `yaml:"index_prefix" json:",omitempty"`
	Index_var           string   `yaml:"index_var" json:",omitempty"`
	Host                []string `yaml:"host" json:",omitempty"`
	Id                  string   `yaml:"id" json:",omitempty"`
	Sendmode            string   `yaml:"sendmode" json:",omitempty"`
	Bulk                int      `yaml:"bulk" json:",omitempty"`
	EsUrl               []string `json:",omitempty"`
	Index_by_day        bool     `yaml:"index_by_day" json:",omitempty"`
	Mapping_load        string   `yaml:"mapping_load" json:",omitempty"`
	Index_var_remove    bool     `yaml:"index_var_remove" json:",omitempty"`
	esmode              EsMode   `yaml:"esmode" `
	es_mappings         string
	indexvarflag        bool
	Cron_desc           string `yaml:"cron"`
	index_url           string
	Mapping_load_enable bool
	Mapping_string      string
	enable              bool
}

type OutputFile_t struct {
	Path      string `yaml:"path" json:",omitempty"`
	LineCount int    `yaml:"linecount" json:",omitempty"`
}

type OutputTcp_t struct {
	Host  []string `yaml:"host" json:",omitempty"`
	Block int      `yaml:"block" json:",omitempty"`
}

type OutputKafka_t struct {
	Host             []string `yaml:"host" json:",omitempty"`
	Topic            string   `yaml:"topic" json:",omitempty"`
	Syncmode         bool     `yaml:"syncmode" json:",omitempty"`
	Partition        int      `yaml:"partition" json:",omitempty"`
	Maxmessagebytes  int      `yaml:"max_message_bytes" json:",omitempty"`
	Max_message_line int      `yaml:"max_message_line" json:",omitempty"`
}

type OutputClickhouse_t struct {
	Host        string `yaml:"host" json:",omitempty"`
	Database    string `yaml:"database" json:",omitempty"`
	TableFirst  string `yaml:"tablefirst" json:",omitempty"`
	TableSecond string `yaml:"tablesecond" json:",omitempty"`

	TableFirstFields  []string `yaml:"tablefirstfields" json:",omitempty"`
	TableSecondFields []string `yaml:"tablesecondfields" json:",omitempty"`
}

type OutputConsole_t struct {
	Quietmode bool `yaml:"quietmode" json:",omitempty"`
}

type ConfigOutputModule_t struct {
	Console       OutputConsole_t       `yaml:"console" json:",omitempty"`
	Tcp           OutputTcp_t           `yaml:"tcp"`
	Elasticsearch OutputElasticsearch_t `yaml:"elasticsearch"`
	File          OutputFile_t          `yaml:"file"`
	Kafka         OutputKafka_t         `yaml:"kafka"`
	Clickhouse    OutputClickhouse_t    `yaml:"clickhouse"`
}

/************ finish ********************************/

type FinType int

const (
	Finsh_Nothing FinType = 0
	Finsh_RM      FinType = 1
)

type ConfigFinishModule_t struct {
	Mode  string  `yaml:"mode" json:",omitempty"`
	ftype FinType `json:",omitempty"`
}

/**********************************************/

type YamlCommonData struct {
	Input          ConfigInputModule_t  `yaml:"input"`
	Filter         ConfigFilterModule_t `yaml:"filter"`
	Output         ConfigOutputModule_t `yaml:"output"`
	Finish         ConfigFinishModule_t `yaml:"finish"`
	configfilename string
}

/**********************************************/

type SysConfig_t struct {
	Parallel_pool_max_num         int    `yaml:"parallel_pool_max_num"`
	File_interval_default         int    `yaml:"file_interval_default"`
	Snmpwalk_interval_default     int    `yaml:"snmpwalk_interval_default"`
	Es_bulk_default_num           int    `yaml:"es_bulk_default_num"`
	Kakfa_output_slice            int    `yaml:"kakfa_output_slice"`
	Kafka_output_max_message_line int    `yaml:"kafka_output_max_message_line"`
	Tcp_buffer_max                int    `yaml:"tcp_buffer_max"`
	Udp_buffer_max                int    `yaml:"udp_buffer_max"`
	Ftp_default_port              int    `yaml:"ftp_default_port"`
	Ftp_active_default_port       int    `yaml:"ftp_active_default_port"`
	Mmdb_path                     string `yaml:"mmdb_path"`
	Web_port                      string `yaml:"web_port"`
	sysconfig_file_path           string
	Sys_cron                      string                 `yaml:"sys_cron"`
	File_listen_interval          int                    `yaml:"file_listen_interval"`
	Input_file_mode_index         string                 `yaml:"input_file_mode_index"`
	Mysql_config                  map[string]interface{} `yaml:"mysql_config"`
}

var SysConfig = SysConfig_t{
	Parallel_pool_max_num:         100,
	File_interval_default:         10,
	Es_bulk_default_num:           10000,
	Tcp_buffer_max:                409600,
	Udp_buffer_max:                1560,
	Ftp_default_port:              21,
	Kakfa_output_slice:            20000,
	Kafka_output_max_message_line: 20000,
	Ftp_active_default_port:       20,
	Mmdb_path:                     "/opt/Geo_db/GeoLite2-City.mmdb",
	Web_port:                      "8001",
	sysconfig_file_path:           "/opt/sysconfig/",
	Sys_cron:                      "0 50 23 * * ?",
	File_listen_interval:          10,
	Input_file_mode_index:         softname + "_filelog_",
}

func SysConfigGet() *SysConfig_t {
	return &SysConfig
}

func SysConfigParse(file string) {

	y, err := ioutil.ReadFile(file)
	if err != nil {
		logger.Info("System Config File Not Exit. Use default")
		return
	}
	_ = yaml.Unmarshal(y, &SysConfig)

	if *web_port != "" {
		SysConfig.Web_port = *web_port
	}

	if *sys_cron != "" {

		SysConfig.Sys_cron = *sys_cron
	}

	if *parallel_num != 0 {
		SysConfig.Parallel_pool_max_num = *parallel_num
		logger.Info("Global Parallel Thread Pool Max Number : ", SysConfig.Parallel_pool_max_num)
	}

	logger.Info("%+v", SysConfig)
}

/************* Parse  Yaml Config File  ******************/

func ParseYamlBaseConfig(yamlfile string) YamlCommonData {

	var config YamlCommonData

	yamlFile, err := ioutil.ReadFile(yamlfile)
	if err != nil {
		logger.Fatal("yamlFile.Get err #%v \n", err)
		os.Exit(1)
	}

	_ = yaml.Unmarshal(yamlFile, &config)

	//logger.Info("%+v", config)

	config.configfilename = filepath.Base(yamlfile)

	return config
}

/************** Config Data Init *********************/

var desc_format = "Input Pugin : %s (%s) ; Filter Pugin : %s ; Output Pugin : %s (%s)"

func InitConfigCommonData(config *YamlCommonData) (string, error) {

	input := &config.Input

	iname := SearchItemName_for_NotZero(input)

	var iaddr string

	switch iname {
	case "SyslogFile":
		iaddr = input.SyslogFile.Dir_path

	case "XdrFile":
		iaddr = input.XdrFile.Bath_path

	case "File":

		iaddr = strings.Join(input.File.Paths, ",")

	case "Tcp":

		iaddr = input.Tcp.Addr

	case "Syslog":
		iaddr = input.Syslog.Addr

	case "DbSyslog":
		iaddr = input.DbSyslog.Addr

	case "Kafka":

		iaddr = strings.Join(input.Kafka.Addr, ",")

		if input.Kafka.Offset == 0 {
			input.Kafka.Offset = sarama.OffsetNewest
		} else if input.Kafka.Offset == -1 {
			input.Kafka.Offset = sarama.OffsetOldest
		}

	case "Snmpwalk":
		iaddr = strings.Join(input.Snmpwalk.Host, ",")

	case "Ftp":
		iaddr = input.Ftp.Addr
	}

	filter := &config.Filter

	fname := SearchItemName_for_NotZero(filter)

	switch fname {

	case "Mutate":

		filter.Mutate.SplitItem.SplitMsg, filter.Mutate.SplitItem.SplitFlag = SplitStringParse(filter.Mutate.Split)

		if len(filter.Mutate.Add_fields_cal) != 0 {
			err := Add_fields_Calculation_Check(filter.Mutate.Add_fields_cal)
			if err != nil {
				return "", err
			}
			filter.Mutate.add_fields_cal_flag = true
		}

	case "Mutatemap":

		filter.Mutatemap.SplitItem.SplitMsg, filter.Mutatemap.SplitItem.SplitFlag = SplitStringParse(filter.Mutatemap.Split)
		filter.Mutatemap.SplitMap_Item.SplitMsg, filter.Mutatemap.SplitMap_Item.SplitFlag = SplitStringParse(filter.Mutatemap.Split_map)

		if len(filter.Mutate.Add_fields_cal) != 0 {
			err := Add_fields_Calculation_Check(filter.Mutate.Add_fields_cal)
			if err != nil {
				return "", err
			}

			filter.Mutate.add_fields_cal_flag = true
		}

	case "Json":

	case "Map":
	}

	output := &config.Output

	oname := SearchItemName_for_NotZero(output)

	var oaddr string

	switch oname {

	case "Elasticsearch":

		output.Elasticsearch.Host = EnvSliceVarCheck(output.Elasticsearch.Host)
		logger.Alert("Elasticsearch.Host :", output.Elasticsearch.Host)

		switch output.Elasticsearch.Sendmode {
		case "copy":
			output.Elasticsearch.esmode = EsMode_COPY
		case "random":
			output.Elasticsearch.esmode = EsMode_RANDOM
		case "polling":
			output.Elasticsearch.esmode = EsMode_POLLING
		default:
			output.Elasticsearch.esmode = EsMode_POLLING
		}

		oaddr = output.Elasticsearch.Sendmode + " Mode : "

		var err error

		if output.Elasticsearch.Index_var != "" && output.Elasticsearch.Index_prefix != "" {
			output.Elasticsearch.indexvarflag = true
		}

		if output.Elasticsearch.Mapping_load != "" {

			output.Elasticsearch.es_mappings, err = MappingFileParse(output.Elasticsearch.Mapping_load)
			if err != nil {
				return "", err
			}

			output.Elasticsearch.Mapping_load_enable = true

			if !output.Elasticsearch.indexvarflag {

				if output.Elasticsearch.esmode == EsMode_COPY {

					for _, v := range output.Elasticsearch.Host {

						base_url := "http://" + v + "/" + output.Elasticsearch.Index

						if output.Elasticsearch.Index_by_day {

							current_date := time.Now().Format("2006-01-02")

							uu := base_url + "-" + current_date

							logger.Alert("Loading es mapping for url(IndexByDay) :", uu)

							EsMappingsSetup(uu, uu, output.Elasticsearch.es_mappings)
						} else {

							logger.Alert("Loading es mapping for url(Fixed) :", base_url)

							EsMappingsSetup(base_url, base_url, output.Elasticsearch.es_mappings)
						}
					}
				} else {

					base_url := "http://" + output.Elasticsearch.Host[0] + "/" + output.Elasticsearch.Index

					logger.Alert("Loading es mapping for Main url :", base_url)

					if output.Elasticsearch.Index_by_day {

						current_date := time.Now().Format("2006-01-02")

						base_url = base_url + "-" + current_date

						EsMappingsSetup(base_url, output.Elasticsearch.Index+"-"+current_date, output.Elasticsearch.es_mappings)
					} else {

						EsMappingsSetup(base_url, output.Elasticsearch.Index, output.Elasticsearch.es_mappings)
					}

				}
			}
		}

		for _, v := range output.Elasticsearch.Host {

			if output.Elasticsearch.Bulk == 0 {
				output.Elasticsearch.Bulk = SysConfig.Es_bulk_default_num
			}

			var a string

			var base_url string

			if output.Elasticsearch.Index != "" {

				base_url = "http://" + v + "/" + output.Elasticsearch.Index

				a = base_url + "/doc/_bulk"

			} else if output.Elasticsearch.Index_prefix != "" {

				a = "http://" + v + "/" + output.Elasticsearch.Index_prefix + "IndexVar" + "/doc/_bulk"

				output.Elasticsearch.Index_var = strings.Trim(output.Elasticsearch.Index_var, " ")

				output.Elasticsearch.indexvarflag = true
			} else {

				logger.Alert("Output Elasticsearch Config Fail")

				return "", errors.New("Output Elasticsearch Config Fail")
			}

			output.Elasticsearch.EsUrl = append(output.Elasticsearch.EsUrl, a)

		}

		oaddr += strings.Join(output.Elasticsearch.Host, ",")

	case "Tcp":

		oaddr = strings.Join(output.Tcp.Host, ",")

	case "File":

		//f, _ := os.OpenFile(output.File.Path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0766)
		//defer f.Close()

		oaddr = output.File.Path

	case "Clickhouse":
		oaddr = output.Clickhouse.Host + "/" + output.Clickhouse.Database

	case "Stdout":

	}

	desc := fmt.Sprintf(desc_format, iname, iaddr, fname, oname, oaddr)

	return desc, nil
}

/*********************************************/

/*********************************************/

func VersionInfo() {
	fmt.Println("")
	fmt.Println(VersionInfoGet())
	fmt.Println("\t", version_desc)
	os.Exit(0)
}

func LoggerInit() {

	configname := filepath.Base(GetDefaultConfigName())

	logger.SetLogger(*log_file_path, configname)

	logger.Info("Start %s %s", softname, version)
	logger.Info(version_desc)
}

func VersionInfoGet() string {
	s := softname + " " + version
	return s
}

func env_var_init() {

	if *sysconfig_dir != "" {

		*sysconfigfile = *sysconfig_dir + "/" + *sysconfigfile
		*log_file_path = *sysconfig_dir + "/" + *log_file_path

	} else {

		if PathExists(SysConfig.sysconfig_file_path) == false {

			tmp_path := "./sysconfig/"

			if PathExists(tmp_path) == false {
				logger.Fatal("Could not find System Config Path : %s and %s", SysConfig.sysconfig_file_path, tmp_path)
			} else {
				SysConfig.sysconfig_file_path = tmp_path
			}
		}
		logger.Info("Load System Config in : %s", SysConfig.sysconfig_file_path)

		*log_file_path = SysConfig.sysconfig_file_path + *log_file_path
		*sysconfigfile = SysConfig.sysconfig_file_path + *sysconfigfile
	}

}

func init() {

	flag.Parse()

	if *version_info == true {
		VersionInfo()
	}

	if *configfile == "" && *InputFilePaths == "" {
		flag.Usage()
		VersionInfo()
	}

	runtime.GOMAXPROCS(runtime.NumCPU())

	rand.New(rand.NewSource(time.Now().UnixNano()))

	if InputFileModeCheck() {
		return
	}

	env_var_init()

	LoggerInit()

	SysConfigParse(*sysconfigfile)

	Utils_init()
}

func DebugModeGet() bool {
	return *DebugMode
}

func ConfigSaveMessage(c *YamlCommonData) {

	b := Json_Marshal(c)

	logger.Info(string(b))
}

func ConfigInit(configfile string) *YamlCommonData {

	config := ParseYamlBaseConfig(configfile)

	go ConfigSaveMessage(&config)

	return &config
}

func ParseConfigFromString(msg string) *YamlCommonData {

	var c YamlCommonData

	_ = json.Unmarshal([]byte(msg), &c)

	go ConfigSaveMessage(&c)

	return &c
}

/**********************************************/

func GetDefaultConfigName() string {
	return *configfile
}

func GetDefaultConfig() *YamlCommonData {

	return ConfigInit(*configfile)
}

/**********************************************/

func InputFileModeCheck() bool {

	if *InputFilePaths == "" {
		return false
	}

	return true
}

var FileLogMapping string = `{
	"settings": {
        "number_of_shards": "1",
        "number_of_replicas": 0,
        "index" : { 
            "refresh_interval" : "60s",
            "translog": {
                "durability": "async",
                "sync_interval": "30s",
                "flush_threshold_size": "4g"
            }   
        }   
    },  
    "mappings":{
        "doc":{
            "properties":{
                "@timestamp":{
                    "type":"date"
                },  
                "Message":{
                    "index":false,
                    "norms":false,
                    "type":"text"
                }, 
                "_Type":{
                    "index":false,
                    "norms":false,
                    "type":"text"
				},
                "_Date":{
                    "index":false,
                    "norms":false,
                    "type":"text"
				},
                "_Source":{
                    "index":false,
                    "norms":false,
                    "type":"text"
				}
			}
		}
	}
}`

func InputFileModeInit() *YamlCommonData {

	var fp = []string{*InputFilePaths}

	c := new(YamlCommonData)

	c.Input.File.Paths = fp

	c.Input.File.Read_mode = *InputFileReadMode
	c.Input.File.Type = *InputFileType
	c.Input.File.Interval = *InputFileInterval

	if *InputFileEsHost != "" {

		var es = []string{*InputFileEsHost}

		c.Output.Elasticsearch.Host = es

		if *InputFileIndex != "" {
			c.Output.Elasticsearch.Index_prefix = *InputFileIndex
		} else {
			c.Output.Elasticsearch.Index_prefix = SysConfig.Input_file_mode_index
		}

		c.Output.Elasticsearch.Index_var = "Indexvar"

		c.Output.Elasticsearch.Index_var_remove = true

		c.Output.Elasticsearch.Mapping_load_enable = true

		c.Output.Elasticsearch.Mapping_string = FileLogMapping

		c.Input.File.Indexvar = []string{"_Type", "_Date"}
	}

	return c
}

/**********************************************/
