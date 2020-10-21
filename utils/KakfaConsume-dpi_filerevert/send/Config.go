package send

import (
	"flag"
	yaml "gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"time"
)

type Kafka_t struct {
	Addr      []string `yaml:"addr"`
	Topic     string   `yaml:"topic"`
	Partition string   `yaml:"partition"`
}

type Es_t struct {
	Host                string `yaml:"host"`
	Index               string `yaml:"index"`
	index_url           string
	Index_by_day        bool   `yaml:"index_by_day"`
	Mapping_load        string `yaml:"mapping_load"`
	Cron_desc           string `yaml:"cron"`
	Mapping_load_enable bool
	Mapping_string      string
	enable              bool
}

type Pic_check_t struct {
	Enable     bool
	Check_addr string   `yaml:"check_addr"`
	Bulk_num   int      `yaml:"bulk_num"`
	File_type  []string `yaml:"file_type"`
	CheckMap   map[string]bool
}

type Httpfs_t struct {
	Host string `yaml:"host"`
	Home string `yaml:"home"`
	Dir  string `yaml:"dir"`
	User string `yaml:"user"`
}

type Hbase_t struct {
	Host   string `yaml:"host"`
	Table  string `yaml:"table"`
	Column string `yaml:"column"`
	Batch  bool   `yaml:"batch"`
	enable bool
}

type YamlCommonData struct {
	Kafka       Kafka_t     `yaml:"kafka"`
	Buffer_dir  string      `yaml:"buffer_dir"`
	Stroage_dir string      `yaml:"stroage_dir"`
	Httpfs      Httpfs_t    `yaml:"httpfs"`
	Hbase       Hbase_t     `yaml:"hbase"`
	Pic_check   Pic_check_t `yaml:"pic_check"`
	Es          Es_t        `yaml:"es"`
}

var configfile = flag.String("f", "config.yaml", "config File path Or dir path")
var parallel_num = flag.Int("p", 10, "Thread Pool Max Number")

var Gconfig YamlCommonData

var es_mapping_load_default_cron_desc = "0 50 23 * * ?"

func init() {

	flag.Parse()

	yamlFile, err := ioutil.ReadFile(*configfile)
	if err != nil {
		log.Printf("yamlFile.Get err #%v ", err)
	}

	_ = yaml.Unmarshal(yamlFile, &Gconfig)

	log.Printf("Config : %+v\n", Gconfig)

	if Gconfig.Es.Host != "" {
		output_es_init()
	}
}

func output_es_init() {

	Gconfig.Es.enable = true

	var err error

	if Gconfig.Es.Mapping_load != "" {

		Gconfig.Es.index_url = Gconfig.Es.Host + "/" + Gconfig.Es.Index

		log.Println("Load Mapping For Index: ", Gconfig.Es.index_url)

		Gconfig.Es.Mapping_load_enable = true

		Gconfig.Es.Mapping_string, err = MappingFileParse(Gconfig.Es.Mapping_load)
		if err != nil {
			log.Println("Init mapping err:", err)
		}

		base_url := Gconfig.Es.index_url

		if Gconfig.Es.Index_by_day {

			if Gconfig.Es.Cron_desc == "" {
				Gconfig.Es.Cron_desc = es_mapping_load_default_cron_desc
			}

			go LoadMapping_CronJob_Start(Gconfig.Es.Cron_desc, Gconfig.Es.index_url, Gconfig.Es.Mapping_string)

			current_date := time.Now().Format("2006-01-02")

			base_url = base_url + "-" + current_date

		}

		log.Println("Loadding Mapping For Index: ", base_url)
		EsMappingsSetup(base_url, Gconfig.Es.Mapping_string)
	}

}
