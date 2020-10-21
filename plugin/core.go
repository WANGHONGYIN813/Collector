package plugin

import (
	"sugon.com/WangHongyin/collecter/cron"
	"sugon.com/WangHongyin/collecter/logger"

	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var lock sync.Mutex
var lock1 sync.Mutex

/*************** Register Info *************************/

type RegisterInfo struct {
	Plugin  string
	Type    string
	SubType string
	Vendor  string
	Desc    string
	init    func(c *YamlCommonData, out interface{}, countptr interface{}, statptr *RouteStatType) interface{}
}

/************** Count Indo *****************************/

type RouteCount struct {
	InputCount   int64
	OutputCount  int64
	ErrCount     int64
	InvalidCount int64
}

type RouteInputCounts struct {
	RouteCount
	InputFilterNumbers int64
}

/*************** Base Info ****************************/

type InputBaseInfo struct {
	config  *YamlCommonData
	rc      *RouteInputCounts
	reginfo *RegisterInfo
}

type BaseInfo struct {
	config  *YamlCommonData
	rc      *RouteCount
	reginfo *RegisterInfo
}

func (r *BaseInfo) CountGet(c *RouteCount) {
	*c = *r.rc
}

/*************** Output Interface **********************/

type OutputCommonCount struct {
	OutputAddr string
	InputLines int
	OutputSend int
}

type OutputCommon struct {
	BaseInfo
	c []OutputCommonCount
}

type OutputInterface interface {
	OutputProcess(msg interface{}, num int)
}

/************** Filter Interface **********************/
type FilterCommon struct {
	BaseInfo
	output *OutputInterface
}

type FilterInterface interface {
	FilterProcess(msg interface{}, num int, addtag interface{})
}

/************** Input Interface ********************/

type InputCommon struct {
	InputBaseInfo
	filter   *FilterInterface
	switcher *int32
}

type InputInterface interface {
	MainLoop() error
	exit()
}

/************  Route Info  ***************************/

/************  Route Count  **************************/

type RouteOpsCounts struct {
	Inputcount  RouteInputCounts
	Filtercount RouteCount
	Outputcount RouteCount
}

type RouteOpsCountInfo struct {
	Stat   RouteOpsStats
	Counts RouteOpsCounts
}

/************  Route Stat  **************************/

type RouteStatType int32

const (
	RouteStat_Off   RouteStatType = 0
	RouteStat_On    RouteStatType = 1
	RouteStat_Ready RouteStatType = 2
)

type RouteOpsStats struct {
	Index       int           `json:",omitempty"`
	TimeStamp   string        `json:",omitempty"`
	StatType    RouteStatType `json:"status,omitempty"`
	Description string        `json:",omitempty"`
	ConfigFile  string        `json:",omitempty"`
}

/************  Route Table  **************************/

type RouteTable_t struct {
	Stat   RouteOpsStats
	Counts RouteOpsCounts
	Config *YamlCommonData
}

/************  Route Ops  **************************/

type RouteOps struct {
	RouteTable RouteTable_t
	Input      InputInterface
	Filter     FilterInterface
	Output     OutputInterface
}

/*************************************************/

var gRouteOps []*RouteOps

var gRouteOpsTodayCounts []*RouteOpsCounts

var gRouteOpsTodayStats []*RouteOpsStats

var gIndex int32

var EverydayCount map[string][]RouteOpsCountInfo //记录每日统计

var kafkaConsumingErrorTimes []string

/***************************************************/

func RouteCountsRegister(r *RouteOpsCounts) {
	gRouteOpsTodayCounts = append(gRouteOpsTodayCounts, r)
}

func RouteStatRegister(r *RouteOpsStats) {
	gRouteOpsTodayStats = append(gRouteOpsTodayStats, r)
}

func RouteOpsRegister(r *RouteOps) {
	gRouteOps = append(gRouteOps, r)
	//atomic.AddInt32(&gIndex, 1)
}

func GetRouteOpsRegisterNum() int {
	return len(gRouteOps)
}

func RouteOpsRemove(s []int, index int) []int {
	return append(s[:index], s[index+1:]...)
}

func RouteOpsUnRegister(index int) error {

	if gRouteOps[index] == nil {
		return errors.New("No such Index")
	}

	gRouteOps = append(gRouteOps[:index], gRouteOps[index+1:]...)
	atomic.AddInt32(&gIndex, -1)

	return nil
}

func RouteCountClear() {
	for _, v := range gRouteOps {
		in := &v.RouteTable.Counts.Inputcount

		atomic.StoreInt64(&in.InputCount, 0)
		atomic.StoreInt64(&in.OutputCount, 0)
		atomic.StoreInt64(&in.ErrCount, 0)
		atomic.StoreInt64(&in.InvalidCount, 0)
		atomic.StoreInt64(&in.InputFilterNumbers, 0)

		f := &v.RouteTable.Counts.Filtercount

		atomic.StoreInt64(&f.InputCount, 0)
		atomic.StoreInt64(&f.OutputCount, 0)
		atomic.StoreInt64(&f.ErrCount, 0)
		atomic.StoreInt64(&f.InvalidCount, 0)

		o := &v.RouteTable.Counts.Outputcount

		atomic.StoreInt64(&o.InputCount, 0)
		atomic.StoreInt64(&o.OutputCount, 0)
		atomic.StoreInt64(&o.ErrCount, 0)
		atomic.StoreInt64(&o.InvalidCount, 0)
	}
}

func RouteCountTodayClear() {
	lock1.Lock()
	for k, _ := range gRouteOpsTodayCounts {
		gRouteOpsTodayCounts[k].Filtercount.ErrCount = gRouteOps[k].RouteTable.Counts.Filtercount.ErrCount
		gRouteOpsTodayCounts[k].Filtercount.InputCount = gRouteOps[k].RouteTable.Counts.Filtercount.InputCount
		gRouteOpsTodayCounts[k].Filtercount.InvalidCount = gRouteOps[k].RouteTable.Counts.Filtercount.InvalidCount
		gRouteOpsTodayCounts[k].Filtercount.OutputCount = gRouteOps[k].RouteTable.Counts.Filtercount.OutputCount

		gRouteOpsTodayCounts[k].Inputcount.InputFilterNumbers = gRouteOps[k].RouteTable.Counts.Inputcount.InputFilterNumbers
		gRouteOpsTodayCounts[k].Inputcount.RouteCount.ErrCount = gRouteOps[k].RouteTable.Counts.Inputcount.RouteCount.ErrCount
		gRouteOpsTodayCounts[k].Inputcount.RouteCount.InputCount = gRouteOps[k].RouteTable.Counts.Inputcount.RouteCount.InputCount
		gRouteOpsTodayCounts[k].Inputcount.RouteCount.InvalidCount = gRouteOps[k].RouteTable.Counts.Inputcount.RouteCount.InvalidCount
		gRouteOpsTodayCounts[k].Inputcount.RouteCount.OutputCount = gRouteOps[k].RouteTable.Counts.Inputcount.RouteCount.OutputCount

		gRouteOpsTodayCounts[k].Outputcount.ErrCount = gRouteOps[k].RouteTable.Counts.Outputcount.ErrCount
		gRouteOpsTodayCounts[k].Outputcount.InputCount = gRouteOps[k].RouteTable.Counts.Outputcount.InputCount
		gRouteOpsTodayCounts[k].Outputcount.InvalidCount = gRouteOps[k].RouteTable.Counts.Outputcount.InvalidCount
		gRouteOpsTodayCounts[k].Outputcount.OutputCount = gRouteOps[k].RouteTable.Counts.Outputcount.OutputCount

	}
	lock1.Unlock()
}

// func RouteCountTodayInit() {
// 	for k, v := range gRouteOps {
// 		gRouteOpsTodayCounts[k] = &(gRouteOps[k].RouteTable.Counts)
// 	}
// }

func RouteCountGet() []RouteOpsCounts {
	var c []RouteOpsCounts
	for _, v := range gRouteOps {
		c = append(c, v.RouteTable.Counts)
	}
	return c
}

func RouteCountInfoGet() []RouteOpsCountInfo {

	var c []RouteOpsCountInfo

	for _, v := range gRouteOps {

		info := RouteOpsCountInfo{Stat: v.RouteTable.Stat, Counts: v.RouteTable.Counts}

		c = append(c, info)
	}

	var sum RouteOpsCountInfo

	sum.Stat.Description = "Sum Counter"

	for _, v := range c {
		sum.Counts.Inputcount.InputCount += v.Counts.Inputcount.InputCount
		sum.Counts.Inputcount.OutputCount += v.Counts.Inputcount.OutputCount
		sum.Counts.Inputcount.ErrCount += v.Counts.Inputcount.ErrCount
		sum.Counts.Inputcount.InvalidCount += v.Counts.Inputcount.InvalidCount

		sum.Counts.Filtercount.InputCount += v.Counts.Filtercount.InputCount
		sum.Counts.Filtercount.OutputCount += v.Counts.Filtercount.OutputCount
		sum.Counts.Filtercount.ErrCount += v.Counts.Filtercount.ErrCount
		sum.Counts.Filtercount.InvalidCount += v.Counts.Filtercount.InvalidCount

		sum.Counts.Outputcount.InputCount += v.Counts.Outputcount.InputCount
		sum.Counts.Outputcount.OutputCount += v.Counts.Outputcount.OutputCount
		sum.Counts.Outputcount.ErrCount += v.Counts.Outputcount.ErrCount
		sum.Counts.Outputcount.InvalidCount += v.Counts.Outputcount.InvalidCount
	}

	c = append(c, sum)

	return c

}

func RouteCountInfoTodayGet() []RouteOpsCountInfo {
	var c []RouteOpsCountInfo
	for k, _ := range gRouteOpsTodayCounts {
		rctoday := CalculationToday(*gRouteOpsTodayCounts[k], gRouteOps[k].RouteTable.Counts) //计算当天数据
		info := RouteOpsCountInfo{Stat: *gRouteOpsTodayStats[k], Counts: rctoday}
		c = append(c, info)
	}

	return c
}

func CalculationToday(today RouteOpsCounts, all RouteOpsCounts) RouteOpsCounts {
	rc := new(RouteOpsCounts)
	lock.Lock()
	rc.Inputcount.RouteCount.ErrCount = all.Inputcount.RouteCount.ErrCount - today.Inputcount.RouteCount.ErrCount
	rc.Inputcount.RouteCount.InputCount = all.Inputcount.RouteCount.InputCount - today.Inputcount.RouteCount.InputCount
	rc.Inputcount.RouteCount.InvalidCount = all.Inputcount.RouteCount.InvalidCount - today.Inputcount.RouteCount.InvalidCount
	rc.Inputcount.RouteCount.OutputCount = all.Inputcount.RouteCount.OutputCount - today.Inputcount.RouteCount.OutputCount
	rc.Inputcount.InputFilterNumbers = all.Inputcount.InputFilterNumbers - today.Inputcount.InputFilterNumbers

	rc.Filtercount.ErrCount = all.Filtercount.ErrCount - today.Filtercount.ErrCount
	rc.Filtercount.InputCount = all.Filtercount.InputCount - today.Filtercount.InputCount
	rc.Filtercount.InvalidCount = all.Filtercount.InvalidCount - today.Filtercount.InvalidCount
	rc.Filtercount.OutputCount = all.Filtercount.OutputCount - today.Filtercount.OutputCount

	rc.Outputcount.ErrCount = all.Outputcount.ErrCount - today.Outputcount.ErrCount
	rc.Outputcount.InputCount = all.Outputcount.InputCount - today.Outputcount.InputCount
	rc.Outputcount.InvalidCount = all.Outputcount.InvalidCount - today.Outputcount.InvalidCount
	rc.Outputcount.OutputCount = all.Outputcount.OutputCount - today.Outputcount.OutputCount
	lock.Unlock()
	return *rc
}

func RouteConfigGet() []*YamlCommonData {
	var c []*YamlCommonData
	for _, v := range gRouteOps {
		c = append(c, v.RouteTable.Config)
	}
	return c
}

func RouteConfigLoad(config string) error {

	id := int(atomic.LoadInt32(&gIndex))

	_, err := NewRouteOps(ParseConfigFromString(config), id)

	atomic.AddInt32(&gIndex, 1)

	return err
}

type TarOpsThread struct {
	TarConfigFileName string
	Err               error
}

func RouteOpsStart(f string, id int, p chan<- TarOpsThread) {

	_, err := NewRouteOps(ConfigInit(f), id)

	r := TarOpsThread{
		TarConfigFileName: f,
		Err:               err,
	}

	p <- r
}

func RouteConfigLoadDefault() error {

	target_path := GetDefaultConfigName()

	f, err := os.Stat(target_path)
	if err != nil {
		return err
	}

	if f.IsDir() {

		filelist, err := WalkDir(target_path, ".yaml")
		if err != nil {
			return err
		}
		if len(filelist) == 0 {
			logger.Error("No Yaml Configs in ", target_path)
			return errors.New("No Yaml Configs")
		}

		logger.Info("Config File List : ", filelist)

		ch := make(chan TarOpsThread)

		num := 0
		for _, v := range filelist {

			num += 1

			id := int(atomic.LoadInt32(&gIndex))

			logger.Info("Start Config : ", v)

			go RouteOpsStart(v, id, ch)

			atomic.AddInt32(&gIndex, 1)
		}

		for i := 0; i < num; i++ {
			ret := <-ch
			if ret.Err != nil {
				err_message := fmt.Sprintln("Config : ", ret.TarConfigFileName, " Run error : ", ret.Err)
				logger.Error(err_message)
				return errors.New(err_message)
			}
		}

		return nil
	} else {

		id := int(atomic.LoadInt32(&gIndex))

		_, err := NewRouteOps(GetDefaultConfig(), id)

		atomic.AddInt32(&gIndex, 1)
		return err
	}
}

func RouteStatGet() []RouteOpsStats {
	var c []RouteOpsStats
	for _, v := range gRouteOps {
		c = append(c, v.RouteTable.Stat)
	}
	return c
}

func RouteTableGet() []RouteTable_t {
	var c []RouteTable_t
	for _, v := range gRouteOps {
		c = append(c, v.RouteTable)
	}
	return c
}

func (r *RouteOps) StatSwtich(s RouteStatType) {
	atomic.StoreInt32((*int32)(&r.RouteTable.Stat.StatType), int32(s))
}

func RouteStatSet(index int, stat string) error {

	if gIndex == 0 {
		return errors.New("Empty List")
	}

	if gRouteOps[index] == nil {
		return errors.New("No such Index")
	}

	v := gRouteOps[index]
	switch stat {
	case "on":

		if v.RouteTable.Stat.StatType == RouteStat_On {
			return errors.New("This index already running")
		}

		v.StatSwtich(RouteStat_Ready)

		RouteOpsRun(index)

	case "off":

		v.StatSwtich(RouteStat_Off)

		v.Input.exit()

	case "del":

		v.StatSwtich(RouteStat_Off)

		v.Input.exit()

		RouteOpsUnRegister(index)

	default:
		return errors.New("Unkown Control Command")
	}

	return nil
}

func RouteOpsWalk() error {

	for _, v := range gRouteOps {

		if v.RouteTable.Stat.StatType == RouteStat_Ready {
			if v.Input != nil {
				err := v.Input.MainLoop()
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func RouteOpsRun(index int) error {

	if gRouteOps[index] == nil {
		return errors.New("No such Index")
	}

	v := gRouteOps[index]

	if v.RouteTable.Stat.StatType == RouteStat_Ready {
		if v.Input != nil {
			err := v.Input.MainLoop()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (r *RouteOps) RouteDataInit(oriconfig *YamlCommonData, index int) error {

	r.RouteTable.Stat.StatType = RouteStat_Ready
	r.RouteTable.Config = oriconfig

	desc, err := InitConfigCommonData(r.RouteTable.Config)
	if err != nil {
		return err
	}

	r.RouteTable.Stat.Index = index

	r.RouteTable.Stat.TimeStamp = time.Now().Local().Format("2006-01-02T15:04:05")

	r.RouteTable.Stat.Description = desc

	r.RouteTable.Stat.ConfigFile = oriconfig.configfilename

	return nil
}

func (r *RouteOps) RouteProbe() error {

	i, err := InputPluginProbe(SearchItemName_for_NotZero(r.RouteTable.Config.Input))
	if err != nil {
		return err
	}

	f, err := FilterPluginProbe(SearchItemName_for_NotZero(r.RouteTable.Config.Filter))
	if err != nil {
		return err
	}

	o, err := OutputPluginProbe(SearchItemName_for_NotZero(r.RouteTable.Config.Output))
	if err != nil {
		return err
	}

	r.Output = o.init(r.RouteTable.Config, nil, &r.RouteTable.Counts.Outputcount, nil).(OutputInterface)

	r.Filter = f.init(r.RouteTable.Config, r.Output, &r.RouteTable.Counts.Filtercount, nil).(FilterInterface)

	r.Input = i.init(r.RouteTable.Config, r.Filter, &r.RouteTable.Counts.Inputcount, &r.RouteTable.Stat.StatType).(InputInterface)

	return nil
}

//获取当天日期
func GetTodayDate() string {
	s := time.Now().String()
	arr := strings.Split(s, " ")
	return arr[0]
}

//获取每日0点时计数总量
func GetTodayBeforedawnTotalCount(r *RouteOps, rdc *RouteOpsCounts) {
	c := cron.New()
	c.AddFunc("0 0 0 * * ?", func() {
		*rdc = r.RouteTable.Counts
	})
	c.Start()
	defer c.Stop()
	select {}

}

//定时获得每日统计，添加到map中
func GetTodayBeforedawnCount() {
	c := cron.New()
	c.AddFunc(*crontime, func() {
		data := RouteCountInfoTodayGet()
		EverydayCount[GetTodayDate()] = data
	})
	c.Start()
	defer c.Stop()
	select {}
}

func NewRouteOps(oriconfig *YamlCommonData, Id int) (*RouteOps, error) {

	r := new(RouteOps)

	rdc := new(RouteOpsCounts)
	rdi := &(r.RouteTable.Stat)

	err := r.RouteDataInit(oriconfig, Id)
	if err != nil {
		return nil, err
	}

	err = r.RouteProbe()
	if err != nil {
		return nil, err
	}

	err = r.Input.MainLoop()
	if err != nil {
		return nil, err
	}

	RouteOpsRegister(r)
	RouteCountsRegister(rdc)
	RouteStatRegister(rdi)

	go GetTodayBeforedawnTotalCount(r, rdc)

	return r, nil
}

/**************************************/

func InputFileRouteStart() error {

	_, err := NewRouteOps(InputFileModeInit(), 1)
	if err != nil {
		return err
	}

	select {}
}

/*************************************/

func PanicHandler() {

	if errs := recover(); errs != nil {
		//exeName := "Collector"
		//now := time.Now()                             //获取当前时间
		pid := os.Getpid() //获取进程ID
		//time_str := now.Format("2006-01-02-15:04:05") //设定时间格式

		// f, err := os.OpenFile("./CollectorPanic.txt", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
		// if err != nil {
		// 	fmt.Println("file Open fail")
		// 	return
		// }

		logger.Error("Panic message %s : ", errs)
		logger.Error("Panic Pid %s : ", pid)
		logger.Error("Stack message %s : ", string(debug.Stack()))

		// f.WriteString(fmt.Sprintf("%v\r\n", errs)) //输出panic信息
		// f.WriteString("==================================\r\n")
		// f.WriteString(exeName + "\r\n")
		// f.WriteString(fmt.Sprintf("%v\r\n", pid))
		// f.WriteString(time_str + "\r\n")
		// f.WriteString(string(debug.Stack()) + "\r\n") //输出堆栈信息
		// defer f.Close()

	}
	return

}
