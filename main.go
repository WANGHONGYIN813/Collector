package main

import (
	"./plugin"
	//"fmt"
	//"os"
)

func main() {
	/*创建panic记录文件*/

	defer plugin.PanicHandler()

	// _, err := os.Stat("./CollectorPanic.txt")
	// if err != nil {
	// 	f, err := os.Create("./CollectorPanic.txt") //如果文件存在，会将文件清空
	// 	if err != nil {
	// 		fmt.Println("file Creat fail")
	// 		return
	// 	}
	// 	f.Close()
	// }

	if plugin.InputFileModeCheck() {

		plugin.InputFileRouteStart()
	}

	go plugin.RouteConfigLoadDefault()

	plugin.HttpServer()
}
