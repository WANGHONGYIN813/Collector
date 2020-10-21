package main

import (
	"./send"
	"log"
)

func main() {

	err := send.ConsumeEntry()
	if err != nil {
		log.Printf("send message err=%s \n", err)
		return
	}
}
