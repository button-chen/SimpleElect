package main

import (
	"github.com/button-chen/SimpleElect/elect"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
)

// test.exe 9000 127.0.0.1:5001 127.0.0.1:5001 127.0.0.1:5002 127.0.0.1:5003

func main() {

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	e := elect.NewSimpleElect()
	e.SetMode(elect.ModeRelease)

	e.AddListenEvent(func(code int, data interface{}) {
		if code == elect.EventAddPeer {
			item := data.(elect.IpVal)
			log.Println("main: 添加成员 ", item.String())
		}
		if code == elect.EventDelPeer {
			item := data.(elect.IpVal)
			log.Println("main: 删除成员 ", item.String())
		}
		if code == elect.EventElectLeader {
			item := data.(elect.IpVal)
			log.Println("main: 领导选举成功 ", item.String())
		}
		if code == elect.EventAddKV {
			item := data.(elect.Command)
			log.Println("main: 添加KV ", item.String())
		}
		if code == elect.EventDelKV {
			item := data.(elect.Command)
			log.Println("main: 删除KV ", item.String())
		}
	})

	localAddr := os.Args[2]
	ips := os.Args[3:]
	e.Init(localAddr, ips)
	e.Start()

	http.HandleFunc("/leader", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(e.Leader()))
	})
	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		v, err := e.Get(key)
		if err != nil {
			w.Write([]byte(err.Error()))
			return
		}
		w.Write([]byte(v))
	})
	http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		val := r.URL.Query().Get("val")
		expired, _ := strconv.Atoi(r.URL.Query().Get("expired"))
		err := e.Set(key, val, expired)
		if err != nil {
			w.Write([]byte(err.Error()))
			return
		}
		w.Write([]byte("ok"))
	})

	go func() {
		log.Fatalln(http.ListenAndServe(":"+os.Args[1], nil))
	}()

	select {
	case <-interrupt:
	}
}
