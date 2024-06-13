package main

import (
	"flag"
	"kagamistoreage/authacess/conf"
	log "kagamistoreage/log/glog"
	"os"
	"os/signal"
	"runtime"
	"syscall"
)

var (
	configFile string
	process    string
)

func init() {
	flag.StringVar(&configFile, "c", "./uploadproxy.toml", " set uploadproxy config file path")
	flag.StringVar(&process, "p", "upload", "if -p is upload this is upload process,-p is manger this is source manger process")
}

func main() {
	var (
		c   *conf.Config
		err error
	)
	flag.Parse()
	defer log.Flush()
	if c, err = conf.NewConfig(configFile); err != nil {
		log.Errorf("NewConfig(\"%s\") error(%v)", configFile, err)
		panic(err)
	}
	runtime.GOMAXPROCS(runtime.NumCPU())
	// init http
	if process == "upload" {
		if err = StartUploadApi(c); err != nil {
			log.Error("http.Init() error(%v)", err)
			panic(err)
		}
	} else if process == "manger" {

		if err = StartManagerApi(c); err != nil {
			log.Error("http.Init() error(%v)", err)
			panic(err)
		}

	} else {
		log.Error("have no this process (%s) modle,please make sure process modle", process)
		return
	}
	if c.PprofEnable {
		log.Infof("init http pprof...")
		StartPprof(c.PprofListen)
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT, syscall.SIGSTOP)
	for {
		s := <-ch
		log.Infof("get a signal %s", s.String())
		switch s {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGSTOP, syscall.SIGINT:
			return
		case syscall.SIGHUP:
			// TODO reload
		default:
			return
		}
	}
}
