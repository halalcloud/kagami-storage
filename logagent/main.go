package main

import (
	"efs/logagent/conf"
	log "efs/log/glog"
	"flag"
	//	"fmt"
	//"os"
	//"os/signal"
	"runtime"
	//"syscall"
)

var (
	configFile string
)

func init() {
	flag.StringVar(&configFile, "c", "./agent.toml", " set agent config file path")
}
func main() {
	var (
		c     *conf.Config
		err   error
		cstop chan int
	)
	flag.Parse()
	defer log.Flush()
	runtime.GOMAXPROCS(runtime.NumCPU())
	if c, err = conf.NewConfig(configFile); err != nil {
		log.Errorf("NewConfig(\"%s\") error(%v)", configFile, err)
		return
	}
	cstop, err = Start(c)
	if err != nil {
		log.Errorf("start failed %v", err)
		return
	}
	_ = <-cstop
	log.Errorf("get sinal stop,exit")
	/*
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
	*/
	return
}
