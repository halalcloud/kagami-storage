package main

import (
	"efs/bucket/conf"
	"flag"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	log "efs/log/glog"
)

var (
	configFile string
)

func init() {
	flag.StringVar(&configFile, "c", "./bucket.toml", " set directory config file path")
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

	if err = StartApi(c); err != nil {
		log.Error("http.Init() error(%v)", err)
		panic(err)
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
