package main

import (
	"flag"
	"kagamistoreage/directory/conf"
	log "kagamistoreage/log/glog"
	"runtime"
)

var (
	configFile string
)

func init() {
	flag.StringVar(&configFile, "c", "./directory.toml", " set directory config file path")
}

func main() {
	var (
		c   *conf.Config
		d   *Directory
		err error
	)
	flag.Parse()
	defer log.Flush()
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.Infof("efs directory start")
	if c, err = conf.NewConfig(configFile); err != nil {
		log.Errorf("NewConfig(\"%s\") error(%v)", configFile, err)
		return
	}
	log.Infof("new directory...")
	if d, err = NewDirectory(c); err != nil {
		log.Errorf("NewDirectory() failed, Quit now error(%v)", err)
		return
	}
	log.Infof("init http api...")
	StartApi(c.ApiListen, d)
	if c.PprofEnable {
		log.Infof("init http pprof...")
		StartPprof(c.PprofListen)
	}
	StartSignal()
	return
}
