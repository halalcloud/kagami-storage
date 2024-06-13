package main

import (
	"efs/bigfile_callbak/conf"
	log "efs/log/glog"
	"flag"
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
		err error
	)
	flag.Parse()
	defer log.Flush()
	runtime.GOMAXPROCS(runtime.NumCPU())
	if c, err = conf.NewConfig(configFile); err != nil {
		log.Errorf("NewConfig(\"%s\") error(%v)", configFile, err)
		return
	}
	err = Start(c)
	if err != nil {
		log.Errorf("start failed %v", err)
		return
	}

	StartSignal()
	return
}
