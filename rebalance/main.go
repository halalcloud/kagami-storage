package main

import (
	"efs/rebalance/conf"
	"flag"
	log "efs/log/glog"
	"runtime"
)

var (
	configFile string
)

func init() {
	flag.StringVar(&configFile, "c", "./rebalance.toml", " set rebalance config file path")
}

func main() {
	var (
		c   *conf.Config
		r   *Rebalance
		err error
	)
	flag.Parse()
	defer log.Flush()
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.Infof("efs rebalance start")
	if c, err = conf.NewConfig(configFile); err != nil {
		log.Errorf("NewConfig(\"%s\") error(%v)", configFile, err)
		return
	}
	log.Infof("new rebalance...")
	if r, err = NewRebalance(c); err != nil {
		log.Errorf("NewRebalance() failed, Quit now error(%v)", err)
		return
	}
	log.Infof("init http api...")
	StartApi(c.ApiListen, c.MaxStoreRebalanceThreadNum, c.MaxDiskRebalanceThreadNum, r)
	if c.PprofEnable {
		log.Infof("init http pprof...")
		StartPprof(c.PprofListen)
	}
	StartSignal()
	return
}
