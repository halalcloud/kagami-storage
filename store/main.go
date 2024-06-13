package main

import (
	"flag"
	log "kagamistoreage/log/glog"
	"kagamistoreage/store/conf"
)

var (
	configFile string
)

func init() {
	flag.StringVar(&configFile, "c", "./store.toml", " set store config file path")
}

func main() {
	var (
		c      *conf.Config
		store  *Store
		server *Server
		err    error
	)
	flag.Parse()
	defer log.Flush()
	log.Infof("efs store[%s] start", Ver)
	defer log.Infof("efs store[%s] stop", Ver)
	if c, err = conf.NewConfig(configFile); err != nil {
		log.Errorf("NewConfig(\"%s\") error(%v)", configFile, err)
		return
	}
	if store, err = NewStore(c); err != nil {
		return
	}
	if server, err = NewServer(store, c); err != nil {
		return
	}
	if err = store.SetZookeeper(); err != nil {
		return
	}
	log.Infof("wait signal...")
	StartSignal(store, server)
	return
}
