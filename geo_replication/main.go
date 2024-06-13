package main

import (
	"flag"
	"kagamistoreage/geo_replication/conf"
	log "kagamistoreage/log/glog"
	"net/http"
	_ "net/http/pprof"
	"runtime"
)

const (
	version = "1.0.0"
)

var (
	configFile string
)

func init() {
	flag.StringVar(&configFile, "c", "./geo_replcation.toml", " set directory config file path")
}

func StartPprof(addr string) {
	go func() {
		var err error
		if err = http.ListenAndServe(addr, nil); err != nil {
			log.Errorf("http.ListenAndServe(\"%s\") error(%v)", addr, err)
			return
		}
	}()
}

func main() {
	var (
		c    *conf.Config
		err  error
		sync *Rsync_efs
	)
	flag.Parse()
	defer log.Flush()
	log.Infof("efs geo-replcation [version: %s] support primary and standby modle", version)
	if c, err = conf.NewConfig(configFile); err != nil {
		log.Errorf("NewConfig(\"%s\") error(%v)", configFile, err)
		panic(err)
	}
	runtime.GOMAXPROCS(runtime.NumCPU())
	// init http
	if sync, err = Sync_init(c); err != nil {
		log.Error("init error(%v)", err)
		panic(err)
	}

	err = sync.Sync_start()
	if err != nil {
		log.Errorf("sync exit err %v", err)
		return
	}

	if c.PprofEnable {
		log.Infof("init http pprof...")
		StartPprof(c.PprofListen)
	}

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
}
