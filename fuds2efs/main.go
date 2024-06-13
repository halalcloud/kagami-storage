package main

import (
	"flag"
	"kagamistoreage/fuds2kagamistoreage/conf"
	log "kagamistoreage/fuds2kagamistoreage/libs/glog"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

var (
	confFile string
)

func init() {
	flag.StringVar(&confFile, "c", "./fuds2efs.toml", "fuds2efs config file")
}

func main() {
	var (
		c    *conf.Config
		strs []string
		err  error

		lp *logParser
		gs *GOSSyncer
	)
	flag.Parse()
	defer log.Flush()

	c, err = conf.NewConfig(confFile)
	if err != nil {
		log.Errorf("parse config file err:%s\n", err.Error())
		return
	}

	bucketAKSK := make(map[string][]string)
	for _, v := range c.Buckets {
		strs = strings.Split(v, ":")
		if len(strs) != 3 {
			log.Errorf("parse config buckets:%s ,not satisfy 3 elem", v)
			return
		}
		bucketAKSK[strs[0]] = append(bucketAKSK[strs[0]], strs[1])
		bucketAKSK[strs[0]] = append(bucketAKSK[strs[0]], strs[2])
	}

	//logparser
	lp = new(logParser)
	lp.logPath = c.LogFilePath
	lp.interval = c.ParseDuration.Duration
	lp.offsetFile = c.OffsetFile
	lp.bucketAKSK = bucketAKSK
	//gossyncer
	gs = new(GOSSyncer)
	gs.fileBasePath = c.BasePath
	gs.bucketAKSK = bucketAKSK
	gs.upHost = c.UpHost
	gs.mgHost = c.MgHost

	taskChan := make(chan *logTask, 10)
	go lp.parse(taskChan)
	go gs.sync(taskChan)

	startSignal()
}

func startSignal() {
	var (
		c chan os.Signal
		s os.Signal
	)

	c = make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM,
		syscall.SIGINT, syscall.SIGSTOP)
	// Block until a signal is received.
	for {
		s = <-c
		log.Infof("get a signal %s", s.String())
		switch s {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGSTOP, syscall.SIGINT:
			return
		case syscall.SIGHUP:
			// TODO reload
			//return
		default:
			return
		}
	}
}
