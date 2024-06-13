package main

import (
	"efs/gos_sync/bucket"
	"efs/gos_sync/conf"
	"efs/gos_sync/gos"
	"efs/gos_sync/task"
	log "efs/log/glog"
	"flag"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	var (
		cpath = flag.String("c", "gos_sync.toml", "toml file path")
		c     *conf.Config
		err   error
		t     *task.Task
	)
	flag.Parse()
	defer log.Flush()

	if c, err = conf.NewConfig(*cpath); err != nil {
		log.Fatalf("new config err:%s\n", err)
		return
	}

	//bucket
	bucket.New(c.BktMSrvAddr, c.BktMSrvAk, c.BktMSrvSk,
		c.BktSSrvAddr, c.BktSSrvAk, c.BktSSrvSk)

	//task
	t = task.New(c.MURL)

	//gos
	g := gos.New(c.Workers, t, c.TaskRemainDuration,
		c.SliceSize, c.DB, c.TCollection, c.FCollection, c.PrxMSrvAddr,
		c.UpHost, c.MgHost)
	g.Start()

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

	for {
		s = <-c
		log.Infof("get a signal %s\n", s.String())
		switch s {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGSTOP, syscall.SIGINT:
			log.Infof("server exit\n")
			return
		case syscall.SIGHUP:
			//server reload TODO
		}
	}
}
