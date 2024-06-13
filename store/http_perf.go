package main

import (
	log "efs/log/glog"
	"net/http"
	_ "net/http/pprof"
)

// StartPprof start a golang pprof.
func StartPprof(addr string) {
	var err error
	if err = http.ListenAndServe(addr, nil); err != nil {
		log.Errorf("http.ListenAndServe(\"%s\") error(%v)", addr, err)
		panic(err)
		return
	}
}
