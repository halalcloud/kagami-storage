package hbase

import (
	"efs/directory/hbase/hbasethrift"
	"time"

	log "efs/log/glog"
)

const (
	_clean_scanID_time_step = 5
	_clean_scanID_TTL_time  = 5
)

type ScanID struct {
	Id   int32
	Time int64
}

var ScanHL map[int32]int64

func ScanHLInit() {
	ScanHL = make(map[int32]int64)

	ScanHLCleanRun()
}

func ScanHLPut(id int32) {
	if id < 0 {
		log.Errorf("Put SCan handle to list failed")
		return
	}
	ScanHL[id] = time.Now().Unix()
}

func ScanHLPop(id int32) {
	if id < 0 {
		log.Errorf("Pop SCan handle to list failed")
		return
	}
	delete(ScanHL, id)
}

func ScanHLUpdata(id int32) {

	if id < 0 {
		log.Errorf("Updata SCan handle to list failed")
		return
	}
	ScanHL[id] = time.Now().Unix()
}

func ScanHLCleanRun() {
	ticker := time.NewTicker(time.Minute * _clean_scanID_time_step)
	go func() {
		var c *hbasethrift.THBaseServiceClient
		var err error
		if c, err = hbasePool.Get(); err != nil {
			log.Errorf("hbasePool.Get() error(%v)", err)
			return
		}
		for _ = range ticker.C {
			for k, v := range ScanHL {
				if int64(v+int64(time.Minute*_clean_scanID_TTL_time)) > time.Now().Unix() {
					if err = c.CloseScanner(k); err != nil {
						log.Errorf("ScanHLCleanRun Close Scanner (%d) Failed", k)
						hbasePool.Put(c, true)
					}
					ScanHLPop(k)
				}
			}
		}
		hbasePool.Put(c, false)
	}()
}
