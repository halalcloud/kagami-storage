package main

import (
	"bufio"
	log "efs/log/glog"
	"efs/logagent/conf"
	"efs/logagent/db"
	b64 "encoding/base64"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	_TAB            = "	"
	_SPACE          = " "
	PARAM_DELIMITER = "---"
)

type log_agent struct {
	File      *os.File
	FileIndex *os.File
	Conf      *conf.Config
	Offset    int64
	Fileinode int64
	Logqueue  chan *logline
	Mongo_op  *db.Mgo_op
	Stop      chan int
}

type logline struct {
	Line   string
	Offset int64
}

func Start(c *conf.Config) (stop chan int, err error) {
	var (
	//indexdata string
	)
	logagent := &log_agent{}
	logagent.Conf = c
	logagent.Mongo_op, err = db.NewSession(c)
	if err != nil {
		log.Errorf("init db failed %v", err)
		return
	}

	logagent.Logqueue = make(chan *logline, 10)
	logagent.Stop = make(chan int)
	stop = logagent.Stop

	err = logagent.Init()
	if err != nil {
		log.Errorf("log agent init failed %v", err)
		return
	}
	return
}

func getlogfileinode(filename string) (inodeid int64, err error) {
	var (
		fileinfo os.FileInfo
	)
	fileinfo, err = os.Stat(filename)
	if err != nil {
		log.Errorf("stat filename %s failed %v", err)
		return
	}
	stat, ok := fileinfo.Sys().(*syscall.Stat_t)
	if !ok {
		err = errors.New("not a syscall stat")
		log.Errorf("not a syscall stat")
		return
	}
	inodeid = int64(stat.Ino)
	return
}

func (lg *log_agent) Init() (err error) {
	var (
		data []byte
	)
	lg.File, err = os.OpenFile(lg.Conf.Logfilename, os.O_RDONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Errorf("open log filename %s failed %v", lg.Conf.Logfilename, err)
		return
	}
	lg.FileIndex, err = os.OpenFile(lg.Conf.AgentIndexfile, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Errorf("open indexfile %s failed %v", lg.Conf.AgentIndexfile, err)
		return
	}
	lg.Fileinode, err = getlogfileinode(lg.Conf.Logfilename)
	if err != nil {
		log.Errorf("not a syscall stat")
		return
	}
	data, err = ioutil.ReadAll(lg.FileIndex)
	if err != nil {
		log.Errorf("read indexfile %s failed %v", lg.Conf.AgentIndexfile, err)
		return
	}
	if len(data) != 0 {

		lg.Offset, err = strconv.ParseInt(string(data), 10, 64)
		if err != nil {
			log.Errorf("parse offset failed %v", err)
			return
		}

		_, err = lg.File.Seek(lg.Offset, 0)
		if err != nil {
			log.Errorf("seek file %s failed %v", lg.Conf.Logfilename, err)
			return
		}
	}
	go lg.dodata()
	go lg.senddata()
	return
}

func (lg *log_agent) checkinode() (change bool, err error) {
	var (
		inodeid int64
	)
	inodeid, err = getlogfileinode(lg.Conf.Logfilename)
	if err != nil {
		log.Errorf("not a syscall stat")
		return
	}
	if inodeid == lg.Fileinode {
		change = false
		return
	} else {
		lg.File, err = os.Open(lg.Conf.Logfilename)
		if err != nil {
			log.Errorf("open log filename %s failed %v", lg.Conf.Logfilename, err)
			return
		}
		change = true
		lg.Fileinode = inodeid
	}
	return
}

func getlogtime(logtime string) (utime int64, err error) {
	var (
		y, m, d, h, min, seconds, nas int
		datas                         []string
	)
	datas = strings.Split(logtime, _SPACE)

	if len(datas) < 3 {
		err = errors.New("log time invalid")
		return
	}
	timshourstring := datas[1]
	timedaystring := datas[0]
	ty := timedaystring[0:4]
	y, err = strconv.Atoi(ty)
	if err != nil {
		log.Errorf("atoi year %s failed %v", ty, err)
		return
	}
	tm := timedaystring[4:6]
	m, err = strconv.Atoi(tm)
	if err != nil {
		log.Errorf("atoi month %s failed %v", tm, err)
		return
	}
	td := timedaystring[6:]
	d, err = strconv.Atoi(td)
	if err != nil {
		log.Errorf("atoi day %s failed %v", td, err)
		return
	}
	tinfos := strings.Split(timshourstring, ":")
	th := tinfos[0]
	h, err = strconv.Atoi(th)
	if err != nil {
		log.Errorf("atoi hour %s failed %v", th, err)
		return
	}
	tmin := tinfos[1]
	min, err = strconv.Atoi(tmin)
	if err != nil {
		log.Errorf("atoi min %s failed %v", tmin, err)
		return
	}
	ntinfos := strings.Split(tinfos[2], ".")
	tseconds := ntinfos[0]
	seconds, err = strconv.Atoi(tseconds)
	if err != nil {
		log.Errorf("atoi second %s failed %v", tseconds, err)
		return
	}
	tnas := ntinfos[1]
	nas, err = strconv.Atoi(tnas)
	if err != nil {
		log.Errorf("atoi nas %s failed %v", tnas, err)
		return
	}
	then := time.Date(y, time.Month(m), d, h, min, seconds, nas, time.UTC)
	//fmt.Println(y, m, d, h, min, seconds, nas)
	utime = then.UnixNano()
	//fmt.Println(utime)
	return
}

func (lg *log_agent) parseline(line string) (ekey, param, op string, addtime int64, err error) {
	var ()
	tinfos := strings.Split(line, "]")
	if len(tinfos) != 2 {
		err = errors.New("split ] invaild")
		log.Errorf("split ] invalid is not 2 line(%s)", line)
		return
	}
	dinfos := tinfos[1]
	dinfo := dinfos[1:]
	datas := strings.Split(dinfo, _TAB)
	if len(datas) != 9 {
		err = errors.New("split statis info invaild")
		log.Errorf("statis info is invalid line(%s)", line)
		return
	}
	if datas[7] != "200" {
		err = errors.New("op failed continue")
		log.Infof("op failed continue")
		return
	}
	addtime, err = getlogtime(tinfos[0])
	if err != nil {
		log.Errorf("line (%s) get log time failed %v", line, err)
		return
	}
	op = datas[0]
	bucket := datas[1]
	if bucket == "-" {
		err = errors.New("op continue")
		log.Infof("bucket is - continue")
		return
	}
	if op == "/r/upload" || op == "/r/mkfile" || op == "/r/prefetch" || op == "/r/fetch" || op == "/r/delete" {
		filename, _ := b64.URLEncoding.DecodeString(datas[2])
		ekey = b64.URLEncoding.EncodeToString([]byte(bucket + ":" + string(filename)))
		if op == "/r/delete" {
			param = ekey
		} else {
			param = ekey + PARAM_DELIMITER + datas[5]
			if datas[8] != "-" && datas[8] != "" {
				param = param + PARAM_DELIMITER + datas[8]
			}
		}

	} else if op == "/r/chgm" {
		filename, _ := b64.URLEncoding.DecodeString(datas[2])
		ekey = b64.URLEncoding.EncodeToString([]byte(bucket + ":" + string(filename)))
		param = ekey + PARAM_DELIMITER + datas[8]
	} else if op == "/r/deleteAfter" {
		filename, _ := b64.URLEncoding.DecodeString(datas[2])
		ekey = b64.URLEncoding.EncodeToString([]byte(bucket + ":" + string(filename)))
		param = ekey + PARAM_DELIMITER + datas[3]
	} else if op == "/r/copy" || op == "/r/move" {
		ekey = bucket
		param = ekey + PARAM_DELIMITER + datas[2]
	} else if op == "/b/create" || op == "/b/delete" {
		ekey = b64.URLEncoding.EncodeToString([]byte(bucket))
		param = ekey
		if op == "/b/create" {
			//bparam, _ := b64.URLEncoding.DecodeString(datas[2])
			param = param + PARAM_DELIMITER + datas[2]
		}
	} else {
		err = errors.New("op continue")
		log.Infof("op %s continue", op)
		return
	}
	return

}

func (lg *log_agent) dodata() {
	var (
		err    error
		change bool
		offset int64
		line   string
	)

	for {
		offset = lg.Offset
		buf := bufio.NewReader(lg.File)
		for {
			line, err = buf.ReadString('\n')
			line = strings.TrimSpace(line)
			if err != nil {
				if err == io.EOF {
					time.Sleep(100 * time.Millisecond)
					change, err = lg.checkinode()
					if err != nil {
						log.Errorf("reading exit")
						lg.Stop <- 1
						return
					}
					if change {
						log.Infof("logfile is split")
						break
					}
				} else {
					log.Errorf("read logfile failed %v", err)
					log.Errorf("reading exit")
					lg.Stop <- 1
					return
				}
			} else {
				offset += (int64(len(line)) + 1)
				info := &logline{line, offset}
				lg.Logqueue <- info
			}
		}

	}
}

func (lg *log_agent) senddata() {
	var (
		info            *logline
		ok              bool
		err             error
		ekey, param, op string
		logtime         int64
	)
	for {
		info, ok = <-lg.Logqueue
		if !ok {
			return
		}
		ekey, param, op, logtime, err = lg.parseline(info.Line)
		if err != nil {
			continue
		}
		err = lg.Mongo_op.Insert(ekey, param, op, logtime)
		if err != nil {
			log.Errorf("mongo op failed ready exit %v", err)
			lg.Stop <- 1
			return
		} else {
			err = lg.Update_offset(info.Offset)
			if err != nil {
				log.Errorf("update offset failed (%v) readv exsit ", err)
				lg.Stop <- 1
				return
			}
		}
	}
}

func (lg *log_agent) Update_offset(offset int64) (err error) {
	var (
		datalen int
	)
	_, err = lg.FileIndex.Seek(0, 0)
	if err != nil {
		return
	}
	data := fmt.Sprintf("%d", offset)
	datalen, err = lg.FileIndex.Write([]byte(data))
	if err != nil {
		return
	}
	err = lg.FileIndex.Truncate(int64(datalen))
	if err != nil {
		return
	}
	return
}
