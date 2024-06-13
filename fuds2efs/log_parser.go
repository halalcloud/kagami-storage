package main

import (
	"bufio"
	log "efs/fuds2efs/libs/glog"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"
)

type logParser struct {
	logPath    string
	interval   time.Duration
	offsetFile string
	offset     int64
	bucketAKSK map[string][]string
}

type logTask struct {
	method   string
	bucket   string
	key      string
	mvkey    string
	filesize int64
}

func (p *logParser) parse(taskChan chan *logTask) {
	//offset
	p.getOffset()
	p.syncOffset()

	for {
		log.Infof("parse now...\n")
		p.parseFile(taskChan)
		time.Sleep(p.interval)
	}
}

func (p *logParser) getOffset() {
	var (
		f    *os.File
		err  error
		data []byte
	)

	f, err = os.Open(p.offsetFile)
	if err != nil {
		log.Fatalf("logparser open offset file err:%s\n", err.Error())
		return
	}
	defer f.Close()

	data, err = ioutil.ReadAll(f)
	if err != nil {
		log.Fatalf("logparser read offset file err:%s\n", err.Error())
		return
	}

	p.offset, err = strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		log.Fatalf("logparser get offset err:%s\n", err.Error())
		return
	}

	return
}

func (p *logParser) syncOffset() {
	var (
		f   *os.File
		err error
	)

	go func() {
		for {
			time.Sleep(1 * time.Second)

			f, err = os.OpenFile(p.offsetFile, os.O_RDWR, 0755)
			if err != nil {
				f.Close()
				log.Errorf("syncoffset open offset file err:%s\n", err.Error())
				continue
			}

			err = f.Truncate(0)
			if err != nil {
				f.Close()
				log.Errorf("syncoffset truncate file err:%s\n", err.Error())
				continue
			}
			_, err = f.Write([]byte(strconv.FormatInt(p.offset, 10)))
			if err != nil {
				f.Close()
				log.Errorf("syncoffset write file err:%s\n", err.Error())
				continue
			}

			f.Close()
		}
	}()
}

func (p *logParser) parseFile(taskChan chan *logTask) {
	var (
		file  *os.File
		finfo os.FileInfo
		bufR  *bufio.Reader
		err   error
		data  []byte
		t     *logTask
	)

	file, err = os.Open(p.logPath)
	if err != nil {
		log.Errorf("parseFile open log file:%s err:%s\n", p.logPath, err.Error())
		return
	}
	defer file.Close()

	finfo, err = file.Stat()
	if err != nil {
		log.Errorf("parseFile get log file:%s stat err:%s\n", p.logPath, err.Error())
		return
	}

	if finfo.Size() < p.offset {
		log.Infof("parseFile log file:%s size:%d lower than offset:%d\n",
			p.logPath, finfo.Size(), p.offset)

		p.offset = 0
		return
	} else if finfo.Size() == p.offset {
		log.Infof("parseFile log file:%s size:%d equal offset:%d\n",
			p.logPath, finfo.Size(), p.offset)
		return
	}

	_, err = file.Seek(p.offset, os.SEEK_SET)
	if err != nil {
		log.Errorf("parseFile seek log file:%s err:%s", p.logPath, err.Error())
		return
	}

	bufR = bufio.NewReader(file)

	for {
		data, err = bufR.ReadBytes('\n')
		if err != nil {
			if err != io.EOF {
				log.Errorf("parseFile readline log file:%s err:%s\n", p.logPath, err.Error())
			}
			return
		}

		p.offset = p.offset + int64(len(data))

		t, err = p.parseLine(string(data))
		if err != nil {
			log.Errorf("parseFile parse data:%s err:%s\n", string(data), err.Error())
			continue
		}

		_, ok := p.bucketAKSK[t.bucket]
		if !ok {
			continue
		}

		taskChan <- t
	}

	return
}

func (p *logParser) parseLine(line string) (t *logTask, err error) {
	t = new(logTask)

	if strings.Contains(line, "OK UPLOAD") {
		t.method = "UPLOAD"
		t.bucket, t.key, t.filesize, err = p.parseUpload(line)
		return
	}

	if strings.Contains(line, "OK DELETE") {
		t.method = "DELETE"
		t.bucket, t.key, err = p.parseDelete(line)
		return
	}

	if strings.Contains(line, "OK RENAME") {
		t.method = "RENAME"
		t.bucket, t.key, t.mvkey, err = p.parseRename(line)
		return
	}

	return
}

func (p *logParser) parseUpload(line string) (bucket, key string, filesize int64, err error) {
	var (
		arrs, paths []string
		space       = " "
		length      int
	)
	arrs = strings.Split(line, space)
	length = len(arrs)

	filesize, err = strconv.ParseInt(arrs[length-3], 10, 64)
	if err != nil {
		return
	}

	paths = strings.Split(
		strings.TrimSuffix(
			strings.TrimPrefix(arrs[length-4], "\""),
			"\","),
		"/")
	if len(paths) < 2 {
		err = errors.New("file path error")
		return
	}

	bucket = paths[1]
	for _, v := range paths[2:] {
		key = key + v + "/"
	}
	key = strings.TrimSuffix(key, "/")

	return
}

func (p *logParser) parseDelete(line string) (bucket, key string, err error) {
	var (
		arrs, paths []string
		space       = " "
		length      int
	)
	arrs = strings.Split(line, space)
	length = len(arrs)

	paths = strings.Split(strings.Trim(
		strings.TrimSpace(arrs[length-1]), "\""),
		"/")
	if len(paths) < 2 {
		err = errors.New("file path error")
		return
	}

	bucket = paths[1]
	for _, v := range paths[2:] {
		key = key + v + "/"
	}
	key = strings.TrimSuffix(key, "/")

	return
}

func (p *logParser) parseRename(line string) (bucket, key string, mvkey string, err error) {
	var (
		arrs, paths []string
		space       = " "
		length      int
	)
	arrs = strings.Split(line, space)
	length = len(arrs)

	paths = strings.Split(strings.TrimPrefix(arrs[length-2], "\""), "/")
	bucket = paths[1]
	for _, v := range paths[2:] {
		key = key + v + "/"
	}
	key = strings.TrimSuffix(key, "/")

	paths = strings.Split(strings.TrimSuffix(
		strings.TrimSpace(arrs[length-1]), "\""), "/")
	for _, v := range paths[2:] {
		mvkey = mvkey + v + "/"
	}
	mvkey = strings.TrimSuffix(mvkey, "/")

	return
}
