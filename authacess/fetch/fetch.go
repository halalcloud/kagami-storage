package fetch

import (
	"efs/authacess/httpcli"
	log "efs/log/glog"
	"errors"
	"io/ioutil"
	"net/http"
	"strconv"
)

func FileData(url string, rangeStr string) (data []byte, mimeType string, err error) {
	var (
		req  *http.Request
		resp *http.Response
	)
	if req, err = http.NewRequest("GET", url, nil); err != nil {
		return
	}
	req.Close = true
	if rangeStr != "" {
		req.Header.Set("Range", rangeStr)
	}

	if resp, err = httpcli.HttpReq(req); err != nil {
		log.Errorf("fileBody() http GET url(%s) error(%s)\n", url, err.Error())
		return
	}
	defer resp.Body.Close()
	//log.Errorf("range str %s ,code %d", rangeStr, resp.StatusCode)
	if rangeStr != "" &&
		(resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK) {
		log.Errorf("fetch server no support range")
		err = errors.New("fetch server no support range")
		return
	}

	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		log.Errorf("fileBody() read body data url(%s) error(%s)\n", url, err.Error())
		return
	}

	mimeType = resp.Header.Get("Content-Type")

	return
}

func FileSize(url string) (size int64, err error) {
	var (
		req  *http.Request
		resp *http.Response
	)

	if req, err = http.NewRequest("HEAD", url, nil); err != nil {
		log.Errorf("http request head url %s failed %v", err)
		return
	}

	if resp, err = httpcli.HttpReq(req); err != nil {
		log.Errorf("filesize() http HEAD url(%s) error(%s)\n", url, err.Error())
		return
	}

	sizeStr := resp.Header.Get("content-length")
	if size, err = strconv.ParseInt(sizeStr, 10, 64); err != nil {
		log.Errorf("strconv parseint %s failed %v", sizeStr, err)
		return
	}

	return
}
