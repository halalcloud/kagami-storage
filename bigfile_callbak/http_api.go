package main

import (
	"bytes"
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"kagamistoreage/bigfile_callbak/conf"
	"kagamistoreage/bigfile_callbak/db"
	"kagamistoreage/libs/errors"
	"kagamistoreage/libs/meta"
	log "kagamistoreage/log/glog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	_directoryMkfileApi = "http://%s/mkbigfile"
	_callbak_url        = "http://%s/callbakurl"
)

var (
	_transport = &http.Transport{
		Dial: func(netw, addr string) (c net.Conn, err error) {
			if c, err = net.DialTimeout(netw, addr, 2*time.Second); err != nil {
				return nil, err
			}
			return c, nil
		},
		DisableCompression: true,
		DisableKeepAlives:  true,
	}
	_client = &http.Client{
		Transport: _transport,
		Timeout:   10 * time.Second,
	}
)

type server struct {
	mongo_op *db.Mgo_op
	c        *conf.Config
}

func Start(c *conf.Config) (err error) {
	s := &server{}
	s.c = c
	s.mongo_op, err = db.NewSession(c)
	if err != nil {
		log.Errorf("init db failed %v", err)
		return
	}
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/mkfile", s.mkfile)
		mux.HandleFunc("/callbakurl", s.callbakurl)

		server := &http.Server{
			Addr:    c.ApiListen,
			Handler: mux,
			//ReadTimeout:  _httpServerReadTimeout,
			//WriteTimeout: _httpServerWriteTimeout,
		}
		if err := server.ListenAndServe(); err != nil {
			return
		}
	}()

	go s.Timeoutcallbak()

	return

}

func HttpMkfileWriter(r *http.Request, wr http.ResponseWriter, start time.Time, res *meta.Response) {
	var (
		err      error
		byteJson []byte
		ret      = res.Ret
	)
	if byteJson, err = json.Marshal(res); err != nil {
		log.Errorf("json.Marshal(\"%v\") failed (%v)", res, err)
		return
	}
	wr.Header().Set("Content-Type", "application/json;charset=utf-8")
	if _, err = wr.Write(byteJson); err != nil {
		log.Errorf("HttpWriter Write error(%v)", err)
		return
	}
	log.Infof("%s path:%s(params:%s,time:%f,ret:%v)", r.Method,
		r.URL.Path, r.Form.Encode(), time.Now().Sub(start).Seconds(), ret)
}

func (s *server) Timeoutcallbak() {
	for {
		urllist, err := s.mongo_op.List()
		if err != nil {
			log.Errorf("get list failed")
		}
		for _, urlinfo := range urllist {
			if time.Now().Unix() > urlinfo.Utime {
				log.Errorf("timeout ekey %s", urlinfo.Ekey)
				s.response(urlinfo.Ekey, "", urlinfo.Callbakurl, 519)
				s.mongo_op.Del(urlinfo.Ekey)
			}
		}

		time.Sleep(30 * time.Second)
	}
}

func (s *server) sendtodirectory(ekey, mime, fsize, buf, callbakurl,
	overwrite, deleteAfterDays string) (err error) {
	var (
		params = url.Values{}
	)
	params.Set("ekey", ekey)
	params.Set("mime", mime)
	params.Set("filesize", fsize)
	//params.Set("body", buf)
	params.Set("overwrite", overwrite)
	params.Set("deleteAfterDays", deleteAfterDays)
	params.Set("callbakurl", callbakurl)

	//efsaddr is directory host and port
	uri := fmt.Sprintf(_directoryMkfileApi, s.c.EfsAddr)
	//get metadata from directory
	//mycontinue 1
	if err = Http("POST", uri, params, []byte(buf), nil); err != nil {
		log.Errorf("Efs Mkfile() http.Post called uri(%s) directory error(%v)", uri, err)
		return
	}
	return
}

func (s *server) mkfile(wr http.ResponseWriter, r *http.Request) {
	var (
		err  error
		res  meta.Response
		body []byte
	)
	defer HttpMkfileWriter(r, wr, time.Now(), &res)
	res.Ret = errors.RetOK
	ekey := r.Header.Get("ekey")
	if ekey == "" {
		log.Errorf("req ekey is nil")
		res.Ret = errors.RetInternalErr
		return
	}

	mime := r.Header.Get("mime")
	fsize := r.Header.Get("filesize")

	overwrite := r.Header.Get("overwrite")
	deleteAfterDays := r.Header.Get("deleteAfterDays")
	if body, err = ioutil.ReadAll(r.Body); err != nil {
		log.Errorf("read http body ctxs faild %v", err)
		res.Ret = errors.RetInternalErr
		return
	}
	buf := string(body)
	callbakurl := r.Header.Get("callbakurl")
	log.Infof("recv mkfile ekeky %s url %s", ekey, callbakurl)
	if callbakurl != "" {
		ctxs := strings.Split(buf, ",")
		callbaktimeout := time.Now().Unix() + int64(len(ctxs)/1000) + 10

		err = s.mongo_op.Insert(ekey, callbakurl, callbaktimeout)
		if err != nil {
			log.Errorf("req callbakurl is nil")
			res.Ret = errors.RetInternalErr
			return
		}
	}

	err = s.sendtodirectory(ekey, mime, fsize, buf, callbakurl,
		overwrite, deleteAfterDays)
	if err != nil {
		log.Errorf("send mkfile to directory failed")
		if callbakurl != "" {
			err = s.mongo_op.Del(ekey)
			if err != nil {
				log.Recoverf("clean mongo db ekey %s failed", ekey)
			}
		}
		return
	}
	return
}

func Http(method, uri string, params url.Values, buf []byte, res interface{}) (err error) {
	var (
		body    []byte
		bufdata = &bytes.Buffer{}
		req     *http.Request
		resp    *http.Response
		ru      string
		enc     string
	)
	enc = params.Encode()
	if enc != "" {
		ru = uri + "?" + enc
	}

	if method == "GET" {
		if req, err = http.NewRequest("GET", ru, nil); err != nil {
			return
		}
	} else {
		if buf == nil {
			if req, err = http.NewRequest("POST", uri, strings.NewReader(enc)); err != nil {
				return
			}
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		} else {
			bufdata = bytes.NewBuffer(buf)
			if req, err = http.NewRequest("POST", uri, bufdata); err != nil {
				return
			}
			for key, _ := range params {
				req.Header.Set(key, params.Get(key))
			}
			req.Header.Set("Content-Type", "application/octet-stream")
			req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
		}
	}
	if resp, err = _client.Do(req); err != nil {
		log.Errorf("_client.Do(%s) error(%v)", ru, err)
		return
	}
	defer resp.Body.Close()
	if res == nil {
		return
	}
	if resp.StatusCode != http.StatusOK {
		log.Errorf("_client.Do(%s) status: %d", ru, resp.StatusCode)
		return
	}
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		log.Errorf("ioutil.ReadAll(%s) uri(%s) error(%v)", body, ru, err)
		return
	}
	if err = json.Unmarshal(body, res); err != nil {
		log.Errorf("json.Unmarshal(%s) uri(%s) error(%v)", body, ru, err)
	}
	return
}

func errortostring(err error) (retcode int, errstring string) {
	var (
		errcode int
		er      string
		ok      bool
		derr    errors.Error
	)

	if err == errors.ErrNeedleExist {
		retcode = errors.RetResExist
		errcode = errors.RetNeedleExist
		er = errors.ErrNeedleExist.Error()
	} else if err == errors.ErrDestBucketNoExist {
		retcode = errors.RetResNoExist
		errcode = errors.RetDestBucketNoExist
		er = errors.ErrDestBucketNoExist.Error()
	} else {
		if derr, ok = (err).(errors.Error); ok {
			errcode = int(derr)
		} else {
			errcode = errors.RetServerFailed
		}
		retcode = errors.RetServerFailed
		er = errors.ErrServerFailed.Error()
	}

	errstring = strconv.Itoa(errcode) + ":" + er
	return
}

type Resmkfile struct {
	Needcallbak bool   `json:needcallbak`
	Hash        string `json:hash`
	Key         string `json:"key"`
	Code        int    `json:"code"`
	Error       string `json:"error"`
}

func (s *server) response(ekey, sha1, callbakurl string, ret int) {
	var (
		//	callbakurl string
		err    error
		data   []byte
		params = url.Values{}
	)
	log.Infof("ekey:%s,sha1:%s,callbakurl:%s,ret:%d",
		ekey, sha1, callbakurl, ret)
	tekey, _ := b64.URLEncoding.DecodeString(ekey)
	ekey = string(tekey)
	t := strings.SplitN(ekey, ":", 2)
	//bucket = t[0]
	filename := t[1]
	res := Resmkfile{}
	res.Code = 200
	res.Needcallbak = true
	if ret != 1 {
		err = errors.Error(ret)
		retcode, errstring := errortostring(err)
		res.Code = retcode
		res.Error = errstring
	} else {
		res.Hash = sha1
		res.Key = filename
	}

	data, err = json.Marshal(res)
	if err != nil {
		log.Errorf("response marshal json error(%v)", err)
		return
	}
	if err = Http("POST", callbakurl, params, data, nil); err != nil {
		log.Errorf("mkfile response called uri(%s) directory error(%v)", callbakurl, err)
		return
	}
	return

}

func (s *server) callbakurl(wr http.ResponseWriter, r *http.Request) {
	var (
		err        error
		ret        int
		callbakurl string
	)
	ekey := r.FormValue("ekey")
	if ekey == "" {
		log.Errorf("req ekey is nil")
		http.Error(wr, "bad ekey", http.StatusBadRequest)
		return
	}
	log.Infof("recv ekey %s callbakurl", ekey)
	sha1 := r.FormValue("sha1")
	if sha1 == "" {
		log.Errorf("req sha1 is nil")
		http.Error(wr, "bad sha1", http.StatusBadRequest)
		return
	}

	if ret, err = strconv.Atoi(r.FormValue("ret")); err != nil {
		log.Errorf("req ret failed %v", err)
		http.Error(wr, "bad overwrite flag", http.StatusBadRequest)
		return
	}

	callbakurl, err = s.mongo_op.Get(ekey)
	if err != nil {
		log.Errorf("get ekey %s callbakurl failed", ekey)
		return
	}

	s.response(ekey, sha1, callbakurl, ret)

}
