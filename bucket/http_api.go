package main

import (
	"efs/bucket/auth"
	"efs/bucket/bmanger"
	"efs/bucket/conf"
	"efs/bucket/database"
	"efs/bucket/global"
	"efs/bucket/rmanger"
	"efs/libs/meta"
	//"io"
	"io/ioutil"
	"net/http"
	//"os"
	//"path"
	"bytes"
	"efs/libs/errors"
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	itime "github.com/Terry-Mao/marmot/time"

	log "efs/log/glog"
)

const (
	_httpServerReadTimeout  = 10 * time.Second
	_httpServerWriteTimeout = 10 * time.Second

	_httpbcreateapi = "http://%s/bcreate"
	_httpbdelapi    = "http://%s/bdelete"
)

var (
	_timer = itime.NewTimer(1024)

	_transport = &http.Transport{
		Dial: func(netw, addr string) (c net.Conn, err error) {
			if c, err = net.DialTimeout(netw, addr, 10*time.Second); err != nil {
				return nil, err
			}
			return c, nil
		},
		DisableCompression: true,
	}
	_client = &http.Client{
		Transport: _transport,
	}
	_canceler = _transport.CancelRequest
)

type server struct {
	buckets *bmanger.Buckets
	//conn    *database.Conn
	region *rmanger.Regions
	c      *conf.Config
	auth   *auth.Auth
}

func statislog(method string, bucket, file *string, overwriteflag *int, oldsize, size *int64, start time.Time, status *int, err *string) {
	if *bucket == "" {
		*bucket = "-"
	}
	if *file == "" {
		*file = "-"
	}
	fname := b64.URLEncoding.EncodeToString([]byte(*file))
	if time.Now().Sub(start).Seconds() > 1.0 {
		log.Statisf("proxymore 1s ============%f", time.Now().Sub(start).Seconds())
	}
	log.Statisf("%s	%s	%s	%d	%d	%d	%f	%d	error(%s)",
		method, *bucket, fname, *overwriteflag, *oldsize, *size, time.Now().Sub(start).Seconds(), *status, *err)
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
	td := _timer.Start(10*time.Second, func() {
		_canceler(req)
	})
	if resp, err = _client.Do(req); err != nil {
		log.Errorf("_client.Do(%s) error(%v)", ru, err)
		return
	}
	td.Stop()
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

// StartApi init the http module.
func StartApi(c *conf.Config) (err error) {
	var s = &server{}
	s.c = c
	if err = database.New(c); err != nil {
		log.Errorf("connect database failed")
		return
	}
	s.buckets = bmanger.New()
	s.region = rmanger.New()
	s.auth = auth.New(c)
	global.Timeout = c.Timeout
	global.Dnssuffix = c.Dnssuffix

	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/bcreate", s.bcreate)
		mux.HandleFunc("/bget", s.bget)
		mux.HandleFunc("/bdelete", s.bdelete)
		mux.HandleFunc("/bsetimgsource", s.bsetimgsource)
		mux.HandleFunc("/bsetproperty", s.bsetproperty)
		mux.HandleFunc("/bsetask", s.bsetask)
		mux.HandleFunc("/bsetstyledelimiter", s.bsetStyleDelimiter)
		mux.HandleFunc("/bsetdpstyle", s.bsetDPStyle)

		mux.HandleFunc("/blist", s.bgetbyuserid)
		mux.HandleFunc("/bgetbyak", s.bgetbybucketak)
		mux.HandleFunc("/bgetbyuserdnsname", s.bgetbyuserdnsname)
		//mux.HandleFunc("/getregion", s.getregion)
		//mux.HandleFunc("/addregion", s.addregion)
		server := &http.Server{
			Addr:         c.HttpAddr,
			Handler:      mux,
			ReadTimeout:  _httpServerReadTimeout,
			WriteTimeout: _httpServerWriteTimeout,
		}
		if err := server.ListenAndServe(); err != nil {
			return
		}
	}()
	return

}

// ret reponse header.
func retCode(wr http.ResponseWriter, status *int, er *string) {
	var (
		res_err error_res
		err     error
		data    []byte
	)
	res_err.Code = *status
	wr.Header().Set("Code", strconv.Itoa(*status))
	wr.Header().Set("Content-Type", "application/json")
	wr.WriteHeader(*status)
	if *status != http.StatusOK {
		res_err.Error = *er
		if data, err = json.Marshal(res_err); err == nil {
			wr.Header().Set("Content-Length", strconv.Itoa(len(data)))
			if _, err = wr.Write(data); err != nil {
				log.Errorf("wr.Write() error(%v)", err)
			}
		} else {
			log.Errorf("json.Marshal() error(%v)", err)
		}
	}

}
func getcode(id int64) (codestring string) {
	var (
		i    int
		code []string
		t    string
	)

	a := 'a'
	for i = 0; i < 36; i++ {
		if i < 26 {
			code = append(code, fmt.Sprintf("%s", string(rune(i+int(a)))))
		} else {
			t := i - 26
			code = append(code, fmt.Sprintf("%d", t))
		}
	}

	i = 0
	for id > 0 {
		remainder := id % 36
		t = t + code[remainder]
		id = id / 36
		i++
	}

	codestring = t
	return
}

func getdnsname(uid int, dnssuffix string) (dnsname string) {
	dnsname = getcode(int64(uid)) + getcode(time.Now().Unix()) + dnssuffix
	return
}

func (s *server) bcreate(wr http.ResponseWriter, r *http.Request) {
	var (
		bucketname, hbase_bucketname, reqdata string
		er                                    string
		regionid                              int
		size                                  int64
		err                                   error
		derr                                  errors.Error

		status = http.StatusOK

		params = url.Values{}

		body  []byte
		breq  bcreate_req
		dres  meta.BucketCreatResponse
		start = time.Now()
	)
	defer retCode(wr, &status, &er)
	flag := 0
	defer statislog("/b/create", &hbase_bucketname, &reqdata,
		&flag, &size, &size, start, &status, &er)

	//auth
	token := r.Header.Get("Authorization")
	ok := s.auth.BucketAuthorize(token, r.URL.Path)
	if !ok {
		status = 401
		er = "Authorize is invalid"
		return
	}

	ekey := r.Header.Get("ekey")
	bname, errb64 := b64.URLEncoding.DecodeString(ekey)
	if errb64 != nil {
		log.Errorf("bucketname base64 decode error %s", bname)
		status = http.StatusBadRequest
		er = "request bucketname base64 decode error"
		return
	}
	bucketname = string(bname)

	if bucketname == "" {
		log.Errorf("bucketname len is 0=== %s", bucketname)
		status = http.StatusBadRequest
		er = "request bucketname len is 0"
		return
	}

	if body, err = ioutil.ReadAll(r.Body); err != nil {
		status = http.StatusBadRequest
		log.Errorf("ioutil.ReadAll(r.Body) error(%s)", err)
		er = "read http body is failed"
		return
	}

	if err = json.Unmarshal(body, &breq); err != nil {
		log.Errorf("json.Unmarshal() error(%v)", err)
		status = http.StatusBadRequest
		er = "json unmarshl failed"
		return
	}
	if breq.Replication < 1 {
		log.Errorf("replication is less 1 value is %d", breq.Replication)
		status = http.StatusBadRequest
		er = "replication value is less 1"
		return
	}

	if regionid, err = s.region.GetRegionid(breq.Region); err != nil {
		status = http.StatusBadRequest
		log.Errorf("get region id failed %s", breq.Region)
		er = "get region id failed"
		return
	}
	if breq.Key == "" || breq.Keysecret == "" {
		log.Errorf("bucket ak or sk len is 0")
		status = http.StatusBadRequest
		er = "request ak or keysecret is null"
		return
	}

	if breq.Userid < 1 {
		log.Errorf("userid is less 1 value is %d", breq.Userid)
		status = http.StatusBadRequest
		er = "replication value is less 1"
		return
	}

	if breq.Dnsname == "" {
		breq.Dnsname = getdnsname(breq.Userid, s.c.Dnssuffix)
	}
	//create bucket data insert into sql
	if err = s.buckets.Bcreate(bucketname, breq.Key, breq.Keysecret, breq.Imgsource,
		breq.Dnsname, breq.Userdnsname, regionid, int(breq.Propety), breq.Replication, breq.Userid); err != nil {
		if err == errors.ErrBucketExist {
			log.Errorf("bucket %s is exist", bucketname)
			status = errors.RetResExist
			er = "bucket is exist"
			return
		}

		log.Errorf("bucket %s create failed", bucketname)
		status = http.StatusInternalServerError
		er = "create bucket failed"
		return
	}

	//request bucket create to efs
	uri := fmt.Sprintf(_httpbcreateapi, s.c.Directoryhttpaddr)
	//hbase bucketname = userid + "_" + bucketname
	hbase_bucketname = strconv.Itoa(breq.Userid) + "_" + bucketname
	ekey = b64.URLEncoding.EncodeToString([]byte(hbase_bucketname))
	params.Set("ekey", ekey)
	params.Set("families", "efsfile")

	if err = Http("POST", uri, params, nil, &dres); err != nil {
		log.Errorf("Efs BucketCreate() called uri(%s) Http error(%v)", uri, err)

		if err = s.buckets.Bdel(bucketname, breq.Userid); err != nil {
			log.Errorf("bucket %s create failed,back sql failed", bucketname)
		}

		status = http.StatusInternalServerError
		er = "bucket create failed"
		return
	}

	if dres.Ret != errors.RetOK {
		log.Errorf("bucket create request failed (%s)", uri)

		if err = s.buckets.Bdel(bucketname, breq.Userid); err != nil {
			log.Errorf("bucket %s create failed,back sql failed", bucketname)
		}

		err = errors.Error(dres.Ret)
		if err == errors.ErrNeedleExist {
			er = errors.ErrNeedleExist.Error()
			status = errors.RetResExist
		} else {
			if derr, ok = (err).(errors.Error); ok {
				status = int(derr)
			} else {
				status = errors.RetServerFailed
			}
			er = errors.ErrServerFailed.Error()
		}
		log.Errorf("bucket create failed error %s status %d", er, status)
	}

	// for write log to rsync
	body, err = json.Marshal(breq)
	if err != nil {
		log.Errorf("json marshl failed ")
		return
	}

	reqdata = string(body)

	//resp.Body.Close()
	return

}

func (s *server) bget(wr http.ResponseWriter, r *http.Request) {
	var (
		bucketname                  string
		region                      string
		regionid                    int
		img_source                  string
		key                         string
		keysecret                   string
		ctime, dnsname, userdnsname string
		er                          string
		propety, replication, uid   int
		styledelimiter, dpstyle     string
		data                        []byte
		get_res                     bget_res

		err error
		//params = r.URL.Query()
		status = http.StatusOK
	)

	defer retCode(wr, &status, &er)
	token := r.Header.Get("Authorization")
	ok := s.auth.BucketAuthorize(token, r.URL.Path)
	if !ok {
		status = 401
		er = "Authorize is invalid"
		return
	}

	ekey := r.Header.Get("ekey")

	bname, _ := b64.URLEncoding.DecodeString(ekey)
	bucketname = string(bname)

	dname := r.Header.Get("dnsname")
	tmpbname, _ := b64.URLEncoding.DecodeString(dname)
	dnsname = string(tmpbname)

	userid := r.Header.Get("Uid")
	if userid != "" {
		uid, err = strconv.Atoi(userid)
		if err != nil {
			log.Errorf("userid parse failed %v", err)
			status = http.StatusBadRequest
			er = "uid is invalid"
			return
		}
	}
	if bucketname == "" && dnsname == "" {
		log.Errorf("bucketname len is 0=== %s and dnsname len is 0", bucketname, dnsname)
		status = http.StatusBadRequest
		er = "request bucketname and dnsname len is 0"
		return
	}

	if bucketname != "" {
		if img_source, key, keysecret, ctime, dnsname, userdnsname, propety, regionid,
			replication, styledelimiter, dpstyle, err = s.buckets.Bget(bucketname, uid); err != nil {
			if err == errors.ErrBucketNotExist {
				status = 612
				er = "have no this bucket"
				return
			}
			status = http.StatusInternalServerError
			er = "get bucket failed"
			return
		}
	} else {
		if bucketname, img_source, key, keysecret, ctime, propety, regionid,
			replication, styledelimiter, dpstyle, userdnsname, uid, err = s.buckets.Bgetbydnsname(dnsname); err != nil {
			if err == errors.ErrBucketNotExist {
				status = 612
				er = "have no this bucket"
				return
			}

			status = http.StatusInternalServerError
			er = "get bucket failed"
			return
		}
	}

	if region, err = s.region.GetRegionname(regionid); err != nil {
		status = http.StatusInternalServerError
		er = "get bucket region failed"
		return
	}
	get_res.Bucketname = bucketname
	get_res.Region = region
	get_res.Imgsource = img_source
	get_res.Key = key
	get_res.Keysecret = keysecret
	get_res.Propety = int64(propety)
	get_res.Ctime = ctime
	get_res.Dnsname = dnsname
	get_res.Userdnsname = userdnsname
	get_res.Replication = replication
	get_res.StyleDelimiter = styledelimiter
	get_res.DPStyle = dpstyle
	get_res.Uid = uid

	if data, err = json.Marshal(get_res); err == nil {
		wr.Header().Set("Content-Length", strconv.Itoa(len(data)))
		if _, err = wr.Write(data); err != nil {
			log.Errorf("wr.Write() error(%v)", err)
			status = http.StatusInternalServerError
			er = "get bucket region failed"
			return
		}
	} else {
		log.Errorf("json.Marshal() error(%v)", err)
		status = http.StatusInternalServerError
		er = "json marshl failed"
	}

	return
}

func (s *server) bdelete(wr http.ResponseWriter, r *http.Request) {
	var (
		bucketname, hbase_bucketname, file string
		er                                 string
		err                                error
		derr                               errors.Error
		size                               int64
		uid                                int
		res                                meta.BucketDeleteResponse
		params                             = url.Values{}
		status                             = http.StatusOK
		start                              = time.Now()
	)

	defer retCode(wr, &status, &er)
	flag := 0
	defer statislog("/b/delete", &hbase_bucketname, &file, &flag, &size, &size, start, &status, &er)
	token := r.Header.Get("Authorization")
	ok := s.auth.BucketAuthorize(token, r.URL.Path)
	if !ok {
		status = 401
		er = "Authorize is invalid"
		return
	}

	ekey := r.Header.Get("ekey")

	bname, b64Err := b64.URLEncoding.DecodeString(ekey)
	if b64Err != nil {
		log.Errorf("bucketname base64 decode error")
		er = "bucketname base64 decode error"
		status = http.StatusBadRequest
		return
	}
	userid := r.Header.Get("Uid")
	uid, err = strconv.Atoi(userid)
	if err != nil {
		log.Errorf("userid parse failed %v", err)
		status = http.StatusBadRequest
		er = "uid is invalid"
		return
	}
	bucketname = string(bname)
	if bucketname == "" {
		log.Errorf("bucketname len is 0=== %s", bucketname)
		status = http.StatusBadRequest
		er = "request bucketname len is 0"
		return
	}

	uri := fmt.Sprintf(_httpbdelapi, s.c.Directoryhttpaddr)
	hbase_bucketname = userid + "_" + bucketname
	ekey = b64.URLEncoding.EncodeToString([]byte(hbase_bucketname))
	params.Set("ekey", ekey)
	if err = Http("POST", uri, params, nil, &res); err != nil {
		log.Errorf("Efs BucketDelete() called uri(%s) Http error(%v)", uri, err)
		status = http.StatusInternalServerError
		er = "http request failed"
		return
	}

	if res.Ret != errors.RetOK {
		log.Errorf("Efs BucketDelete() called uri(%s) directory res.Ret(%d), params(%v)", uri, res.Ret, params)
		err = errors.Error(res.Ret)
		if err == errors.ErrSrcBucketNoExist {
			er = errors.ErrSrcBucketNoExist.Error()
			status = errors.RetResNoExist
		} else {
			if derr, ok = (err).(errors.Error); ok {
				status = int(derr)
			} else {
				status = errors.RetServerFailed
			}
			er = errors.ErrServerFailed.Error()
		}
		return
	}
	//	fmt.Println("================")
	err = s.buckets.Bdel(bucketname, uid)
	if err != nil {
		log.Recoverf("failed delete bucketname %s mysql data error(%v)", bucketname, err)
		er = "clean data failed"
		return
	}
	return
}

func (s *server) bsetimgsource(wr http.ResponseWriter, r *http.Request) {
	var (
		bucketname string
		uid        int
		er         string
		err        error
		body       []byte
		status     = http.StatusOK

		isource Imagesource
	)
	defer retCode(wr, &status, &er)

	token := r.Header.Get("Authorization")
	ok := s.auth.BucketAuthorize(token, r.URL.Path)
	if !ok {
		status = 401
		er = "Authorize is invalid"
		return
	}
	userid := r.Header.Get("Uid")
	uid, err = strconv.Atoi(userid)
	if err != nil {
		log.Errorf("userid parse failed %v", err)
		status = http.StatusBadRequest
		er = "uid is invalid"
		return
	}

	ekey := r.Header.Get("ekey")

	bname, errb64 := b64.URLEncoding.DecodeString(ekey)
	if errb64 != nil {
		log.Errorf("bucketname base64 decode error %s", bname)
		status = http.StatusBadRequest
		er = "request bucketname base64 decode error"
		return
	}
	bucketname = string(bname)

	if bucketname == "" {
		log.Errorf("bucketname len is 0=== %s", bucketname)
		status = http.StatusBadRequest
		er = "request bucketname len is 0"
		return
	}
	if body, err = ioutil.ReadAll(r.Body); err != nil {
		status = http.StatusBadRequest
		log.Errorf("ioutil.ReadAll(r.Body) error(%s)", err)
		er = "read http body is failed"
		return
	}

	if err = json.Unmarshal(body, &isource); err != nil {
		log.Errorf("json.Unmarshal() error(%v)", err)
		status = http.StatusBadRequest
		er = "json unmarshl failed"
		return
	}

	imgSourceStr, imgErr := b64.URLEncoding.DecodeString(isource.Imgsource)
	if imgErr != nil {
		log.Errorf("imgsource base64 decode error")
		status = http.StatusBadRequest
		er = "imgsource base64 decode error"
		return
	}

	err = s.buckets.Bsetimgsource(bucketname, string(imgSourceStr), uid)
	if err != nil {
		status = http.StatusInternalServerError
		er = "bucket imagesource set failed"
	}
	return

}

func (s *server) bsetproperty(wr http.ResponseWriter, r *http.Request) {
	var (
		bucketname string

		propety Propety
		er      string
		err     error
		body    []byte
		uid     int

		status = http.StatusOK
	)
	defer retCode(wr, &status, &er)

	token := r.Header.Get("Authorization")
	ok := s.auth.BucketAuthorize(token, r.URL.Path)
	if !ok {
		status = 401
		er = "Authorize is invalid"
		return
	}

	ekey := r.Header.Get("ekey")

	bname, _ := b64.URLEncoding.DecodeString(ekey)
	bucketname = string(bname)

	if bucketname == "" {
		log.Errorf("bucketname len is 0=== %s", bucketname)
		status = http.StatusBadRequest
		er = "request bucketname len is 0"
		return
	}
	if body, err = ioutil.ReadAll(r.Body); err != nil {
		status = http.StatusBadRequest
		log.Errorf("ioutil.ReadAll(r.Body) error(%s)", err)
		er = "read http body is failed"
		return
	}

	if err = json.Unmarshal(body, &propety); err != nil {
		log.Errorf("json.Unmarshal() error(%v)", err)
		status = http.StatusBadRequest
		er = "json unmarshl failed"
		return
	}
	if propety.Property < 0 || propety.Property > 3 {
		log.Errorf("request property error")
		status = http.StatusBadRequest
		er = "request property error"
		return

	}

	userid := r.Header.Get("Uid")
	uid, err = strconv.Atoi(userid)
	if err != nil {
		log.Errorf("userid parse failed %v", err)
		status = http.StatusBadRequest
		er = "uid is invalid"
		return
	}

	err = s.buckets.Bsetpropety(bucketname, propety.Property, uid)
	if err != nil {
		status = http.StatusInternalServerError
		er = "bucket propety set failed"
	}
	return
}

func (s *server) bsetask(wr http.ResponseWriter, r *http.Request) {
	var (
		bucketname string

		akinfo AkSkInfo
		er     string
		err    error
		body   []byte

		uid int

		status = http.StatusOK
	)
	defer retCode(wr, &status, &er)

	token := r.Header.Get("Authorization")
	ok := s.auth.BucketAuthorize(token, r.URL.Path)
	if !ok {
		status = 401
		er = "Authorize is invalid"
		return
	}

	ekey := r.Header.Get("ekey")

	bname, _ := b64.URLEncoding.DecodeString(ekey)
	bucketname = string(bname)

	if bucketname == "" {
		log.Errorf("bucketname len is 0=== %s", bucketname)
		status = http.StatusBadRequest
		er = "request bucketname len is 0"
		return
	}
	if body, err = ioutil.ReadAll(r.Body); err != nil {
		status = http.StatusBadRequest
		log.Errorf("ioutil.ReadAll(r.Body) error(%s)", err)
		er = "read http body is failed"
		return
	}

	if err = json.Unmarshal(body, &akinfo); err != nil {
		log.Errorf("json.Unmarshal() error(%v)", err)
		status = http.StatusBadRequest
		er = "json unmarshl failed"
		return
	}

	userid := r.Header.Get("Uid")
	uid, err = strconv.Atoi(userid)
	if err != nil {
		log.Errorf("userid parse failed %v", err)
		status = http.StatusBadRequest
		er = "uid is invalid"
		return
	}

	err = s.buckets.Bsetask(bucketname, akinfo.Acesskey, akinfo.Secertkey, uid)
	if err != nil {
		log.Errorf("set ak  sk to sql failed")
		status = http.StatusInternalServerError
		er = "bucket propety set failed"
	}
	return
}

func (s *server) bsetStyleDelimiter(wr http.ResponseWriter, r *http.Request) {
	var (
		bucketname string
		er         string
		err        error
		body       []byte
		uid        int

		status = http.StatusOK
	)
	defer retCode(wr, &status, &er)

	token := r.Header.Get("Authorization")
	ok := s.auth.BucketAuthorize(token, r.URL.Path)
	if !ok {
		status = 401
		er = "Authorize is invalid"
		return
	}

	ekey := r.Header.Get("ekey")

	bname, _ := b64.URLEncoding.DecodeString(ekey)
	bucketname = string(bname)

	if bucketname == "" {
		log.Errorf("bucketname len is 0=== %s", bucketname)
		status = http.StatusBadRequest
		er = "request bucketname len is 0"
		return
	}
	if body, err = ioutil.ReadAll(r.Body); err != nil {
		status = http.StatusBadRequest
		log.Errorf("ioutil.ReadAll(r.Body) error(%s)", err)
		er = "read http body is failed"
		return
	}

	userid := r.Header.Get("Uid")
	uid, err = strconv.Atoi(userid)
	if err != nil {
		log.Errorf("userid parse failed %v", err)
		status = http.StatusBadRequest
		er = "uid is invalid"
		return
	}

	err = s.buckets.BSetStyleDelimiter(bucketname, string(body), uid)
	if err != nil {
		log.Errorf("set styleDelimiter to sql failed")
		status = http.StatusInternalServerError
		er = "bucket styleDelimiter set failed"
	}
	return
}

func (s *server) bsetDPStyle(wr http.ResponseWriter, r *http.Request) {
	var (
		bucketname string
		er         string
		err        error
		body       []byte

		uid int

		status = http.StatusOK
	)
	defer retCode(wr, &status, &er)

	token := r.Header.Get("Authorization")
	ok := s.auth.BucketAuthorize(token, r.URL.Path)
	if !ok {
		status = 401
		er = "Authorize is invalid"
		return
	}

	ekey := r.Header.Get("ekey")

	bname, _ := b64.URLEncoding.DecodeString(ekey)
	bucketname = string(bname)

	if bucketname == "" {
		log.Errorf("bucketname len is 0=== %s", bucketname)
		status = http.StatusBadRequest
		er = "request bucketname len is 0"
		return
	}
	if body, err = ioutil.ReadAll(r.Body); err != nil {
		status = http.StatusBadRequest
		log.Errorf("ioutil.ReadAll(r.Body) error(%s)", err)
		er = "read http body is failed"
		return
	}

	userid := r.Header.Get("Uid")
	uid, err = strconv.Atoi(userid)
	if err != nil {
		log.Errorf("userid parse failed %v", err)
		status = http.StatusBadRequest
		er = "uid is invalid"
		return
	}

	err = s.buckets.BSetDPStyle(bucketname, string(body), uid)
	if err != nil {
		log.Errorf("set DPStyle to sql failed")
		status = http.StatusInternalServerError
		er = "bucket DPStyle set failed"
	}
	return
}

func (s *server) bgetbyuserid(wr http.ResponseWriter, r *http.Request) {
	var (
		uid     int
		buckets []*meta.Bucket_item
		data    []byte
		err     error
		er      string
		status  = http.StatusOK
	)

	defer retCode(wr, &status, &er)

	token := r.Header.Get("Authorization")
	ok := s.auth.BucketAuthorize(token, r.URL.Path)
	if !ok {
		status = 401
		er = "Authorize is invalid"
		return
	}

	userid := r.Header.Get("Uid")
	uid, err = strconv.Atoi(userid)
	if err != nil {
		log.Errorf("userid parse failed %v", err)
		status = http.StatusBadRequest
		er = "uid is invalid"
		return
	}
	buckets, err = s.buckets.BgetByUserid(uid)
	if err != nil {
		er = "server failed"
		log.Errorf("get userid failed %v", err)
		status = http.StatusServiceUnavailable
		return
	}

	for _, binfo := range buckets {
		if binfo.Region, err = s.region.GetRegionname(binfo.RegionId); err != nil {
			status = http.StatusInternalServerError
			er = "get bucket region failed"
			return
		}
	}

	if data, err = json.Marshal(buckets); err == nil {
		wr.Header().Set("Content-Length", strconv.Itoa(len(data)))
		wr.Header().Set("Content-Type", "application/json")
		if _, err = wr.Write(data); err != nil {
			log.Errorf("wr.Write() error(%v)", err)
			status = http.StatusInternalServerError
			er = "get buckets failed"
			return
		}
	} else {
		log.Errorf("json.Marshal() error(%v)", err)
		status = http.StatusInternalServerError
		er = "json marshl failed"
	}
	return

}

func (s *server) bgetbybucketak(wr http.ResponseWriter, r *http.Request) {
	var (
		binfo          *meta.Bucket_item
		data           []byte
		err            error
		er, bucketname string
		status         = http.StatusOK
	)

	defer retCode(wr, &status, &er)

	token := r.Header.Get("Authorization")
	ok := s.auth.BucketAuthorize(token, r.URL.Path)
	if !ok {
		status = 401
		er = "Authorize is invalid"
		return
	}

	ak := r.Header.Get("Ak")

	ekey := r.Header.Get("ekey")

	bname, _ := b64.URLEncoding.DecodeString(ekey)
	bucketname = string(bname)

	binfo, err = s.buckets.BgetByak(bucketname, ak)
	if err != nil {
		er = "server failed"
		log.Errorf("get userid failed %v", err)
		status = http.StatusServiceUnavailable
		return
	}

	if data, err = json.Marshal(binfo); err == nil {
		wr.Header().Set("Content-Length", strconv.Itoa(len(data)))
		if _, err = wr.Write(data); err != nil {
			log.Errorf("wr.Write() error(%v)", err)
			status = http.StatusInternalServerError
			er = "get buckets failed"
			return
		}
	} else {
		log.Errorf("json.Marshal() error(%v)", err)
		status = http.StatusInternalServerError
		er = "json marshl failed"
	}
	return

}

func (s *server) bgetbyuserdnsname(wr http.ResponseWriter, r *http.Request) {
	var (
		bucketname                  string
		region                      string
		regionid                    int
		img_source                  string
		key                         string
		keysecret                   string
		ctime, dnsname, userdnsname string
		er                          string
		propety, replication, uid   int
		styledelimiter, dpstyle     string
		data                        []byte
		get_res                     bget_res

		err error

		status = http.StatusOK
	)

	defer retCode(wr, &status, &er)

	token := r.Header.Get("Authorization")
	ok := s.auth.BucketAuthorize(token, r.URL.Path)
	if !ok {
		status = 401
		er = "Authorize is invalid"
		return
	}

	udnsname := r.Header.Get("Udnsname")
	if udnsname == "" {
		er = "user dnsname input null"
		status = http.StatusServiceUnavailable
		log.Errorf("input udnsname %s is null", udnsname)
		return
	}
	//log.Errorf("get by udnsname %s", udnsname)
	if bucketname, img_source, key, keysecret, ctime, propety, regionid,
		replication, styledelimiter, dpstyle, userdnsname, uid, err = s.buckets.BgetByuserdnsname(udnsname); err != nil {
		if err == errors.ErrBucketNotExist {
			status = 612
			er = "have no this bucket"
			return
		}

		status = http.StatusInternalServerError
		er = "get bucket failed"
		return
	}

	if region, err = s.region.GetRegionname(regionid); err != nil {
		status = http.StatusInternalServerError
		er = "get bucket region failed"
		return
	}
	get_res.Bucketname = bucketname
	get_res.Region = region
	get_res.Imgsource = img_source
	get_res.Key = key
	get_res.Keysecret = keysecret
	get_res.Propety = int64(propety)
	get_res.Ctime = ctime
	get_res.Dnsname = dnsname
	get_res.Userdnsname = userdnsname
	get_res.Replication = replication
	get_res.StyleDelimiter = styledelimiter
	get_res.DPStyle = dpstyle
	get_res.Uid = uid

	if data, err = json.Marshal(get_res); err == nil {
		wr.Header().Set("Content-Length", strconv.Itoa(len(data)))
		if _, err = wr.Write(data); err != nil {
			log.Errorf("wr.Write() error(%v)", err)
			status = http.StatusInternalServerError
			er = "get bucket region failed"
			return
		}
	} else {
		log.Errorf("json.Marshal() error(%v)", err)
		status = http.StatusInternalServerError
		er = "json marshl failed"
	}

	return

}
