package main

import (
	"efs/libs/errors"
	"efs/libs/meta"
	b64 "encoding/base64"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	log "efs/log/glog"
)

const (
	_pingOk               = 0
	EXPIRE_FILE_DELIMITER = "_"
)

type server struct {
	d *Directory
}

// StartApi start api http listen.
func StartApi(addr string, d *Directory) {
	var s = &server{d: d}
	go func() {
		var (
			err      error
			serveMux = http.NewServeMux()
		)
		serveMux.HandleFunc("/get", s.get)
		serveMux.HandleFunc("/dispatcher", s.dispatcher)
		serveMux.HandleFunc("/upload", s.upload)
		serveMux.HandleFunc("/del", s.del)
		serveMux.HandleFunc("/deltmp", s.deltmp)
		serveMux.HandleFunc("/stat", s.stat)
		serveMux.HandleFunc("/chgm", s.chgm)
		serveMux.HandleFunc("/chgexp", s.chgexp)
		serveMux.HandleFunc("/copy", s.copy)
		serveMux.HandleFunc("/move", s.move)
		serveMux.HandleFunc("/list", s.list)
		serveMux.HandleFunc("/getpartid", s.getpartid)
		serveMux.HandleFunc("/mkblk", s.mkblk)   //new multipart
		serveMux.HandleFunc("/bput", s.bput)     // not use
		serveMux.HandleFunc("/mkfile", s.mkfile) //new multipart
		serveMux.HandleFunc("/mkbigfile", s.mkbigfile)
		serveMux.HandleFunc("/destroylist", s.destroylist)
		serveMux.HandleFunc("/destroyfile", s.destroyfile)
		serveMux.HandleFunc("/destroyexpire", s.destroyexpire)
		serveMux.HandleFunc("/ping", s.ping) //no use
		serveMux.HandleFunc("/bcreate", s.bcreate)
		serveMux.HandleFunc("/brename", s.brename)
		serveMux.HandleFunc("/bdelete", s.bdelete)
		serveMux.HandleFunc("/bdestroy", s.bdestroy)
		serveMux.HandleFunc("/blist", s.blist)
		serveMux.HandleFunc("/bstat", s.bstat)
		serveMux.HandleFunc("/getneedle", s.getneedle)
		//serveMux.HandleFunc("/cleantimeoutfile", s.clean)
		if err = http.ListenAndServe(addr, serveMux); err != nil {
			log.Errorf("http.ListenAndServe(\"%s\") error(%v)", addr, err)
			return
		}
	}()
	return
}

func (s *server) get(wr http.ResponseWriter, r *http.Request) {
	var (
		ok       bool
		bucket   string
		filename string
		start    = time.Now()
		res      meta.SliceResponse
		stores   *[]meta.StoreS
		n        []*meta.Needle
		f        *meta.File
		uerr     errors.Error
		err      error
	)

	ekey := r.FormValue("ekey")
	if ekey == "" {
		http.Error(wr, "bad ekey request", http.StatusBadRequest)
		return
	}
	tekey, _ := b64.URLEncoding.DecodeString(ekey)
	ekey = string(tekey)
	t := strings.SplitN(ekey, ":", 2)
	bucket = t[0]
	filename = t[1]
	if bucket == "" || filename == "" {
		http.Error(wr, "bad ekey is null request", http.StatusBadRequest)
		return
	}

	defer HttpGetWriter(r, wr, start, &res)
	if n, f, stores, err = s.d.GetStores(bucket, filename); err != nil {
		log.Errorf("GetStores() error(%v)\n", err)
		if uerr, ok = err.(errors.Error); ok {
			res.Ret = int(uerr)
		} else {
			res.Ret = errors.RetInternalErr
		}
		return
	}

	res.Ret = errors.RetOK
	res.Res = make([]meta.Response, len(n))
	res.Mine = f.Mine
	res.Fsize = f.Filesize
	if f.MTime != 0 {
		res.MTime = f.MTime
	}
	res.Sha1 = f.Sha1
	for index, v := range n {
		res.Res[index].Key = v.Key
		res.Res[index].Cookie = v.Cookie
		res.Res[index].Vid = v.Vid
		res.Res[index].Mine = f.Mine
		if f.MTime != 0 {
			res.Res[index].MTime = f.MTime
		} else {
			res.Res[index].MTime = v.MTime
		}
		res.Res[index].Sha1 = f.Sha1
		res.Res[index].Stores = (*stores)[index].Stores
	}
	res.Keys = f.Key
	return
}

func (s *server) dispatcher(wr http.ResponseWriter, r *http.Request) {
	var (
		err           error
		n             *meta.Needle
		bucket        string
		filename      string
		lastVid       int
		overWriteFlag int
		replication   int
		arr           []string
		data          []byte
		res           meta.Response
		ok            bool
		uerr          errors.Error
	)
	ekey := r.FormValue("ekey")
	if ekey == "" {
		log.Errorf("ekey is invalid,eky=%s", ekey)
		http.Error(wr, "bad ekey", http.StatusBadRequest)
		return
	}

	data, _ = b64.URLEncoding.DecodeString(ekey)
	arr = strings.SplitN(string(data), ":", 2)

	if len(arr) == 1 {
		bucket = arr[0]
		filename = ""
	} else if len(arr) == 2 {
		bucket = arr[0]
		filename = arr[1]
	}

	if bucket == "" {
		log.Errorf("bucket=%s or filename=%s is null ", bucket, filename)
		http.Error(wr, "bad ekey is null request", http.StatusBadRequest)
		return
	}

	if lastVid, err = strconv.Atoi(r.FormValue("lastvid")); err != nil {
		log.Errorf("lasvid = %s is invaild", r.FormValue("lastvid"))
		http.Error(wr, "bad lastvid", http.StatusBadRequest)
		return
	}

	if overWriteFlag, err = strconv.Atoi(r.FormValue("overwrite")); err != nil {
		log.Errorf("overwrite = %s is invaild", r.FormValue("overwrite"))
		http.Error(wr, "bad overwrite flag", http.StatusBadRequest)
		return
	}

	if replication, err = strconv.Atoi(r.FormValue("replication")); err != nil {
		log.Errorf("replication = %s is invaild", r.FormValue("replicaiton"))
		http.Error(wr, "bad replication", http.StatusBadRequest)
		return
	}

	defer HttpDispatcherWriter(r, wr, time.Now(), &res)

	res.Ret = errors.RetOK
	if n, res.Stores, err = s.d.Dispatcher(bucket, filename, int32(lastVid),
		overWriteFlag, replication); err != nil {
		log.Errorf("dispatcher() error(%v)", err)
		if uerr, ok = err.(errors.Error); ok {
			res.Ret = int(uerr)
		} else {
			res.Ret = errors.RetInternalErr
		}
		return
	}

	res.Key = n.Key
	res.Cookie = n.Cookie
	res.Vid = n.Vid
	return
}

func (s *server) upload(wr http.ResponseWriter, r *http.Request) {
	var (
		err                            error
		tInt64                         int64
		n                              *meta.Needle
		f                              *meta.File
		bucket, deleteAfterDays_s      string
		overWriteFlag, deleteAfterDays int
		res                            meta.Response
		ok                             bool
		uerr                           errors.Error
	)
	f = new(meta.File)
	f.Key = make([]int64, 0)
	n = new(meta.Needle)

	ekey := r.FormValue("ekey")
	if ekey == "" {
		http.Error(wr, "bad ekey request", http.StatusBadRequest)
		return
	}
	tekey, _ := b64.URLEncoding.DecodeString(ekey)
	ekey = string(tekey)
	t := strings.SplitN(ekey, ":", 2)
	bucket = t[0]
	f.Filename = t[1]
	if bucket == "" || f.Filename == "" {
		http.Error(wr, "bad ekey is null request", http.StatusBadRequest)
		return
	}

	if f.Sha1 = r.FormValue("sha1"); f.Sha1 == "" {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}
	if f.Mine = r.FormValue("mime"); f.Mine == "" {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}
	if f.Filesize = r.FormValue("filesize"); f.Filesize == "" {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}

	if n.Key, err = strconv.ParseInt(r.FormValue("nkey"), 10, 64); n.Key == 0 || err != nil {
		http.Error(wr, "bad request nkey error", http.StatusBadRequest)
		return
	}
	if tInt64, err = strconv.ParseInt(r.FormValue("vid"), 10, 64); tInt64 == 0 || err != nil {
		http.Error(wr, "bad request vid error", http.StatusBadRequest)
		return
	}
	n.Vid = int32(tInt64)

	if tInt64, err = strconv.ParseInt(r.FormValue("cookie"), 10, 64); tInt64 == 0 || err != nil {
		http.Error(wr, "bad request vid error", http.StatusBadRequest)
		return
	}
	n.Cookie = int32(tInt64)

	if overWriteFlag, err = strconv.Atoi(r.FormValue("overwrite")); err != nil {
		http.Error(wr, "bad overwrite flag", http.StatusBadRequest)
		return
	}
	if deleteAfterDays_s = r.FormValue("deleteAfterDays"); deleteAfterDays_s == "" {
		deleteAfterDays = 0
	} else {
		if deleteAfterDays, err = strconv.Atoi(deleteAfterDays_s); err != nil {
			log.Errorf("deleteAfterDays %s string to int failed %v", deleteAfterDays_s, err)
			http.Error(wr, "bad deleteAfterDays", http.StatusBadRequest)
			return
		}
	}

	n.Link = 0
	f.Key = append(f.Key, n.Key)

	defer HttpUploadWriter(r, wr, time.Now(), &res)

	res.Ret = errors.RetOK
	if res.OFSize, err = s.d.Upload(bucket, f, n, overWriteFlag, deleteAfterDays); err != nil {
		log.Errorf("UploadStores() error(%v)", err)
		if uerr, ok = err.(errors.Error); ok {
			res.Ret = int(uerr)
		} else {
			res.Ret = errors.RetInternalErr
		}
		return
	}

	return
}

func (s *server) getpartid(wr http.ResponseWriter, r *http.Request) {
	var (
		err  error
		res  meta.Response
		key  int64
		ok   bool
		uerr errors.Error
	)

	defer HttpMkblkWriter(r, wr, time.Now(), &res)

	res.Ret = errors.RetOK
	if key, err = s.d.Getkeyid(); err != nil {
		log.Errorf("get keyid error(%v)", err)
		if uerr, ok = err.(errors.Error); ok {
			res.Ret = int(uerr)
		} else {
			res.Ret = errors.RetInternalErr
		}
		return
	}
	res.Key = key

	return

}

func (s *server) mkblk(wr http.ResponseWriter, r *http.Request) {
	var (
		err    error
		n      *meta.Needle //key cookie vid mtime
		f      *meta.File   //filename key sha1 mime status mtime
		bucket string
		res    meta.Response //ret key cookie vid stores mtime sha1 mime
		ok     bool
		uerr   errors.Error
		tInt64 int64
	)

	n = new(meta.Needle)
	f = new(meta.File)
	f.Key = make([]int64, 0)

	ekey := r.FormValue("ekey")
	if ekey == "" {
		log.Errorf("mkblk input ekey is null")
		http.Error(wr, "bad ekey request", http.StatusBadRequest)
		return
	}
	tekey, _ := b64.URLEncoding.DecodeString(ekey)
	ekey = string(tekey)
	t := strings.SplitN(ekey, ":", 2)
	bucket = t[0]
	f.Filename = t[1]
	if bucket == "" {
		log.Errorf("mkblk bucket is null")
		http.Error(wr, "bad ekey is null request", http.StatusBadRequest)
		return
	}

	if f.Sha1 = r.FormValue("sha1"); f.Sha1 == "" {
		log.Errorf("mkblk input sha1 is null")
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}

	f.Mine = r.FormValue("mime")

	if f.Filesize = r.FormValue("filesize"); f.Filesize == "" {
		http.Error(wr, "bad request", http.StatusBadRequest)
		log.Errorf("mkblk input filesize is null")
		return
	}
	/*
		if res.Offset, err = strconv.ParseInt(f.Filesize, 10, 64); err != nil {
			http.Error(wr, "bad request", http.StatusBadRequest)
			return
		}
	*/

	if n.Key, err = strconv.ParseInt(r.FormValue("nkey"), 10, 64); n.Key == 0 || err != nil {
		log.Errorf("mkblk input key is invald")
		http.Error(wr, "bad request nkey error", http.StatusBadRequest)
		return
	}
	if tInt64, err = strconv.ParseInt(r.FormValue("vid"), 10, 64); tInt64 == 0 || err != nil {
		log.Errorf("mkblk input vid is invalid")
		http.Error(wr, "bad request vid error", http.StatusBadRequest)
		return
	}
	n.Vid = int32(tInt64)

	if tInt64, err = strconv.ParseInt(r.FormValue("cookie"), 10, 64); tInt64 == 0 || err != nil {
		log.Errorf("mkblk input cookie is invalid")
		http.Error(wr, "bad request vid error", http.StatusBadRequest)
		return
	}
	n.Cookie = int32(tInt64)

	n.Link = 0
	f.Key = append(f.Key, n.Key)

	defer HttpMkblkWriter(r, wr, time.Now(), &res)

	res.Ret = errors.RetOK
	if err = s.d.Mkblk(bucket, f, n); err != nil {
		log.Errorf("Mkblk() error(%v)", err)
		if uerr, ok = err.(errors.Error); ok {
			res.Ret = int(uerr)
		} else {
			res.Ret = errors.RetInternalErr
		}
		return
	}
	res.Key = n.Key
	res.Cookie = n.Cookie
	res.Vid = n.Vid
	return
}

//not use
func (s *server) bput(wr http.ResponseWriter, r *http.Request) {
	var (
		err       error
		n         *meta.Needle //key cookie vid mtime
		f         *meta.File   //filename key sha1 mime status mtime
		bucket    string
		res       meta.Response //ret key cookie vid stores mtime sha1 mime
		ok        bool
		uerr      errors.Error
		ctx       string
		offsets   string
		offset    int64
		retoffset int64
		id        string
		tInt64    int64
		tekey     []byte
	)

	n = new(meta.Needle)
	f = new(meta.File)
	f.Key = make([]int64, 0)

	ekey := r.FormValue("ekey")
	if ekey == "" {
		http.Error(wr, "bad ekey request", http.StatusBadRequest)
		return
	}
	tekey, _ = b64.URLEncoding.DecodeString(ekey)
	ekey = string(tekey)
	t := strings.SplitN(ekey, ":", 2)
	bucket = t[0]
	f.Filename = t[1]
	if bucket == "" {
		http.Error(wr, "bad ekey is null request", http.StatusBadRequest)
		return
	}

	if f.Sha1 = r.FormValue("sha1"); f.Sha1 == "" {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}
	if f.Mine = r.FormValue("mime"); f.Mine == "" {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}
	if f.Filesize = r.FormValue("filesize"); f.Filesize == "" {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}

	if ctx = r.FormValue("ctx"); ctx == "" {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}

	if id = r.FormValue("id"); id == "" {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}

	if offsets = r.FormValue("offset"); offsets == "" {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}
	if offset, err = strconv.ParseInt(offsets, 10, 64); err != nil {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}

	if n.Key, err = strconv.ParseInt(r.FormValue("nkey"), 10, 64); n.Key == 0 || err != nil {
		http.Error(wr, "bad request nkey error", http.StatusBadRequest)
		return
	}
	if tInt64, err = strconv.ParseInt(r.FormValue("vid"), 10, 64); tInt64 == 0 || err != nil {
		http.Error(wr, "bad request vid error", http.StatusBadRequest)
		return
	}
	n.Vid = int32(tInt64)

	if tInt64, err = strconv.ParseInt(r.FormValue("cookie"), 10, 64); tInt64 == 0 || err != nil {
		http.Error(wr, "bad request vid error", http.StatusBadRequest)
		return
	}
	n.Cookie = int32(tInt64)

	n.Link = 0
	f.Key = append(f.Key, n.Key)
	//response for http request, key cookie vid
	defer HttpBputWriter(r, wr, time.Now(), &res)

	res.Ret = errors.RetOK
	if retoffset, err = s.d.Bput(bucket, f, n, ctx, id, offset); err != nil {
		log.Errorf("BputStores() error(%v)", err)
		if uerr, ok = err.(errors.Error); ok {
			res.Ret = int(uerr)
		} else {
			res.Ret = errors.RetInternalErr
		}
		return
	}

	res.Key = n.Key
	res.Offset = retoffset
	return
}

func (s *server) mkfile(wr http.ResponseWriter, r *http.Request) {
	var (
		err                            error
		f                              *meta.File //filename key sha1 mime status mtime
		bucket                         string
		res                            meta.Response //ret key cookie vid stores mtime sha1 mime
		ok                             bool
		uerr                           errors.Error
		sha1                           string
		body                           string
		deleteAfterDays_s              string
		overWriteFlag, deleteAfterDays int
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	f = new(meta.File)
	//slice upload

	ekey := r.FormValue("ekey")
	if ekey == "" {
		http.Error(wr, "bad ekey request", http.StatusBadRequest)
		return
	}
	tekey, _ := b64.URLEncoding.DecodeString(ekey)
	ekey = string(tekey)
	t := strings.SplitN(ekey, ":", 2)
	bucket = t[0]
	f.Filename = t[1]
	if bucket == "" {
		http.Error(wr, "bad ekey is null request", http.StatusBadRequest)
		return
	}

	//get form params mime
	if f.Mine = r.FormValue("mime"); f.Mine == "" {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}

	if deleteAfterDays_s = r.FormValue("deleteAfterDays"); deleteAfterDays_s == "" {
		deleteAfterDays = 0
	} else {
		if deleteAfterDays, err = strconv.Atoi(deleteAfterDays_s); err != nil {
			log.Errorf("deleteAfterDays %s string to int failed %v", deleteAfterDays_s, err)
			http.Error(wr, "bad deleteAfterDays", http.StatusBadRequest)
			return
		}
	}

	//get form params filesize
	if f.Filesize = r.FormValue("filesize"); f.Filesize == "" {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}

	//get form params body
	if body = r.FormValue("body"); body == "" {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}

	if overWriteFlag, err = strconv.Atoi(r.FormValue("overwrite")); err != nil {
		http.Error(wr, "bad overwrite flag", http.StatusBadRequest)
		return
	}

	//response for http request, key cookie vid
	defer HttpMkfileWriter(r, wr, time.Now(), &res)
	res.Ret = errors.RetOK
	if res.OFSize, sha1, err = s.d.MkfileStores(bucket, f, body, overWriteFlag, deleteAfterDays); err != nil {
		if err == errors.ErrNeedleExist {
			res.Ret = errors.RetNeedleExist
			return
		} else {
			log.Errorf("MkfileStores() error(%v)", err)
			if uerr, ok = err.(errors.Error); ok {
				res.Ret = int(uerr)
			} else {
				res.Ret = errors.RetInternalErr
			}
			return
		}
	}
	res.Sha1 = sha1
	return
}

func (s *server) mkbigfile(wr http.ResponseWriter, r *http.Request) {
	var (
		err                            error
		f                              *meta.File //filename key sha1 mime status mtime
		bucket                         string
		res                            meta.Response //ret key cookie vid stores mtime sha1 mime
		ok                             bool
		uerr                           errors.Error
		sha1, callbakurl               string
		body                           []byte
		deleteAfterDays_s              string
		overWriteFlag, deleteAfterDays int
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	f = new(meta.File)
	//slice upload

	ekey := r.Header.Get("ekey")
	if ekey == "" {
		http.Error(wr, "bad ekey request", http.StatusBadRequest)
		return
	}
	tekey, _ := b64.URLEncoding.DecodeString(ekey)
	ekey = string(tekey)
	t := strings.SplitN(ekey, ":", 2)
	bucket = t[0]
	f.Filename = t[1]
	if bucket == "" {
		http.Error(wr, "bad ekey is null request", http.StatusBadRequest)
		return
	}

	//get form params mime
	if f.Mine = r.Header.Get("mime"); f.Mine == "" {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}

	if deleteAfterDays_s = r.Header.Get("deleteAfterDays"); deleteAfterDays_s == "" {
		deleteAfterDays = 0
	} else {
		if deleteAfterDays, err = strconv.Atoi(deleteAfterDays_s); err != nil {
			log.Errorf("deleteAfterDays %s string to int failed %v", deleteAfterDays_s, err)
			http.Error(wr, "bad deleteAfterDays", http.StatusBadRequest)
			return
		}
	}

	//get form params filesize
	if f.Filesize = r.Header.Get("filesize"); f.Filesize == "" {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}

	if body, err = ioutil.ReadAll(r.Body); err != nil {
		log.Errorf("read http body ctxs faild %v", err)
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}

	callbakurl = r.Header.Get("callbakurl")

	if overWriteFlag, err = strconv.Atoi(r.Header.Get("overwrite")); err != nil {
		http.Error(wr, "bad overwrite flag", http.StatusBadRequest)
		return
	}

	//response for http request, key cookie vid
	res.Ret = errors.RetOK
	HttpMkfileWriter(r, wr, time.Now(), &res)
	go func() {
		if res.OFSize, sha1, err = s.d.MkbigfileStores(bucket, f, string(body), overWriteFlag, deleteAfterDays); err != nil {
			if err == errors.ErrNeedleExist {
				res.Ret = errors.RetNeedleExist
				return
			} else {
				log.Errorf("MkfileStores() error(%v)", err)
				if uerr, ok = err.(errors.Error); ok {
					res.Ret = int(uerr)
				} else {
					res.Ret = errors.RetInternalErr
				}
				return
			}
		}
		res.Sha1 = sha1

		if callbakurl != "" {
			err = reponse_callbak(res, ekey, callbakurl)
			if err != nil {
				log.Errorf("response callbak failed %v", err)
			}
		}
	}()
	return
}

func (s *server) del(wr http.ResponseWriter, r *http.Request) {
	var (
		err      error
		bucket   string
		filename string
		f        *meta.File
		res      meta.SliceResponse
		ok       bool
		uerr     errors.Error
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ekey := r.FormValue("ekey")
	if ekey == "" {
		log.Errorf("ekey empty\n")
		http.Error(wr, "bad ekey request", http.StatusBadRequest)
		return
	}
	tekey, _ := b64.URLEncoding.DecodeString(ekey)
	ekey = string(tekey)
	t := strings.SplitN(ekey, ":", 2)
	bucket = t[0]
	filename = t[1]
	if bucket == "" || filename == "" {
		log.Errorf("bad ekey:%s\n", ekey)
		http.Error(wr, "bad ekey is null request", http.StatusBadRequest)
		return
	}
	defer HttpDelWriter(r, wr, time.Now(), &res)

	if f, err = s.d.DelStores(bucket, filename); err != nil {
		log.Errorf("DelStores(b:%s f:%s) error(%v)", bucket, filename, err)
		if uerr, ok = err.(errors.Error); ok {
			res.Ret = int(uerr)
		} else {
			res.Ret = errors.RetInternalErr
		}
		return
	}

	res.Ret = errors.RetOK
	res.Fsize = f.Filesize

	return
}

func (s *server) deltmp(wr http.ResponseWriter, r *http.Request) {
	var (
		err      error
		n        []*meta.Needle
		bucket   string
		filename string
		res      meta.SliceResponse
		ok       bool
		uerr     errors.Error
		stores   *[]meta.StoreS
		id       string
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ekey := r.FormValue("ekey")
	if ekey == "" {
		http.Error(wr, "bad ekey request", http.StatusBadRequest)
		return
	}
	tekey, _ := b64.URLEncoding.DecodeString(ekey)
	ekey = string(tekey)
	t := strings.SplitN(ekey, ":", 2)
	bucket = t[0]
	filename = t[1]
	if bucket == "" || filename == "" {
		http.Error(wr, "bad ekey is null request", http.StatusBadRequest)
		return
	}
	if id = r.FormValue("id"); id == "" {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}
	defer HttpDelWriter(r, wr, time.Now(), &res)
	if n, stores, err = s.d.DelTmpStores(bucket, filename, id); err != nil {

		log.Errorf("DelTmpStores() error(%v)", err)
		if uerr, ok = err.(errors.Error); ok {
			res.Ret = int(uerr)
		} else {
			res.Ret = errors.RetInternalErr
		}
		return
	}
	//needle have link
	if n[0].Link != 0 {
		res.Ret = errors.RemoveLinkOK
	} else {
		res.Ret = errors.RetOK
	}
	res.Res = make([]meta.Response, len(n))
	for index, v := range n {
		res.Res[index].Key = v.Key
		res.Res[index].Cookie = v.Cookie
		res.Res[index].Vid = v.Vid
		res.Res[index].Stores = (*stores)[index].Stores
	}
	return
}

func (s *server) ping(wr http.ResponseWriter, r *http.Request) {
	var (
		byteJson []byte
		res      = map[string]interface{}{"code": _pingOk}
		err      error
	)
	if byteJson, err = json.Marshal(res); err != nil {
		log.Errorf("json.Marshal(\"%v\") failed (%v)", res, err)
		return
	}
	wr.Header().Set("Content-Type", "application/json;charset=utf-8")
	if _, err = wr.Write(byteJson); err != nil {
		log.Errorf("HttpWriter Write error(%v)", err)
	}
	return
}

func (s *server) getneedle(wr http.ResponseWriter, r *http.Request) {
	var (
		res   meta.Response
		n     *meta.Needle
		addrs []string
		uerr  errors.Error
		err   error
		key   int64
		ok    bool
	)
	defer HttpStatWriter(r, wr, time.Now(), &res)
	keystring := r.FormValue("key")
	if keystring == "" {
		res.Ret = errors.RetInternalErr
		log.Errorf("req key is null")
		return
	}
	key, err = strconv.ParseInt(keystring, 10, 64)
	if err != nil {
		log.Errorf("parse key %s failed %v", keystring, err)
		res.Ret = errors.RetInternalErr
		return
	}
	if n, addrs, err = s.d.Getneedle(key); err != nil {
		log.Errorf("GetStat() error(%v)", err)
		if uerr, ok = err.(errors.Error); ok {
			res.Ret = int(uerr)
		} else {
			res.Ret = errors.RetInternalErr
		}
		return
	}
	res.Key = key
	res.Vid = n.Vid
	res.Cookie = n.Cookie
	res.Stores = addrs
	res.Ret = errors.RetOK
	return
}

func (s *server) list(wr http.ResponseWriter, r *http.Request) {
	var (
		ok        bool
		bucket    string
		limit     string
		prefix    string
		delimiter string
		marker    string
		res       *meta.FileListResponse
		rest      meta.FileListResponse
		uerr      errors.Error
		err       error
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if bucket = r.FormValue("ekey"); bucket == "" {
		http.Error(wr, "bad request ekey", http.StatusBadRequest)
		return
	}
	ekey, _ := b64.URLEncoding.DecodeString(bucket)
	bucket = string(ekey)

	limit = r.FormValue("limit")
	prefix = r.FormValue("prefix")
	delimiter = r.FormValue("delimiter")
	marker = r.FormValue("marker")

	defer HttpListWriter(r, wr, time.Now(), &rest)

	if res, err = s.d.GetFileList(bucket, limit, prefix, delimiter, marker); err != nil {
		log.Errorf("GetFileList() error(%v)", err)
		if uerr, ok = err.(errors.Error); ok {
			rest.Ret = int(uerr)
		} else {
			rest.Ret = errors.RetInternalErr
		}
		return
	}
	rest = *res
	rest.Ret = errors.RetOK
	return
}

func (s *server) stat(wr http.ResponseWriter, r *http.Request) {
	var (
		ok       bool
		bucket   string
		filename string
		res      meta.Response
		f        *meta.File
		uerr     errors.Error
		err      error
	)
	if r.Method != "GET" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ekey := r.FormValue("ekey")
	if ekey == "" {
		http.Error(wr, "bad ekey request", http.StatusBadRequest)
		return
	}
	tekey, _ := b64.URLEncoding.DecodeString(ekey)
	ekey = string(tekey)
	t := strings.SplitN(ekey, ":", 2)
	bucket = t[0]
	filename = t[1]
	if bucket == "" || filename == "" {
		http.Error(wr, "bad ekey is null request", http.StatusBadRequest)
		return
	}

	defer HttpStatWriter(r, wr, time.Now(), &res)
	if f, err = s.d.GetStat(bucket, filename); err != nil {
		log.Errorf("GetStat() error(%v)", err)
		if uerr, ok = err.(errors.Error); ok {
			res.Ret = int(uerr)
		} else {
			res.Ret = errors.RetInternalErr
		}
		return
	}
	res.Ret = errors.RetOK
	res.Key = f.Key[0]
	res.Mine = f.Mine
	res.Fsize = f.Filesize
	if f.MTime != 0 {
		res.MTime = f.MTime
	}
	//change uniux time to days
	if f.DeleteAftertime != 0 {
		res.DeleteAftertime = int((f.DeleteAftertime-f.MTime)/(24*3600)) + 1
	} else {
		res.DeleteAftertime = 0
	}

	res.Sha1 = f.Sha1
	return
}

func (s *server) chgm(wr http.ResponseWriter, r *http.Request) {
	var (
		err    error
		f      *meta.File //filename key sha1 mime status mtime
		bucket string
		res    meta.Response //ret key cookie vid stores mtime sha1 mime
		ok     bool
		uerr   errors.Error
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	f = new(meta.File)
	ekey := r.FormValue("ekey")
	if ekey == "" {
		http.Error(wr, "bad ekey request", http.StatusBadRequest)
		return
	}
	tekey, _ := b64.URLEncoding.DecodeString(ekey)
	ekey = string(tekey)
	t := strings.SplitN(ekey, ":", 2)
	bucket = t[0]
	f.Filename = t[1]
	if bucket == "" || f.Filename == "" {
		http.Error(wr, "bad ekey is null request", http.StatusBadRequest)
		return
	}

	//get form params mime
	f.Mine = r.FormValue("mime")

	//response for http request, key cookie vid
	defer HttpChgmWriter(r, wr, time.Now(), &res)

	res.Ret = errors.RetOK
	if err = s.d.UpdataMetas(bucket, f); err != nil {
		log.Errorf("UpdataMetas() error(%v)", err)
		if uerr, ok = err.(errors.Error); ok {
			res.Ret = int(uerr)
		} else {
			res.Ret = errors.RetInternalErr
		}
		return
	}
	return
}

func (s *server) chgexp(wr http.ResponseWriter, r *http.Request) {

	var (
		err    error
		f      *meta.File
		bucket string
		res    meta.Response
		ok     bool
		uerr   errors.Error
	)

	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	f = new(meta.File)
	ekey := r.FormValue("ekey")
	if ekey == "" {
		http.Error(wr, "bad ekey request", http.StatusBadRequest)
		return
	}
	tekey, _ := b64.URLEncoding.DecodeString(ekey)
	ekey = string(tekey)
	t := strings.SplitN(ekey, ":", 2)
	bucket = t[0]
	f.Filename = t[1]
	if bucket == "" || f.Filename == "" {
		http.Error(wr, "bad ekey is null request", http.StatusBadRequest)
		return
	}

	//get form params days
	if f.DeleteAftertime, err = strconv.ParseInt(r.FormValue("expire"), 10, 64); err != nil {
		http.Error(wr, "bad days request", http.StatusBadRequest)
		return
	}

	//response for http request, key cookie vid
	defer HttpChgexpWriter(r, wr, time.Now(), &res)

	res.Ret = errors.RetOK

	if err = s.d.UpdateExp(bucket, f); err != nil {
		log.Errorf("UpdataExp() error(%v)", err)
		if uerr, ok = err.(errors.Error); ok {
			res.Ret = int(uerr)
		} else {
			res.Ret = errors.RetInternalErr
		}
		return
	}

	return
}

func (s *server) copy(wr http.ResponseWriter, r *http.Request) {
	var (
		err           error
		bucket        string
		destbucket    string
		filename      string
		destfname     string
		overWriteFlag int
		res           meta.Response //ret key cookie vid stores mtime sha1 mime
		ok            bool
		uerr          errors.Error
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ekey := r.FormValue("srcekey")
	if ekey == "" {
		http.Error(wr, "bad ekey request", http.StatusBadRequest)
		return
	}
	tekey, _ := b64.URLEncoding.DecodeString(ekey)
	ekey = string(tekey)
	t := strings.SplitN(ekey, ":", 2)
	bucket = t[0]
	filename = t[1]
	if bucket == "" || filename == "" {
		http.Error(wr, "bad ekey is null request", http.StatusBadRequest)
		return
	}

	ekey = r.FormValue("destekey")
	if ekey == "" {
		http.Error(wr, "bad ekey request", http.StatusBadRequest)
		return
	}
	tekey, _ = b64.URLEncoding.DecodeString(ekey)
	ekey = string(tekey)
	t = strings.SplitN(ekey, ":", 2)
	destbucket = t[0]
	destfname = t[1]
	if destbucket == "" || destfname == "" {
		http.Error(wr, "bad ekey is null request", http.StatusBadRequest)
		return
	}

	if overWriteFlag, err = strconv.Atoi(r.FormValue("overwrite")); err != nil {
		http.Error(wr, "bad overwrite flag", http.StatusBadRequest)
		return
	}

	//response for http request, key cookie vid
	defer HttpCopyWriter(r, wr, time.Now(), &res)

	res.Ret = errors.RetOK
	if res.Fsize, res.OFSize, err = s.d.UpdataCopyMetas(bucket, filename, destbucket, destfname, overWriteFlag); err != nil {
		log.Errorf("UpdataCopyMetas() error(%v)", err)
		if uerr, ok = err.(errors.Error); ok {
			res.Ret = int(uerr)
		} else {
			res.Ret = errors.RetInternalErr
		}
		return
	}
	return
}

func (s *server) move(wr http.ResponseWriter, r *http.Request) {
	var (
		err           error
		bucket        string
		destbucket    string
		filename      string
		destfname     string
		overWriteFlag int
		res           meta.Response //ret key cookie vid stores mtime sha1 mime
		ok            bool
		uerr          errors.Error
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ekey := r.FormValue("srcekey")
	if ekey == "" {
		http.Error(wr, "bad ekey request", http.StatusBadRequest)
		return
	}
	tekey, _ := b64.URLEncoding.DecodeString(ekey)
	ekey = string(tekey)
	t := strings.SplitN(ekey, ":", 2)
	bucket = t[0]
	filename = t[1]
	if bucket == "" || filename == "" {
		http.Error(wr, "bad ekey is null request", http.StatusBadRequest)
		return
	}

	ekey = r.FormValue("destekey")
	if ekey == "" {
		http.Error(wr, "bad ekey request", http.StatusBadRequest)
		return
	}
	tekey, _ = b64.URLEncoding.DecodeString(ekey)
	ekey = string(tekey)
	t = strings.SplitN(ekey, ":", 2)
	destbucket = t[0]
	destfname = t[1]
	if destbucket == "" || destfname == "" {
		http.Error(wr, "bad ekey is null request", http.StatusBadRequest)
		return
	}

	if overWriteFlag, err = strconv.Atoi(r.FormValue("overwrite")); err != nil {
		http.Error(wr, "bad overwrite flag", http.StatusBadRequest)
		return
	}

	//response for http request, key cookie vid
	defer HttpMoveWriter(r, wr, time.Now(), &res)

	res.Ret = errors.RetOK
	if res.Fsize, res.OFSize, err = s.d.UpdataMoveMetas(bucket, filename, destbucket, destfname, overWriteFlag); err != nil {
		log.Errorf("UpdataMoveMetas() error(%v)", err)
		if uerr, ok = err.(errors.Error); ok {
			res.Ret = int(uerr)
		} else {
			res.Ret = errors.RetInternalErr
		}
		return
	}
	return
}

func (s *server) bcreate(wr http.ResponseWriter, r *http.Request) {
	var (
		err      error
		bucket   string
		families string
		res      meta.BucketCreatResponse //ret key cookie vid stores mtime sha1 mine
		ok       bool
		uerr     errors.Error
		ekey     []byte
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	//get form params bucket
	if bucket = r.FormValue("ekey"); bucket == "" {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}
	ekey, _ = b64.URLEncoding.DecodeString(bucket)
	bucket = string(ekey)
	//get form params families
	if families = r.FormValue("families"); families == "" {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}

	//response for http request, key cookie vid
	defer HttpBcreatWriter(r, wr, time.Now(), &res)

	res.Ret = errors.RetOK

	//var family []string
	//family = append(family, "basic")
	//family = append(family, "efsfile")
	if err = s.d.BucketCreate(bucket, families); err != nil {
		log.Errorf("CreateTable() error(%v)", err)
		if uerr, ok = err.(errors.Error); ok {
			res.Ret = int(uerr)
		} else {
			res.Ret = errors.RetInternalErr
		}
		return
	}
	return
}

func (s *server) brename(wr http.ResponseWriter, r *http.Request) {
	var (
		ok         bool
		bucket_src string
		bucket_dst string
		res        meta.BucketRenameResponse
		uerr       errors.Error
		err        error
		ekey       []byte
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if bucket_src = r.FormValue("srcekey"); bucket_src == "" {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}
	ekey, _ = b64.URLEncoding.DecodeString(bucket_src)
	bucket_src = string(ekey)
	if bucket_dst = r.FormValue("destekey"); bucket_dst == "" {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}
	ekey, _ = b64.URLEncoding.DecodeString(bucket_dst)
	bucket_dst = string(ekey)
	defer HttpBrenameWriter(r, wr, time.Now(), &res)
	if err = s.d.BucketRename(bucket_src, bucket_dst); err != nil {
		log.Errorf("BucketRename() error(%v)", err)
		if uerr, ok = err.(errors.Error); ok {
			res.Ret = int(uerr)
		} else {
			res.Ret = errors.RetInternalErr
		}
		return
	}
	res.Ret = errors.RetOK
	return
}

func (s *server) bdelete(wr http.ResponseWriter, r *http.Request) {
	var (
		ok     bool
		bucket string
		res    meta.BucketDeleteResponse
		uerr   errors.Error
		err    error
		ekey   []byte
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if bucket = r.FormValue("ekey"); bucket == "" {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}
	ekey, _ = b64.URLEncoding.DecodeString(bucket)
	bucket = string(ekey)
	defer HttpBdeleteWriter(r, wr, time.Now(), &res)
	if err = s.d.BucketDelete(bucket); err != nil {
		log.Errorf("BucketDelete() error(%v)", err)
		if uerr, ok = err.(errors.Error); ok {
			res.Ret = int(uerr)
		} else {
			res.Ret = errors.RetInternalErr
		}
		return
	}
	res.Ret = errors.RetOK
	return
}

func (s *server) bdestroy(wr http.ResponseWriter, r *http.Request) {
	var (
		ok     bool
		bucket string
		res    meta.BucketDestroyResponse
		uerr   errors.Error
		err    error
		ekey   []byte
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if bucket = r.FormValue("ekey"); bucket == "" {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}
	ekey, _ = b64.URLEncoding.DecodeString(bucket)
	bucket = string(ekey)
	defer HttpBdestroyWriter(r, wr, time.Now(), &res)
	if err = s.d.BucketDestroy(bucket); err != nil {
		log.Errorf("BucketDestroy() error(%v)", err)
		if uerr, ok = err.(errors.Error); ok {
			res.Ret = int(uerr)
		} else {
			res.Ret = errors.RetInternalErr
		}
		return
	}
	res.Ret = errors.RetOK
	return
}
func (s *server) blist(wr http.ResponseWriter, r *http.Request) {
	var (
		ok      bool
		res     meta.BucketListResponse
		uerr    errors.Error
		err     error
		list    []string
		regular string
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if regular = r.FormValue("regular"); regular == "" {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}
	defer HttpBlistWriter(r, wr, time.Now(), &res)
	if list, err = s.d.BucketList(regular); err != nil {
		log.Errorf("BucketList() error(%v)", err)
		if uerr, ok = err.(errors.Error); ok {
			res.Ret = int(uerr)
		} else {
			res.Ret = errors.RetInternalErr
		}
		return
	}
	res.Ret = errors.RetOK
	res.List = list
	return
}

func (s *server) bstat(wr http.ResponseWriter, r *http.Request) {
	var (
		ok     bool
		bucket string
		res    meta.BucketStatResponse
		uerr   errors.Error
		err    error
		ekey   []byte
		exsit  bool
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if bucket = r.FormValue("ekey"); bucket == "" {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}
	ekey, _ = b64.URLEncoding.DecodeString(bucket)
	bucket = string(ekey)
	defer HttpBStatWriter(r, wr, time.Now(), &res)
	if exsit, err = s.d.BucketStat(bucket); err != nil {
		log.Errorf("BucketStat() error(%v)", err)
		if uerr, ok = err.(errors.Error); ok {
			res.Ret = int(uerr)
		} else {
			res.Ret = errors.RetInternalErr
		}
		return
	}
	res.Ret = errors.RetOK
	res.Exist = exsit
	return
}

//destroy list
func (s *server) destroylist(wr http.ResponseWriter, r *http.Request) {
	var (
		res            *meta.DestroyListResponse
		resRt          meta.DestroyListResponse
		ekey           string
		data           []byte
		bucket         string
		limit          string
		marker         string
		err            error
		uerr           errors.Error
		ok, trash_flag bool
	)
	res = new(meta.DestroyListResponse)

	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ekey = r.FormValue("ekey")
	data, _ = b64.URLEncoding.DecodeString(ekey)
	bucket = string(data)
	if ekey == "" || bucket == "" {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}

	limit = r.FormValue("limit")
	marker = r.FormValue("marker")
	defer HttpDestroyListWriter(r, wr, time.Now(), &resRt)

	if res, trash_flag, err = s.d.GetDestroyFileList(bucket, limit, marker); err != nil {
		log.Errorf("GetDestroyFileList error(%v), bucket(%s), limit(%s), marker(%s)", err, bucket, limit, marker)
		if uerr, ok = err.(errors.Error); ok {
			res.Ret = int(uerr)
			resRt = *res
		} else {
			res.Ret = errors.RetInternalErr
			resRt = *res
		}
		return
	}

	res.Ret = errors.RetOK
	res.Trash_flag = trash_flag
	resRt = *res
	return
}

/*
func (s *server) clean(wr http.ResponseWriter, r *http.Request) {
	var (
		ekey       string
		begin, end int64
		err        error
		res        meta.CleanTimeoutResponse
	)
	ekey = r.FormValue("ekey")
	data, _ := b64.URLEncoding.DecodeString(ekey)
	bucket := string(data)
	if ekey == "" || bucket == "" {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}

	b := r.FormValue("begin")
	e := r.FormValue("end")
	if begin, err = strconv.ParseInt(b, 10, 64); err != nil {
		log.Errorf("input begin time %s  is invalid", b)
		http.Error(wr, "begintime is invalid", http.StatusBadRequest)
		return
	}
	if end, err = strconv.ParseInt(e, 10, 64); err != nil {
		log.Errorf("input end time %s is invalid", e)
		http.Error(wr, "endtime is invalid", http.StatusBadRequest)
		return
	}
	if end < begin {
		log.Errorf("begin %d  > end %d is invalid", begin, end)
		http.Error(wr, "begin is more end is invalid", http.StatusBadRequest)
		return
	}

	defer HttpCleantimeoutWriter(r, wr, time.Now(), &res)
	res.Failfiles, err = s.d.Clean_timeout_file(bucket, begin, end)
	if err != nil {
		if uerr, ok := err.(errors.Error); ok {
			res.Ret = int(uerr)
		} else {
			res.Ret = errors.RetInternalErr
		}
		return
	}
	res.Ret = errors.RetOK
	return

}
*/
//destory file
func (s *server) destroyfile(wr http.ResponseWriter, r *http.Request) {
	var (
		ekey   string
		data   []byte
		strArr []string
		bucket string
		file   string
		err    error
		uerr   errors.Error
		ok     bool
		res    meta.DestroyFileResponse
	)

	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ekey = r.FormValue("ekey")
	data, _ = b64.URLEncoding.DecodeString(ekey)
	strArr = strings.SplitN(string(data), ":", 2)
	if len(strArr) != 3 && len(strArr) != 2 {
		http.Error(wr, "bad request,ekey1", http.StatusBadRequest)
		return
	}
	bucket = strArr[0]
	file = strArr[1]
	if len(strArr) == 3 {
		file = strArr[1] + ":" + strArr[2]
	}
	if bucket == "" || file == "" {
		http.Error(wr, "bad request,ekey2", http.StatusBadRequest)
		return
	}

	defer HttpDestroyFileWriter(r, wr, time.Now(), &res)
	if err = s.d.DestroyStore(bucket, file); err != nil {
		log.Errorf("d.DestroyStore error(%v), bucket(%s), file(%s)", err, bucket, file)
		if uerr, ok = err.(errors.Error); ok {
			res.Ret = int(uerr)
		} else {
			res.Ret = errors.RetInternalErr
		}
		return
	}

	res.Ret = errors.RetOK
	return
}

//destory expire
func (s *server) destroyexpire(wr http.ResponseWriter, r *http.Request) {
	var (
		res meta.DestroyExpireResponse
	)

	defer HttpDestroyExpireWriter(r, wr, time.Now(), &res)

	go s.d.DestroyExpire()

	res.Ret = errors.RetOK
	return
}
