package main

import (
	"encoding/json"
	"kagamistoreage/libs/meta"
	"net/http"
	"time"

	"bytes"
	"fmt"
	"io/ioutil"
	log "kagamistoreage/log/glog"
	"net"
	"net/url"
	"strconv"
	"strings"
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
		Timeout:   1 * time.Second,
	}
)

// HttpGetWriter
func HttpGetWriter(r *http.Request, wr http.ResponseWriter, start time.Time, res *meta.SliceResponse) {
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

// HttpStatWriter
func HttpStatWriter(r *http.Request, wr http.ResponseWriter, start time.Time, res *meta.Response) {
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

// HttpUploadWriter
func HttpUploadWriter(r *http.Request, wr http.ResponseWriter, start time.Time, res *meta.Response) {
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

// HttpDispatcher
func HttpDispatcherWriter(r *http.Request, wr http.ResponseWriter, start time.Time, res *meta.Response) {
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
		log.Errorf("HttpDispatcher Write error(%v)", err)
		return
	}
	log.Infof("%s path:%s(params:%s,time:%f,ret:%v)", r.Method,
		r.URL.Path, r.Form.Encode(), time.Now().Sub(start).Seconds(), ret)
}

// HttpMkblkWriter
func HttpMkblkWriter(r *http.Request, wr http.ResponseWriter, start time.Time, res *meta.Response) {
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

// HttpBputWriter
func HttpBputWriter(r *http.Request, wr http.ResponseWriter, start time.Time, res *meta.Response) {
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

// HttpMkfileWriter
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

// HttpDelWriter
func HttpDelWriter(r *http.Request, wr http.ResponseWriter, start time.Time, res *meta.SliceResponse) {
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

// HttpChgmWriter
func HttpChgmWriter(r *http.Request, wr http.ResponseWriter, start time.Time, res *meta.Response) {
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

// HttpChgexpWriter
func HttpChgexpWriter(r *http.Request, wr http.ResponseWriter, start time.Time, res *meta.Response) {
	var (
		err      error
		byteJson []byte
		ret      = res.Ret
	)
	if byteJson, err = json.Marshal(res); err != nil {
		log.Errorf("json encode err(%s)", err.Error())
		return
	}
	wr.Header().Set("Content-Type", "application/json;charset=utf-8")
	wr.Write(byteJson)
	log.Infof("%s path:%s(params:%s,time:%f,ret:%v)", r.Method,
		r.URL.Path, r.Form.Encode(), time.Now().Sub(start).Seconds(), ret)
}

// HttpCopyWriter
func HttpCopyWriter(r *http.Request, wr http.ResponseWriter, start time.Time, res *meta.Response) {
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

// HttpMoveWriter
func HttpMoveWriter(r *http.Request, wr http.ResponseWriter, start time.Time, res *meta.Response) {
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

// HttpListWriter
func HttpListWriter(r *http.Request, wr http.ResponseWriter, start time.Time, res *meta.FileListResponse) {
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

// HttpDestroyListWriter
func HttpDestroyListWriter(r *http.Request, wr http.ResponseWriter, start time.Time, res *meta.DestroyListResponse) {
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

// HttpDestroyFileWriter
func HttpDestroyFileWriter(r *http.Request, wr http.ResponseWriter, start time.Time, res *meta.DestroyFileResponse) {
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

// HttpDestroyExpireWriter
func HttpDestroyExpireWriter(r *http.Request, wr http.ResponseWriter,
	start time.Time, res *meta.DestroyExpireResponse) {
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

// HttpBcreatWriter
func HttpBcreatWriter(r *http.Request, wr http.ResponseWriter, start time.Time, res *meta.BucketCreatResponse) {
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

func HttpCleantimeoutWriter(r *http.Request, wr http.ResponseWriter, start time.Time, res *meta.CleanTimeoutResponse) {
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

// HttpBrenameWriter
func HttpBrenameWriter(r *http.Request, wr http.ResponseWriter, start time.Time, res *meta.BucketRenameResponse) {
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

// HttpBdeleteWriter
func HttpBdeleteWriter(r *http.Request, wr http.ResponseWriter, start time.Time, res *meta.BucketDeleteResponse) {
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

// HttpBdestroyWriter
func HttpBdestroyWriter(r *http.Request, wr http.ResponseWriter, start time.Time, res *meta.BucketDestroyResponse) {
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

// HttpBlistWriter
func HttpBlistWriter(r *http.Request, wr http.ResponseWriter, start time.Time, res *meta.BucketListResponse) {
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

// HttpBdeleteWriter
func HttpBStatWriter(r *http.Request, wr http.ResponseWriter, start time.Time, res *meta.BucketStatResponse) {
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
func reponse_callbak(res meta.Response, ekey, callbakurl string) (err error) {
	var (
		params = url.Values{}
	)
	params.Set("ekey", ekey)
	params.Set("sha1", res.Sha1)
	params.Set("ret", fmt.Sprintf("%d", res.Ret))
	if err = Http("POST", callbakurl, params, nil, nil); err != nil {
		log.Errorf("mkfile response called uri(%s) directory error(%v)", callbakurl, err)
		return
	}
	return
}
