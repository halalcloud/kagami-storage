package main

import (
	"bytes"
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	itime "github.com/Terry-Mao/marmot/time"
)

var (
	_timer     = itime.NewTimer(1024)
	_transport = &http.Transport{
		Dial: func(netw, addr string) (c net.Conn, err error) {
			if c, err = net.DialTimeout(netw, addr, 2*time.Second); err != nil {
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

const (
	_bcreate_uri       = "http://127.0.0.1:2232/bcreate"
	_bdel_uri          = "http://127.0.0.1:2232/bdelete"
	_bget_uri          = "http://127.0.0.1:2232/bget"
	_bsetproperty_uri  = "http://127.0.0.1:2232/bsetproperty"
	_bsetimgsource_uri = "http://127.0.0.1:2232/bsetimgsource"
	_list_uri          = "http://192.168.200.60:2232/r/list"
	_chgm_uri          = "http://192.168.200.60:2232/r/chgm"
	_copy_uri          = "http://192.168.200.60:2232/r/copy"
	_move_uri          = "http://192.168.200.60:2232/r/move"
	_stat_uri          = "http://192.168.200.60:2232/r/stat"
	_get_uri           = "http://192.168.200.60:2232/r/get"
	_mkblk_uri         = "http://192.168.200.60:2232/r/mkblk"
	_bput_uri          = "http://192.168.200.60:2232/r/bput"
	_mkfile_uri        = "http://192.168.200.60:2232/r/mkfile"

	_host       = "localhost:8080"
	_batch_uri  = "http://" + _host + "r/batch"
	_rdel_uri   = "http://" + _host + "/r/delete"
	_upload_uri = "http://" + _host + "/r/upload"
)

// Http params
func Http(method, uri string, params url.Values, buf []byte, res interface{}) (err error) {
	var (
		body    []byte
		w       *multipart.Writer
		bw      io.Writer
		bufdata = &bytes.Buffer{}
		req     *http.Request
		resp    *http.Response
		ru      string
		enc     string
		ctype   string
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
			w = multipart.NewWriter(bufdata)
			if bw, err = w.CreateFormFile("file", "1.jpg"); err != nil {
				return
			}
			if _, err = bw.Write(buf); err != nil {
				return
			}
			for key, _ := range params {
				w.WriteField(key, params.Get(key))
			}
			ctype = w.FormDataContentType()
			if err = w.Close(); err != nil {
				return
			}
			if req, err = http.NewRequest("POST", uri, bufdata); err != nil {
				return
			}
			req.Header.Set("Content-Type", ctype)
		}
	}
	td := _timer.Start(5*time.Second, func() {
		_canceler(req)
	})
	if resp, err = _client.Do(req); err != nil {
		fmt.Printf("_client.Do(%s) error(%v)", ru, err)
		return
	}
	td.Stop()
	defer resp.Body.Close()
	if res == nil {
		return
	}
	if resp.StatusCode != http.StatusOK {
		fmt.Printf("_client.Do(%s) status: %d", ru, resp.StatusCode)
		return
	}
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		fmt.Printf("ioutil.ReadAll(%s) uri(%s) error(%v)", body, ru, err)
		return
	}
	if err = json.Unmarshal(body, res); err != nil {
		fmt.Printf("json.Unmarshal(%s) uri(%s) error(%v)", body, ru, err)
	}
	return
}

type Bcreate struct {
	Region    string `json:"region"`
	Imgsource string `json:"imgsource"`
	Key       string `json:"key"`
	Keysecret string `json:"keysecret"`
	Propety   int64  `json:"propety"`
}

type Bget struct {
	Region    string `json:"region"`
	Imgsource string `json:"imgsource"`
	Key       string `json:"key"`
	Keysecret string `json:"keysecret"`
	Propety   int64  `json:"propety"`
	Ctime     string `json:"ctime"`
}

type Propety struct {
	Property int64 `json:"property"`
}

type Imagesource struct {
	Imgsource string `json:"imgsource"`
}

type error_res struct {
	Error string `json:"error"`
}

func bcreate_test() {
	var (
		req     *http.Request
		resp    *http.Response
		bucket  string
		bcdata  Bcreate
		err     error
		b, body []byte
		res     error_res
	)
	bucket = "test"
	bcdata.Region = "test"
	bcdata.Imgsource = "test"
	bcdata.Key = "test"
	bcdata.Keysecret = "test"
	bcdata.Propety = 1

	b, err = json.Marshal(bcdata)
	if err != nil {
		fmt.Printf("json.marshal() error(%v)", err)

		return
	}

	tdata := bytes.NewBuffer([]byte(b))

	if req, err = http.NewRequest("POST", _bcreate_uri, tdata); err != nil {

		return
	}
	req.Header.Set("Content-Type", "application/json")
	estring := b64.URLEncoding.EncodeToString([]byte(bucket))
	req.Header.Set("ekey", estring)
	if resp, err = _client.Do(req); err != nil {
		fmt.Println("failed:", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		fmt.Printf("_client.Do status: %d", resp.StatusCode)
		return
	}
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		fmt.Printf("ioutil.ReadAll(%s)  error(%v)", body, err)
		return
	}
	if err = json.Unmarshal(body, &res); err != nil {
		fmt.Printf("json.Unmarshal(%s)  error(%v) errstring %s", body, err, res.Error)
	}
	fmt.Println("err:", res.Error)
}
func bdel_test() {
	var (
		req    *http.Request
		resp   *http.Response
		bucket string
		err    error
		res    error_res
		body   []byte
	)
	bucket = "test"
	if req, err = http.NewRequest("POST", _bdel_uri, nil); err != nil {

		return
	}
	req.Header.Set("Content-Type", "application/json")
	estring := b64.URLEncoding.EncodeToString([]byte(bucket))
	req.Header.Set("ekey", estring)
	if resp, err = _client.Do(req); err != nil {
		fmt.Println("failed:", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		fmt.Printf("_client.Do status: %d", resp.StatusCode)
		return
	}
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		fmt.Printf("ioutil.ReadAll(%s)  error(%v)", body, err)
		return
	}
	if err = json.Unmarshal(body, &res); err != nil {
		fmt.Printf("json.Unmarshal(%s)  error(%v) errstring %s", body, err, res.Error)
	}
	fmt.Println("err:", res.Error)
}

func bget_test() {
	var (
		req     *http.Request
		resp    *http.Response
		bucket  string
		res     error_res
		getdata Bget
		body    []byte
		err     error
	)
	bucket = "test"
	if req, err = http.NewRequest("POST", _bget_uri, nil); err != nil {

		return
	}
	req.Header.Set("Content-Type", "application/json")
	estring := b64.URLEncoding.EncodeToString([]byte(bucket))
	req.Header.Set("ekey", estring)
	if resp, err = _client.Do(req); err != nil {
		fmt.Println("failed:", err)
		return
	}
	defer resp.Body.Close()

	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		fmt.Printf("ioutil.ReadAll(%s)  error(%v)", body, err)
		return
	}
	if resp.StatusCode == http.StatusOK {
		if err = json.Unmarshal(body, &getdata); err != nil {
			fmt.Printf("json.Unmarshal(%s)  error(%v) errstring %s", body, err, res.Error)
		}
		fmt.Printf("_client.Do status: %d", resp.StatusCode)
		fmt.Println("data:", getdata)
		return
	} else {
		if err = json.Unmarshal(body, &res); err != nil {
			fmt.Printf("json.Unmarshal(%s)  error(%v) errstring %s", body, err, res.Error)
		}
	}
	fmt.Println("err:", res.Error)

}

func bsetproperty_test() {
	var (
		req     *http.Request
		resp    *http.Response
		bucket  string
		res     error_res
		setdata Propety
		err     error
		b, body []byte
	)
	bucket = "test"
	setdata.Property = 0
	b, err = json.Marshal(setdata)
	if err != nil {
		fmt.Printf("json.marshal() error(%v)", err)

		return
	}

	tdata := bytes.NewBuffer([]byte(b))
	if req, err = http.NewRequest("POST", _bsetproperty_uri, tdata); err != nil {

		return
	}
	req.Header.Set("Content-Type", "application/json")
	estring := b64.URLEncoding.EncodeToString([]byte(bucket))
	req.Header.Set("ekey", estring)
	if resp, err = _client.Do(req); err != nil {
		fmt.Println("failed:", err)
		return
	}
	defer resp.Body.Close()

	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		fmt.Printf("ioutil.ReadAll(%s)  error(%v)", body, err)
		return
	}
	if resp.StatusCode == http.StatusOK {

		fmt.Printf("_client.Do status: %d", resp.StatusCode)

		return
	} else {
		if err = json.Unmarshal(body, &res); err != nil {
			fmt.Printf("json.Unmarshal(%s)  error(%v) errstring %s", body, err, res.Error)
		}
	}
	fmt.Println("err:", res.Error)
}

func bsetimgsource_test() {
	var (
		req     *http.Request
		resp    *http.Response
		bucket  string
		setdata Imagesource
		res     error_res
		err     error
		b, body []byte
	)
	bucket = "test"
	setdata.Imgsource = "bj"
	b, err = json.Marshal(setdata)
	if err != nil {
		fmt.Printf("json.marshal() error(%v)", err)

		return
	}

	tdata := bytes.NewBuffer([]byte(b))
	if req, err = http.NewRequest("POST", _bsetimgsource_uri, tdata); err != nil {

		return
	}
	req.Header.Set("Content-Type", "application/json")
	estring := b64.URLEncoding.EncodeToString([]byte(bucket))
	req.Header.Set("ekey", estring)
	if resp, err = _client.Do(req); err != nil {
		fmt.Println("failed:", err)
		return
	}
	defer resp.Body.Close()

	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		fmt.Printf("ioutil.ReadAll(%s)  error(%v)", body, err)
		return
	}
	if resp.StatusCode == http.StatusOK {

		fmt.Printf("_client.Do status: %d", resp.StatusCode)

		return
	} else {
		if err = json.Unmarshal(body, &res); err != nil {
			fmt.Printf("json.Unmarshal(%s)  error(%v) errstring %s", body, err, res.Error)
		}
	}
	fmt.Println("err:", res.Error)
}

type titem struct {
	Key      string `json:"key"`
	PutTime  int64  `json:"putTime"`
	Hash     string `json:"hash"`
	Fsize    int64  `json:"fsize"`
	MimeType string `json:"mimeType"`
	Customer string `json:"customer"`
}

type tlist struct {
	Marker         string   `json:"marker"`
	CommonPrefixes []string `json:"commonPrefixes"`
	Items          []titem  `json:"items"`
}

func list_test() {
	var (
		req    *http.Request
		resp   *http.Response
		bucket string
		err    error
		res    error_res
		body   []byte
		uri    string
		l      tlist
	)
	bucket = "test"
	params := url.Values{}
	params.Set("limit", "5")
	params.Set("delimiter", "")
	params.Set("marker", "")
	uri = _list_uri + "?" + params.Encode()
	if req, err = http.NewRequest("POST", uri, nil); err != nil {

		return
	}
	req.Header.Set("Content-Type", "application/json")
	estring := b64.URLEncoding.EncodeToString([]byte(bucket))
	req.Header.Set("ekey", estring)
	if resp, err = _client.Do(req); err != nil {
		fmt.Println("failed:", err)
		return
	}
	defer resp.Body.Close()
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		fmt.Printf("ioutil.ReadAll(%s)  error(%v)", body, err)
		return
	}

	if resp.StatusCode == http.StatusOK {
		fmt.Printf("sucess status: %d", resp.StatusCode)
		if err = json.Unmarshal(body, &l); err != nil {
			fmt.Printf("json.Unmarshal(%s)  error(%v) errstring %s", body, err, res.Error)
		}
		fmt.Printf(" data== %v", l)
		return
	}

	if err = json.Unmarshal(body, &res); err != nil {
		fmt.Printf("json.Unmarshal(%s)  error(%v) errstring %s", body, err, res.Error)
	}
	fmt.Printf("error data %v", res)
}

func chgm_test() {
	var (
		req    *http.Request
		resp   *http.Response
		bucket string
		err    error
		res    error_res
		body   []byte
		uri    string
		mime   map[string]string
	)
	bucket = "efs:test"
	uri = _chgm_uri

	mime = make(map[string]string)
	mime["mime"] = "jpg"

	b, err := json.Marshal(mime)
	if err != nil {
		fmt.Printf("json.marshal() error(%v)", err)
		return
	}

	tdata := bytes.NewBuffer([]byte(b))

	if req, err = http.NewRequest("POST", uri, tdata); err != nil {

		return
	}
	req.Header.Set("Content-Type", "application/json")
	estring := b64.URLEncoding.EncodeToString([]byte(bucket))
	req.Header.Set("ekey", estring)
	if resp, err = _client.Do(req); err != nil {
		fmt.Println("failed:", err)
		return
	}
	defer resp.Body.Close()
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		fmt.Printf("ioutil.ReadAll(%s)  error(%v)", body, err)
		return
	}

	if resp.StatusCode == http.StatusOK {
		fmt.Printf("sucess status: %d", resp.StatusCode)
		return
	}

	if err = json.Unmarshal(body, &res); err != nil {
		fmt.Printf("json.Unmarshal(%s)  error(%v) errstring %s", body, err, res.Error)
	}
	fmt.Printf("error data %v", res)
}

func copy_test() {
	var (
		req     *http.Request
		resp    *http.Response
		bucket  string
		err     error
		res     error_res
		body    []byte
		uri     string
		dest    map[string]string
		dbucket string
	)
	bucket = "efs:test"
	dbucket = "efs:test10"
	uri = _copy_uri

	dest = make(map[string]string)
	dstring := b64.URLEncoding.EncodeToString([]byte(dbucket))
	dest["dest"] = dstring

	b, err := json.Marshal(dest)
	if err != nil {
		fmt.Printf("json.marshal() error(%v)", err)
		return
	}

	tdata := bytes.NewBuffer([]byte(b))

	if req, err = http.NewRequest("POST", uri, tdata); err != nil {

		return
	}
	req.Header.Set("Content-Type", "application/json")
	estring := b64.URLEncoding.EncodeToString([]byte(bucket))
	req.Header.Set("ekey", estring)
	if resp, err = _client.Do(req); err != nil {
		fmt.Println("failed:", err)
		return
	}
	defer resp.Body.Close()
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		fmt.Printf("ioutil.ReadAll(%s)  error(%v)", body, err)
		return
	}

	if resp.StatusCode == http.StatusOK {
		fmt.Printf("sucess status: %d", resp.StatusCode)
		return
	}

	if err = json.Unmarshal(body, &res); err != nil {
		fmt.Printf("json.Unmarshal(%s)  error(%v) errstring %s", body, err, res.Error)
	}
	fmt.Printf("error data %v", res)
}

func move_test() {
	var (
		req     *http.Request
		resp    *http.Response
		bucket  string
		err     error
		res     error_res
		body    []byte
		uri     string
		dest    map[string]string
		dbucket string
	)
	bucket = "efs:test"
	dbucket = "efs:test10"
	uri = _move_uri

	dest = make(map[string]string)
	dstring := b64.URLEncoding.EncodeToString([]byte(dbucket))
	dest["dest"] = dstring

	b, err := json.Marshal(dest)
	if err != nil {
		fmt.Printf("json.marshal() error(%v)", err)
		return
	}

	tdata := bytes.NewBuffer([]byte(b))

	if req, err = http.NewRequest("POST", uri, tdata); err != nil {

		return
	}
	req.Header.Set("Content-Type", "application/json")
	estring := b64.URLEncoding.EncodeToString([]byte(bucket))
	req.Header.Set("ekey", estring)
	if resp, err = _client.Do(req); err != nil {
		fmt.Println("failed:", err)
		return
	}
	defer resp.Body.Close()
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		fmt.Printf("ioutil.ReadAll(%s)  error(%v)", body, err)
		return
	}

	if resp.StatusCode == http.StatusOK {
		fmt.Printf("sucess status: %d", resp.StatusCode)
		return
	}

	if err = json.Unmarshal(body, &res); err != nil {
		fmt.Printf("json.Unmarshal(%s)  error(%v) errstring %s", body, err, res.Error)
	}
	fmt.Printf("error data %v", res)
}

type fstat struct {
	Fsize    int64  `json:"fsize"`
	Hash     string `json:"hash"`
	MimeType string `json:"mimeType"`
	PutTime  int64  `json:putTime`
}

func stat_test() {
	var (
		req    *http.Request
		resp   *http.Response
		bucket string
		err    error
		res    error_res
		body   []byte
		uri    string
		l      fstat
	)
	bucket = "efs:test"

	uri = _stat_uri
	if req, err = http.NewRequest("POST", uri, nil); err != nil {

		return
	}
	req.Header.Set("Content-Type", "application/json")
	estring := b64.URLEncoding.EncodeToString([]byte(bucket))
	req.Header.Set("ekey", estring)
	if resp, err = _client.Do(req); err != nil {
		fmt.Println("failed:", err)
		return
	}
	defer resp.Body.Close()
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		fmt.Printf("ioutil.ReadAll(%s)  error(%v)", body, err)
		return
	}

	if resp.StatusCode == http.StatusOK {
		fmt.Printf("sucess status: %d", resp.StatusCode)
		if err = json.Unmarshal(body, &l); err != nil {
			fmt.Printf("json.Unmarshal(%s)  error(%v) errstring %s", body, err, res.Error)
		}
		fmt.Printf(" data== %v", l)
		return
	}

	if err = json.Unmarshal(body, &res); err != nil {
		fmt.Printf("json.Unmarshal(%s)  error(%v) errstring %s", body, err, res.Error)
	}
	fmt.Printf("error data %v", res)
}

func get_test() {
	var (
		req    *http.Request
		resp   *http.Response
		bucket string
		err    error
		res    error_res
		body   []byte
		uri    string
		l      fstat
		fs     *os.File
	)
	bucket = "efs:test"

	uri = _get_uri
	if req, err = http.NewRequest("POST", uri, nil); err != nil {

		return
	}
	req.Header.Set("Content-Type", "application/json")
	estring := b64.URLEncoding.EncodeToString([]byte(bucket))
	req.Header.Set("ekey", estring)
	req.Header.Set("range", "10240000-20000000")
	if resp, err = _client.Do(req); err != nil {
		fmt.Println("failed:", err)
		return
	}
	defer resp.Body.Close()
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		fmt.Printf("ioutil.ReadAll(%s)  error(%v)", body, err)
		return
	}

	if resp.StatusCode == http.StatusOK {
		fmt.Printf("sucess status: %d", resp.StatusCode)
		if fs, err = os.OpenFile("/root/test_file", os.O_RDWR|os.O_CREATE, 0664); err != nil {
			fmt.Println(err)
		}
		io.Copy(fs, resp.Body)
		fs.Close()
		fmt.Printf(" data== %v", l)
		return
	}

	if err = json.Unmarshal(body, &res); err != nil {
		fmt.Printf("json.Unmarshal(%s)  error(%v) errstring %s", body, err, res.Error)
	}
	fmt.Printf("error data %v", res)
}

type mkblk struct {
	Ctx      string `json:"ctx"`
	Id       string `json:"id"`
	Checksum string `json:"checksum"`
	Crc32    int64  `json:"crc32"`
	Offset   int64  `json:"offset"`
	Host     string `json:host`
}

type re_err struct {
	Code  int    `json:"code"`
	Error string `json:"error"`
}

func mkblk_test(buf []byte, n int) (ctx, id string, offset int64, err error) {
	var (
		req    *http.Request
		resp   *http.Response
		bucket string
		res    re_err
		body   []byte
		uri    string
		mk     mkblk
	)
	bucket = "efs:test"

	uri = _mkblk_uri

	tdata := bytes.NewBuffer(buf)

	if req, err = http.NewRequest("POST", uri, tdata); err != nil {

		return
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	estring := b64.URLEncoding.EncodeToString([]byte(bucket))
	req.Header.Set("ekey", estring)
	req.Header.Set("Content-Length", fmt.Sprintf("%d", n))
	if resp, err = _client.Do(req); err != nil {
		fmt.Println("failed:", err)
		return
	}
	defer resp.Body.Close()
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		fmt.Printf("ioutil.ReadAll(%s)  error(%v)", body, err)
		return
	}

	if resp.StatusCode == http.StatusOK {
		if err = json.Unmarshal(body, &mk); err != nil {
			fmt.Printf("json.Unmarshal(%s)  error(%v) errstring", body, err)
		}
		fmt.Printf("sucess stat: %d data=%v", resp.StatusCode, mk)
		ctx = mk.Ctx
		id = mk.Id
		offset = mk.Offset
		return
	}

	if err = json.Unmarshal(body, &res); err != nil {
		fmt.Printf("json.Unmarshal(%s)  error(%v) errstring %s", body, err, res.Error)
	}
	fmt.Printf("error data %v", res)
	return

}

type bput struct {
	Ctx      string `json:"ctx"`
	Checksum string `json:"checksum"`
	Crc32    int64  `json:"crc32"`
	Offset   int64  `json:"offset"`
	Host     string `json:host`
}

func bput_test(buf []byte, n int, ctx, id string, offset int64) (ctx1 string, offset1 int64, err error) {
	var (
		req    *http.Request
		resp   *http.Response
		bucket string
		res    re_err
		body   []byte
		uri    string
		mk     bput
	)
	bucket = "efs:test"

	uri = _bput_uri

	tdata := bytes.NewBuffer(buf)

	if req, err = http.NewRequest("POST", uri, tdata); err != nil {

		return
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	estring := b64.URLEncoding.EncodeToString([]byte(bucket))
	req.Header.Set("ekey", estring)
	req.Header.Set("Content-Length", fmt.Sprintf("%d", n))
	req.Header.Set("ctx", ctx)
	req.Header.Set("id", id)
	req.Header.Set("offset", fmt.Sprintf("%ld", offset))
	if resp, err = _client.Do(req); err != nil {
		fmt.Println("failed:", err)
		return
	}
	defer resp.Body.Close()
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		fmt.Printf("ioutil.ReadAll(%s)  error(%v)", body, err)
		return
	}

	if resp.StatusCode == http.StatusOK {
		if err = json.Unmarshal(body, &mk); err != nil {
			fmt.Printf("json.Unmarshal(%s)  error(%v) errstring", body, err)
		}
		fmt.Printf("sucess stat: %d data=%v", resp.StatusCode, mk)
		ctx1 = mk.Ctx
		offset1 = mk.Offset
		return
	}

	if err = json.Unmarshal(body, &res); err != nil {
		fmt.Printf("json.Unmarshal(%s)  error(%v) errstring %s", body, err, res.Error)
	}
	fmt.Printf("error data %v", res)
	return
}

type mkfile_req struct {
	Id       string `json:"id"`
	Filesize int64  `json:"filesize"`
	Mime     string `json:"mime"`
	Buf      string `json:"buf"`
}

type mkfile_res struct {
	Hash string `json:"hash"`
	Key  string `json:"key"`
}

func mkfile_test(id, ctxs, mime string, fsize int64) {
	var (
		req     *http.Request
		resp    *http.Response
		bucket  string
		res     re_err
		err     error
		body, b []byte
		uri     string
		mkreq   mkfile_req
		mkres   mkfile_res
	)
	bucket = "efs:test"

	mkreq.Id = id
	mkreq.Filesize = fsize
	mkreq.Mime = mime
	mkreq.Buf = ctxs

	uri = _bput_uri

	b, err = json.Marshal(mkreq)
	if err != nil {
		fmt.Printf("json.marshal() error(%v)", err)
		return
	}

	tdata := bytes.NewBuffer([]byte(b))

	if req, err = http.NewRequest("POST", uri, tdata); err != nil {

		return
	}
	req.Header.Set("Content-Type", "application/json")
	estring := b64.URLEncoding.EncodeToString([]byte(bucket))
	req.Header.Set("ekey", estring)

	if resp, err = _client.Do(req); err != nil {
		fmt.Println("failed:", err)
		return
	}
	defer resp.Body.Close()
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		fmt.Printf("ioutil.ReadAll(%s)  error(%v)", body, err)
		return
	}

	if resp.StatusCode == http.StatusOK {
		if err = json.Unmarshal(body, &mkres); err != nil {
			fmt.Printf("json.Unmarshal(%s)  error(%v) errstring", body, err)
		}
		fmt.Printf("sucess stat: %d data=%v", resp.StatusCode, mkres)

		return
	}

	if err = json.Unmarshal(body, &res); err != nil {
		fmt.Printf("json.Unmarshal(%s)  error(%v) errstring %s", body, err, res.Error)
	}
	fmt.Printf("error data %v", res)
	return

}

func bigput_test() {
	var (
		err error

		fs      *os.File
		send    int64
		n       int
		flag    bool
		id, ctx string
		offset  int64
		ctxs    []string
		buf     []byte
	)
	flag = false

	buf = make([]byte, 4*1024*1024)
	defer fs.Close()
	if fs, err = os.OpenFile("/root/test_file", os.O_RDWR|os.O_CREATE, 0664); err != nil {
		fmt.Println(err)
	}

	for {
		if n, err = fs.ReadAt(buf, send); err != nil {
			if err == io.EOF {
				if !flag {
					fmt.Printf("less 4m direct upload")
					return
				} else {
					ctx, offset, err = bput_test(buf[:n], n, ctx, id, offset)
					if err != nil {
						fmt.Println("bput failed")
						return
					}
					ctxs = append(ctxs, ctx)
				}

				break
			} else {
				fmt.Printf("read file error(%v)", err)
				time.Sleep(time.Second * 1)
				continue
			}
		} else {
			if !flag {
				ctx, id, offset, err = mkblk_test(buf[:n], n)
				if err != nil {
					fmt.Println("mkblk failed ")
					return
				}
				ctxs = append(ctxs, ctx)
				flag = true
			} else {
				ctx, offset, err = bput_test(buf[:n], n, ctx, id, offset)
				if err != nil {
					fmt.Println("bput failed")
					return
				}
				ctxs = append(ctxs, ctx)
			}

		}
		send = send + int64(n) + 1

	}
	send = send + int64(n)
	cs := strings.Join(ctxs, ",")
	mkfile_test(id, cs, "txt", send)

}

func rdel_test() {
	var (
		bucket string
		file   string
		ekey   string
		req    *http.Request
		resp   *http.Response
		data   []byte
		m      map[string]string
		err    error
	)

	bucket = ""
	file = ""
	ekey = b64.URLEncoding.EncodeToString([]byte(bucket + ":" + file))

	if req, err = http.NewRequest("POST", _rdel_uri, nil); err != nil {
		fmt.Println("rdel: new request error, " + err.Error())
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("ekey", ekey)

	if resp, err = _client.Do(req); err != nil {
		fmt.Println("rdel: request error, " + err.Error())
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		fmt.Println("rdel: request success")
		return
	}

	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		fmt.Println("rdel: get body data err, " + err.Error())
		return
	}

	if err = json.Unmarshal(data, &m); err != nil {
		fmt.Println("rdel: json unmarshal error, " + err.Error())
		return
	}

	fmt.Println("rdel: error json,", m)
}

func rbatch_test() {
	var (
		bucketSrc  string
		fileSrc    string
		ekeySrc    string
		bucketDest string
		fileDest   string
		ekeyDest   string
		req        *http.Request
		resp       *http.Response
		data       []byte
		m          map[string]string
		reqM       []map[string]string
		respM      []map[string]interface{}
		err        error
	)

	ekeySrc = b64.URLEncoding.EncodeToString([]byte(bucketSrc + ":" + fileSrc))
	ekeyDest = b64.URLEncoding.EncodeToString([]byte(bucketDest + ":" + fileDest))

	//stat
	m = make(map[string]string)
	bucketSrc = ""
	fileSrc = ""
	ekeySrc = b64.URLEncoding.EncodeToString([]byte(bucketSrc + ":" + fileSrc))
	m["op"] = "stat"
	m["src"] = ekeySrc
	reqM = append(reqM, m)

	//copy
	m = make(map[string]string)
	bucketSrc = ""
	fileSrc = ""
	bucketDest = ""
	fileDest = ""
	ekeySrc = b64.URLEncoding.EncodeToString([]byte(bucketSrc + ":" + fileSrc))
	ekeyDest = b64.URLEncoding.EncodeToString([]byte(bucketDest + ":" + bucketDest))
	m["op"] = "copy"
	m["src"] = ekeySrc
	m["dest"] = ekeyDest
	reqM = append(reqM, m)

	//move
	m = make(map[string]string)
	bucketSrc = ""
	fileSrc = ""
	bucketDest = ""
	fileDest = ""
	ekeySrc = b64.URLEncoding.EncodeToString([]byte(bucketSrc + ":" + fileSrc))
	ekeyDest = b64.URLEncoding.EncodeToString([]byte(bucketDest + ":" + bucketDest))
	m["op"] = "move"
	m["src"] = ekeySrc
	m["dest"] = ekeyDest
	reqM = append(reqM, m)

	//delete
	m = make(map[string]string)
	bucketSrc = ""
	fileSrc = ""
	ekeySrc = b64.URLEncoding.EncodeToString([]byte(bucketSrc + ":" + fileSrc))
	m["op"] = "delete"
	m["src"] = ekeySrc
	reqM = append(reqM, m)

	if data, err = json.Marshal(reqM); err != nil {
		fmt.Println("batch: json marshal error, " + err.Error())
		return
	}
	tbuf := bytes.NewBuffer(data)

	if req, err = http.NewRequest("POST", _batch_uri, tbuf); err != nil {
		fmt.Println("batch: new request error, " + err.Error())
		return
	}

	req.Header.Set("Content-Type", "application/json")

	if resp, err = _client.Do(req); err != nil {
		fmt.Println("batch: request error, " + err.Error())
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		fmt.Println("batch: all request success")
		return
	}

	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		fmt.Println("batch: get body data err, " + err.Error())
		return
	}

	if err = json.Unmarshal(data, &respM); err != nil {
		fmt.Println("batch: json unmarshal error, " + err.Error())
		return
	}

	if resp.StatusCode == 298 {
		fmt.Println("batch: some error")
	} else if resp.StatusCode == 400 {
		fmt.Println("batch: request error")
	} else if resp.StatusCode == 599 {
		fmt.Println("batch: server error")
	} else {
		fmt.Println("batch: other error, ", resp.StatusCode)
	}

	fmt.Println()

	for _, vo := range respM {
		fmt.Println(vo)
	}
}

func rupload_test() {
	var (
		ekey          string
		bucket        string
		file          string
		buf           *bytes.Buffer
		writer        *multipart.Writer
		fileWriter    io.Writer
		req           *http.Request
		resp          *http.Response
		localFile     *os.File
		localFileName string
		contentType   string
		data          []byte
		respM         map[string]string
		err           error
	)

	localFileName = ""
	bucket = ""
	file = ""
	ekey = b64.URLEncoding.EncodeToString([]byte(bucket + ":" + file))

	buf = &bytes.Buffer{}
	writer = multipart.NewWriter(buf)
	if fileWriter, err = writer.CreateFormFile("fileDirect", file); err != nil {
		fmt.Println("upload direct: create fileWriter error", err.Error())
		return
	}

	if localFile, err = os.Open(localFileName); err != nil {
		fmt.Println("upload direct: open local file error", err.Error())
		return
	}
	defer localFile.Close()

	if _, err = io.Copy(fileWriter, localFile); err != nil {
		fmt.Println("upload direct: io copy error", err.Error())
		return
	}
	if err = writer.Close(); err != nil {
		fmt.Println("upload direct: writer close error", err.Error())
		return
	}

	contentType = writer.FormDataContentType()

	if req, err = http.NewRequest("POST", _upload_uri, buf); err != nil {
		fmt.Println("upload direct: http request error, ", err.Error())
		return
	}

	req.Header.Set("Content-Type", contentType)
	req.Header.Set("ekey", ekey)
	if resp, err = _client.Do(req); err != nil {
		fmt.Println("upload direct: request error, ", err.Error())
		return
	}
	defer resp.Body.Close()

	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		fmt.Println("upload direct: get body data err, " + err.Error())
		return
	}

	if err = json.Unmarshal(data, &respM); err != nil {
		fmt.Println("batch: json unmarshal error, " + err.Error())
		return
	}

	fmt.Println("upload direct status code:", resp.StatusCode)
	fmt.Println("upload direct json:", respM)
}

func main() {
	// test bucket create
	bcreate_test()
	bdel_test()
	bget_test()          //ok
	bsetproperty_test()  //ok
	bsetimgsource_test() //ok
	list_test()
	chgm_test() //ok
	copy_test() //ok
	move_test() //ok
	stat_test() //ok
	get_test()
	bigput_test()
	rdel_test()
	rbatch_test()
	rupload_test()

	return
}
