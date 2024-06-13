package efs

import (
	"io/ioutil"

	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	log "efs/log/glog"
	b64 "encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
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
	// random store node

)

func Http(method, uri string, params url.Values, header url.Values, buf []byte) (data []byte, retcode int, err error) {
	var (
		bufdata = &bytes.Buffer{}
		req     *http.Request
		resp    *http.Response
		ru      string
		enc     string
	)
	retcode = 400
	enc = params.Encode()
	ru = uri

	if enc != "" {
		ru = uri + "?" + enc
	}

	if method == "GET" || method == "get" {
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
	for key, _ := range header {
		req.Header.Set(key, header.Get(key))
	}
	if resp, err = _client.Do(req); err != nil {
		log.Errorf("_client.Do(%s) error(%v)", ru, err)
		return
	}
	defer resp.Body.Close()
	retcode = resp.StatusCode
	if resp.StatusCode != http.StatusOK {
		log.Errorf("_client.Do(%s) status: %d", ru, resp.StatusCode)
		err = errors.New("get file failed")
		return
	}
	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		log.Errorf("ioutil.ReadAll uri(%s) error(%v)", ru, err)
		return
	}

	return
}

func gettoken(ak, sk, data string) (auth string) {
	mac := hmac.New(sha1.New, []byte(sk))
	mac.Write([]byte(data))
	bk := b64.URLEncoding.EncodeToString(mac.Sum(nil))
	auth = ak + ":" + bk
	return
}

func (s *Src_io) Getrangedata(bucket, filename, frange, ak, sk, dnsname string, public bool) (data []byte, retcode int, err error) {
	var (
		params = url.Values{}
		header = url.Values{}
		geturl string
	)

	//ekey := bucket + ":" + filename
	//estring := b64.URLEncoding.EncodeToString([]byte(ekey))
	//header.Set("ekey", estring)
	header.Set("Range", frange)
	geturl = fmt.Sprintf("http://%s/%s", dnsname, filename)
	if !public {
		e := time.Now().Unix() + 600
		estring := strconv.FormatInt(e, 10)
		tokendata := fmt.Sprintf("%s?e=%s", geturl, estring)
		token := gettoken(ak, sk, tokendata)
		params.Set("e", estring)
		params.Set("token", token)
		//geturl = geturl + "?" + "e=" + estring + "&" + "token=" + token
	}
	geturl = fmt.Sprintf("http://%s:%s/%s", dnsname, s.c.DownloadPort, filename)
	if data, retcode, err = Http("GET", geturl, params, header, nil); err != nil {
		log.Errorf("get bucket %s filename %s failed ", bucket, filename)
		err = errors.New("get file failed")
		return
	}
	return
}

func (s *Src_io) Getdata(bucket, filename, ak, sk, dnsname string, public bool) (data []byte, retcode int, err error) {
	var (
		params = url.Values{}
		header = url.Values{}
		geturl string
	)
	//ekey := bucket + ":" + filename
	//estring := b64.URLEncoding.EncodeToString([]byte(ekey))
	//header.Set("ekey", estring)
	geturl = fmt.Sprintf("http://%s/%s", dnsname, filename)
	if !public {
		e := time.Now().Unix() + 600
		estring := strconv.FormatInt(e, 10)
		tokendata := fmt.Sprintf("%s?e=%s", geturl, estring)
		token := gettoken(ak, sk, tokendata)
		params.Set("e", estring)
		params.Set("token", token)
		//log.Errorf("e=%s&token=%s", estring, token)
		//geturl = geturl + "?" + "e=" + estring + "&" + "token=" + token
	}
	geturl = fmt.Sprintf("http://%s:%s/%s", dnsname, s.c.DownloadPort, filename)
	if data, retcode, err = Http("GET", geturl, params, header, nil); err != nil {
		log.Errorf("get bucket %s filename %s failed ", bucket, filename)
		err = errors.New("get file failed")
		return
	}
	return
}
