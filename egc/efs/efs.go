package efs

import (
	"bytes"
	"efs/egc/conf"
	"efs/libs/errors"
	"efs/libs/meta"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	log "efs/log/glog"
)

const (
	// api
	// _directoryGetApi    = "http://%s/get"
	// _directoryStatApi   = "http://%s/stat"
	// _directoryUploadApi = "http://%s/upload"
	// _directoryMkblkApi  = "http://%s/mkblk"
	// _directoryBputApi   = "http://%s/bput"
	// _directoryMkfileApi = "http://%s/mkfile"
	// _directoryChgmApi   = "http://%s/chgm"
	// _directoryCopyApi   = "http://%s/copy"
	// _directoryMoveApi   = "http://%s/move"
	// _directoryListApi   = "http://%s/list"
	// _storeGetApi        = "http://%s/get"
	// _storeUploadApi     = "http://%s/upload"
	// _bucketcreatApi         = "http://%s/bcreate"
	// _bucketrenameApi        = "http://%s/brename"
	// _directoryDelApi        = "http://%s/del"
	// _directoryDelTmpApi     = "http://%s/deltmp"
	_storeDelApi                 = "http://%s/del"
	_bucketdeleteApi             = "http://%s/bdestroy"
	_bucketlistApi               = "http://%s/blist"
	_directorDestorylistApi      = "http://%s/destroylist"
	_directorDestoryfileApi      = "http://%s/destroyfile"
	_directoryDeletetimeoutfiles = "http://%s/cleantimeoutfile"
	_directoryDeleteExpire       = "http://%s/destroyexpire"
	_directoryGetneedle          = "http://%s/getneedle"
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
	}
	_client = &http.Client{
		Transport: _transport,
		Timeout:   20 * time.Second,
	}
	// random store node
	_rand = rand.New(rand.NewSource(time.Now().UnixNano()))
)

type Efs struct {
	c *conf.Config
}

func New(c *conf.Config) (b *Efs) {
	b = &Efs{}
	b.c = c
	return
}

// DeleteTmp
func (b *Efs) DeleteTmp(Key, Vid, host string) (err error) {
	var (
		params = url.Values{}
		uri    string
		sRet   meta.StoreRet
	)

	params = url.Values{}
	params.Set("key", Key)
	params.Set("vid", Vid)
	uri = fmt.Sprintf(_storeDelApi, host)
	//send request efs storage
	//mycontinue 2
	if err = Http("POST", uri, params, nil, &sRet); err != nil {
		log.Errorf("Update called Http error(%v)", err)
		return
	}
	if sRet.Ret != 1 {
		log.Errorf("Delete store sRet.Ret: %d  %s", sRet.Ret, uri)
		err = errors.Error(sRet.Ret)
		return
	}
	return
}

func (b *Efs) Getkeyaddr(key int64) (res meta.Response, err error) {
	var (
		params = url.Values{}
	)
	keystring := strconv.FormatInt(key, 10)
	params.Set("key", keystring)
	uri := fmt.Sprintf(_directoryGetneedle, b.c.EfsAddr)
	//request directory get metadata
	// mycontinue 1
	if err = Http("POST", uri, params, nil, &res); err != nil {
		log.Errorf("Efs Get() directory called uri(%s) Http error(%v)", uri, err)
		return
	}
	if res.Ret != errors.RetOK {
		log.Errorf("Efs Get() directory called uri(%s) res.Ret(%d), paramas(%v)", uri, res.Ret, params)
		err = errors.Error(res.Ret)
		return
	}
	return

}

// Ping
func (b *Efs) Ping() error {
	return nil
}

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

// bucket delete
func (b *Efs) BucketDelete(ekey string) (err error) {
	var (
		uri    string
		res    meta.BucketDeleteResponse
		params = url.Values{}
	)
	params.Set("ekey", ekey)
	uri = fmt.Sprintf(_bucketdeleteApi, b.c.EfsAddr)
	//request directory get metadata
	if err = Http("POST", uri, params, nil, &res); err != nil {
		log.Errorf("Efs BucketDelete() called uri(%s) Http error(%v)", uri, err)
		return
	}
	if res.Ret != errors.RetOK {
		log.Errorf("Efs BucketDelete() called uri(%s) directory res.Ret(%d), params(%v)", uri, res.Ret, params)
		err = errors.Error(res.Ret)
		return
	}
	return
}

// bucket list
func (b *Efs) BucketList(regular string) (list []string, err error) {
	var (
		uri    string
		res    meta.BucketListResponse
		params = url.Values{}
	)
	params.Set("regular", regular)
	uri = fmt.Sprintf(_bucketlistApi, b.c.EfsAddr)

	if err = Http("POST", uri, params, nil, &res); err != nil {
		log.Errorf("Efs BucketList() called uri(%s) Http error(%v)", uri, err)
		return
	}
	if res.Ret != errors.RetOK {
		log.Errorf("Efs BucketList() called uri(%s) directory res.Ret(%d), params(%v)", uri, res.Ret, params)
		err = errors.Error(res.Ret)
		return
	}

	list = res.List
	return
}

func (b *Efs) DestoryList(ekey, limit, marker string) (result *meta.GCDestoryListRetOK, err error) {
	var (
		uri    string
		res    meta.DestroyListResponse
		gcres  meta.GCDestoryListRetOK
		params = url.Values{}
	)
	params.Set("ekey", ekey)
	params.Set("limit", limit)
	params.Set("marker", marker)
	uri = fmt.Sprintf(_directorDestorylistApi, b.c.EfsAddr)
	//request directory get metadata
	if err = Http("POST", uri, params, nil, &res); err != nil {
		log.Errorf("Efs DestoryList() called uri(%s) Http error(%s)", uri, err.Error())
		return
	}
	if res.Ret != errors.RetOK {
		log.Errorf("Efs DestoryList() called uri(%s), directory resp no OK res.Ret(%d), params(%v)", uri, res.Ret, params)
		err = errors.Error(res.Ret)
		return
	}

	gcres.Marker = res.Marker
	gcres.DList = res.DList
	gcres.Trash_flag = res.Trash_flag
	result = &gcres
	// just you know, by dylan 20161123
	// for _, v := range res.DList {
	// 	var df *meta.GCDestroyFile
	// 	df.FileName = v.FileName
	// 	for _, vv := range v.FileNeedle {
	// 		var dfn *meta.GCDFileNeedle
	// 		dfn.Key = vv.Key
	// 		dfn.Vid = vv.Vid
	// 		dfn.Stores = vv.Stores
	// 		df.FileNeedle = append(df.FileNeedle, dfn)
	// 	}
	// 	drlist.DList = append(drlist.DList, df)
	// }
	// result = &drlist

	return
}

func (b *Efs) Destoryfile(ekey string) (err error) {
	var (
		params = url.Values{}
		//host   string
		uri string
		res meta.DestroyFileResponse
	//	sRet   meta.StoreRet
	)
	params.Set("ekey", ekey)
	uri = fmt.Sprintf(_directorDestoryfileApi, b.c.EfsAddr)

	if err = Http("POST", uri, params, nil, &res); err != nil {
		log.Errorf("Efs Destoryfile() called uri(%s) directory Http error(%v)", uri, err)
		return
	}
	if res.Ret != errors.RetOK {
		log.Errorf("Efs Destoryfile() called uri(%s), directory res.Ret:(%d), params(%v)", uri, res.Ret, params)
		err = errors.Error(res.Ret)
		return
	}
	return
}

func (b *Efs) DeleteExpire() {
	var (
		err    error
		uri    string
		params = url.Values{}
		res    meta.DestroyExpireResponse
	)
	uri = fmt.Sprintf(_directoryDeleteExpire, b.c.EfsAddr)

	if err = Http("POST", uri, params, nil, &res); err != nil {
		log.Errorf("Efs DeleteExpire() called uri(%s) directory Http error(%v)", uri, err)
		return
	}

	if res.Ret != errors.RetOK {
		log.Errorf("Efs DeleteExpire() called uri(%s), directory res.Ret:(%d)", uri, res.Ret)
		return
	}
	return
}
