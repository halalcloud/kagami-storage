package efs

import (
	"bytes"
	"crypto/sha1"
	b64 "encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"kagamistoreage/authacess/conf"
	"kagamistoreage/authacess/fetch"
	"kagamistoreage/authacess/httpcli"
	"kagamistoreage/authacess/mimetype"
	"kagamistoreage/authacess/multipartupload"
	"kagamistoreage/libs/errors"
	"kagamistoreage/libs/meta"
	log "kagamistoreage/log/glog"
	"math/rand"
	"mime/multipart"
	"os"

	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	KEY_DELIMITER = ":"

	_proxy_mkblk  = "http://%s/r/mkblk"
	_proxy_bput   = "http://%s/r/bput"
	_proxy_mkfile = "http://%s/r/mkfile"
	_proxy_stat   = "http://%s/r/stat"
	_proxy_chgm   = "http://%s/r/chgm"
	_proxy_move   = "http://%s/r/move"
	_proxy_copy   = "http://%s/r/copy"
	_proxy_delete = "http://%s/r/delete"
	_proxy_list   = "http://%s/r/list"
	_proxy_get    = "http://%s/r/get"
	_proxy_batch  = "http://%s/r/batch"

	_storeDelApi            = "http://%s/del"
	_directoryDispatcherApi = "http://%s/dispatcher"
	_storeUploadApi         = "http://%s/upload"
	_directoryUploadApi     = "http://%s/upload"
	_directoryMkblkApi      = "http://%s/mkblk"
	_directoryBputApi       = "http://%s/bput"
	_directoryMkfileApi     = "http://%s/mkfile"
	_directoryGetApi        = "http://%s/get"

	//multipart upload
	_directoryGetPartid = "http://%s/getpartid"

	//manager
	_directoryStatApi   = "http://%s/stat"
	_directoryChgmApi   = "http://%s/chgm"
	_directoryChgexpApi = "http://%s/chgexp"
	_directoryCopyApi   = "http://%s/copy"
	_directoryMoveApi   = "http://%s/move"
	_directoryDelApi    = "http://%s/del"
	_directoryListApi   = "http://%s/list"

	//store
	_storeGetApi = "http://%s/get"

	UPLOAD_STORE_RETRY = 2
	Unknow_File_Type   = "unkonw"
)

var (
	_transport = &http.Transport{
		Dial: func(netw, addr string) (c net.Conn, err error) {
			if c, err = net.DialTimeout(netw, addr, 2*time.Second); err != nil {
				return nil, err
			}
			return c, nil
		},
		//DisableCompression:  true,
		//DisableKeepAlives:   true,
		//MaxIdleConns:        20,
		MaxIdleConnsPerHost: 20,
	}
	_client = &http.Client{
		Transport: _transport,
		Timeout:   2 * time.Second,
	}
	// random store node

)

type Efs struct {
	c         *conf.Config
	multipart *multipartupload.Multipart
}

func New(c *conf.Config, mpart *multipartupload.Multipart) (b *Efs) {
	b = &Efs{}
	b.c = c
	b.multipart = mpart

	return
}

func Httptostore(method, uri string, params url.Values, buf []byte, res interface{}) (err error) {
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

// Http params
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
	/*
		if resp, err = _client.Do(req); err != nil {
			log.Errorf("_client.Do(%s) error(%v)", ru, err)
			return
		}
	*/
	if resp, err = httpcli.HttpReq(req); err != nil {
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

type upload_res struct {
	Hash string `json:"hash"`
	Key  string `json:"key"`
}

type proxy_errres struct {
	Code  int    `json:"code"`
	Error string `json:"error"`
}
type sizer interface {
	Size() int64
}

// checkFileSize get multipart.File size
func checkFileSize(file io.Reader) (size int64, err error) {
	var (
		ok bool
		sr sizer
		fr *os.File
		fi os.FileInfo
	)
	if sr, ok = file.(sizer); ok {
		size = sr.Size()
	} else if fr, ok = file.(*os.File); ok {
		if fi, err = fr.Stat(); err != nil {
			log.Errorf("file.Stat() error(%v)", err)
			return
		}
		size = fi.Size()
	}
	return
}

func (b *Efs) dispatcher(ekey string, overWriteFlag int, lastvid int32, replication int) (resRet *meta.Response, err error) {
	var (
		params = url.Values{}
		uri    string
		res    meta.Response
	)
	resRet = &res

	params.Set("ekey", ekey)
	params.Set("overwrite", strconv.Itoa(overWriteFlag))
	params.Set("lastvid", strconv.Itoa(int(lastvid)))
	params.Set("replication", strconv.Itoa(replication))
	uri = fmt.Sprintf(_directoryDispatcherApi, b.c.EfsAddr)

	if err = Http("POST", uri, params, nil, &res); err != nil {
		log.Errorf("Efs Dispatcher() Post called uri(%s) error(%s)", uri, err)
		return
	}
	if res.Ret != errors.RetOK {
		log.Errorf("Efs Dispatcher() Post called uri(%s), directory res.Ret(%d), params(%v)", uri, res.Ret, params)
		err = errors.Error(res.Ret)
		return
	}
	if res.Vid == 0 || res.Vid < 0 {
		log.Recoverf("dispathcher error vid = %d ********ekey=%s*******", res.Vid, ekey)
	}

	return
}

func sendFileToStore(res *meta.Response, buf *[]byte, host string, ch *chan meta.StoreRet,
	deleteAfterDays int) {
	var (
		params = url.Values{}
		uri    string
		sRet   meta.StoreRet
		err    error
	)

	params.Set("key", strconv.FormatInt((*res).Key, 10))
	params.Set("cookie", strconv.FormatInt(int64((*res).Cookie), 10))
	params.Set("vid", strconv.FormatInt(int64((*res).Vid), 10))
	params.Set("deleteAfterDays", strconv.FormatInt(int64(deleteAfterDays), 10))
	uri = fmt.Sprintf(_storeUploadApi, host)
	if err = Httptostore("POST", uri, params, *buf, &sRet); err != nil {
		log.Errorf("Efs sendFileToStore() http.Post send file to store(%s) request faild(%s),params: %d %d %d %d",
			uri, err.Error(), (*res).Key, (*res).Cookie, (*res).Vid, len(*buf))
		goto fini
	}
	if sRet.Ret != 1 {
		log.Errorf("Efs sendFileToStore() http.Post to store(%s) sRet.Ret(%d),params: %d %d %d %d",
			uri, sRet.Ret, (*res).Key, (*res).Cookie, (*res).Vid, len(*buf))
		err = errors.Error(sRet.Ret)
		goto fini
	}
fini:
	sRet.Host = host
	sRet.Err = err
	*ch <- sRet
	return
}

func getFileToStoreResponse(res *meta.Response, ch *chan meta.StoreRet) (fsRes *meta.StoreRets, err error) {
	var (
		sRet meta.StoreRet
	)
	fsRes = new(meta.StoreRets)
	fsRes.SRets = make([]meta.StoreRet, len(res.Stores))

	for i := 0; i < len(res.Stores); i++ {
		select {
		case sRet = <-*ch:
			if sRet.Ret != 1 {
				err = sRet.Err
			}
			fsRes.SRets[i] = sRet
		}
	}

	if err != nil {
		msg_j, ok := json.Marshal(fsRes)
		if ok != nil {
			log.Errorf("Efs getFileToStoreResponse() json marshal Failed, fsRes(%v)", fsRes)
			return
		}
		log.Errorf("Efs getFileToStoreResponse() no OK: %d, %d, %d, msg: %s", (*res).Key, (*res).Cookie, (*res).Vid, msg_j)
		return
	}

	//log.Infof("getFileToStoreResponse() OK:  %d, %d, %d", (*res).Key, (*res).Cookie, (*res).Vid)
	return
}

func storeFileDel(host string, key int64, cookie int32, vid int32) {
	var (
		params = url.Values{}
		uri    string
		sRet   meta.StoreRet
		err    error
	)

	params.Set("key", strconv.FormatInt(key, 10))
	params.Set("vid", strconv.FormatInt(int64(vid), 10))
	params.Set("cookie", strconv.FormatInt(int64(cookie), 10))
	uri = fmt.Sprintf(_storeDelApi, host)
	if err = Httptostore("POST", uri, params, nil, &sRet); err != nil {
		log.Recoverf("Efs storeFileDel() http.Post (%s) request faild(%s),params: key(%d) cookie(%d) vid(%d)",
			uri, err.Error(), key, cookie, vid)
		return
	}
	if sRet.Ret != 1 {
		log.Recoverf("Efs storeFileDel() http.Post (%s) status faild(%d),params: key(%d) cookie(%d) vid(%d)",
			uri, sRet.Ret, key, cookie, vid)
		return
	}

	return
}

func uploadStoredelclean(storeRets *meta.StoreRets, res *meta.Response) {
	for _, s := range storeRets.SRets {
		//if s.Err == nil {
		storeFileDel(s.Host, res.Key, res.Cookie, res.Vid)
		//}
	}
}

func uploadStore(res *meta.Response, buf []byte, deleteAfterDays int) (storeRets *meta.StoreRets, err error) {
	var (
		host string

		ch chan meta.StoreRet
	)

	ch = make(chan meta.StoreRet)

	for _, host = range res.Stores {
		go sendFileToStore(res, &buf, host, &ch, deleteAfterDays)
	}

	if storeRets, err = getFileToStoreResponse(res, &ch); err != nil {
		uploadStoredelclean(storeRets, res)
	}
	return
}

func (b *Efs) uploadDirectory(ekey, mime, sha1 string, filesize int, nkey int64,
	vid int32, cookie int32, overWriteFlag, deleteAfterDays int) (oFileSize int64, err error) {
	var (
		params = url.Values{}
		uri    string
		res    meta.Response
	)

	params.Set("ekey", ekey)
	params.Set("mime", mime)
	params.Set("sha1", sha1)
	params.Set("filesize", strconv.Itoa(filesize))
	params.Set("nkey", strconv.FormatInt(nkey, 10))
	params.Set("vid", strconv.FormatInt(int64(vid), 10))
	params.Set("cookie", strconv.FormatInt(int64(cookie), 10))
	params.Set("overwrite", strconv.Itoa(overWriteFlag))
	params.Set("deleteAfterDays", strconv.FormatInt(int64(deleteAfterDays), 10))
	uri = fmt.Sprintf(_directoryUploadApi, b.c.EfsAddr)

	if err = Http("POST", uri, params, nil, &res); err != nil {
		log.Recoverf("Efs UploadDirectory() Post called uri(%s), error(%s), params(%v)", uri, err, params)
		return
	}
	if res.Ret != errors.RetOK {
		log.Recoverf("Efs UploadDirectory() Post called uri(%s), directory res.Ret(%d), params(%v)", uri, res.Ret, params)
		err = errors.Error(res.Ret)
		return
	}
	oFileSize, _ = strconv.ParseInt(res.OFSize, 10, 32)
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

// partid is null,not modify key .not null modify key to partid
func (b *Efs) retryuploadtostore(ekey string, overWriteFlag, replication, deleteAfterDays int, body []byte,
	partid string) (retcode int, errstring string, res *meta.Response, storeRets *meta.StoreRets) {
	var (
		lastvid int32
		err     error
	)
	lastvid = -1
	retcode = 200
	for i := 0; i <= UPLOAD_STORE_RETRY; i++ {

		if i == UPLOAD_STORE_RETRY {
			retcode, errstring = errortostring(err)
			log.Errorf("upload store failed,try two times failed")
			return
		}

		if res, err = b.dispatcher(ekey, overWriteFlag, lastvid, replication); err != nil {
			//dispatcher failed
			//retcode, errstring = errortostring(err)
			continue
		}
		if res.Vid == 0 || res.Vid < 0 {
			log.Recoverf("dispathcher error vid = %d ********ekey=%s******* store1 =%s", res.Vid, ekey, res.Stores[0])
		}

		lastvid = res.Vid
		if partid != "" {
			res.Key, err = strconv.ParseInt(partid, 10, 64)
			if err != nil {
				retcode = 599
				errstring = "server failed"
				log.Errorf("parse int partid %s failed %v", partid, err)
				return
			}
		}
		if storeRets, err = uploadStore(res, body, deleteAfterDays); err != nil {
			continue //success
		} else {
			break
		}

	}
	if err != nil {
		retcode, errstring = errortostring(err)
		log.Errorf("retry upload failed")
	}
	return
}

func (b *Efs) sliceupload(bucket, filename, content string, overWriteFlag, replication,
	deleteAfterDays int, body []byte) (hash, key string, retcode int, errstring string,
	oFileSize int64) {
	var (
		data           []byte
		index, readNum int
		err            error
		mkblkres       *Mkblk_res
		ctxlist        string
		filesize       int64
		buff           *bytes.Buffer
	)
	retcode = 200
	buff = bytes.NewBuffer(body)
	data = make([]byte, int(b.c.SliceFileSize))
	filesize = int64(len(body))
	for {
		if readNum, err = io.ReadFull(buff, data); err != nil {
			if err == io.EOF {
				break
			} else if err == io.ErrUnexpectedEOF {
				data = data[:readNum]
			} else {
				retcode = errors.RetServerFailed
				errstring = strconv.Itoa(errors.RetAuthacessIoErr) + ":" + errors.ErrServerFailed.Error()
				return
			}
		}

		mkblkres, retcode, errstring = b.Multipart_mkblk(bucket, filename, content, data,
			int64(len(data)), replication, deleteAfterDays)
		if retcode != 200 {
			return
		}
		if index == 0 {
			ctxlist = mkblkres.Ctx
		} else {
			ctxlist = ctxlist + "," + mkblkres.Ctx
		}
		/*
			ctx = mkblkres.Ctx
			if index == 0 {
				mkblkres, retcode, errstring = b.Mkblk(bucket, filename, content, data, overWriteFlag, replication)
				if retcode != 200 {
					return
				}
				nextchuckoffset = mkblkres.Offset
				ctxlist = mkblkres.Ctx
				ctx = mkblkres.Ctx
			} else {
				if index == 1 {
					ctx = mkblkres.Ctx
					id = mkblkres.Id
					nextchuckoffset = mkblkres.Offset
				} else {
					ctx = bputres.Ctx
					nextchuckoffset = bputres.Offset
				}
				bputres, retcode, errstring = b.Bput(bucket, filename, ctx, id, nextchuckoffset, data, overWriteFlag, replication)
				if retcode != 200 {
					return
				}
				//if last bput ,so do this
				nextchuckoffset = bputres.Offset
				ctxlist = ctxlist + "," + bputres.Ctx
			}
		*/

		index++
	}
	hash, oFileSize, retcode, errstring = b.multiparts_mkfile(bucket, filename,
		content, ctxlist, filesize, overWriteFlag, deleteAfterDays)
	if retcode != 200 {
		log.Errorf("sclice upload faied")
	}
	if filename == "" {
		filename = hash
	}
	key = filename

	return
}

func (b *Efs) directupload(bucket, filename, content string, overWriteFlag, replication,
	deleteAfterDays int, body []byte) (hash, key string, retcode int, errstring string,
	oFileSize int64) {
	var (
		err       error
		storeRets *meta.StoreRets
		res       *meta.Response
	)

	sha := sha1.Sum(body)
	sha1sum := hex.EncodeToString(sha[:])
	if filename == "" {
		filename = sha1sum
	}

	tekey := bucket + ":" + filename
	ekey := b64.URLEncoding.EncodeToString([]byte(tekey))
	retcode, errstring, res, storeRets = b.retryuploadtostore(ekey, overWriteFlag, replication,
		deleteAfterDays, body, "")
	if retcode != 200 {
		return
	}

	if oFileSize, err = b.uploadDirectory(ekey, content, sha1sum, len(body),
		res.Key, res.Vid, res.Cookie, overWriteFlag, deleteAfterDays); err != nil {

		uploadStoredelclean(storeRets, res)
		retcode, errstring = errortostring(err)
		return
	}
	retcode = 200
	hash = sha1sum
	key = filename
	//	log.Errorf("oldfilsize=%d", oFileSize)
	return

}

func (b *Efs) Upload(bucket, filename, content string, repeat_flag, replication,
	deleteAfterDays int, body []byte) (hash, key string,
	retcode int, errstring string, oFileSize, filesize int64) {

	filesize = int64(len(body))

	if filesize > int64(b.c.SliceFileSize) {
		hash, key, retcode, errstring, oFileSize = b.sliceupload(bucket, filename,
			content, repeat_flag, replication, deleteAfterDays, body)
		return
	} else {
		hash, key, retcode, errstring, oFileSize = b.directupload(bucket, filename, content,
			repeat_flag, replication, deleteAfterDays, body)
		return
	}
	return

}

/*
	func (b *Efs) Upload(bucket, filname, content string, repeat_flag int, fpart io.Reader) (hash, key string, retcode int, errstring string) {
		var (
			ekey, uri string
			params    = url.Values{}
			header    = url.Values{}
			code      int
			err       error
			rebody    []byte
			ures      upload_res
			errres    proxy_errres
		)
		retcode = 200
		uri = fmt.Sprintf(_proxy_upload, b.c.ProxyAddr)
		ekey = fmt.Sprintf("%s:%s", bucket, filname)
		b64ekey := b64.URLEncoding.EncodeToString([]byte(ekey))
		header.Set("ekey", b64ekey)
		//	header.Set("Content-Type", content)
		header.Set("overwrite", fmt.Sprintf("%d", repeat_flag))
		//fmt.Println("-------", repeat_flag)
		rebody, err, code = Http_upload("POST", uri, params, header, fpart)
		if err != nil {
			retcode = 401
			errstring = "server failed"
			return
		} else {
			if code == http.StatusOK {
				if err = json.Unmarshal(rebody, &ures); err != nil {
					log.Errorf("json unmarshl failed ")
					retcode = 401
					errstring = "server failed"
					return
				}
				retcode = 200
				hash = ures.Hash
				key = ures.Key
				return
			} else {
				if err = json.Unmarshal(rebody, &errres); err != nil {
					log.Errorf("json unmarshl failed ")
					retcode = 401
					errstring = "server failed"
					return
				}
				retcode = errres.Code
				errstring = errres.Error
				return
			}

		}
		return

}
*/
type Mkblk_res struct {
	Ctx      string `json:"ctx"`
	Checksum string `json:"checksum"`
	Id       string `json:"id"`
	Crc32    int64  `json:"crc32"`
	Offset   int64  `json:"offset"`
	Host     string `json:"host"`
}

type Bput_res struct {
	Ctx      string `json:"ctx"`
	Checksum string `json:"checksum"`
	Crc32    int64  `json:"crc32"`
	Offset   int64  `json:"offset"`
	Host     string `json:"host"`
}

type mkfile_req struct {
	Id       string `json:"id"`
	Filesize int64  `json:"filesize"`
	Mime     string `json:"mime"`
	Buf      string `json:"buf"`
}

func (b *Efs) mkblktodirectory(ekey, mime, sha1 string, filesize int, nkey int64,
	vid int32, cookie int32) (ret *meta.PMkblkRetOK, err error) {
	var (
		params = url.Values{}
		uri    string
		res    meta.Response
	)
	ret = new(meta.PMkblkRetOK)

	params.Set("ekey", ekey)
	params.Set("mime", mime)
	params.Set("sha1", sha1)
	params.Set("filesize", strconv.Itoa(filesize))
	params.Set("nkey", strconv.FormatInt(nkey, 10))
	params.Set("vid", strconv.FormatInt(int64(vid), 10))
	params.Set("cookie", strconv.FormatInt(int64(cookie), 10))

	uri = fmt.Sprintf(_directoryMkblkApi, b.c.EfsAddr)

	if err = Http("POST", uri, params, nil, &res); err != nil {
		log.Errorf("Efs Mkblk() called uri(%s) Http error(%v)", uri, err)
		return
	}
	if res.Ret != errors.RetOK {
		log.Errorf("Efs Mkblk() http.Post called uri(%s) directory res.Ret(%d), params(%v)", uri, res.Ret, params)
		err = errors.Error(res.Ret)
		return
	}

	ret.Ctx = strconv.FormatInt(int64(res.Key), 10)
	ret.Id = ret.Ctx
	ret.Offset = res.Offset

	return
}

/*
func (b *Efs) Mkblk(bucket, filename, mime string, body []byte, overWriteFlag, replication int) (resbody *Mkblk_res, retcode int, errstring string) {
	var (
		err       error
		storeRets *meta.StoreRets
		res       *meta.Response
		respOK    *meta.PMkblkRetOK
		ures      Mkblk_res
	)

	tekey := bucket + ":" + filename
	ekey := b64.URLEncoding.EncodeToString([]byte(tekey))
	retcode, errstring, res, storeRets = b.retryuploadtostore(ekey, overWriteFlag,
		replication, body, "")
	if retcode != 200 {
		return
	}

	sha := sha1.Sum(body)
	sha1sum := hex.EncodeToString(sha[:])

	if respOK, err = b.mkblktodirectory(ekey, mime, sha1sum, len(body),
		res.Key, res.Vid, res.Cookie); err != nil {

		uploadStoredelclean(storeRets, res)
		retcode, errstring = errortostring(err)
		return
	}

	ures.Ctx = respOK.Ctx
	ures.Host = ""
	ures.Crc32 = int64(crc32.ChecksumIEEE(body))
	ures.Checksum = fmt.Sprintf("%d", respOK.Crc32)
	ures.Id = respOK.Id
	ures.Offset = respOK.Offset

	retcode = 200
	resbody = &ures

	return
}
*/

func (b *Efs) Get_partid() (partid string, err error) {
	var (
		params = url.Values{}
		uri    string
		res    meta.Response
	)

	uri = fmt.Sprintf(_directoryGetPartid, b.c.EfsAddr)

	if err = Http("POST", uri, params, nil, &res); err != nil {
		log.Errorf("Efs get partid Post called uri(%s) error(%s)", uri, err)
		return
	}
	if res.Ret != errors.RetOK {
		log.Errorf("Efs Get_partid() Post called uri(%s), directory res.Ret(%d), params(%v)", uri, res.Ret, params)
		err = errors.Error(res.Ret)
		return
	}
	partid = strconv.FormatInt(res.Key, 10)

	return
}

func (b *Efs) upload_object_to_directory(ekey, mime, sha1 string, filesize int, nkey int64,
	vid int32, cookie int32, deleteAfterDays int) (err error) {
	var (
		params = url.Values{}
		uri    string
		res    meta.Response
	)

	params.Set("ekey", ekey)
	params.Set("mime", mime)
	params.Set("sha1", sha1)
	params.Set("filesize", strconv.Itoa(filesize))
	params.Set("nkey", strconv.FormatInt(nkey, 10))
	params.Set("vid", strconv.FormatInt(int64(vid), 10))
	params.Set("cookie", strconv.FormatInt(int64(cookie), 10))
	params.Set("deleteAfterDays", strconv.FormatInt(int64(deleteAfterDays), 10))

	uri = fmt.Sprintf(_directoryMkblkApi, b.c.EfsAddr)

	if err = Http("POST", uri, params, nil, &res); err != nil {
		log.Errorf("Efs Mkblk() called uri(%s) Http error(%v)", uri, err)
		return
	}
	if res.Ret != errors.RetOK {
		log.Errorf("Efs Mkblk() http.Post called uri(%s) directory res.Ret(%d), params(%v)", uri, res.Ret, params)
		err = errors.Error(res.Ret)
		return
	}
	return
}

func (b *Efs) upload_objectpart(bucket, partid string, data []byte, replication,
	deleteAfterDays int) (retcode int, errstring string) {
	var (
		ekey      string
		storeRets *meta.StoreRets
		res       *meta.Response
		err       error
	)

	tekey := bucket + ":" + partid
	ekey = b64.URLEncoding.EncodeToString([]byte(tekey))
	retcode, errstring, res, storeRets = b.retryuploadtostore(ekey, 0, replication, deleteAfterDays, data,
		partid)
	if retcode != 200 {
		log.Errorf("upload block data to store failed")
		return
	}

	sha := sha1.Sum(data)
	sha1sum := hex.EncodeToString(sha[:])
	if err = b.upload_object_to_directory(ekey, "", sha1sum, len(data),
		res.Key, res.Vid, res.Cookie, deleteAfterDays); err != nil {

		uploadStoredelclean(storeRets, res)
		retcode, errstring = errortostring(err)
		return
	}
	retcode = 200
	return

}

func (b *Efs) Multipart_mkblk(bucket, filename, mime string, body []byte,
	blocksize int64, replication, deleteAfterDays int) (resbody *Mkblk_res, retcode int, errstring string) {
	var (
		partid  string
		err     error
		ures    Mkblk_res
		datalen int
	)
	partid, err = b.Get_partid() //PART TODO
	if err != nil {
		log.Errorf("get partid failed %v", err)
		retcode = 599
		errstring = "serverfailed"
		return
	}

	datalen = len(body)
	if datalen == int(blocksize) {
		//log.Errorf("datalen %d blocksize %d ", datalen, blocksize)
		retcode, errstring = b.upload_objectpart(bucket, partid, body, replication,
			deleteAfterDays)
		if retcode != 200 {
			log.Errorf("upload object partid failed %v", err)
			return
		}

	} else {
		err = b.multipart.Regist_partid(partid, blocksize, body)
		if err != nil {
			log.Errorf("regist partid failed %v", err)
			retcode = 599
			errstring = "serverfailed"
			return
		}
	}

	ures.Ctx = partid
	ures.Host = b.c.Hostname
	ures.Crc32 = int64(crc32.ChecksumIEEE(body))
	ures.Offset = int64(datalen)
	sha := sha1.Sum(body)
	sha1sum := hex.EncodeToString(sha[:])
	ures.Checksum = sha1sum

	retcode = 200
	resbody = &ures
	return
}

func (b *Efs) Multipart_bput(bucket, filename, ctx string, offset int64, body []byte,
	replication, deleteAfterDays int) (resbody *Bput_res, retcode int, errstring string) {
	var (
		err                  error
		resoffset, blocksize int64
		data                 []byte
		partid               string
		ures                 Bput_res
	)
	partid = ctx
	resoffset, blocksize, err = b.multipart.Put_partid(partid, offset, body) //TODO partid not exsit err
	if err != nil {
		log.Errorf("put data partid failed %v", err)
		retcode, errstring = errortostring(err)
		return
	}
	if blocksize == 0 {
		blocksize = b.c.SliceFileSize
	}
	if resoffset == blocksize {
		data, err = b.multipart.Getdata_partid(partid)
		if err != nil {
			log.Errorf("read partid %s failed %v", partid, err)
			retcode = 599
			errstring = "server failed"
			return
		}
		retcode, errstring = b.upload_objectpart(bucket, partid, data, replication,
			deleteAfterDays) //PART TODO
		if retcode != 200 {
			b.multipart.Partid_back_offset(partid, int64(len(body)))
			log.Errorf("upload object partid failed %v", err)
			return
		}
		err = b.multipart.Destory_partid(partid)
		if err != nil {
			log.Recoverf("destory partid file %s failed %v", partid)
		}
	}
	ures.Ctx = partid
	ures.Host = b.c.Hostname
	ures.Crc32 = int64(crc32.ChecksumIEEE(body))
	ures.Offset = resoffset
	sha := sha1.Sum(body)
	sha1sum := hex.EncodeToString(sha[:])
	ures.Checksum = sha1sum

	retcode = 200
	resbody = &ures
	return
}

func (b *Efs) multiparts_mkfile(bucket, filename, mime, partids string, filesize int64,
	overWriteFlag, deleteAfterDays int) (filemd5 string, oldfilsize int64, retcode int, errstring string) {
	var (
		respOK meta.PMkfileRetOK
		err    error
	)
	retcode = 200
	if respOK, oldfilsize, err = b.mkfiletodirectory(bucket, filename, overWriteFlag,
		filesize, mime, partids, deleteAfterDays); err != nil {

		retcode, errstring = errortostring(err)
		return
	}
	filemd5 = respOK.Hash
	return

}

func (b *Efs) callbak_mkfile(bucket, filename, mime, partids string, filesize int64,
	overWriteFlag, deleteAfterDays int, callbakurl string) (retcode int, errstring string) {
	var (
		//	respOK meta.PMkfileRetOK
		err error
	)
	retcode = 200

	if err = b.mkfiletocallbak(bucket, filename, overWriteFlag,
		filesize, mime, partids, deleteAfterDays, callbakurl); err != nil {

		retcode, errstring = errortostring(err)
		return
	}
	return

}

func (b *Efs) Multipart_mkfile(bucket, filename, mime, ctxs string, filesize int64,
	overWriteFlag, replication, deleteAfterDays int, callbakurl string) (hash, key string, retcode int, errstring string, oFileSize int64, len_ctxs int) {
	var (
		err        error
		partids    []string
		lastctx    int
		lastpartid string
		data       []byte
		flen       int64
		filemd5    string
		ok         bool
	)

	partids = strings.Split(ctxs, ",")
	len_ctxs = len(partids)
	lastctx = len_ctxs - 1
	lastpartid = partids[lastctx]
	ok = b.multipart.Is_exsit_partid(lastpartid)
	if ok {
		//log.Errorf("mkfile last ok")
		flen = int64(lastctx) * b.c.SliceFileSize

		data, err = b.multipart.Getdata_partid(lastpartid)
		if err != nil {
			log.Errorf("read partid %s failed %v", lastpartid, err)
			retcode = 599
			errstring = "server failed"
			return
		}

		flen += int64(len(data))
		//	log.Errorf("last data len(%d) req len %d blen %d", len(data), filesize, flen)
		if flen != filesize {
			retcode = 400
			errstring = "filesize is not match"
			return
		}
		retcode, errstring = b.upload_objectpart(bucket, lastpartid, data, replication, deleteAfterDays) //PART TODO
		if retcode != 200 {
			log.Errorf("upload object partid failed %v", err)
			return
		}
		err = b.multipart.Destory_partid(lastpartid)
		if err != nil {
			log.Recoverf("destory partid file %s failed %v", lastpartid)
		}
	}
	//log.Errorf("ctx len %d max ctx len %d", len_ctxs, b.c.Maxctxs)
	if len_ctxs > b.c.Maxctxs {
		retcode, errstring = b.callbak_mkfile(bucket, filename, mime,
			ctxs, filesize, overWriteFlag, deleteAfterDays, callbakurl)
		if retcode != 200 {
			log.Errorf("mkfile failed retcode %d err %s", retcode, errstring)
			return
		}
	} else {
		filemd5, oFileSize, retcode, errstring = b.multiparts_mkfile(bucket, filename, mime,
			ctxs, filesize, overWriteFlag, deleteAfterDays) //TODO
		if retcode != 200 {
			log.Errorf("mkfile failed retcode %d err %s", retcode, errstring)
			return
		}
	}
	if filename == "" {
		filename = filemd5
	}

	hash = filemd5
	key = filename
	retcode = 200
	return

}

/*
func (b *Efs) Mkblk(bucket, filname string, body []byte) (resbody *Mkblk_res, retcode int, errstring string) {
	var (
		err    error
		ures   Mkblk_res
		header = url.Values{}
		params = url.Values{}
		rebody []byte
		code   int

		errres proxy_errres
	)
	uri := fmt.Sprintf(_proxy_mkblk, b.c.ProxyAddr)
	ekey := fmt.Sprintf("%s:%s", bucket, filname)
	b64ekey := b64.URLEncoding.EncodeToString([]byte(ekey))
	header.Set("ekey", b64ekey)
	header.Set("Content-Type", "application/octet-stream")
	//header.Set("overwrite", fmt.Sprintf("%s", repeat_flag))
	rebody, err, code = Http_binary("POST", uri, params, header, body)
	if err != nil {
		retcode = 401
		errstring = "server failed"
		return
	} else {
		if code == http.StatusOK {
			if err = json.Unmarshal(rebody, &ures); err != nil {
				log.Errorf("json unmarshl failed ")
				retcode = 401
				errstring = "server failed"
				return
			}
			retcode = 200
			resbody = &ures
			return
		} else {
			if err = json.Unmarshal(rebody, &errres); err != nil {
				log.Errorf("json unmarshl failed ")
				retcode = 401
				errstring = "server failed"
				return
			}
			retcode = errres.Code
			errstring = errres.Error
			return
		}

	}
	return
}
*/

func (b *Efs) bputtodirectory(ekey, ctx string, id string, offset int64, mime string, sha1 string,
	filesize int64, nkey int64, vid int32, cookie int32) (ret meta.PBputRetOK, err error) {
	var (
		params = url.Values{}
		uri    string
		res    meta.Response
	)
	//params is map
	params.Set("ekey", ekey)
	params.Set("mime", mime)
	params.Set("sha1", sha1)
	params.Set("ctx", ctx)
	params.Set("id", id)
	params.Set("offset", strconv.FormatInt(int64(offset), 10))
	params.Set("filesize", strconv.FormatInt(int64(filesize), 10))
	params.Set("nkey", strconv.FormatInt(nkey, 10))
	params.Set("vid", strconv.FormatInt(int64(vid), 10))
	params.Set("cookie", strconv.FormatInt(int64(cookie), 10))
	uri = fmt.Sprintf(_directoryBputApi, b.c.EfsAddr)

	if err = Http("POST", uri, params, nil, &res); err != nil {
		log.Errorf("Efs bput() called uri(%s) directory error(%v)", uri, err)
		return
	}
	if res.Ret != errors.RetOK {
		log.Errorf("Efs bput() called uri(%s) directory res.Ret(%d), params(%v)", uri, res.Ret, params)
		err = errors.Error(res.Ret)
		return
	}

	ret.Ctx = strconv.FormatInt(int64(res.Key), 10)
	ret.Offset = res.Offset
	return
}

/*
	func (b *Efs) Bput(bucket, filename, ctx, id string, offset int64, body []byte, overWriteFlag, replication int) (resbody *Bput_res, retcode int, errstring string) {
		var (
			err       error
			storeRets *meta.StoreRets
			res       *meta.Response
			respOK    meta.PBputRetOK
			ures      Bput_res
			filesize  int
		)

		tekey := bucket + ":" + filename
		ekey := b64.URLEncoding.EncodeToString([]byte(tekey))
		retcode, errstring, res, storeRets = b.retryuploadtostore(ekey, overWriteFlag, replication, body, "")
		if retcode != 200 {
			return
		}

		sha := sha1.Sum(body)
		sha1sum := hex.EncodeToString(sha[:])

		mime := "jpg"
		filesize = len(body)
		if respOK, err = b.bputtodirectory(ekey, ctx, id, offset, mime, sha1sum,
			int64(filesize), res.Key, res.Vid, res.Cookie); err != nil {

			uploadStoredelclean(storeRets, res)
			retcode, errstring = errortostring(err)
			return
		}

		ures.Ctx = respOK.Ctx
		ures.Host = ""
		ures.Crc32 = int64(crc32.ChecksumIEEE(body))
		ures.Checksum = fmt.Sprintf("%d", respOK.Crc32)
		ures.Offset = respOK.Offset

		retcode = 200
		resbody = &ures

		return
	}

	func (b *Efs) Bput(bucket, filname, ctx, id string, offset int64, body []byte) (resbody *Bput_res, retcode int, errstring string) {
		var (
			err    error
			ures   Bput_res
			header = url.Values{}
			params = url.Values{}
			rebody []byte
			code   int

			errres proxy_errres
		)
		uri := fmt.Sprintf(_proxy_bput, b.c.ProxyAddr)
		ekey := fmt.Sprintf("%s:%s", bucket, filname)
		b64ekey := b64.URLEncoding.EncodeToString([]byte(ekey))
		header.Set("ekey", b64ekey)
		header.Set("Content-Type", "application/octet-stream")
		//header.Set("overwrite", fmt.Sprintf("%s", repeat_flag))
		header.Set("ctx", ctx)
		header.Set("id", id)
		header.Set("offset", strconv.FormatInt(offset, 10))
		rebody, err, code = Http_binary("POST", uri, params, header, body)
		if err != nil {
			retcode = 401
			errstring = "server failed"
			return
		} else {
			if code == http.StatusOK {
				if err = json.Unmarshal(rebody, &ures); err != nil {
					log.Errorf("json unmarshl failed ")
					retcode = 401
					errstring = "server failed"
					return
				}
				retcode = 200
				resbody = &ures
				return
			} else {
				if err = json.Unmarshal(rebody, &errres); err != nil {
					log.Errorf("json unmarshl failed ")
					retcode = 401
					errstring = "server failed"
					return
				}
				retcode = errres.Code
				errstring = errres.Error
				return
			}

		}
		return
	}
*/
func (b *Efs) mkfiletodirectory(bucket, filename string, overWriteFlag int,
	filesize int64, mime string, buf string, deleteAfterDays int) (ret meta.PMkfileRetOK, oFileSize int64, err error) {
	var (
		params = url.Values{}
		uri    string
		res    meta.Response
	)

	tekey := bucket + ":" + filename
	ekey := b64.URLEncoding.EncodeToString([]byte(tekey))

	//params is map
	params.Set("ekey", ekey)
	params.Set("mime", mime)
	//params.Set("id", id)
	params.Set("filesize", strconv.FormatInt(int64(filesize), 10))
	params.Set("body", buf)
	params.Set("overwrite", strconv.Itoa(overWriteFlag))
	params.Set("deleteAfterDays", strconv.Itoa(deleteAfterDays))

	//efsaddr is directory host and port
	uri = fmt.Sprintf(_directoryMkfileApi, b.c.EfsAddr)
	//get metadata from directory
	//mycontinue 1
	if err = Http("POST", uri, params, nil, &res); err != nil {
		log.Errorf("Efs Mkfile() http.Post called uri(%s) directory error(%v)", uri, err)
		return
	}
	if res.Ret != errors.RetOK {
		log.Errorf("Efs Mkfile() http.Post called uri(%s) directory res.Ret(%d), params(%v)", uri, res.Ret, params)
		err = errors.Error(res.Ret)
		return
	}

	ret.Hash = res.Sha1
	if filename == "" {
		ret.Key = res.Sha1
	} else {
		ret.Key = filename
	}

	oFileSize, _ = strconv.ParseInt(res.OFSize, 10, 32)

	//log.Infof("efs.mkfile bucket:%s filename:%s key:%d cookie:%d vid:%d", bucket, filename, res.Key, res.Cookie, res.Vid)
	return
}

func (b *Efs) mkfiletocallbak(bucket, filename string, overWriteFlag int,
	filesize int64, mime string, buf string, deleteAfterDays int,
	callbakurl string) (err error) {
	var (
		params = url.Values{}
		uri    string
		res    meta.Response
	)

	tekey := bucket + ":" + filename
	ekey := b64.URLEncoding.EncodeToString([]byte(tekey))

	//params is map
	params.Set("ekey", ekey)
	params.Set("mime", mime)
	params.Set("filesize", strconv.FormatInt(int64(filesize), 10))
	params.Set("callbakurl", callbakurl)
	params.Set("overwrite", strconv.Itoa(overWriteFlag))
	params.Set("deleteAfterDays", strconv.Itoa(deleteAfterDays))

	//efsaddr is directory host and port
	uri = fmt.Sprintf(_directoryMkfileApi, b.c.CallbakAddr)
	//get metadata from directory
	//mycontinue 1
	if err = Http("POST", uri, params, []byte(buf), &res); err != nil {
		log.Errorf("Efs Mkfile() http.Post called uri(%s) directory error(%v)", uri, err)
		return
	}
	if res.Ret != errors.RetOK {
		log.Errorf("Efs Mkfile() http.Post called uri(%s) directory res.Ret(%d), params(%v)", uri, res.Ret, params)
		err = errors.Error(res.Ret)
		return
	}

	//log.Infof("efs.mkfile bucket:%s filename:%s key:%d cookie:%d vid:%d", bucket, filename, res.Key, res.Cookie, res.Vid)
	return
}

/*
	func (b *Efs) Mkfile(bucket, filename, id, mime, buf string, filesize int64, overWriteFlag int) (hash, key string, retcode int, errstring string, oFileSize int64) {
		var (
			err    error
			respOK meta.PMkfileRetOK
		)

		//fpart.Close()

		if respOK, oFileSize, err = b.mkfiletodirectory(bucket, filename, overWriteFlag, filesize, mime, buf); err != nil {

			retcode, errstring = errortostring(err)
			return
		}

		hash = respOK.Hash
		key = respOK.Key

		retcode = 200
		//	log.Errorf("oldfilsize=%d", oFileSize)
		return
	}

	func (b *Efs) Mkfile(bucket, filname, id, mime, buf string, filesize int64, repeat_flag int) (hash, key string, retcode int, errstring string) {
		var (
			err           error
			ures          upload_res
			header        = url.Values{}
			params        = url.Values{}
			rebody, rbody []byte
			code          int
			req           mkfile_req
			errres        proxy_errres
		)
		uri := fmt.Sprintf(_proxy_mkfile, b.c.ProxyAddr)
		ekey := fmt.Sprintf("%s:%s", bucket, filname)
		b64ekey := b64.URLEncoding.EncodeToString([]byte(ekey))
		header.Set("ekey", b64ekey)
		header.Set("Content-Type", "application/octet-stream")
		header.Set("overwrite", fmt.Sprintf("%d", repeat_flag))
		req.Id = id
		req.Mime = mime
		req.Filesize = filesize
		req.Buf = buf
		rbody, err = json.Marshal(req)
		if err != nil {
			log.Errorf("call proxy mkfile body marshal json error(%v)", err)
			retcode = 401
			errstring = "server failed"
			return
		}
		rebody, err, code = Http_binary("POST", uri, params, header, rbody)
		if err != nil {
			retcode = 401
			errstring = "server failed"
			return
		} else {
			if code == http.StatusOK {
				if err = json.Unmarshal(rebody, &ures); err != nil {
					log.Errorf("json unmarshl failed ")
					retcode = 401
					errstring = "server failed"
					return
				}
				hash = ures.Hash
				key = ures.Key
				retcode = 200
				return
			} else {
				if err = json.Unmarshal(rebody, &errres); err != nil {
					log.Errorf("json unmarshl failed ")
					retcode = 401
					errstring = "server failed"
					return
				}
				retcode = errres.Code
				errstring = errres.Error
				return
			}

		}
		return
	}
*/
func (b *Efs) FetchUpload(bucket, filename, fetchUrl string, size int64, replication int) (hash, key, mimeType string,
	code int, errMsg string, oFileSize int64) {
	if size <= int64(b.c.MaxFileSize) {
		hash, key, mimeType, code, errMsg, oFileSize = b.fetchDirectly(bucket, filename, fetchUrl, size, replication)
	} else {
		hash, key, mimeType, code, errMsg, oFileSize = b.fetchSlice(bucket, filename, fetchUrl, size, replication)
	}

	return
}

func (b *Efs) fetchDirectly(bucket, filename, fetchUrl string, size int64, replication int) (hash, key, mimeType string,
	code int, errMsg string, oFileSize int64) {
	var (
		data, typehead []byte
		//bufdata = &bytes.Buffer{}
		endstr string
		err    error
	)
	endstr = strconv.FormatInt(size, 10)
	if data, mimeType, err = fetch.FileData(fetchUrl, "bytes=0"+"-"+endstr); err != nil {
		log.Errorf("FetchUpload FileData(%s) error(%s)", fetchUrl, err.Error())
		code, errMsg = http.StatusInternalServerError, "fetch file data err"
		return
	}
	fsize := int64(len(data))
	if fsize > 10 {
		typehead = data[:10]
	}
	if mimeType == "" {
		mimeType = mimetype.Check_uploadfile_type(filename, filename,
			typehead, Unknow_File_Type)
	}

	hash, key, code, errMsg, oFileSize = b.directupload(bucket, filename, mimeType, 1, replication, 0, data)

	return
}

func (b *Efs) fetchSlice(bucket, filename, fetchUrl string, size int64, replication int) (hash, key, mimeType string,
	code int, errMsg string, oFileSize int64) {
	var (
		data, typehead   []byte
		err              error
		SliceFileSize    int64
		sliceNum         int64
		index            int64
		start, end       int64
		startStr, endStr string
		mkblkRet         *Mkblk_res
		ctx, ctxs        string
	)

	SliceFileSize = int64(b.c.SliceFileSize)
	sliceNum = size / SliceFileSize
	if !((size % SliceFileSize) == 0) {
		sliceNum++
	}

	for index = 1; index <= sliceNum; index++ {
		if index == sliceNum {
			start, end = (index-1)*SliceFileSize, size-1
		} else {
			start, end = (index-1)*SliceFileSize, index*SliceFileSize-1
		}
		startStr = strconv.FormatInt(start, 10)
		endStr = strconv.FormatInt(end, 10)
		if data, mimeType, err = fetch.FileData(fetchUrl, "bytes="+startStr+"-"+endStr); err != nil {
			log.Errorf("FetchUpload FileData(%s) error(%s)", fetchUrl, err.Error())
			code, errMsg = http.StatusInternalServerError, "fetch file data err"
			return
		}
		mkblkRet, code, errMsg = b.Multipart_mkblk(bucket, filename,
			mimeType, data, int64(len(data)), replication, 0)
		if code != http.StatusOK {
			return
		}

		ctx = mkblkRet.Ctx
		if index == sliceNum {
			ctxs = ctxs + ctx
		} else {
			ctxs += ctx + ","
		}

	}

	if mimeType == "" {
		mimeType = mimetype.Check_uploadfile_type(filename, filename,
			typehead, Unknow_File_Type)
	}
	log.Errorf("ctxs %s len %d", ctxs, sliceNum)
	hash, oFileSize, code, errMsg = b.multiparts_mkfile(bucket, filename, mimeType, ctxs,
		size, 1, 0)
	//hash, key, code, errMsg, oFileSize = b.Mkfile(bucket, filename, id, mimeType, ctxs, offset, 0)
	if filename == "" {
		key = hash
	} else {
		key = filename
	}
	return
}

/*
//be delete
func (b *Efs) fetchSlice_del(bucket, filename, fetchUrl string, size int64) (hash, key, mimeType string, code int, errMsg string) {
	var (
		data             []byte
		err              error
		SliceFileSize    int64
		sliceNum         int64
		index            int64
		start, end       int64
		startStr, endStr string
		mkblkRet         *Mkblk_res
		bputRet          *Bput_res
		ctx, ctxs, id    string
		offset           int64
	)
	//overwriteflag ===??? TODO
	overWriteFlag := 0
	SliceFileSize = int64(b.c.SliceFileSize)
	sliceNum = size / SliceFileSize
	if !((size % SliceFileSize) == 0) {
		sliceNum++
	}

	for index = 1; index <= sliceNum; index++ {
		if index == 1 {
			start, end = (index-1)*SliceFileSize, index*SliceFileSize-1
			startStr = strconv.FormatInt(start, 10)
			endStr = strconv.FormatInt(end, 10)
			if data, mimeType, err = fetch.FileData(fetchUrl, "bytes="+startStr+"-"+endStr); err != nil {
				log.Errorf("FetchUpload FileData(%s) error(%s)", fetchUrl, err.Error())
				code, errMsg = http.StatusInternalServerError, "fetch file data err"
				return
			}
			if mkblkRet, code, errMsg = b.Mkblk(bucket, filename, data, overWriteFlag); code != http.StatusOK {
				return
			}
			ctx = mkblkRet.Ctx
			id = mkblkRet.Id
			offset = mkblkRet.Offset
			ctxs += ctx + ","
		} else if index != sliceNum {
			start, end = (index-1)*SliceFileSize, index*SliceFileSize-1
			startStr = strconv.FormatInt(start, 10)
			endStr = strconv.FormatInt(end, 10)
			if data, _, err = fetch.FileData(fetchUrl, "bytes="+startStr+"-"+endStr); err != nil {
				log.Errorf("FetchSlice FileData(%s) error(%s)", fetchUrl, err.Error())
				code, errMsg = http.StatusInternalServerError, "fetch file data err"
				return
			}
			if bputRet, code, errMsg = b.Bput(bucket, filename, ctx, id, offset, data, overWriteFlag); code != http.StatusOK {
				return
			}
			ctx = bputRet.Ctx
			offset = bputRet.Offset
			ctxs += ctx + ","
		} else {
			start, end = (index-1)*SliceFileSize, size-1
			startStr = strconv.FormatInt(start, 10)
			endStr = strconv.FormatInt(end, 10)
			if data, _, err = fetch.FileData(fetchUrl, "bytes="+startStr+"-"+endStr); err != nil {
				log.Errorf("FetchSlice FileData(%s) error(%s)", fetchUrl, err.Error())
				code, errMsg = http.StatusInternalServerError, "fetch file data err"
				return
			}
			if bputRet, code, errMsg = b.Bput(bucket, filename, ctx, id, offset, data, overWriteFlag); code != http.StatusOK {
				return
			}
			ctx = bputRet.Ctx
			offset = bputRet.Offset
			ctxs += ctx
		}
	}

	if hash, key, code, errMsg, _ = b.Mkfile(bucket, filename, id, mimeType, ctxs, offset, 0); code != http.StatusOK {
		return
	}

	return
}
*/

func (b *Efs) DeleteAfterDays(bucket, filename string, days int) (code int, errMsg string) {
	var (
		urlStr string
		data   []byte
		res    meta.Response
		err    error
		params = url.Values{}
		req    *http.Request
		resp   *http.Response
	)
	code = http.StatusOK
	urlStr = fmt.Sprintf(_directoryChgexpApi, b.c.EfsAddr)
	params.Set("ekey", b.makeEkey(bucket, filename))
	params.Set("expire", strconv.FormatInt(int64(days*24*3600)+time.Now().Unix(), 10))
	if req, err = http.NewRequest("POST", urlStr, strings.NewReader(params.Encode())); err != nil {
		code, errMsg = http.StatusInternalServerError, "make req error"
		return
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	if resp, err = httpcli.HttpReq(req); err != nil {
		log.Errorf("deleteafterdays http req error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "http req error"
		return
	}

	if resp.StatusCode != http.StatusOK {
		log.Errorf("deleteafterdays http resp code(%d) no ok", resp.StatusCode)
		code, errMsg = resp.StatusCode, "http resp code no ok"
		return
	}
	defer resp.Body.Close()

	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		log.Errorf("deleteafterdays read data error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "read data error"
		return
	}

	if err = json.Unmarshal(data, &res); err != nil {
		log.Errorf("json decode data error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "data json decode error"
		return
	}

	if res.Ret != errors.RetOK {
		log.Errorf("resp res.Ret(%d) no ok", res.Ret)

		if res.Ret == errors.RetNeedleNotExist {
			code = errors.RetResNoExist
			errMsg = strconv.Itoa(errors.RetNeedleNotExist) + ": " + errors.ErrNeedleNotExist.Error()
		} else if res.Ret == errors.RetSrcBucketNoExist {
			code = errors.RetResNoExist
			errMsg = strconv.Itoa(errors.RetSrcBucketNoExist) + ": " + errors.ErrSrcBucketNoExist.Error()
		} else {
			code = errors.RetServerFailed
			errMsg = strconv.Itoa(res.Ret) + ": " + errors.ErrServerFailed.Error()
		}

		return
	}

	return
}

func (b *Efs) Stat(bucket, filename string) (hash, mimeType string,
	putTime, size int64, deleteAfterDays, code int, errMsg string) {
	var (
		data   []byte
		res    meta.Response
		urlStr string
		params = url.Values{}
		err    error
		req    *http.Request
		resp   *http.Response
	)
	code = http.StatusOK

	urlStr = fmt.Sprintf(_directoryStatApi, b.c.EfsAddr)
	params.Set("ekey", b.makeEkey(bucket, filename))
	params.Encode()
	urlStr += "?" + params.Encode()
	if req, err = http.NewRequest("GET", urlStr, nil); err != nil {
		log.Errorf("stat NewRequest error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "make stat request error"
		return
	}

	if resp, err = httpcli.HttpReq(req); err != nil {
		log.Errorf("Stat HttpReq error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "stat request error"
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Errorf("Stat efs resp code error %d", resp.StatusCode)
		code, errMsg = resp.StatusCode, "request error"
		return
	}

	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		log.Errorf("Stat Read resp error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "read stat resp error"
		return
	}

	if err = json.Unmarshal(data, &res); err != nil {
		log.Errorf("Stat resp json decode ok data error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "resp json decode error"
		return
	}

	if res.Ret != errors.RetOK {
		log.Errorf("Stat call urlStr(%s) error, res.Ret(%d)", urlStr, res.Ret)
		if res.Ret == errors.RetNeedleNotExist {
			code = errors.RetResNoExist
			errMsg = strconv.Itoa(errors.RetNeedleNotExist) + ": " + errors.ErrNeedleNotExist.Error()
		} else if res.Ret == errors.RetSrcBucketNoExist {
			code = errors.RetResNoExist
			errMsg = strconv.Itoa(errors.RetSrcBucketNoExist) + ": " + errors.ErrSrcBucketNoExist.Error()
		} else {
			code = errors.RetServerFailed
			errMsg = strconv.Itoa(res.Ret) + ": " + errors.ErrServerFailed.Error()
		}

		return
	}

	if size, err = strconv.ParseInt(res.Fsize, 10, 64); err != nil {
		log.Errorf("Stat parse file size(%s) error", res.Fsize)
		code, errMsg = http.StatusInternalServerError, "parse file size error"
		return
	}

	hash = res.Sha1
	mimeType = res.Mine
	putTime = res.MTime
	deleteAfterDays = res.DeleteAftertime

	return
}

func (b *Efs) Stat_del(bucket, filename string) (hash, mimeType string,
	putTime, size int64, code int, errMsg string) {
	var (
		data      []byte
		url       string
		err       error
		req       *http.Request
		resp      *http.Response
		efsRet    meta.PFStatRetOK
		efsFailed meta.PFStatFailed
	)
	code = http.StatusOK
	url = fmt.Sprintf(_proxy_stat, b.c.ProxyAddr)

	if req, err = http.NewRequest("GET", url, nil); err != nil {
		log.Errorf("Stat NewRequest error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "make stat request error"
		return
	}

	req.Header.Set("Ekey", b.makeEkey(bucket, filename))

	if resp, err = httpcli.HttpReq(req); err != nil {
		log.Errorf("Stat HttpReq error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "stat request error"
		return
	}
	defer resp.Body.Close()

	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		log.Errorf("Stat Read resp error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "read stat resp error"
		return
	}

	if resp.StatusCode != http.StatusOK {
		if err = json.Unmarshal(data, &efsFailed); err != nil {
			log.Errorf("Stat resp json decode failed data error(%s)", err.Error())
			code, errMsg = http.StatusInternalServerError, "json decode error"
			return
		}

		code, errMsg = resp.StatusCode, efsFailed.Error
		return
	}

	if err = json.Unmarshal(data, &efsRet); err != nil {
		log.Errorf("Stat resp json decode ok data error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "resp json decode error"
		return
	}

	size = efsRet.FSize
	hash = efsRet.Hash
	mimeType = efsRet.MimeType
	putTime = efsRet.PutTime
	return
}

func (b *Efs) Chgm(bucket, filename, mimeType string) (code int, errMsg string) {
	var (
		data   []byte
		urlStr string
		err    error
		req    *http.Request
		resp   *http.Response
		res    meta.Response
		params = url.Values{}
	)
	code = http.StatusOK
	urlStr = fmt.Sprintf(_directoryChgmApi, b.c.EfsAddr)

	params.Set("ekey", b.makeEkey(bucket, filename))
	params.Set("mime", mimeType)

	if req, err = http.NewRequest("POST", urlStr, strings.NewReader(params.Encode())); err != nil {
		log.Errorf("Chgm NewRequest error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "make chgm request error"
		return
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	if resp, err = httpcli.HttpReq(req); err != nil {
		log.Errorf("Chgm HttpReq error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "chgm request error"
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Errorf("Chgm req resp code wrong(%d)", resp.StatusCode)
		code, errMsg = resp.StatusCode, "chgm resp code error"
		return
	}

	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		log.Errorf("chgm read body data error")
		code, errMsg = http.StatusInternalServerError, "read body data error"
		return
	}

	if err = json.Unmarshal(data, &res); err != nil {
		log.Errorf("json decode error")
		code, errMsg = http.StatusInternalServerError, "json decode error"
		return
	}

	if res.Ret != errors.RetOK {
		log.Errorf("Efs Chgm() POST called uri(%s)no OK res.Ret(%d), params(%v)", urlStr, res.Ret, params)
		if res.Ret == errors.RetNeedleNotExist {
			code = errors.RetResNoExist
			errMsg = strconv.Itoa(errors.RetNeedleNotExist) + ": " + errors.ErrNeedleNotExist.Error()
		} else if res.Ret == errors.RetSrcBucketNoExist {
			code = errors.RetResNoExist
			errMsg = strconv.Itoa(errors.RetSrcBucketNoExist) + ": " + errors.ErrSrcBucketNoExist.Error()
		} else {
			code = errors.RetServerFailed
			errMsg = strconv.Itoa(res.Ret) + ": " + errors.ErrServerFailed.Error()
		}

		return
	}

	return
}

func (b *Efs) Chgm_del(bucket, filename, mimeType string) (code int, errMsg string) {
	var (
		data      []byte
		url       string
		err       error
		req       *http.Request
		resp      *http.Response
		body      io.Reader
		efsReq    map[string]string
		efsFailed meta.PFStatChgFailed
	)
	code = http.StatusOK
	url = fmt.Sprintf(_proxy_chgm, b.c.ProxyAddr)
	efsReq = make(map[string]string)
	efsReq["mime"] = mimeType

	if data, err = json.Marshal(efsReq); err != nil {
		code, errMsg = http.StatusInternalServerError, "json encode error"
		return
	}
	body = bytes.NewReader(data)

	if req, err = http.NewRequest("POST", url, body); err != nil {
		log.Errorf("Chgm NewRequest error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "make stat request error"
		return
	}
	req.Header.Set("Ekey", b.makeEkey(bucket, filename))

	if resp, err = httpcli.HttpReq(req); err != nil {
		log.Errorf("Chgm HttpReq error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "stat request error"
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Errorf("Chgm req resp code wrong(%d)", resp.StatusCode)

		if data, err = ioutil.ReadAll(resp.Body); err != nil {
			log.Errorf("Chgm Read resp error(%s)", err.Error())
			code, errMsg = http.StatusInternalServerError, "read chgm resp error"
			return
		}

		if err = json.Unmarshal(data, &efsFailed); err != nil {
			log.Errorf("Chgm resp json decode failed data error(%s)", err.Error())
			code, errMsg = http.StatusInternalServerError, "json decode error"
			return
		}

		code, errMsg = resp.StatusCode, efsFailed.Error
		return
	}

	return
}

func (b *Efs) Move(srcBucket, srcKey, destBucket, destKey string,
	isforce bool) (fsize, oldfize int64, code int, errMsg string) {
	var (
		data   []byte
		urlStr string
		err    error
		req    *http.Request
		resp   *http.Response
		res    meta.Response
		params = url.Values{}
	)
	code = http.StatusOK
	urlStr = fmt.Sprintf(_directoryMoveApi, b.c.EfsAddr)

	params.Set("srcekey", b.makeEkey(srcBucket, srcKey))
	params.Set("destekey", b.makeEkey(destBucket, destKey))
	if isforce {
		params.Set("overwrite", strconv.Itoa(1))
	} else {
		params.Set("overwrite", strconv.Itoa(0))
	}

	if req, err = http.NewRequest("POST", urlStr, strings.NewReader(params.Encode())); err != nil {
		log.Errorf("Move NewRequest error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "make stat request error"
		return
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Ekey", b.makeEkey(srcBucket, srcKey))

	if resp, err = httpcli.HttpReq(req); err != nil {
		log.Errorf("Move HttpReq error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "move request error"
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Errorf("Move HttpReq resp code(%d) error", resp.StatusCode)
		code, errMsg = resp.StatusCode, "move resp code error"
		return

	}

	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		log.Errorf("move read body data error")
		code, errMsg = http.StatusInternalServerError, "read body data error"
		return
	}

	if err = json.Unmarshal(data, &res); err != nil {
		log.Errorf("json decode error")
		code, errMsg = http.StatusInternalServerError, "json decode error"
		return
	}

	if res.Ret != errors.RetOK {
		log.Errorf("move res.Ret(%d) no ok", res.Ret)
		if res.Ret == errors.RetNeedleNotExist {
			code = errors.RetResNoExist
			errMsg = strconv.Itoa(errors.RetNeedleNotExist) + ": " + errors.ErrNeedleNotExist.Error()
		} else if res.Ret == errors.RetNeedleExist {
			code = errors.RetResExist
			errMsg = strconv.Itoa(errors.RetNeedleExist) + ": " + errors.ErrNeedleExist.Error()
		} else if res.Ret == errors.RetSrcBucketNoExist {
			code = errors.RetResNoExist
			errMsg = strconv.Itoa(errors.RetSrcBucketNoExist) + ": " + errors.ErrSrcBucketNoExist.Error()
		} else if res.Ret == errors.RetDestBucketNoExist {
			code = errors.RetResNoExist
			errMsg = strconv.Itoa(errors.RetDestBucketNoExist) + ": " + errors.ErrDestBucketNoExist.Error()
		} else {
			code = errors.RetServerFailed
			errMsg = strconv.Itoa(res.Ret) + ": " + errors.ErrServerFailed.Error()
		}

		return
	}
	if fsize, err = strconv.ParseInt(res.Fsize, 10, 64); err != nil {

		log.Errorf("move parse file size(%s) error", res.Fsize)
		code, errMsg = http.StatusInternalServerError, "parse file size error"
		return

	}
	if oldfize, err = strconv.ParseInt(res.OFSize, 10, 64); err != nil {

		log.Errorf("move parse file oldsize(%s) error", res.OFSize)
		code, errMsg = http.StatusInternalServerError, "parse file size error"
		return
	}

	return
}

func (b *Efs) Move_del(srcBucket, srcKey, destBucket, destKey string, isforce bool) (code int, errMsg string) {
	var (
		data      []byte
		url       string
		err       error
		req       *http.Request
		resp      *http.Response
		body      io.Reader
		efsReq    map[string]string
		efsFailed meta.PFMvFailed
	)
	code = http.StatusOK
	url = fmt.Sprintf(_proxy_move, b.c.ProxyAddr)
	efsReq = make(map[string]string)
	efsReq["dest"] = b.makeEkey(destBucket, destKey)
	if isforce {
		efsReq["overwrite"] = strconv.Itoa(1)
	} else {
		efsReq["overwrite"] = strconv.Itoa(0)
	}

	if data, err = json.Marshal(efsReq); err != nil {
		code, errMsg = http.StatusInternalServerError, "json encode error"
		return
	}
	body = bytes.NewReader(data)

	if req, err = http.NewRequest("POST", url, body); err != nil {
		log.Errorf("Move NewRequest error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "make stat request error"
		return
	}
	req.Header.Set("Ekey", b.makeEkey(srcBucket, srcKey))

	if resp, err = httpcli.HttpReq(req); err != nil {
		log.Errorf("Move HttpReq error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "stat request error"
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if data, err = ioutil.ReadAll(resp.Body); err != nil {
			log.Errorf("Move Read resp error(%s)", err.Error())
			code, errMsg = http.StatusInternalServerError, "read move resp error"
			return
		}

		if err = json.Unmarshal(data, &efsFailed); err != nil {
			log.Errorf("Move resp json decode failed data error(%s)", err.Error())
			code, errMsg = http.StatusInternalServerError, "json decode error"
			return
		}

		code, errMsg = resp.StatusCode, efsFailed.Error
		return
	}

	return
}

func (b *Efs) Copy(srcBucket, srcKey, destBucket, destKey string,
	isforce bool) (fsize, oldfize int64, code int, errMsg string) {
	var (
		data   []byte
		urlStr string
		err    error
		req    *http.Request
		resp   *http.Response
		res    meta.Response
		params = url.Values{}
	)
	code = http.StatusOK
	urlStr = fmt.Sprintf(_directoryCopyApi, b.c.EfsAddr)

	params.Set("srcekey", b.makeEkey(srcBucket, srcKey))
	params.Set("destekey", b.makeEkey(destBucket, destKey))
	if isforce {
		params.Set("overwrite", strconv.Itoa(1))
	} else {
		params.Set("overwrite", strconv.Itoa(0))
	}

	if req, err = http.NewRequest("POST", urlStr, strings.NewReader(params.Encode())); err != nil {
		log.Errorf("Copy NewRequest error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "make copy request error"
		return
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	if resp, err = httpcli.HttpReq(req); err != nil {
		log.Errorf("Copy HttpReq error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "copy request error"
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Errorf("copy resp code(%d) error", resp.StatusCode)
		code, errMsg = resp.StatusCode, "copy resp code error"
		return
	}

	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		log.Errorf("Copy Read data error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "copy read data error"
		return
	}

	if err = json.Unmarshal(data, &res); err != nil {
		log.Errorf("Copy resp json decode failed data error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "json decode error"
		return
	}

	if res.Ret != errors.RetOK {
		log.Errorf("copy res.Ret(%d) no ok", res.Ret)
		if res.Ret == errors.RetNeedleNotExist {
			code = errors.RetResNoExist
			errMsg = strconv.Itoa(errors.RetNeedleNotExist) + ": " + errors.ErrNeedleNotExist.Error()
		} else if res.Ret == errors.RetNeedleExist {
			code = errors.RetResExist
			errMsg = strconv.Itoa(errors.RetNeedleExist) + ": " + errors.ErrNeedleExist.Error()
		} else if res.Ret == errors.RetSrcBucketNoExist {
			code = errors.RetResNoExist
			errMsg = strconv.Itoa(errors.RetSrcBucketNoExist) + ": " + errors.ErrSrcBucketNoExist.Error()
		} else if res.Ret == errors.RetDestBucketNoExist {
			code = errors.RetResNoExist
			errMsg = strconv.Itoa(errors.RetDestBucketNoExist) + ": " + errors.ErrDestBucketNoExist.Error()
		} else {
			code = errors.RetServerFailed
			errMsg = strconv.Itoa(res.Ret) + ": " + errors.ErrServerFailed.Error()
		}

		return
	}
	if fsize, err = strconv.ParseInt(res.Fsize, 10, 64); err != nil {

		log.Errorf("move parse file size(%s) error", res.Fsize)
		code, errMsg = http.StatusInternalServerError, "parse file size error"
		return

	}
	if oldfize, err = strconv.ParseInt(res.OFSize, 10, 64); err != nil {

		log.Errorf("move parse file oldsize(%s) error", res.OFSize)
		code, errMsg = http.StatusInternalServerError, "parse file size error"
		return
	}

	return
}

func (b *Efs) Copy_del(srcBucket, srcKey, destBucket, destKey string, isforce bool) (code int, errMsg string) {
	var (
		data      []byte
		url       string
		err       error
		req       *http.Request
		resp      *http.Response
		body      io.Reader
		efsReq    map[string]string
		efsFailed meta.PFCopyFailed
	)
	code = http.StatusOK
	url = fmt.Sprintf(_proxy_copy, b.c.ProxyAddr)
	efsReq = make(map[string]string)
	efsReq["dest"] = b.makeEkey(destBucket, destKey)
	if isforce {
		efsReq["overwrite"] = strconv.Itoa(1)
	} else {
		efsReq["overwrite"] = strconv.Itoa(0)
	}

	if data, err = json.Marshal(efsReq); err != nil {
		code, errMsg = http.StatusInternalServerError, "json encode error"
		return
	}
	body = bytes.NewReader(data)

	if req, err = http.NewRequest("POST", url, body); err != nil {
		log.Errorf("Copy NewRequest error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "make copy request error"
		return
	}
	req.Header.Set("Ekey", b.makeEkey(srcBucket, srcKey))

	if resp, err = httpcli.HttpReq(req); err != nil {
		log.Errorf("Copy HttpReq error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "copy request error"
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if data, err = ioutil.ReadAll(resp.Body); err != nil {
			log.Errorf("Copy Read resp error(%s)", err.Error())
			code, errMsg = http.StatusInternalServerError, "read copy resp error"
			return
		}

		if err = json.Unmarshal(data, &efsFailed); err != nil {
			log.Errorf("Copy resp json decode failed data error(%s)", err.Error())
			code, errMsg = http.StatusInternalServerError, "json decode error"
			return
		}

		code, errMsg = resp.StatusCode, efsFailed.Error
		return
	}

	return
}

func (b *Efs) Delete(bucket, key string) (fsize int64, code int, errMsg string) {
	var (
		data   []byte
		urlStr string
		err    error
		req    *http.Request
		resp   *http.Response
		res    meta.SliceResponse
		params = url.Values{}
	)
	code = http.StatusOK
	urlStr = fmt.Sprintf(_directoryDelApi, b.c.EfsAddr)

	params.Set("ekey", b.makeEkey(bucket, key))
	if req, err = http.NewRequest("POST", urlStr, strings.NewReader(params.Encode())); err != nil {
		log.Errorf("Delete NewRequest error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "make delete request error"
		return
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	if resp, err = httpcli.HttpReq(req); err != nil {
		log.Errorf("delete HttpReq error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "delete request error"
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Errorf("delete resp code(%d) error", resp.StatusCode)
		code, errMsg = resp.StatusCode, "delete resp code error"
		return
	}

	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		log.Errorf("Delete Read resp error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "read delete resp error"
		return
	}

	if err = json.Unmarshal(data, &res); err != nil {
		log.Errorf("delete resp json decode failed data error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "json decode error"
		return
	}

	if res.Ret == errors.RemoveLinkOK {
		return
	}

	if res.Ret != errors.RetOK {
		log.Errorf("delete res.Ret(%d) no ok", res.Ret)
		if res.Ret == errors.RetNeedleNotExist {
			code = errors.RetResNoExist
			errMsg = strconv.Itoa(errors.RetNeedleNotExist) + ": " + errors.ErrNeedleNotExist.Error()
		} else if res.Ret == errors.RetSrcBucketNoExist {
			code = errors.RetResNoExist
			errMsg = strconv.Itoa(errors.RetSrcBucketNoExist) + ": " + errors.ErrSrcBucketNoExist.Error()
		} else {
			code = errors.RetServerFailed
			errMsg = strconv.Itoa(res.Ret) + ": " + errors.ErrServerFailed.Error()
		}

		return
	}

	if fsize, err = strconv.ParseInt(res.Fsize, 10, 64); err != nil {
		log.Errorf("move parse file size(%s) error", res.Fsize)
		code, errMsg = http.StatusInternalServerError, "parse file size error"
		return
	}

	return
}

func (b *Efs) Delete_del(bucket, key string) (code int, errMsg string) {
	var (
		data      []byte
		url       string
		err       error
		req       *http.Request
		resp      *http.Response
		efsFailed meta.PFDelFailed
	)
	code = http.StatusOK
	url = fmt.Sprintf(_proxy_delete, b.c.ProxyAddr)

	if req, err = http.NewRequest("POST", url, nil); err != nil {
		log.Errorf("Delete NewRequest error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "make delete request error"
		return
	}
	req.Header.Set("Ekey", b.makeEkey(bucket, key))

	if resp, err = httpcli.HttpReq(req); err != nil {
		log.Errorf("delete HttpReq error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "delete request error"
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if data, err = ioutil.ReadAll(resp.Body); err != nil {
			log.Errorf("Delete Read resp error(%s)", err.Error())
			code, errMsg = http.StatusInternalServerError, "read delete resp error"
			return
		}

		if err = json.Unmarshal(data, &efsFailed); err != nil {
			log.Errorf("delete resp json decode failed data error(%s)", err.Error())
			code, errMsg = http.StatusInternalServerError, "json decode error"
			return
		}

		code, errMsg = resp.StatusCode, efsFailed.Error
		return
	}

	return
}

func (b *Efs) List(bucket string, marker string, limit int, prefix string,
	delimiter string) (listRet *meta.PFListRetOK, code int, errMsg string) {
	var (
		data   []byte
		urlStr string
		err    error
		param  = url.Values{}
		req    *http.Request
		resp   *http.Response
		res    meta.FileListResponse
		tflist meta.PFListRetOK
	)
	code = http.StatusOK
	urlStr = fmt.Sprintf(_directoryListApi, b.c.EfsAddr)

	param.Set("ekey", b.makeEkey(bucket, ""))
	param.Set("limit", strconv.Itoa(limit))
	param.Set("prefix", prefix)
	param.Set("delimiter", delimiter)
	param.Set("marker", marker)

	if req, err = http.NewRequest("POST", urlStr, strings.NewReader(param.Encode())); err != nil {
		log.Errorf("List NewRequest error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "make list request error"
		return
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	if resp, err = httpcli.HttpReq(req); err != nil {
		log.Errorf("List HttpReq error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "list request error"
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Errorf("List resp json decode failed data error(%s)", err.Error())
		code, errMsg = resp.StatusCode, "list resp code error"
		return
	}

	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		log.Errorf("List Read resp error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "read list resp error"
		return
	}

	if err = json.Unmarshal(data, &res); err != nil {
		log.Errorf("List resp json decode failed data error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "json decode error"
		return
	}

	if res.Ret != errors.RetOK {
		log.Errorf("list res.Ret(%d) no ok", res.Ret)
		if res.Ret == errors.RetSrcBucketNoExist {
			code = errors.RetResNoExist
			errMsg = strconv.Itoa(res.Ret) + ": " + errors.ErrSrcBucketNoExist.Error()
		} else {
			code = errors.RetServerFailed
			errMsg = strconv.Itoa(res.Ret) + ": " + errors.ErrServerFailed.Error()
		}

		return
	}

	tflist.Marker = res.Flist.Marker
	tflist.CommonPrefixes = res.Flist.CommonPrefixes
	for _, vo := range res.Flist.Items {
		var fitem meta.FItem
		fitem.Key = vo.Key
		fitem.PutTime = vo.PutTime
		fitem.Hash = vo.Hash
		fitem.FSize = vo.Fsize
		fitem.MimeType = vo.MimeType
		fitem.Customer = vo.Customer
		tflist.FItems = append(tflist.FItems, fitem)
	}

	listRet = &tflist
	return
}

func (b *Efs) List_del(bucket string, marker string, limit int, prefix string,
	delimiter string) (listRet *meta.PFListRetOK, code int, errMsg string) {
	var (
		data      []byte
		listUrl   string
		err       error
		param     = url.Values{}
		req       *http.Request
		resp      *http.Response
		efsRet    meta.PFListRetOK
		efsFailed meta.PFListFailed
	)
	code = http.StatusOK
	listUrl = fmt.Sprintf(_proxy_list, b.c.ProxyAddr)

	param.Set("limit", strconv.Itoa(limit))
	param.Set("delimiter", delimiter)
	param.Set("prefix", prefix)
	param.Set("marker", marker)

	if req, err = http.NewRequest("POST", listUrl, strings.NewReader(param.Encode())); err != nil {
		log.Errorf("List NewRequest error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "make list request error"
		return
	}
	req.Header.Set("Ekey", b.makeEkey(bucket, ""))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	if resp, err = httpcli.HttpReq(req); err != nil {
		log.Errorf("List HttpReq error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "list request error"
		return
	}
	defer resp.Body.Close()

	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		log.Errorf("List Read resp error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "read list resp error"
		return
	}

	if resp.StatusCode != http.StatusOK {
		if err = json.Unmarshal(data, &efsFailed); err != nil {
			log.Errorf("List resp json decode failed data error(%s)", err.Error())
			code, errMsg = http.StatusInternalServerError, "json decode error"
			return
		}

		code, errMsg = resp.StatusCode, efsFailed.Error
		return
	}

	if err = json.Unmarshal(data, &efsRet); err != nil {
		log.Errorf("List resp json decode failed data error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "json decode error"
		return
	}

	listRet = &efsRet
	return
}

// compute hash
func (b *Efs) DownloadDirect(ekey string) (buf []byte, code int, errMsg string) {
	var (
		_rand  = rand.New(rand.NewSource(time.Now().UnixNano()))
		res    meta.SliceResponse
		urlStr string
		params = url.Values{}
		req    *http.Request
		resp   *http.Response
		data   []byte
		err    error
		buff   *bytes.Buffer
	)
	buff = new(bytes.Buffer)

	urlStr = fmt.Sprintf(_directoryGetApi, b.c.EfsAddr)
	params.Set("ekey", ekey)
	if req, err = http.NewRequest("POST", urlStr, strings.NewReader(params.Encode())); err != nil {
		log.Errorf("direct download make req err(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "direct download make req error"
		return
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	if resp, err = httpcli.HttpReq(req); err != nil {
		log.Errorf("direct download req err(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "direct download req error"
		return
	}
	defer resp.Body.Close()

	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		log.Errorf("direct download read data err(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "direct download read data error"
		return
	}

	if err = json.Unmarshal(data, &res); err != nil {
		log.Errorf("direct download decode data err(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "direct download decode data error"
		return
	}

	if res.Ret != errors.RetOK {
		log.Errorf("direct download res.Ret(%d) no ok", res.Ret)
		if res.Ret == errors.RetNeedleNotExist {
			code, errMsg = errors.RetResNoExist, errors.ErrNeedleNotExist.Error()
		} else if res.Ret == errors.RetSrcBucketNoExist {
			code, errMsg = errors.RetResNoExist, errors.ErrSrcBucketNoExist.Error()
		} else {
			code, errMsg = errors.RetServerFailed, errors.ErrServerFailed.Error()
		}
		return
	}

	var (
		l, ix int
	)
	for _, v := range res.Res {
		params = url.Values{}
		l = len(v.Stores)
		ix = _rand.Intn(l)
		for i := 0; i < l; i++ {
			code = http.StatusOK
			params.Set("key", strconv.FormatInt(v.Key, 10))
			params.Set("cookie", strconv.FormatInt(int64(v.Cookie), 10))
			params.Set("vid", strconv.FormatInt(int64(v.Vid), 10))
			urlStore := fmt.Sprintf(_storeGetApi, v.Stores[(ix+i)%l]) + "?" + params.Encode()

			if req, err = http.NewRequest("GET", urlStore, nil); err != nil {
				code, errMsg = http.StatusInternalServerError, "direct download make store req error"
				log.Errorf("direct download make store req err(%s)", err.Error())
				continue
			}
			if resp, err = httpcli.HttpReq(req); err != nil {
				code, errMsg = http.StatusInternalServerError, "direct download store req error"
				log.Errorf("direct download store req err(%s)", err.Error())
				continue
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				code, errMsg = resp.StatusCode, "resp code error"
				log.Errorf("direct download store resp code(%d) no ok", resp.StatusCode)
				continue
			}
			if _, err = io.Copy(buff, resp.Body); err != nil {
				code, errMsg = http.StatusInternalServerError, "read store data error"
				log.Errorf("direct download copy data err(%s)", err.Error())
				continue
			}

			break
		}

		if code != http.StatusOK {
			return
		}
	}

	buf = buff.Bytes()
	return
}

// compute hash
func (b *Efs) DownloadSlice(ekey string, start, end int64) (buf []byte, code int, errMsg string) {
	var (
		_rand  = rand.New(rand.NewSource(time.Now().UnixNano()))
		res    meta.SliceResponse
		urlStr string
		params = url.Values{}
		req    *http.Request
		resp   *http.Response
		data   []byte
		err    error
		buff   *bytes.Buffer
	)
	buff = new(bytes.Buffer)

	urlStr = fmt.Sprintf(_directoryGetApi, b.c.EfsAddr)
	params.Set("ekey", ekey)
	if req, err = http.NewRequest("POST", urlStr, strings.NewReader(params.Encode())); err != nil {
		log.Errorf("slice download make req err(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "download slice make efs req error"
		return
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	if resp, err = httpcli.HttpReq(req); err != nil {
		log.Errorf("slice download req err(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "download slice efs req error"
		return
	}
	defer resp.Body.Close()

	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		log.Errorf("slice download read data err(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "download slice efs read data error"
		return
	}

	if err = json.Unmarshal(data, &res); err != nil {
		log.Errorf("slice download decode data err(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "slice download json decode error"
		return
	}

	if res.Ret != errors.RetOK {
		log.Errorf("slice download res.Ret(%d) no ok", res.Ret)
		if res.Ret == errors.RetNeedleNotExist {
			code, errMsg = errors.RetResNoExist, errors.ErrNeedleNotExist.Error()
		} else if res.Ret == errors.RetSrcBucketNoExist {
			code, errMsg = errors.RetResNoExist, errors.ErrSrcBucketNoExist.Error()
		} else {
			code, errMsg = errors.RetServerFailed, errors.ErrServerFailed.Error()
		}
		return
	}

	var (
		l, ix               int
		sIndex, eIndex      int
		firstSeek, lastSeek int64
	)

	sIndex = int(start / int64(b.c.SliceFileSize))
	eIndex = int((end / int64(b.c.SliceFileSize)) + 1)
	firstSeek = start % b.c.SliceFileSize
	lastSeek = (end % b.c.SliceFileSize) + 1

	for index, v := range res.Res[sIndex:eIndex] {
		params = url.Values{}
		l = len(v.Stores)
		ix = _rand.Intn(l)
		for i := 0; i < l; i++ {
			code = http.StatusOK
			params.Set("key", strconv.FormatInt(v.Key, 10))
			params.Set("cookie", strconv.FormatInt(int64(v.Cookie), 10))
			params.Set("vid", strconv.FormatInt(int64(v.Vid), 10))
			urlStore := fmt.Sprintf(_storeGetApi, v.Stores[(ix+i)%l]) + "?" + params.Encode()

			if req, err = http.NewRequest("GET", urlStore, nil); err != nil {
				code, errMsg = http.StatusInternalServerError, "slice download make store req error"
				log.Errorf("slice download make store req err(%s)", err.Error())
				continue
			}
			if resp, err = httpcli.HttpReq(req); err != nil {
				code, errMsg = http.StatusInternalServerError, "slice download store req error"
				log.Errorf("slice download store req err(%s)", err.Error())
				continue
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				code, errMsg = resp.StatusCode, "slice download store resp code error"
				log.Errorf("slice download store resp code(%d) no ok", resp.StatusCode)
				continue
			}

			break
		}

		if code != http.StatusOK {
			return
		}

		if index == 0 {
			var step int64
			step = lastSeek
			if index < eIndex-sIndex-1 {
				step = b.c.SliceFileSize
			}
			tbuf := make([]byte, step)

			if _, err = io.ReadFull(resp.Body, tbuf); err != nil {
				log.Errorf("download slice Read Slice error(%s)", err.Error())
				code, errMsg = errors.RetServerFailed, "download slice read store data error"
				continue
			}
			tReader := bytes.NewReader(tbuf[firstSeek:step])
			io.Copy(buff, tReader)
		} else if index == eIndex-sIndex-1 {
			io.CopyN(buff, resp.Body, lastSeek)
		} else {
			io.Copy(buff, resp.Body)
		}
	}

	buf = buff.Bytes()
	return
}

func (b *Efs) Get_del(ekey, rangeStr string) (data []byte, code int, errMsg string) {
	var (
		url  string
		err  error
		req  *http.Request
		resp *http.Response
	)
	url = fmt.Sprintf(_proxy_get, b.c.ProxyAddr)

	if req, err = http.NewRequest("GET", url, nil); err != nil {
		log.Errorf("Get NewRequest error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "make get request error"
		return
	}
	req.Header.Set("Ekey", ekey)

	if rangeStr != "" {
		req.Header.Set("range", rangeStr)
	}

	if resp, err = httpcli.HttpReq(req); err != nil {
		log.Errorf("Get HttpReq error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "get request error"
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		code, errMsg = resp.StatusCode, "get file data error"
		return
	}

	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		log.Errorf("Get Read resp error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "get stat resp error"
		return
	}

	code, errMsg = http.StatusOK, ""
	return
}

func (b *Efs) Batch_del(ops []map[string]string) (retData []byte, code int, errMsg string) {
	var (
		data     []byte
		batchUrl string
		err      error
		req      *http.Request
		resp     *http.Response
	)

	batchUrl = fmt.Sprintf(_proxy_batch, b.c.ProxyAddr)

	if data, err = json.Marshal(ops); err != nil {
		code, errMsg = http.StatusInternalServerError, "ops json encode error"
		return
	}

	if req, err = http.NewRequest("POST", batchUrl, strings.NewReader(string(data))); err != nil {
		log.Errorf("Batch NewRequest error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "make batch request error"
		return
	}

	if resp, err = httpcli.HttpReq(req); err != nil {
		log.Errorf("Batch HttpReq error(%s)", err.Error())
		code, errMsg = http.StatusInternalServerError, "batch request error"
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != errors.RetPartialFailed {
		code, errMsg = resp.StatusCode, "server error"
		return
	}

	if retData, err = ioutil.ReadAll(resp.Body); err != nil {
		code, errMsg = http.StatusInternalServerError, "read body data error"
		return
	}

	code = resp.StatusCode
	return
}

func (b *Efs) makeMultiPartFile(bufdata *bytes.Buffer, data []byte, ctype *string) (err error) {
	var (
		w  *multipart.Writer
		bw io.Writer
	)

	w = multipart.NewWriter(bufdata)
	if bw, err = w.CreateFormFile("file", "file"); err != nil {
		return
	}
	if _, err = bw.Write(data); err != nil {
		return
	}
	*ctype = w.FormDataContentType()
	if err = w.Close(); err != nil {
		return
	}

	return
}

func (b *Efs) makeEkey(bucket, key string) (ekey string) {
	if key == "" {
		ekey = b64.URLEncoding.EncodeToString([]byte(bucket))
		return
	} else {
		ekey = b64.URLEncoding.EncodeToString([]byte(bucket + KEY_DELIMITER + key))
		return
	}
}
