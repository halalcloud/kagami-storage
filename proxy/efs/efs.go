package efs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"kagamistoreage/libs/errors"
	"kagamistoreage/libs/meta"
	"kagamistoreage/proxy/conf"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	log "kagamistoreage/log/glog"
)

const (
	// api
	_directoryGetApi        = "http://%s/get"
	_directoryStatApi       = "http://%s/stat"
	_directoryDispatcherApi = "http://%s/dispatcher"
	_directoryUploadApi     = "http://%s/upload"
	_directoryMkblkApi      = "http://%s/mkblk"
	_directoryBputApi       = "http://%s/bput"
	_directoryMkfileApi     = "http://%s/mkfile"
	_directoryDelApi        = "http://%s/del"
	_directoryDelTmpApi     = "http://%s/deltmp"
	_directoryChgmApi       = "http://%s/chgm"
	_directoryCopyApi       = "http://%s/copy"
	_directoryMoveApi       = "http://%s/move"
	_directoryListApi       = "http://%s/list"
	_storeGetApi            = "http://%s/get"
	_storeUploadApi         = "http://%s/upload"
	_storeDelApi            = "http://%s/del"
	_bucketcreatApi         = "http://%s/bcreate"
	_bucketrenameApi        = "http://%s/brename"
	_bucketdeleteApi        = "http://%s/bdelete"
	_bucketlistApi          = "http://%s/blist"
	_bucketStatApi          = "http://%s/bstat"
	_directoryGetneedle     = "http://%s/getneedle"
)

var (
	_transport = &http.Transport{
		Dial: func(netw, addr string) (c net.Conn, err error) {
			if c, err = net.DialTimeout(netw, addr, 2*time.Second); err != nil {
				return nil, err
			}
			return c, nil
		},
		//DisableCompression: true,
		//DisableKeepAlives:  true,
		MaxIdleConnsPerHost: 20,
	}
	_client = &http.Client{
		Transport: _transport,
		Timeout:   2 * time.Second,
	}
)

type Efs struct {
	c *conf.Config
}

type Responsedata struct {
	err  error
	data io.ReadCloser
}

func New(c *conf.Config) (b *Efs) {
	b = &Efs{}
	b.c = c
	return
}

func (b *Efs) getkeyfromstore(v meta.Response) (resdata io.ReadCloser, err error) {
	var (
		req  *http.Request
		resp *http.Response
	)
	_rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	l := len(v.Stores)
	ix := _rand.Intn(l)
	params := url.Values{}

	for i := 0; i < l; i++ {
		params.Set("key", strconv.FormatInt(v.Key, 10))
		params.Set("cookie", strconv.FormatInt(int64(v.Cookie), 10))
		params.Set("vid", strconv.FormatInt(int64(v.Vid), 10))
		uri := fmt.Sprintf(_storeGetApi, v.Stores[(ix+i)%l]) + "?" + params.Encode()

		if req, err = http.NewRequest("GET", uri, nil); err != nil {
			log.Errorf("Efs Get() store called uri(%s) http request error(%v)", uri, err)
			continue
		}
		if resp, err = _client.Do(req); err != nil {
			log.Errorf("Efs Get() store _client.do(%s) error(%v)", uri, err)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			log.Errorf("Efs Get() store _client.do(%s) no StatusOK, params(%v)", uri, params)
			resp.Body.Close()
			err = errors.ErrStoreNotAvailable
			continue
		}
		resdata = resp.Body
		break
	}
	return
}
func (b *Efs) getkeyaddr(key int64) (res meta.Response, err error) {
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

func (b *Efs) getdatabykey(key int64) (resdata io.ReadCloser, err error) {
	var (
		res meta.Response
	)
	if res, err = b.getkeyaddr(key); err != nil {
		log.Errorf("get key %d failed from directory %v", key, err)
		return
	}
	if resdata, err = b.getkeyfromstore(res); err != nil {
		log.Errorf("get vid %d key %d from store failed %v", res.Key, res.Vid, err)
		return
	}

	return
}

func (b *Efs) getfilefromdirectory(ekey string) (res meta.SliceResponse, err error) {
	var (
		params = url.Values{}
		uri    string
	)
	params.Set("ekey", ekey)
	uri = fmt.Sprintf(_directoryGetApi, b.c.EfsAddr)
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
	if res.Res == nil {
		log.Errorf("Efs Get() directory res.Res is nil")
		err = errors.ErrNeedleNotExist
		return
	}
	return
}

func (b *Efs) resgetheader(res meta.SliceResponse, wr http.ResponseWriter, r *http.Request) (direct_res bool) {
	direct_res = false
	mtime := res.Res[0].MTime
	sha1 := res.Res[0].Sha1
	mime := res.Res[0].Mine
	last_modify := time.Unix(0, mtime).Format(http.TimeFormat)
	modify := r.Header.Get("If-Modified-Since")
	if modify != "" && modify == last_modify {
		wr.Header().Set("Content-Type", mime)
		wr.Header().Set("Last-Modified", last_modify)
		wr.Header().Set("Accept-Ranges", "bytes")
		wr.Header().Set("Etag", sha1)
		status := http.StatusNotModified
		wr.WriteHeader(status)
		direct_res = true
		return
	}

	wr.Header().Set("Content-Length", res.Fsize)
	wr.Header().Set("Content-Type", mime)
	wr.Header().Set("Accept-Ranges", "bytes")
	wr.Header().Set("Last-Modified", time.Unix(0, mtime).Format(http.TimeFormat))
	wr.Header().Set("Etag", sha1)
	if r.Method == "HEAD" || r.Method == "head" {
		status := http.StatusOK
		wr.WriteHeader(status)
		direct_res = true
		return
	}
	return
}

func (b *Efs) get_first_resdata_to_queue(res meta.Response, chdata chan *Responsedata) {
	rdata := new(Responsedata)
	if rdata.data, rdata.err = b.getkeyfromstore(res); rdata.err != nil {
		log.Errorf("get keyid %d vid %d from store failed %v", res.Key, res.Vid, rdata.err)
		return
	}
	if chdata != nil {
		chdata <- rdata
	}

}

func (b *Efs) res_copy_data(wr http.ResponseWriter, resdata io.ReadCloser) error {
	defer resdata.Close()
	_, err := io.Copy(wr, resdata)
	return err

}

func (b *Efs) get_ahead_data(key int64, chdata chan *Responsedata) {
	rdata := new(Responsedata)
	if rdata.data, rdata.err = b.getdatabykey(key); rdata.err != nil {
		log.Errorf("get key %d failed %v", key, rdata.err)
	}
	if chdata != nil {
		chdata <- rdata
	}
}

func (b *Efs) get_response_data(res meta.SliceResponse, wr http.ResponseWriter) (err error) {
	var (
		rd                                      [10]chan *Responsedata
		keylen, indexkey, aheadindex, chanindex int
		responsedata                            *Responsedata
	)
	for i := 0; i < 10; i++ {
		rd[i] = make(chan *Responsedata)
	}

	keylen = len(res.Keys)

	for i, needleinfo := range res.Res {
		go b.get_first_resdata_to_queue(needleinfo, rd[i])
	}

	chanindex = 0
	indexkey = 0
	if keylen > 10 {
		aheadindex = 10
	} else {
		aheadindex = keylen
	}

	for {
		if indexkey > (keylen - 1) {
			break
		}
		if chanindex > 9 {
			chanindex = 0
		}
		responsedata = <-rd[chanindex]
		if responsedata.err == nil {
			err = b.res_copy_data(wr, responsedata.data)
			if err != nil {
				log.Errorf("reponse copy data failed %v", err)
				log.Errorf("index key=%d aheadindex=%d chanindex=%d", indexkey, aheadindex, chanindex)
				break
			}
			if aheadindex > (keylen - 1) {
				//nothing
			} else {
				go b.get_ahead_data(res.Keys[aheadindex], rd[chanindex])
				aheadindex++
			}

		} else {
			log.Errorf("get needle key %d failed %v", res.Keys[indexkey], err)
			break
		}

		indexkey++
		chanindex++
	}
	//接收完所有的chan,再close.
	for i := indexkey + 1; i < aheadindex; i++ {
		chanindex++
		if chanindex > 9 {
			chanindex = 0
		}
		_ = <-rd[chanindex]
	}

	for i := 0; i < 10; i++ {
		close(rd[i])
	}

	return
}

func (b *Efs) Get_stream(ekey string, wr http.ResponseWriter, r *http.Request) (ctlen int64, err error) {
	var (
		res meta.SliceResponse
	)

	if res, err = b.getfilefromdirectory(ekey); err != nil {
		log.Errorf("get needle keys from directory failed %v", err)
		return
	}

	//判断method 是否为head 和判断If-Modified-Since
	direct_res := b.resgetheader(res, wr, r)
	if direct_res {
		return
	}

	if ctlen, err = strconv.ParseInt(res.Fsize, 10, 64); err != nil {
		log.Errorf("Efs Get() res.Fsize:(%v) parseInt error(%v)", res.Fsize, err)
		err = errors.ErrServerFailed
		return
	}

	err = b.get_response_data(res, wr)
	return

}

func (b *Efs) resgetrangeheader(res meta.SliceResponse, s, e, ctlen int64, wr http.ResponseWriter, r *http.Request) (direct_res bool) {
	direct_res = false
	mtime := res.Res[0].MTime
	sha1 := res.Res[0].Sha1
	mime := res.Res[0].Mine
	last_modify := time.Unix(0, mtime).Format(http.TimeFormat)
	modify := r.Header.Get("If-Modified-Since")
	if modify != "" && modify == last_modify {
		wr.Header().Set("Content-Type", mime)
		wr.Header().Set("Last-Modified", last_modify)
		wr.Header().Set("Accept-Ranges", "bytes")
		wr.Header().Set("Etag", sha1)
		status := http.StatusNotModified
		wr.WriteHeader(status)
		direct_res = true
		return
	}

	reslen := e - s + 1

	wr.Header().Set("Content-Range", "bytes "+
		strconv.FormatInt(s, 10)+"-"+strconv.FormatInt(e, 10)+"/"+
		strconv.FormatInt(ctlen, 10))

	wr.Header().Set("Content-Length", strconv.FormatInt(int64(reslen), 10))
	wr.Header().Set("Content-Type", mime)
	wr.Header().Set("Accept-Ranges", "bytes")

	wr.Header().Set("Server", "efs")
	wr.Header().Set("Last-Modified", time.Unix(0, mtime).Format(http.TimeFormat))
	wr.Header().Set("Etag", sha1)
	status := http.StatusPartialContent
	wr.WriteHeader(status)
	if r.Method == "HEAD" || r.Method == "head" {
		direct_res = true
		return
	}
	return
}

func (b *Efs) res_copy_firstindex_data(wr http.ResponseWriter,
	resdata io.ReadCloser, firstseek int64) error {
	var (
		err error
		buf []byte
	)
	defer resdata.Close()
	if buf, err = ioutil.ReadAll(resdata); err != nil {
		log.Errorf("downloadSlice() Read Slice error(%s)", err.Error())
		return err
	}
	sreader := bytes.NewReader(buf[firstseek:])
	_, err = io.Copy(wr, sreader)

	return err

}

func (b *Efs) res_copy_endindex_data(wr http.ResponseWriter,
	resdata io.ReadCloser, lastseek int64) error {
	defer resdata.Close()
	_, err := io.CopyN(wr, resdata, lastseek+1)
	return err

}

func (b *Efs) res_copy_firstendindex_data(wr http.ResponseWriter,
	resdata io.ReadCloser, firstseek, lastseek int64) error {
	var (
		err error
		buf []byte
	)
	defer resdata.Close()
	if buf, err = ioutil.ReadAll(resdata); err != nil {
		log.Errorf("downloadSlice() Read Slice error(%s)", err.Error())
		return err
	}
	sreader := bytes.NewReader(buf[firstseek : lastseek+1])
	_, err = io.Copy(wr, sreader)
	return err
}

func (b *Efs) get_range_response_data(res meta.SliceResponse, wr http.ResponseWriter,
	s, e int64) (err error) {
	var (
		rd                                 [10]chan *Responsedata
		indexkey, aheadindex, chanindex, i int
		responsedata                       *Responsedata
		blocks                             int
	)
	for i := 0; i < 10; i++ {
		rd[i] = make(chan *Responsedata)
	}

	sIndex := int(s / int64(b.c.SliceFileSize))
	eIndex := int(e / int64(b.c.SliceFileSize))

	firstseek := s % b.c.SliceFileSize
	lastseek := e % b.c.SliceFileSize

	blocks = eIndex - sIndex
	chanindex = 0
	//log.Errorf("first seek %d lastseek %d,sindex %d eindex %d", firstseek, lastseek, sIndex, eIndex)

	if blocks > 10 {
		for i = sIndex; i < sIndex+10; i++ {
			if i < 10 {
				go b.get_first_resdata_to_queue(res.Res[i], rd[chanindex])
			} else {
				go b.get_ahead_data(res.Keys[i], rd[chanindex])
			}

			chanindex++
		}
	} else {
		for i = sIndex; i < eIndex+1; i++ {
			if i < 10 {
				go b.get_first_resdata_to_queue(res.Res[i], rd[chanindex])
			} else {
				go b.get_ahead_data(res.Keys[i], rd[chanindex])
			}
			chanindex++
		}
	}

	//接收数据响应和预读
	aheadindex = i
	chanindex = 0
	for i = sIndex; i < eIndex+1; i++ {

		if chanindex > 9 {
			chanindex = 0
		}
		responsedata = <-rd[chanindex]
		if responsedata.err == nil {
			if i == sIndex && i == eIndex {
				err = b.res_copy_firstendindex_data(wr, responsedata.data, firstseek, lastseek)
				//log.Errorf("firstend")
			} else if i == sIndex {
				err = b.res_copy_firstindex_data(wr, responsedata.data, firstseek)
				//log.Errorf("first")
			} else if i == eIndex {
				//log.Errorf("end")
				err = b.res_copy_endindex_data(wr, responsedata.data, lastseek)
			} else {
				//log.Errorf("middle")
				err = b.res_copy_data(wr, responsedata.data)
			}
			if err != nil {
				log.Errorf("reponse copy data failed %v", err)
				log.Errorf("index key=%d aheadindex=%d chanindex=%d", indexkey, aheadindex, chanindex)
				break
			}
			if aheadindex > eIndex {
				//nothing
			} else {
				go b.get_ahead_data(res.Keys[aheadindex], rd[chanindex])
				aheadindex++
			}

		} else {
			log.Errorf("get needle key %d failed %v", res.Keys[indexkey], err)
			break
		}
		chanindex++
	}

	//接收完所有的chan,再close.
	indexkey = i + 1
	for i = indexkey; i < aheadindex; i++ {
		chanindex++
		if chanindex > 9 {
			chanindex = 0
		}
		_ = <-rd[chanindex]
	}

	for i := 0; i < 10; i++ {
		close(rd[i])
	}
	return
}

func (b *Efs) GetRangeStream(ekey string, rangeStart int64, rangeEnd *int64,
	wr http.ResponseWriter, r *http.Request) (err error) {
	var (
		res   meta.SliceResponse
		ctlen int64
	)

	if res, err = b.getfilefromdirectory(ekey); err != nil {
		log.Errorf("get needle keys from directory failed %v", err)
		return
	}

	if ctlen, err = strconv.ParseInt(res.Fsize, 10, 64); err != nil {
		log.Errorf("Efs GetRangeStream() directory parseInt res.Fsize(%v) error", res.Fsize)
		err = errors.ErrServerFailed
		return
	}
	if *rangeEnd >= ctlen {
		*rangeEnd = ctlen - 1
	}
	if rangeStart > ctlen-1 {
		err = errors.ErrParam
		return
	}

	//判断method 是否为head 和判断If-Modified-Since
	direct_res := b.resgetrangeheader(res, rangeStart, *rangeEnd, ctlen, wr, r)
	if direct_res {
		return
	}

	err = b.get_range_response_data(res, wr, rangeStart, *rangeEnd)
	return

}

func (b *Efs) sendFileToStore(res *meta.Response, buf *[]byte, host string, ch *chan meta.StoreRet) {
	var (
		params = url.Values{}
		uri    string
		sRet   meta.StoreRet
		err    error
	)

	params.Set("key", strconv.FormatInt((*res).Key, 10))
	params.Set("cookie", strconv.FormatInt(int64((*res).Cookie), 10))
	params.Set("vid", strconv.FormatInt(int64((*res).Vid), 10))
	uri = fmt.Sprintf(_storeUploadApi, host)
	if err = Http("POST", uri, params, *buf, &sRet); err != nil {
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

func (b *Efs) StoreFileDel(host string, key int64, cookie int32, vid int32) {
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
	if err = Http("POST", uri, params, nil, &sRet); err != nil {
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

func (b *Efs) getFileToStoreResponse(res *meta.Response, ch *chan meta.StoreRet) (fsRes *meta.StoreRets, err error) {
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

	log.Info("getFileToStoreResponse() OK:  %d, %d, %d", (*res).Key, (*res).Cookie, (*res).Vid)
	return
}

// dispatcher
func (b *Efs) Dispatcher(ekey string, lastVid int32, overWriteFlag, replication int) (resRet *meta.Response, err error) {
	var (
		params = url.Values{}
		uri    string
		res    meta.Response
	)
	resRet = &res

	params.Set("ekey", ekey)
	params.Set("lastvid", strconv.Itoa(int(lastVid)))
	params.Set("overwrite", strconv.Itoa(overWriteFlag))
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

	return
}

// Upload Store
func (b *Efs) UploadStore(res *meta.Response, buf []byte) (err error) {
	var (
		host      string
		storeRets *meta.StoreRets
		retry_n   int
	)

	for {
		var ch chan meta.StoreRet
		ch = make(chan meta.StoreRet)

		for _, host = range res.Stores {
			go b.sendFileToStore(res, &buf, host, &ch)
		}
		if storeRets, err = b.getFileToStoreResponse(res, &ch); err != nil {
			//delete upload ok store file
			for _, s := range storeRets.SRets {
				if s.Err == nil {
					b.StoreFileDel(s.Host, res.Key, res.Cookie, res.Vid)
				}
			}

			if retry_n > 0 {
				log.Errorf("efs.uploadstore error(%s),key:%d cookie:%d vid:%d",
					err.Error(), res.Key, res.Cookie, res.Vid)
				return
			} else {
				log.Warningf("UploadStore failed,badvid =%d :retry", res.Vid)
				retry_n++
				continue
			}
		}

		log.Infof("efs.uploadstore key:%d cookie:%d vid:%d", res.Key, res.Cookie, res.Vid)
		break
	}
	return
}

// upload directory
func (b *Efs) UploadDirectory(ekey, mime, sha1 string, filesize int, nkey int64,
	vid int32, cookie int32, overWriteFlag int) (oFileSize int64, err error) {
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

// slice Upload Mkblk
func (b *Efs) Mkblk(ekey, mime, sha1 string, filesize int, nkey int64, vid int32, cookie int32) (ret *meta.PMkblkRetOK, err error) {
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

// slice Upload Bput
func (b *Efs) Bput(ekey, ctx string, id string, offset int64, mime string, sha1 string,
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

	log.Infof("efs.Bput key:%d cookie:%d vid:%d", res.Key, res.Cookie, res.Vid)

	return
}

// slice Upload Mkfile
func (b *Efs) Mkfile(ekey, bucket, filename string, overWriteFlag int, id string,
	filesize int64, mime string, buf string) (ret meta.PMkfileRetOK, oFileSize int64, err error) {
	var (
		params = url.Values{}
		uri    string
		res    meta.Response
	)
	//params is map
	params.Set("ekey", ekey)
	params.Set("mime", mime)
	params.Set("id", id)
	params.Set("filesize", strconv.FormatInt(int64(filesize), 10))
	params.Set("body", buf)
	params.Set("overwrite", strconv.Itoa(overWriteFlag))

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
	ret.Key = filename

	oFileSize, _ = strconv.ParseInt(res.OFSize, 10, 32)

	log.Infof("efs.mkfile bucket:%s filename:%s key:%d cookie:%d vid:%d", bucket, filename, res.Key, res.Cookie, res.Vid)
	return
}

// Delete
func (b *Efs) Delete(ekey, bucket, filename string) (oFileSize int64, err error) {
	var (
		params = url.Values{}
		//host   string
		uri string
		res meta.SliceResponse
	//	sRet   meta.StoreRet
	)
	params.Set("ekey", ekey)
	uri = fmt.Sprintf(_directoryDelApi, b.c.EfsAddr)

	if err = Http("POST", uri, params, nil, &res); err != nil {
		log.Errorf("Efs Delete() called uri(%s) directory Http error(%v)", uri, err)
		return
	}
	//delete meta file,no delete needle
	if res.Ret == errors.RemoveLinkOK {
		return
	}
	if res.Ret != errors.RetOK {
		log.Errorf("Efs Delete() called uri(%s), directory res.Ret:(%d), params(%v)", uri, res.Ret, params)
		err = errors.Error(res.Ret)
		return
	}
	oFileSize, _ = strconv.ParseInt(res.Fsize, 10, 64)

	return
}

// DeleteTmp
func (b *Efs) DeleteTmp_del(ekey string, id string) (err error) {
	var (
		params = url.Values{}
		host   string
		uri    string
		res    meta.SliceResponse
		sRet   meta.StoreRet
	)
	params.Set("ekey", ekey)
	params.Set("id", id)
	uri = fmt.Sprintf(_directoryDelTmpApi, b.c.EfsAddr)
	//request directory get metadata
	//mycontinue 1
	if err = Http("POST", uri, params, nil, &res); err != nil {
		log.Errorf("Delete called Http error(%v)", err)
		return
	}
	//delete meta file,no delete needle
	if res.Ret == errors.RemoveLinkOK {
		return
	}
	if res.Ret != errors.RetOK {
		log.Errorf("http.Get directory res.Ret: %d %s", res.Ret, uri)
		err = errors.Error(res.Ret)
		return
	}

	params = url.Values{}
	for _, v := range res.Res {
		for _, host = range v.Stores {
			params.Set("key", strconv.FormatInt(v.Key, 10))
			params.Set("vid", strconv.FormatInt(int64(v.Vid), 10))
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
		}
	}
	return
}

func (b *Efs) Move(srcekey, destekey string, overWriteFlag int) (err error) {
	var (
		uri    string
		res    meta.Response
		params = url.Values{}
	)
	params.Set("srcekey", srcekey)
	params.Set("destekey", destekey)
	params.Set("overwrite", strconv.Itoa(overWriteFlag))
	uri = fmt.Sprintf(_directoryMoveApi, b.c.EfsAddr)
	//request directory get metadata
	if err = Http("POST", uri, params, nil, &res); err != nil {
		log.Errorf("Efs Move() POST called uri(%s) Http error(%v)", uri, err)
		return
	}
	if res.Ret != errors.RetOK {
		log.Errorf("Efs Move() http.POST called uri(%s), directory res.Ret(%d), params(%v)", uri, res.Ret, params)
		err = errors.Error(res.Ret)
		return
	}
	return
}

func (b *Efs) Copy(srcekey, destekey string, overWriteFlag int) (err error) {
	var (
		uri    string
		res    meta.Response
		params = url.Values{}
	)
	params.Set("srcekey", srcekey)
	params.Set("destekey", destekey)
	params.Set("overwrite", strconv.Itoa(overWriteFlag))
	uri = fmt.Sprintf(_directoryCopyApi, b.c.EfsAddr)
	//request directory get metadata
	if err = Http("POST", uri, params, nil, &res); err != nil {
		log.Errorf("Efs Copy() POST called uri(%s) Http error(%v)", uri, err)
		return
	}
	if res.Ret != errors.RetOK {
		log.Errorf("Efs Copy() http.POST called uri(%s), directory res.Ret(%d), params(%v)", uri, res.Ret, params)
		err = errors.Error(res.Ret)
		return
	}
	return
}

func (b *Efs) Chgm(ekey string, newmime string) (err error) {
	var (
		uri    string
		res    meta.Response
		params = url.Values{}
	)
	params.Set("ekey", ekey)
	params.Set("mime", newmime)
	uri = fmt.Sprintf(_directoryChgmApi, b.c.EfsAddr)
	//request directory get metadata
	if err = Http("POST", uri, params, nil, &res); err != nil {
		log.Errorf("Efs Chgm() POST called uri(%s) Http error(%s)", uri, err.Error())
		return
	}
	if res.Ret != errors.RetOK {
		log.Errorf("Efs Chgm() POST called uri(%s)no OK res.Ret(%d), params(%v)", uri, res.Ret, params)
		err = errors.Error(res.Ret)
		return
	}
	return
}

func (b *Efs) Stat(ekey, bucket, filename string) (fsize string, mtime int64, sha1, mime string, err error) {
	var (
		uri    string
		res    meta.Response
		params = url.Values{}
	)
	params.Set("ekey", ekey)
	uri = fmt.Sprintf(_directoryStatApi, b.c.EfsAddr)
	//request directory get metadata
	if err = Http("GET", uri, params, nil, &res); err != nil {
		log.Errorf("Efs Stat() POST called uri(%s) Http error(%s)", uri, err.Error())
		return
	}
	if res.Ret != errors.RetOK {
		log.Errorf("Efs Stat() GET called uri(%s)no OK res.Ret(%d), params(%v)", uri, res.Ret, params)
		err = errors.Error(res.Ret)
		return
	}
	//file meta last modify time
	fsize = res.Fsize
	mtime = res.MTime
	sha1 = res.Sha1
	mime = res.Mine
	return
}

func (b *Efs) List(bucket, limit, prefix, delimiter, marker string) (flist *meta.PFListRetOK, err error) {
	var (
		uri    string
		res    meta.FileListResponse
		tflist meta.PFListRetOK
		params = url.Values{}
	)
	params.Set("ekey", bucket)
	params.Set("limit", limit)
	params.Set("prefix", prefix)
	params.Set("delimiter", delimiter)
	params.Set("marker", marker)
	uri = fmt.Sprintf(_directoryListApi, b.c.EfsAddr)
	//request directory get metadata
	if err = Http("POST", uri, params, nil, &res); err != nil {
		log.Errorf("Efs list() called uri(%s) Http error(%s)", uri, err.Error())
		return
	}
	if res.Ret != errors.RetOK {
		log.Errorf("Efs list() called uri(%s), directory resp no OK res.Ret(%d), params(%v)", uri, res.Ret, params)
		err = errors.Error(res.Ret)
		return
	}
	/*
		flist = res.Flist
		flist.CommonPrefixes = make([]string, len(res.Flist.CommonPrefixes))
		copy(flist.CommonPrefixes, res.Flist.CommonPrefixes)
		flist.Items = make([]meta.Item, len(res.Flist.Items))
		copy(flist.Items, res.Flist.Items)*/
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

	flist = &tflist
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

// BucketCreat
func (b *Efs) BucketCreate(ekey string, families string) (err error) {
	var (
		uri    string
		res    meta.BucketCreatResponse
		params = url.Values{}
	)
	params.Set("ekey", ekey)
	params.Set("families", families)
	uri = fmt.Sprintf(_bucketcreatApi, b.c.EfsAddr)
	if err = Http("POST", uri, params, nil, &res); err != nil {
		log.Errorf("Efs BucketCreate() called uri(%s) Http error(%v)", uri, err)
		return
	}
	if res.Ret != errors.RetOK {
		log.Errorf("Efs BucketCreate() called uri(%s) directory res.Ret(%d), params(%v)", uri, res.Ret, params)
		err = errors.Error(res.Ret)
		return
	}

	return
}

// bucket reanme
func (b *Efs) BucketRename(bucket_src, bucket_dst string) (err error) {
	var (
		uri    string
		res    meta.BucketRenameResponse
		params = url.Values{}
	)
	params.Set("srcekey", bucket_src)
	params.Set("destekey", bucket_dst)
	uri = fmt.Sprintf(_bucketrenameApi, b.c.EfsAddr)
	//request directory get metadata
	if err = Http("POST", uri, params, nil, &res); err != nil {
		log.Errorf("Efs BucketRename() called uri(%s) Http error(%v)", uri, err)
		return
	}
	if res.Ret != errors.RetOK {
		log.Errorf("Efs BucketRename() called uri(%s), directory res.Ret(%d), params(%v)", uri, err, params)
		err = errors.Error(res.Ret)
		return
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

// bucket stat
func (b *Efs) BucketStat(ekey string) (exist bool, err error) {
	var (
		uri    string
		res    meta.BucketStatResponse
		params = url.Values{}
	)
	params.Set("ekey", ekey)
	uri = fmt.Sprintf(_bucketStatApi, b.c.EfsAddr)
	//request directory get metadata
	if err = Http("POST", uri, params, nil, &res); err != nil {
		log.Errorf("Efs BucketStat() called uri(%s) Http error(%v)", uri, err)
		return
	}
	if res.Ret != errors.RetOK {
		log.Errorf("Efs BucketStat() called uri(%s) directory res.Ret(%d), params(%v)", uri, res.Ret, params)
		err = errors.Error(res.Ret)
		return
	}
	if res.Exist {
		exist = true
	}
	return
}
