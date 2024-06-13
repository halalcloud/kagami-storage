package bucket

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"efs/authacess/conf"
	"efs/libs/meta"
	log "efs/log/glog"
	"encoding/base64"
	b64 "encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
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
		Timeout:   5 * time.Second,
	}
)

const (
	_http_bucket_get = "http://%s/bgetbyak"

	// status bit
	_privateReadBit  = 0
	_privateWriteBit = 1
	// status
	_public           = int(0)
	_privateRead      = int(1 << _privateReadBit)
	_privateWrite     = int(1 << _privateWriteBit)
	_privateReadWrite = int(_privateRead | _privateWrite)
)

/*
type Item struct {
	Bname       string
	RegionId    int
	Keysecret   string
	Key         string
	Imgsource   string
	Propety     int
	Dnsname     string
	Replication int
	Ctime       string
	Timeout     int64
}
*/
type BucketInfo struct {
	Buckets map[string][]*meta.Bucket_item
	c       *conf.Config
	block   *sync.RWMutex
}

func Http(method, uri string, params url.Values, header url.Values, buf []byte, res interface{}) (err error, code int) {
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
	code = 200
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
			//req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
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

	for key, _ := range header {
		req.Header.Set(key, header.Get(key))
	}
	if resp, err = _client.Do(req); err != nil {
		code = 400
		log.Errorf("_client.Do(%s) error(%v)", ru, err)
		return
	}
	defer resp.Body.Close()
	if res == nil {
		return
	}
	if resp.StatusCode != http.StatusOK {
		log.Errorf("_client.Do(%s) status: %d", ru, resp.StatusCode)
		code = resp.StatusCode
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

func Bucket_init(c *conf.Config) (info *BucketInfo) {
	info = &BucketInfo{}
	info.c = c
	info.Buckets = make(map[string][]*meta.Bucket_item)
	info.block = &sync.RWMutex{}
	return
}

func (bs *BucketInfo) Public(propety int, read bool) bool {
	if read {
		return propety&_privateRead == 0
	} else {
		return propety&_privateWrite == 0
	}
}

func gettoken(ak, sk, data string) (auth string) {
	mac := hmac.New(sha1.New, []byte(sk))
	mac.Write([]byte(data))
	bk := base64.URLEncoding.EncodeToString(mac.Sum(nil))
	auth = ak + ":" + bk
	return
}
func (bs *BucketInfo) getbycache(bname, ak string) (item *meta.Bucket_item) {
	bs.block.RLock()
	defer bs.block.RUnlock()
	buckets, ok := bs.Buckets[bname]
	if ok {
		for _, tmp_item := range buckets {
			if tmp_item.Key == ak {
				item = tmp_item
				break
			}
		}
	}
	return
}

func item_copy(ritem, binfo *meta.Bucket_item) {
	ritem.Imgsource = binfo.Imgsource
	ritem.Key = binfo.Key
	ritem.Keysecret = binfo.Keysecret
	ritem.Propety = binfo.Propety
	ritem.Replication = binfo.Replication
	ritem.Uid = binfo.Uid
}

func (bs *BucketInfo) add_item_incache(bname string, item *meta.Bucket_item) {
	buckets, ok := bs.Buckets[bname]
	if ok {
		buckets = append(buckets, item)
	} else {
		var binfos []*meta.Bucket_item
		binfos = append(binfos, item)
		bs.Buckets[bname] = binfos
	}
}

func (bs *BucketInfo) Getkey(bname, ak string) (acesskey, secretkey string,
	propety int, imgsource string, replication, uid int, err error, retcode int) {
	var (
		params = url.Values{}
		header = url.Values{}
		binfo  = &meta.Bucket_item{}
		ritem  *meta.Bucket_item
		flag   bool
	)
	retcode = 200
	flag = false
	ritem = bs.getbycache(bname, ak)
	if ritem != nil {
		//log.Errorf("time now %d cache time %d bname %s", time.Now().Unix(), ritem.Timeout, bname)
		if (time.Now().Unix() - ritem.Timeout) > int64(bs.c.BcacheTimeout) {
			flag = true
		} else {
			acesskey = ritem.Key
			secretkey = ritem.Keysecret
			imgsource = ritem.Imgsource
			replication = ritem.Replication
			propety = ritem.Propety
			uid = ritem.Uid
			return
		}
	}

	estring := b64.URLEncoding.EncodeToString([]byte(bname))
	header.Set("ekey", estring)
	header.Set("Ak", ak)
	token := gettoken(bs.c.BucketAk, bs.c.BucketSk, "/bgetbyak")
	header.Set("Authorization", token)
	geturi := fmt.Sprintf(_http_bucket_get, bs.c.BucketAddr)
	if err, retcode = Http("get", geturi, params, header, nil, binfo); err != nil {
		log.Errorf("get bucket url %s failed", geturi)
		err = errors.New("get bucket info failed")
		return
	} else {
		if retcode != 200 {
			log.Errorf("get bucket url %s failed,retcode=%d", geturi, retcode)
			err = errors.New("get bucket info failed")
			return
		}
	}

	acesskey = binfo.Key
	secretkey = binfo.Keysecret
	imgsource = binfo.Imgsource
	propety = binfo.Propety
	replication = binfo.Replication
	uid = binfo.Uid
	bs.block.Lock()
	defer bs.block.Unlock()
	if flag {
		item_copy(ritem, binfo)
		ritem.Timeout = time.Now().Unix()
	} else {
		binfo.Timeout = time.Now().Unix()
		bs.add_item_incache(bname, binfo)
	}

	return

}
