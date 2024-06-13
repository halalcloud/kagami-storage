package bucket

import (
	"crypto/hmac"
	"crypto/sha1"
	b64 "encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"kagamistoreage/gos_sync/libs/httpcli"
	log "kagamistoreage/log/glog"
	"net/http"
	"strings"
)

const (
	BUCKET_DELIMITER = "_"
	_BUCKET_SERVER   = "http://%s/bget"
	_BUCKET_DELETE   = "http://%s/bdelete"
	_BUCKET_CREATE   = "http://%s/bcreate"
)

var (
	mServer string
	mAk     string
	mSk     string
	sServer string
	sAk     string
	sSk     string
	//items  map[string]*itemMeta
	//lock   *sync.RWMutex
)

type bucketItem struct {
	Uid       int    `json:"uid"`
	Bname     string `json:"bucketname"`
	Dnsname   string `json:"dnsname"`
	Key       string `json:"key"`
	Keysecret string `json:"keysecret"`
}

func New(mserver, mak, msk, sserver, sak, ssk string) {
	mServer, mAk, mSk, sServer, sAk, sSk = mserver, mak, msk, sserver, sak, ssk
}

func Create(ubucket, jsonStr string) (err error) {
	var (
		bucket, token, urlStr string
		i                     int
		req                   *http.Request
		resp                  *http.Response
	)
	i = strings.Index(ubucket, BUCKET_DELIMITER)
	_, bucket = ubucket[:i], ubucket[i+1:]
	token = mktoken(sAk, sSk, "/bcreate")
	urlStr = fmt.Sprintf(_BUCKET_CREATE, sServer)

	if req, err = http.NewRequest("GET", urlStr, strings.NewReader(jsonStr)); err != nil {
		log.Errorf("bucket delete request err:%s\n", err)
		return
	}
	req.Header.Add("ekey", b64.URLEncoding.EncodeToString([]byte(bucket)))
	req.Header.Add("Authorization", token)

	if resp, err = httpcli.Req(req); err != nil {
		log.Errorf("http resp err:%s\n", err)
		return
	}

	if resp.StatusCode != http.StatusOK {
		log.Errorf("resp statuscode:%d err\n", resp.StatusCode)
		err = errors.New("http resp code err")
		return
	}

	return
}

func Delete(ubucket string) (err error) {
	var (
		uidStr, bucket, token, urlStr string
		i                             int
		req                           *http.Request
		resp                          *http.Response
	)
	i = strings.Index(ubucket, BUCKET_DELIMITER)
	uidStr, bucket = ubucket[:i], ubucket[i+1:]
	token = mktoken(sAk, sSk, "/bdelete")
	urlStr = fmt.Sprintf(_BUCKET_DELETE, sServer)

	if req, err = http.NewRequest("GET", urlStr, nil); err != nil {
		log.Errorf("bucket delete request err:%s\n", err)
		return
	}
	req.Header.Add("ekey", b64.URLEncoding.EncodeToString([]byte(bucket)))
	req.Header.Add("Uid", uidStr)
	req.Header.Add("Authorization", token)

	if resp, err = httpcli.Req(req); err != nil {
		log.Errorf("bucket response err:%s\n", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Errorf("bucket delete respcode:%d err\n", resp.StatusCode)
		err = errors.New("bucket delete error")
		return
	}

	return
}

func GetASKey(ubucket string) (ak, sk, dnsname string, err error) {
	var (
		uidStr, bucket, token, urlStr string
		i                             int
		req                           *http.Request
		resp                          *http.Response
		data                          []byte
		bitem                         *bucketItem
	)

	i = strings.Index(ubucket, BUCKET_DELIMITER)
	uidStr = ubucket[:i]
	bucket = ubucket[i+1:]
	token = mktoken(mAk, mSk, "/bget")
	urlStr = fmt.Sprintf(_BUCKET_SERVER, mServer)

	if req, err = http.NewRequest("GET", urlStr, nil); err != nil {
		log.Errorf("bucket request err:%s\n", err)
		return
	}
	req.Header.Add("ekey", b64.URLEncoding.EncodeToString([]byte(bucket)))
	req.Header.Add("Uid", uidStr)
	req.Header.Add("Authorization", token)

	if resp, err = httpcli.Req(req); err != nil {
		log.Errorf("bucket response err:%s\n", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Errorf("resp code:%d err\n", resp.StatusCode)
		err = errors.New("resp code error")
		return
	}

	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		log.Errorf("read resp data err:%s\n", err)
		return
	}

	if err = json.Unmarshal(data, &bitem); err != nil {
		log.Errorf("json decode err:%s\n", err)
		return
	}

	ak, sk, dnsname = bitem.Key, bitem.Keysecret, bitem.Dnsname
	return
}

func mktoken(ak, sk, data string) (token string) {
	mac := hmac.New(sha1.New, []byte(sk))
	mac.Write([]byte(data))
	bk := b64.URLEncoding.EncodeToString(mac.Sum(nil))
	token = ak + ":" + bk
	return
}
