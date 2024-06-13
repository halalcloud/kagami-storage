package pfop

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"kagamistoreage/authacess/conf"
	"kagamistoreage/authacess/httpcli"
	log "kagamistoreage/log/glog"
	"net/http"
	"net/url"
	"strings"
)

const (
	_pfop = "http://%s/pfop"
)

type Pfop struct {
	c *conf.Config
}

func New(c *conf.Config) (p *Pfop) {
	p = &Pfop{}
	p.c = c
	return
}

func (p *Pfop) Add(bucket, key, fops, notifyURL, pipeline string) (pTaskId string, code int, err error) {
	var (
		params   url.Values
		req      *http.Request
		resp     *http.Response
		enc      string
		data     []byte
		pfopTask *PfopTask
	)
	code = http.StatusOK
	url := fmt.Sprintf(_pfop, p.c.PfopAddr)

	b64Bucket := base64.URLEncoding.EncodeToString([]byte(bucket))
	b64Key := base64.URLEncoding.EncodeToString([]byte(key))
	b64Fops := base64.URLEncoding.EncodeToString([]byte(fops))
	b64NURL := base64.URLEncoding.EncodeToString([]byte(notifyURL))

	params.Set("bucket", b64Bucket)
	params.Set("key", b64Key)
	params.Set("fops", b64Fops)
	params.Set("notifyURL", b64NURL)
	params.Set("force", "0")
	params.Set("pipeline", pipeline)
	enc = params.Encode()

	if req, err = http.NewRequest("POST", url, strings.NewReader(enc)); err != nil {
		code = http.StatusInternalServerError
		log.Errorf("make new request error:%s\n", err.Error())
		return
	}

	if resp, err = httpcli.HttpReq(req); err != nil {
		code = http.StatusInternalServerError
		log.Errorf("http req error:%s, bucket:%s, key:%s, fops:%s, notifyURL:%s, pipeline:%s \n",
			err.Error(), bucket, key, fops, notifyURL, pipeline)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		code = resp.StatusCode
		switch resp.StatusCode {
		case http.StatusBadRequest:
			err = errors.New("bad request")
		case http.StatusNotFound:
			err = errors.New("resource no exist")
		case 599:
			err = errors.New("internal server error")
		default:
			err = errors.New("internal server error")
		}
		log.Errorf("http req error:%s, bucket:%s, key:%s, fops:%s, notifyURL:%s, pipeline:%s \n",
			err.Error(), bucket, key, fops, notifyURL, pipeline)
		return
	}

	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		code = http.StatusInternalServerError
		log.Errorf("read resp data error:%s, bucket:%s, key:%s, fops:%s, notifyURL:%s, pipeline:%s \n",
			err.Error(), bucket, key, fops, notifyURL, pipeline)
		return
	}

	if err = json.Unmarshal(data, pfopTask); err != nil {
		code = http.StatusInternalServerError
		log.Errorf("json unmarshal error:%s, bucket:%s, key:%s, fops:%s, notifyURL:%s, pipeline:%s \n",
			err.Error(), bucket, key, fops, notifyURL, pipeline)
		return
	}

	pTaskId = pfopTask.PersistentId
	return
}
