package dataprocess

import (
	"efs/authacess/conf"
	"efs/authacess/httpcli"
	log "efs/log/glog"
	"encoding/base64"
	"encoding/json"
	"io/ioutil"
	"net/http"
)

const (
	EKEY_DELIMITER = ":"
)

type DataProcess struct {
	c *conf.Config
}

func New(conf *conf.Config) (dp *DataProcess) {
	dp = &DataProcess{c: conf}
	return
}

type imageInfo struct {
	Size       int64  `json:"size"`
	Format     string `json:"format"`
	Width      int    `json:"width"`
	Height     int    `json:"height"`
	ColorModel string `json:"colorModel"`
}

func (dp *DataProcess) GetImageInfo(bucket, key string) (size int64, format, colorModel string,
	width, height int, err error) {
	var (
		fop    = "imageInfo"
		ekey   string
		req    *http.Request
		resp   *http.Response
		urlStr string
		ii     *imageInfo
		data   []byte
	)

	ekey = base64.URLEncoding.EncodeToString([]byte(bucket + EKEY_DELIMITER + key))
	urlStr = "http://" + dp.c.DpProxy + "/" + ekey + "?f=" + fop

	if req, err = http.NewRequest("GET", urlStr, nil); err != nil {
		log.Errorf("GetImageInfo make request url:%s ekey:%s err:%s\n", urlStr, ekey, err)
		return
	}

	if resp, err = httpcli.HttpReq(req); err != nil {
		log.Errorf("GetImageInfo req url:%s ekey:%s err:%s\n", urlStr, ekey, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Errorf("GetImageInfo url:%s ekey:%s status:%d wrong\n", urlStr, ekey, resp.StatusCode)
		return
	}

	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		log.Errorf("GetImageInfo url:%s read data err:%s\n", urlStr, err)
		return
	}

	if err = json.Unmarshal(data, &ii); err != nil {
		log.Errorf("GetImageInfo url:%s json decode data err:%s\n", err)
		return
	}

	size, format, colorModel, width, height = ii.Size, ii.Format, ii.ColorModel, ii.Width, ii.Height

	return
}

func (dp *DataProcess) GetAVInfo(bucket, key string) (err error) {
	var (
		fop    = "avinfo"
		ekey   string
		urlStr string
		req    *http.Request
		resp   *http.Response
		data   []byte
	)

	ekey = base64.URLEncoding.EncodeToString([]byte(bucket + EKEY_DELIMITER + key))
	urlStr = "http://" + dp.c.DpProxy + "/" + ekey + "?f=" + fop

	if req, err = http.NewRequest("GET", urlStr, nil); err != nil {
		log.Errorf("GetAVInfo url:%s ekey:%s make request err:%s\n", urlStr, ekey, err)
		return
	}

	if resp, err = httpcli.HttpReq(req); err != nil {
		log.Errorf("GetAVInfo url:%s ekey:%s response err:%s\n", urlStr, ekey, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Errorf("GetAVInfo url:%s ekey%s response code:%d wrong\n", urlStr, ekey, resp.StatusCode)
		return
	}

	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		log.Errorf("GetAVInfo url:%s ekey:%s read resp data err:%s\n", urlStr, ekey, err)
		return
	}

	if err = json.Unmarshal(data, nil); err != nil { //TODO
		log.Errorf("GetAVInfo url:%s ekey:%s json decode err:%s\n", urlStr, ekey, err)
		return
	}

	return
}
