package gos

import (
	"crypto/sha1"
	b64 "encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	bkt "kagamistoreage/gos_sync/bucket"
	"kagamistoreage/gos_sync/libs/httpcli"
	"kagamistoreage/gos_sync/task"
	log "kagamistoreage/log/glog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"ecloud_gosdk.v1/conf"
	"ecloud_gosdk.v1/ecloud"
	"ecloud_gosdk.v1/ecloudcli"
)

const (
	EKEY_DELIMITER   = ":"
	PARAM_DELIMITER  = "---"
	BUCKET_DELIMITER = "_"

	UPLOAD_OP            = "/r/upload"
	MKFILE_OP            = "/r/mkfile"
	CHGM_OP              = "/r/chgm"
	MOVE_OP              = "/r/move"
	COPY_OP              = "/r/copy"
	DELETE_AFTER_DAYS_OP = "/r/deleteAfter"
	DELETE_OP            = "/r/delete"
	FETCH_OP             = "/r/fetch"
	PREFETCH_OP          = "/r/prefetch"
	BCREATE_OP           = "/b/create"
	BDELETE_OP           = "/b/delete"

	OP_SUCCESS = "success"
	OP_FAIL    = "fail"
	TMP_DIR    = "/tmp"
)

type Gos struct {
	Workers           int
	SliceSize         int64
	DB                string
	Task_Collection   string
	Finish_Collection string
	PxyMSrvAddr       string
	UpHost            string
	MgHost            string

	T   *task.Task
	TRD int64 //second
}

func New(w int, t *task.Task, trd, ssize int64, db, tc, fc, pms, uh, mh string) (g *Gos) {
	g = &Gos{Workers: w, T: t, TRD: trd, SliceSize: ssize,
		DB: db, Task_Collection: tc, Finish_Collection: fc, PxyMSrvAddr: pms,
		UpHost: uh, MgHost: mh}
	return
}

func (g *Gos) Start() {
	//start work
	for i := 0; i < g.Workers; i++ {
		go g.work(i)
		log.Infof("start gos worker id:%d\n", i)
	}
}

type recordItem struct {
	Op      string
	Param   string
	AddTime int64
	Ekey    string
}

// work with i task chan
func (g *Gos) work(i int) {
	var (
		ekey, op, param string
		addTime         int64
		n, hv           int
		results         string
		err             error
		errCode         int
		rs              []recordItem
	)
	rs = make([]recordItem, 1)

	for {
	start:
		if err != nil {
			time.Sleep(60 * time.Second)
		}

		n = g.Workers * 2
		if results, err = g.T.GetN(g.DB, g.Task_Collection, "{}", n); err != nil {
			log.Errorf("get results err:%s\n", err)
			continue
		}

		if err = json.Unmarshal([]byte(results), &rs); err != nil {
			log.Errorf("json decode results:%s err:%s\n", results, err)
			continue
		}
		if len(rs) == 0 {
			time.Sleep(5 * time.Second)
			continue
		}

		for _, v := range rs {
			ekey, op, param, addTime = v.Ekey, v.Op, v.Param, v.AddTime

			if time.Now().Unix()-v.AddTime/1000/1000/1000 > g.TRD {
				log.Errorf("TASK OVERTIME op:%s ekey:%s param:%s\n", op, ekey, param)
			}

			hv = int(genValue(ekey))
			if i != hv%g.Workers {
				continue
			}
			log.Infof("go %d run, op:%s ekey:%s param:%s \n", i, op, ekey, param)
			switch op {
			case UPLOAD_OP:
				err, errCode = g.upload(param, "")
				if err != nil {
					if errCode == http.StatusNotFound {
						g.uploadException(ekey)
					} else {
						log.Errorf("UPLOAD PARAM:%s err:%s errCode:%d\n", param, err, errCode)
					}
					goto start
				} else {
					g.taskFinish(ekey, op, param, addTime, OP_SUCCESS)
				}
			case MKFILE_OP:
				err, errCode = g.upload(param, "")
				if err != nil {
					if errCode == http.StatusNotFound {
						g.uploadException(ekey)
					} else {
						log.Errorf("UPLOAD PARAM:%s err:%s\n", param, err)
					}
					goto start
				} else {
					g.taskFinish(ekey, op, param, addTime, OP_SUCCESS)
				}
			case FETCH_OP:
				err, errCode = g.upload(param, "")
				if err != nil {
					if errCode == http.StatusNotFound {
						g.uploadException(ekey)
					} else {
						log.Errorf("UPLOAD PARAM:%s err:%s\n", param, err)
					}
					goto start
				} else {

					g.taskFinish(ekey, op, param, addTime, OP_SUCCESS)
				}
			case PREFETCH_OP:
				err, errCode = g.upload(param, "")
				if err != nil {
					if errCode == http.StatusNotFound {
						g.uploadException(ekey)
					} else {
						log.Errorf("UPLOAD PARAM:%s err:%s\n", param, err)
					}
					goto start
				} else {
					g.taskFinish(ekey, op, param, addTime, OP_SUCCESS)
				}
			case CHGM_OP:
				err = g.chgm(param)
				if err != nil {
					log.Errorf("CHGM PARAM:%s err:%s\n", param, err)
				} else {
					g.taskFinish(ekey, op, param, addTime, OP_SUCCESS)
				}
			case MOVE_OP:
				err = g.move(param)
				if err != nil {
					log.Errorf("MOVE PARAM:%s err:%s\n", param, err)
				} else {
					g.taskFinish(ekey, op, param, addTime, OP_SUCCESS)
				}
			case COPY_OP:
				err = g.copyop(param)
				if err != nil {
					log.Errorf("COPY PARAM:%s err:%s\n", param, err)
				} else {
					g.taskFinish(ekey, op, param, addTime, OP_SUCCESS)
				}
			case DELETE_AFTER_DAYS_OP:
				err = g.deleteAfterDays(param)
				if err != nil {
					log.Errorf("DELETEAFTERDAYS PARAM:%s err:%s\n", param, err)
				} else {
					g.taskFinish(ekey, op, param, addTime, OP_SUCCESS)
				}
			case DELETE_OP:
				err = g.deleteop(param)
				if err != nil {
					log.Errorf("DELETE PARAM:%s err：%s\n", param, err)
				} else {
					g.taskFinish(ekey, op, param, addTime, OP_SUCCESS)
				}
			case BCREATE_OP:
				err = g.bcreate(param)
				if err != nil {
					log.Errorf("BCREATE PARAM:%s err:%s\n", param, err)
				} else {
					g.taskFinish(ekey, op, param, addTime, OP_SUCCESS)
				}
			case BDELETE_OP:
				err = g.bdelete(param)
				if err != nil {
					log.Errorf("BDELETE PARAM:%s err:%s\n", param, err)
				} else {
					g.taskFinish(ekey, op, param, addTime, OP_SUCCESS)
				}
			default:
				log.Errorf("ekey:%s bad op:%s\n", ekey, op)
				err = errors.New("bad op")
			} //switch{}
		} //for{}
	} //for{}
}

// upload fetch prefetch
// da deleteafter
func (g *Gos) uploadParse(param string) (dekey, ubucket, file string, size int64, da int, err error) {
	var (
		data        []byte
		strs, tstrs []string
	)

	if strs = strings.Split(param, PARAM_DELIMITER); len(strs) != 3 && len(strs) != 2 {
		log.Errorf("param:%s format err\n", param)
		err = errors.New("param format error")
		return
	}
	dekey = strs[0]

	if data, err = b64.URLEncoding.DecodeString(strs[0]); err != nil {
		log.Errorf("ekey:%s format err:%s\n", strs[0], err)
		return
	}

	if tstrs = strings.Split(string(data), EKEY_DELIMITER); len(tstrs) != 2 {
		log.Errorf("ekey:%s format err\n", string(data))
		err = errors.New("ekey format err")
		return
	}
	ubucket, file = tstrs[0], tstrs[1]

	if size, err = strconv.ParseInt(strs[1], 10, 64); err != nil {
		log.Errorf("size:%s parseInt err:%s\n", strs[1], err)
		return
	}

	if len(strs) == 3 {
		if da, err = strconv.Atoi(strs[2]); err != nil {
			log.Errorf("da:%s atoi err:%s\n", strs[2], err)
			return
		}
	}

	return
}

// sekey is master cluster ekey
func (g *Gos) upload(param, sekey string) (err error, errCode int) {
	var (
		dekey, bucket, ubucket, file, durl string
		size                               int64
		accessKey, secretKey               string
		i, da                              int
	)

	if dekey, ubucket, file, size, da, err = g.uploadParse(param); err != nil {
		log.Errorf("uploadParse param:%v err:%s\n", param, err)
		return
	}

	if accessKey, secretKey, _, err = bkt.GetASKey(ubucket); err != nil {
		log.Errorf("get bucket:%s ak sk err:%s\n", ubucket, err)
		return
	}

	/*
		if sekey != "" {
			if data, err = b64.URLEncoding.DecodeString(sekey); err != nil {
				log.Errorf("base64 decode ekey:%s err:%s\n", sekey, err)
				return
			}
			if strs = strings.Split(string(data), EKEY_DELIMITER); len(strs) != 2 {
				log.Errorf("ekey:%s format err\n", string(data))
				err = errors.New("ekey format error")
				return
			}
			sfile = strs[1]

			if _, _, _, err = bkt.GetASKey(strs[0]); err != nil {
				log.Errorf("get bucket:%s domain err:%s\n", strs[0], err)
				return
			}
		} else {
			sfile = file
		}
	*/
	/*
		if durl, err = g.downloadUrl(domain, sfile, accessKey, secretKey); err != nil {
			log.Errorf("domain:%s ubucket:%s file:%s make downloadUrl err:%s\n",
				domain, ubucket, file)
			return
		}
	*/
	if sekey != "" {
		dekey = sekey
	}
	durl = g.PxyMSrvAddr

	i = strings.Index(ubucket, BUCKET_DELIMITER)
	_, bucket = ubucket[:i], ubucket[i+1:]

	if size <= g.SliceSize {
		err, errCode = g.directUpload(durl, dekey, bucket, file, size, da, accessKey, secretKey)
	} else {
		err, errCode = g.sliceUpload(durl, dekey, bucket, file, size, da, accessKey, secretKey)
	}

	return
}

func (g *Gos) directUpload(durl, dekey, bucket, file string,
	size int64, da int, ak, sk string) (err error, errCode int) {
	var (
		req  *http.Request
		resp *http.Response
		cnf  = &ecloud.Config{}
	)
	cnf.AccessKey, cnf.SecretKey = ak, sk

	if req, err = http.NewRequest("GET", durl, nil); err != nil {
		log.Errorf("make durl:%s request err:%s\n", durl, err)
		return
	}
	req.Header.Set("ekey", dekey)

	if resp, err = httpcli.Req(req); err != nil {
		log.Errorf("durl:%s response err:%s\n", durl, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Errorf("durl:%s statuscode:%d err\n", durl, resp.StatusCode)
		err, errCode = errors.New("response statuscode err"), resp.StatusCode
		return
	}

	zone := 0
	conf.Zones[zone].UpHosts[0] = g.UpHost
	conf.Zones[zone].MgHosts[0] = g.MgHost

	//upload
	c := ecloud.New(zone, cnf)
	policy := &ecloud.PutPolicy{
		Scope:      bucket + ":" + file,
		Expires:    3600 * 12,
		InsertOnly: 0,
	}
	if da > 0 {
		policy.DeleteAfterDays = da
	}
	token := c.MakeUptoken(policy)
	uploader := ecloudcli.NewUploader(0, nil)
	if err = uploader.Put(nil, nil, token, file, resp.Body, size, nil); err != nil {
		log.Errorf("upload bucket:%s file:%s size:%d err:%s\n", bucket, file, size, err)
		return
	}

	return
}

func (g *Gos) sliceUpload(durl, dekey, bucket, file string, size int64,
	da int, ak, sk string) (err error, errCode int) {
	var (
		req  *http.Request
		resp *http.Response
		cnf  = &ecloud.Config{}
		sra  = &selfReaderAt{fsize: size, fekey: dekey, furl: durl}
		terr error
	)

	zone := 0
	conf.Zones[zone].UpHosts[0] = g.UpHost
	conf.Zones[zone].MgHosts[0] = g.MgHost

	cnf.AccessKey, cnf.SecretKey = ak, sk
	c := ecloud.New(zone, cnf)
	policy := &ecloud.PutPolicy{
		Scope:      bucket + ":" + file,
		Expires:    3600 * 12,
		InsertOnly: 0,
	}
	if da > 0 {
		policy.DeleteAfterDays = da
	}
	token := c.MakeUptoken(policy)
	uploader := ecloudcli.NewUploader(0, nil)
	if err = uploader.Rput(nil, nil, token, file, sra, size, nil); err == nil {
		return
	}

	//upload error
	log.Errorf("rput bucket:%s file:%s size:%d err:%s\n", bucket, file, size, err)

	if req, terr = http.NewRequest("GET", durl, nil); terr != nil {
		log.Errorf("durl:%s make request err:%s\n", durl, terr)
		return
	}

	if resp, terr = httpcli.Req(req); err != nil {
		log.Errorf("durl:%s response err:%s\n", durl, terr)
	}

	if resp.StatusCode == http.StatusNotFound {
		errCode = http.StatusNotFound
	}

	return
}

func (g *Gos) downloadUrl(domain, key, ak, sk string) (durl string, err error) {
	var (
		cnf = &ecloud.Config{}
	)

	cnf.AccessKey, cnf.SecretKey = ak, sk
	baseUrl := ecloud.MakeBaseUrl(domain, key)
	policy := ecloud.GetPolicy{}
	c := ecloud.New(0, cnf)

	durl = c.MakePrivateUrl(baseUrl, &policy)
	return
}

func (g *Gos) chgmParse(str string) (ubucket, file, mime string, err error) {
	var (
		data        []byte
		tstrs, strs []string
	)

	if strs = strings.Split(str, PARAM_DELIMITER); len(strs) != 2 {
		log.Errorf("chgm param:%s format err", str)
		err = errors.New("param format error")
		return
	}

	if data, err = b64.URLEncoding.DecodeString(strs[0]); err != nil {
		log.Errorf("base64 decode ekey:%s err:%s\n", strs[0], err)
		return
	}

	if tstrs = strings.Split(string(data), EKEY_DELIMITER); len(tstrs) != 2 {
		log.Errorf("ekey:%s format err\n", string(data))
		err = errors.New("ekey format error")
		return
	}

	ubucket, file = tstrs[0], tstrs[1]
	mime = strs[1]
	return
}
func (g *Gos) chgm(param string) (err error) {
	var (
		cnf                         = &ecloud.Config{}
		ubucket, bucket, file, mime string
	)

	if ubucket, file, mime, err = g.chgmParse(param); err != nil {
		log.Errorf("chgmParse param:%s err:%s\n", param, err)
		return
	}

	if cnf.AccessKey, cnf.SecretKey, _, err = bkt.GetASKey(ubucket); err != nil {
		log.Errorf("get bucket:%s ak sk err:%s\n", ubucket, err)
		return
	}

	i := strings.Index(ubucket, BUCKET_DELIMITER)
	_, bucket = ubucket[:i], ubucket[i+1:]

	c := ecloud.New(0, cnf)
	p := c.Bucket(bucket)
	if err = p.ChangeMime(nil, file, mime); err != nil {
		log.Errorf("change bucket:%s file:%s mime:%s err:%s\n", bucket, file, mime, err)
		return
	}

	return
}

func (g *Gos) moveParse(param string) (sb, db, sf, df string, err error) {
	var (
		data        []byte
		strs, tstrs []string
	)

	if strs = strings.Split(param, PARAM_DELIMITER); len(strs) != 2 {
		log.Errorf("move param:%s format err\n", param)
		err = errors.New("param format error")
		return
	}

	if data, err = b64.URLEncoding.DecodeString(strs[0]); err != nil {
		log.Errorf("b64 decode str:%s err:%s\n", strs[0], err)
		return
	}
	if tstrs = strings.Split(string(data), EKEY_DELIMITER); len(tstrs) != 2 {
		log.Errorf("move sekey:%s format err\n", string(data))
		err = errors.New("move sekey format err")
		return
	}
	sb, sf = tstrs[0], tstrs[1]

	if data, err = b64.URLEncoding.DecodeString(strs[1]); err != nil {
		log.Errorf("b64 decode str:%s err:%s\n", strs[1], err)
		return
	}
	if tstrs = strings.Split(string(data), EKEY_DELIMITER); len(tstrs) != 2 {
		log.Errorf("move dekey:%s format err\n", string(data))
		err = errors.New("move dekey format err")
		return
	}
	db, df = tstrs[0], tstrs[1]

	return
}
func (g *Gos) move(param string) (err error) {
	var (
		cnf                      = &ecloud.Config{}
		ubucket, _, sFile, dFile string
		bucket1                  string
	)

	if ubucket, _, sFile, dFile, err = g.moveParse(param); err != nil {
		log.Errorf("moveParse param:%s err:%s\n", param, err)
		return
	}

	if cnf.AccessKey, cnf.SecretKey, _, err = bkt.GetASKey(ubucket); err != nil {
		log.Errorf("get bucket:%s ak sk err:%s\n", ubucket, err)
		return
	}

	i := strings.Index(ubucket, BUCKET_DELIMITER)
	_, bucket1 = ubucket[:i], ubucket[i+1:]

	c := ecloud.New(0, cnf)
	p := c.Bucket(bucket1)

	if err = p.Move(nil, sFile, dFile); err != nil {
		log.Errorf("move bucket:%s sFile:%s to dFile:%s err:%s\n",
			bucket1, sFile, dFile, err)
		return
	}

	return
}

func (g *Gos) copyParse(param string) (sb, db, sf, df string, err error) {
	var (
		data        []byte
		strs, tstrs []string
	)

	if strs = strings.Split(param, PARAM_DELIMITER); len(strs) != 2 {
		log.Errorf("param:%s format err\n", param)
		err = errors.New("param format error")
		return
	}

	if data, err = b64.URLEncoding.DecodeString(strs[0]); err != nil {
		log.Errorf("base64 decode str:%s err:%s\n", strs[0], err)
		return
	}
	if tstrs = strings.Split(string(data), EKEY_DELIMITER); len(tstrs) != 2 {
		log.Errorf("ekey:%s format err\n", string(data))
		err = errors.New("ekey format error")
		return
	}
	sb, sf = tstrs[0], tstrs[1]

	if data, err = b64.URLEncoding.DecodeString(strs[1]); err != nil {
		log.Errorf("base64 decode str:%s err:%s\n", strs[1], err)
		return
	}
	if tstrs = strings.Split(string(data), EKEY_DELIMITER); len(tstrs) != 2 {
		log.Errorf("ekey:%s format err\n", string(data))
		err = errors.New("ekey format error")
		return
	}
	db, df = tstrs[0], tstrs[1]

	return
}
func (g *Gos) copyop(param string) (err error) {
	var (
		cnf                              = &ecloud.Config{}
		bucket, ubucket, _, sFile, cFile string
	)

	if ubucket, _, sFile, cFile, err = g.copyParse(param); err != nil {
		log.Errorf("copyParse param:%s err:%s\n", param, err)
		return
	}

	if cnf.AccessKey, cnf.SecretKey, _, err = bkt.GetASKey(ubucket); err != nil {
		log.Errorf("get bucket:%s ak sk err:%s\n", ubucket, err)
		return
	}

	i := strings.Index(ubucket, BUCKET_DELIMITER)
	_, bucket = ubucket[:i], ubucket[i+1:]

	c := ecloud.New(0, cnf)
	p := c.Bucket(bucket)
	if err = p.Copy(nil, sFile, cFile); err != nil {
		log.Errorf("copy bucket:%s sFile:%s to cFile:%s err:%s\n", bucket, sFile, cFile, err)
		return
	}

	return
}

func (g *Gos) deleteADParse(param string) (ubucket, file string, days int, err error) {
	var (
		data        []byte
		strs, tstrs []string
	)

	if strs = strings.Split(param, PARAM_DELIMITER); len(strs) != 2 {
		log.Errorf("param:%s format err\n", param)
		err = errors.New("param format error")
		return
	}

	if data, err = b64.URLEncoding.DecodeString(strs[0]); err != nil {
		log.Errorf("base64 decode ekey:%s err:%s\n", strs[0], err)
		return
	}

	if tstrs = strings.Split(string(data), EKEY_DELIMITER); len(tstrs) != 2 {
		log.Errorf("ekey:%s format err\n", string(data))
		err = errors.New("ekey format error")
		return
	}
	ubucket, file = tstrs[0], tstrs[1]

	if days, err = strconv.Atoi(strs[1]); err != nil {
		log.Errorf("atoi str:%s err:%s\n", strs[1], err)
		return
	}

	return
}
func (g *Gos) deleteAfterDays(param string) (err error) {
	var (
		cnf                   = &ecloud.Config{}
		bucket, ubucket, file string
		days                  int
	)

	if ubucket, file, days, err = g.deleteADParse(param); err != nil {
		log.Errorf("deleteADParse param:%s err:%s\n", param, err)
		return
	}

	if cnf.AccessKey, cnf.SecretKey, _, err = bkt.GetASKey(ubucket); err != nil {
		log.Errorf("get bucket:%s ak sk err:%s\n", ubucket, err)
		return
	}

	i := strings.Index(ubucket, BUCKET_DELIMITER)
	_, bucket = ubucket[:i], ubucket[i+1:]

	c := ecloud.New(0, cnf)
	p := c.Bucket(bucket)
	if err = p.DeleteAfterDays(nil, file, days); err != nil {
		log.Errorf("deleteAfterDays bucket:%s file:%s days:%d err:%s\n", bucket, file, days, err)
		return
	}

	return
}

func (g *Gos) deleteParse(param string) (ubucket, file string, err error) {
	var (
		data []byte
		strs []string
	)

	if data, err = b64.URLEncoding.DecodeString(param); err != nil {
		log.Errorf("base64 decode param:%s err:%s\n", param, err)
		return
	}

	if strs = strings.Split(string(data), EKEY_DELIMITER); len(strs) != 2 {
		log.Errorf("ekey:%s format err\n", string(data))
		err = errors.New("ekey format error")
		return
	}
	ubucket, file = strs[0], strs[1]

	return
}
func (g *Gos) deleteop(param string) (err error) {
	var (
		bucket, ubucket, file string
		cnf                   = &ecloud.Config{}
	)

	if ubucket, file, err = g.deleteParse(param); err != nil {
		log.Errorf("deleteParse param:%s err:%s\n", param, err)
		return
	}

	if cnf.AccessKey, cnf.SecretKey, _, err = bkt.GetASKey(ubucket); err != nil {
		log.Errorf("get bucket:%s ak sk err:%s\n", ubucket, err)
		return
	}

	i := strings.Index(ubucket, BUCKET_DELIMITER)
	_, bucket = ubucket[:i], ubucket[i+1:]

	c := ecloud.New(0, cnf)
	p := c.Bucket(bucket)
	if err = p.Delete(nil, file); err != nil {
		log.Errorf("delete bucket:%s file:%s err:%s\n", bucket, file, err)
		return
	}

	return
}

func (g *Gos) bcreateParse(param string) (ubucket, jsonStr string, err error) {
	var (
		strs []string
		data []byte
	)

	if strs = strings.Split(param, PARAM_DELIMITER); len(strs) != 2 {
		log.Errorf("bcreate param:%s format err\n", param)
		err = errors.New("param format error")
		return
	}

	if data, err = b64.URLEncoding.DecodeString(strs[0]); err != nil {
		log.Errorf("base64 decode str:%s err:%s\n", strs[0], err)
		return
	}
	ubucket = string(data)

	if data, err = b64.URLEncoding.DecodeString(strs[1]); err != nil {
		log.Errorf("base64 decode str:%s err:%s\n", strs[1], err)
		return
	}
	jsonStr = string(data)

	return
}
func (g *Gos) bcreate(param string) (err error) {
	var (
		ubucket, jsonStr string
	)

	if ubucket, jsonStr, err = g.bcreateParse(param); err != nil {
		log.Errorf("bcreat parse param:%s err:%s\n", param, err)
		return
	}

	if err = bkt.Create(ubucket, jsonStr); err != nil {
		log.Errorf("btk param:%s create err:%s\n", param, err)
		return
	}

	return
}

func (g *Gos) bdeleteParse(param string) (ubucket string, err error) {
	var (
		data []byte
	)

	if data, err = b64.URLEncoding.DecodeString(param); err != nil {
		log.Errorf("base64 decode str:%s err:%s\n", param, err)
		return
	}

	ubucket = string(data)
	return
}
func (g *Gos) bdelete(param string) (err error) {
	var (
		ubucket string
	)

	if ubucket, err = g.bdeleteParse(param); err != nil {
		log.Errorf("parse param:%s err:%s\n", param, err)
		return
	}

	if err = bkt.Delete(ubucket); err != nil {
		log.Errorf("delete ubucket:%s err:%s\n", ubucket, err)
		return
	}

	return
}

// upload exception
func (g *Gos) uploadException(ekey string) (err error) {
	var (
		errCode                   int
		strs                      []string
		results, query, moveParam string
		ms                        []recordItem
		delFlag, moveFlag         bool
	)
	ms = make([]recordItem, 1)
	query = fmt.Sprintf(`{"ekey":"%s"}`, ekey)

	if results, err = g.T.GetAll(g.DB, g.Task_Collection, query); err != nil {
		log.Errorf("uploadException getall ekey:%s record err:%s\n", ekey, err)
		return
	}

	if err = json.Unmarshal([]byte(results), &ms); err != nil {
		log.Errorf("json decode results:%s err:%s\n", results, err)
		return
	}

	for _, v := range ms {
		if v.Op == DELETE_OP {
			delFlag = true
		}
		if v.Op == MOVE_OP {
			moveFlag = true
			moveParam = v.Param
		}
	}

	//hava delete op
	if delFlag {
		for _, v := range ms {
			g.taskFinish(v.Ekey, v.Op, v.Param, v.AddTime, OP_SUCCESS)

			//search op before delete
			if v.Op == DELETE_OP {
				break
			}
		}

		return
	}

	if strs = strings.Split(moveParam, PARAM_DELIMITER); len(strs) != 2 {
		log.Errorf("moveParam:%s format err\n", moveParam)
		err = errors.New("moveParam format error")
		return
	}
	//hava move op
	if moveFlag {
		for _, v := range ms {
			//search op before move
			if v.Op == MOVE_OP {
				break
			}

			//reupload
			if v.Op == UPLOAD_OP {
				if err, errCode = g.upload(v.Param, strs[1]); err != nil {
					log.Errorf("uploadExceptin param:%s sekey:%s upload errCode:%d err:%s\n",
						v.Param, strs[1], errCode, err)
					return
				} else {
					g.taskFinish(v.Ekey, v.Op, v.Param, v.AddTime, OP_SUCCESS)
					return
				}
			}
		}
	}

	return
}

// task finish
func (g *Gos) taskFinish(ekey, op, param string, addTime int64, op_stat string) {
	var (
		doc      *task.FinishItem
		selector *recordItem
		err      error
	)

	selector = &recordItem{op, param, addTime, ekey}
	if err = g.T.Del(g.DB, g.Task_Collection, selector); err != nil {
		log.Errorf("del selector:%+v err:%s\n", selector, err)
	}

	doc = &task.FinishItem{op, param, addTime, ekey, op_stat}
	if err = g.T.Insert(g.DB, g.Finish_Collection, doc); err != nil {
		log.Errorf("insert doc:%+v err:%s\n", doc, err)
		return
	}

	return
}

// hash genvalue
func genValue(s string) uint32 {
	var (
		bs []byte
	)
	hash := sha1.New()
	hash.Write([]byte(s))
	hashBytes := hash.Sum(nil)

	bs = hashBytes[6:10]
	if len(bs) < 4 {
		return 0
	}
	v := (uint32(bs[3]) << 24) | (uint32(bs[2]) << 16) | (uint32(bs[1]) << 8) | (uint32(bs[0]))

	return v
}

type selfReaderAt struct {
	fsize int64
	fekey string
	furl  string
}

func (sra *selfReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	var (
		req      *http.Request
		resp     *http.Response
		rangeStr string
		buff     []byte
	)

	if req, err = http.NewRequest("GET", sra.furl, nil); err != nil {
		log.Errorf("make fekey:%s request err:%s\n", sra.fekey, err)
		return
	}
	req.Header.Set("ekey", sra.fekey)

	rangeStr = fmt.Sprintf("bytes=%d-%d", off, off+int64(len(p))-1)
	req.Header.Set("Range", rangeStr)
	if resp, err = httpcli.Req(req); err != nil {
		log.Errorf("fekey:%s, response err:%s\n", sra.fekey, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent &&
		resp.StatusCode != http.StatusOK {
		log.Errorf("fekey:%s statuscode:%d err\n", sra.fekey, resp.StatusCode)
		err = errors.New("response statuscode err")
		return
	}

	if buff, err = ioutil.ReadAll(resp.Body); err != nil {
		log.Errorf("get fekey:%s range:%s data err:%s\n", sra.fekey, rangeStr, err)
		return
	}

	if n = copy(p, buff); n != len(p) {
		log.Errorf("copy data size err psize:%d bsize:%d\n", len(p), len(buff))
		return
	}

	return
}
