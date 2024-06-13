package main

import (
	"bytes"
	"crypto/md5"
	"crypto/sha1"
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"kagamistoreage/authacess/auth"
	"kagamistoreage/authacess/compressupload"
	"kagamistoreage/authacess/conf"
	"kagamistoreage/authacess/dataprocess"
	"kagamistoreage/authacess/efs"
	"kagamistoreage/authacess/fetch"
	"kagamistoreage/authacess/mimetype"
	"kagamistoreage/authacess/multipartupload"
	"kagamistoreage/authacess/pfop"
	"kagamistoreage/authacess/upload_strategy"
	"kagamistoreage/authacess/variable"
	"kagamistoreage/libs/errors"
	"kagamistoreage/libs/meta"
	"net/http"
	"strconv"
	"strings"
	"time"

	log "kagamistoreage/log/glog"
)

const (
	_httpServerReadTimeout  = 50 * time.Second
	_httpServerWriteTimeout = 50 * time.Second
	Unknow_File_Type        = "unkonw"
)

type server struct {
	efs  *efs.Efs
	dp   *dataprocess.DataProcess
	p    *pfop.Pfop
	c    *conf.Config
	Auth *auth.Auth
}

// StartApi init the http module.
func StartUploadApi(c *conf.Config) (err error) {
	var (
		s         *server
		multipart *multipartupload.Multipart
	)

	s = &server{}
	multipart, err = multipartupload.Multipart_init(c)
	if err != nil {
		log.Errorf("init multipartupload faild %v", err)
		return
	}
	s.efs = efs.New(c, multipart)
	s.p = pfop.New(c)
	s.dp = dataprocess.New(c)
	s.c = c
	if s.Auth, err = auth.New(c); err != nil {
		return
	}
	//	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 100
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/", s.doupload)

		//mux.HandleFunc("/ping", s.ping)
		server := &http.Server{
			Addr:         c.HttpAddr,
			Handler:      mux,
			ReadTimeout:  _httpServerReadTimeout,
			WriteTimeout: _httpServerWriteTimeout,
		}
		if err := server.ListenAndServe(); err != nil {
			return
		}
	}()
	return
}

func statislog(method string, bucket, file *string, overwriteflag *int, oldsize, size *int64, start time.Time, status *int, err *string) {
	if *bucket == "" {
		*bucket = "-"
	}
	if *file == "" {
		*file = "-"
	}
	if *err == "" {
		*err = "-"
	}
	fname := b64.URLEncoding.EncodeToString([]byte(*file))
	if time.Now().Sub(start).Seconds() > 1.0 {
		log.Statisf("proxymore 1s ============%f", time.Now().Sub(start).Seconds())
	}
	log.Statisf("%s	%s	%s	%d	%d	%d	%f	%d	%s",
		method, *bucket, fname, *overwriteflag, *oldsize, *size, time.Now().Sub(start).Seconds(), *status, *err)
}

func infolog(method string, bucket, file *string, overwriteflag *int, oldsize, size *int64, start time.Time, status *int, err *string) {
	if *bucket == "" {
		*bucket = "-"
	}
	if *file == "" {
		*file = "-"
	}
	fname := b64.URLEncoding.EncodeToString([]byte(*file))
	if time.Now().Sub(start).Seconds() > 1.0 {
		log.Infof("proxymore 1s ============%f", time.Now().Sub(start).Seconds())
	}
	log.Infof("%s	%s	%s	%d	%d	%d	%f	%d	error(%s)",
		method, *bucket, fname, *overwriteflag, *oldsize, *size, time.Now().Sub(start).Seconds(), *status, *err)
}

func (s *server) doupload(wr http.ResponseWriter, r *http.Request) {
	var (
		urlpath string
		start   = time.Now()
	)

	urlpath = r.URL.Path
	if urlpath == "/" {
		s.upload(wr, r)
	} else {
		switch {
		case strings.HasPrefix(urlpath, "/mkblk"):
			s.mkblk(wr, r)
		case strings.HasPrefix(urlpath, "/bput"):
			s.bput(wr, r)
		case strings.HasPrefix(urlpath, "/mkfile"):
			s.mkfile(wr, r)
		case strings.HasPrefix(urlpath, "/compressupload"):
			s.compress(wr, r)
		default:
			bucket := "-"
			file := "-"
			size := int64(0)
			status := 400
			flag := 0
			err := "invalid request"
			statislog("-", &bucket, &file, &flag, &size, &size, start, &status, &err)
			http.Error(wr, "bad request url", http.StatusBadRequest)
		}
	}

}

func (s *server) upload(wr http.ResponseWriter, r *http.Request) {
	var (
		req                                    = &Upload_req{}
		res                                    = &Upload_res{}
		err                                    error
		retcode, repeat_flag, replication, uid int
		start                                  = time.Now()
		//	status                                 = http.StatusOK
		fsize, oldsize                                        int64
		errstring, bucket, filename, key, hash, ak, sk, ctype string
		pOps, pNotifyUrl, pPipeLine                           string
		rbody                                                 []byte
		cbkerres                                              upload_strategy.ResCallbakErr
		v                                                     = &variable.Variable{}
		body, typehead                                        []byte
		putpolicy                                             *auth.PutPolicy
	)
	v.ImageInfo, v.AvInfo = &variable.ImageInfoS{}, &variable.AvInfoS{}
	v.AvInfo.Audio, v.AvInfo.Format, v.AvInfo.Video = &variable.AudioS{},
		&variable.FormatS{}, &variable.VideoS{}

	defer statislog("/r/upload", &bucket, &filename, &repeat_flag, &oldsize, &fsize,
		start, &res.Res.Code, &res.Res.Error)
	res.Res.Code = 200
	if r.Method == "OPTIONS" {
		res.Res.Code = 200
		//	res.Res.Error = "http method not post"
		res.Upload_response(wr)
		return
	}
	if r.Method != "POST" && r.Method != "post" {
		res.Res.Code = 400
		res.Res.Error = "http method " + r.Method + " not post"
		res.ResponseErr(wr)
		return
	}

	req.R = r
	defer r.Body.Close()
	//	fmt.Println("----------")
	retcode, errstring = req.Parms(s.c)
	if retcode != 200 {
		res.Res.Code = retcode
		res.Res.Error = errstring
		res.ResponseErr(wr)
		return
	}
	defer req.FileClose()

	//token check
	//putpolicy_data, bucket, filename, ak, sk, replication, pOps, pNotifyUrl,
	//	pPipeLine, err, retcode = s.Auth.UploadAuthorize(req.Token)
	//	fmt.Println("----------")
	putpolicy, uid, replication, ak, sk, err, retcode = s.Auth.UploadAuthorize(req.Token)
	if err != nil {
		log.Errorf("auth check failed %v", err)
		res.Res.Code = retcode
		res.Res.Error = err.Error()
		res.ResponseErr(wr)
		return
	}

	//文件名优先顺序，scope 的 filenmae > form 中的key > file hash值
	ekey := strings.Split(putpolicy.Scope, ":")
	if len(ekey) == 1 {
		bucket = ekey[0]
		filename = ""
	} else {
		bucket = ekey[0]
		filename = ekey[1]
	}
	bucket = meta.Get_hbase_bucketname(uid, bucket) // uid_bucketname
	if filename == "" {
		filename = req.Key
	}

	//确定覆盖上传
	repeat_flag = upload_strategy.Upload_isoverwrite(putpolicy.InsertOnly, filename)

	if body, err = ioutil.ReadAll(req.FileBinaryData); err != nil {
		res.Res.Code = 400
		res.Res.Error = "FileBinaryData is invalid"
		res.ResponseErr(wr)
		return
	}
	fsize = int64(len(body))

	/*
		1.获取文件类型 优先 文件名 其次 文件内容前5字节，最后unknown
		2.首先根据form上传文件判断，其次根据key 判断
	*/

	if fsize > 10 {
		typehead = body[:10]
	}
	ctype = mimetype.Check_uploadfile_type(req.File, filename, typehead, Unknow_File_Type)

	//limit 限制
	if (putpolicy.FsizeLimit != 0 && fsize > putpolicy.FsizeLimit) ||
		(putpolicy.FsizeMin != 0 && fsize < putpolicy.FsizeMin) {
		res.Res.Code = E_Limit
		res.Res.Error = "file size limit"
		res.ResponseErr(wr)
		return
	}

	//deleteAfterDays
	hash, key, retcode, errstring, oldsize, _ = s.efs.Upload(bucket, filename, ctype,
		repeat_flag, replication, putpolicy.DeleteAfterDays, body)
	if retcode != 200 {
		res.Res.Code = retcode
		res.Res.Error = errstring
		res.ResponseErr(wr)
		return
	} else {
		res.Ures.Hash = hash
		res.Ures.Key = key
		//if putpolicy.DeleteAfterDays != 0 {
		res.Res.Error = fmt.Sprintf("%d", putpolicy.DeleteAfterDays)
		//}

	}

	if filename == "" {
		filename = hash
	}

	//variable set
	v.Etag = hash
	v.Fsize = fmt.Sprintf("%d", fsize)
	v.Key = filename
	v.Mimetype = ctype
	v.Bucket = bucket
	v.Fname = req.File
	v.Customvariable = make(map[string]string)
	for vkey, vvalue := range req.XVariable {
		v.Customvariable[vkey] = vvalue
	}

	//persistentOps
	var pTaskId string
	pOps = putpolicy.PersistentOps
	pNotifyUrl = putpolicy.PersistentNotifyUrl
	pPipeLine = putpolicy.PersistentPipeline
	if pOps != "" {
		if pTaskId, retcode, err = s.p.Add(bucket, key, pOps, pNotifyUrl, pPipeLine); err != nil {
			res.Res.Code = retcode
			res.Res.Error = err.Error()
			res.ResponseErr(wr)
			return
		}

		v.PersistentId = pTaskId
	}

	//-----------callbak
	if putpolicy.CallbackUrl != "" && putpolicy.CallbackBody != "" {
		if strings.Contains(putpolicy.CallbackBody, "imageInfo") {
			if v.ImageInfo.Size, v.ImageInfo.Format, v.ImageInfo.ColorModel,
				v.ImageInfo.Width, v.ImageInfo.Height, err =
				s.dp.GetImageInfo(v.Bucket, v.Key); err != nil {

				log.Errorf("getImageInfo bucket:%s key:%s err:%s\n", v.Bucket, v.Key, err)
				res.Res.Code, res.Res.Error = E_ServerFail, "get image info err"
				res.ResponseErr(wr)
				return
			}
		}

		if strings.Contains(putpolicy.CallbackBody, "avinfo") {
			//get audio video info
		}

		retcode, errstring, rbody, cbkerres = upload_strategy.Upload_callbak(putpolicy.CallbackUrl,
			putpolicy.CallbackHost, putpolicy.CallbackBody, putpolicy.CallbackBodyType, v, ak, sk)
		if retcode == 579 {
			res.Res.Code = retcode
			res.Res.Error = errstring
			cbkerres.Hash = hash
			cbkerres.Key = key
			tmpb := make(map[string]upload_strategy.ResCallbakErr)
			tmpb["error"] = cbkerres
			rbody, err = json.Marshal(tmpb)
			if err != nil {
				log.Errorf("callbak body marshal json error(%v)", err)
			}
			Responsecbk(wr, retcode, rbody)
			return
		} else {
			Responsecbk(wr, retcode, rbody)
			return
		}
	}

	//--------- return body
	if putpolicy.ReturnBody != "" {
		if strings.Contains(putpolicy.ReturnBody, "imageInfo") {
			if v.ImageInfo.Size, v.ImageInfo.Format, v.ImageInfo.ColorModel,
				v.ImageInfo.Width, v.ImageInfo.Height, err =
				s.dp.GetImageInfo(v.Bucket, v.Key); err != nil {

				log.Errorf("getImageInfo bucket:%s key:%s err:%s\n", v.Bucket, v.Key, err)
				res.Res.Code, res.Res.Error = E_ServerFail, "get image info err"
				res.ResponseErr(wr)
				return
			}
		}

		if strings.Contains(putpolicy.ReturnBody, "avinfo") {
			//TODO
		}

		//do returnurl
		retcode, errstring, rbody = upload_strategy.Upload_returnurl(wr, r,
			putpolicy.ReturnUrl, putpolicy.ReturnBody, v)
		if retcode == E_BadMessage {
			res.Res.Code = retcode
			res.Res.Error = errstring
			res.ResponseErr(wr)
			return
		}
		if retcode == http.StatusSeeOther {
			return
		}
		if retcode == http.StatusOK {
			Responsecbk(wr, retcode, rbody)
			return
		}
	}

	//normal return
	res.Upload_response(wr)

	return
}

func (s *server) compress(wr http.ResponseWriter, r *http.Request) {
	var (
		req                                              = &Upload_req{}
		res                                              = &Upload_res{}
		err                                              error
		uretcode, retcode, repeat_flag, replication, uid int
		start                                            = time.Now()
		//	status                                 = http.StatusOK
		fsize                                                 int64
		errstring, bucket, filename, key, hash, ak, sk, ctype string
		pOps, pNotifyUrl, pPipeLine                           string
		//	putpolicy_data                                        []byte
		rbody, urbody []byte
		cbkerres      upload_strategy.ResCallbakErr
		v             = &variable.Variable{}
		putpolicy     *auth.PutPolicy
	)
	oldsize := int64(0)
	overwriteflag := 0
	defer statislog("compress", &bucket, &filename, &overwriteflag, &oldsize, &fsize, start, &res.Res.Code, &res.Res.Error)
	res.Res.Code = 200
	if r.Method != "POST" && r.Method != "post" {
		res.Res.Code = 400
		res.Res.Error = "http method " + r.Method + " not post"
		res.ResponseErr(wr)
		return
	}
	req.R = r
	retcode, errstring = req.Parms(s.c)
	if retcode != 200 {
		res.Res.Code = retcode
		res.Res.Error = errstring
		res.ResponseErr(wr)
		return
	}
	defer req.FileClose()

	fsize = req.Size
	v.Fsize = fmt.Sprintf("%d", fsize)

	putpolicy, uid, replication, ak, sk, err, retcode = s.Auth.UploadAuthorize(req.Token)
	if err != nil {
		log.Errorf("auth check failed %v", err)
		res.Res.Code = retcode
		res.Res.Error = err.Error()
		res.ResponseErr(wr)
		return
	}

	//文件名优先顺序，scope 的 filenmae > form 中的key > file hash值
	ekey := strings.Split(putpolicy.Scope, ":")
	if len(ekey) == 1 {
		bucket = ekey[0]
		filename = ""
	} else {
		bucket = ekey[0]
		filename = ekey[1]
	}
	if filename == "" {
		filename = req.Key
	}
	if filename == "" {
		filename = req.File
	}
	bucket = meta.Get_hbase_bucketname(uid, bucket) // uid_bucketname

	/*
		1.获取文件类型 优先 文件名 其次 文件内容前5字节，最后unknown
		2.首先根据form上传文件判断，其次根据key 判断
	*/

	ctype = mimetype.Check_uploadfile_type(req.File, filename, nil, Unknow_File_Type)
	/*
		err = json.Unmarshal(putpolicy_data, &putpolicy)
		if err != nil {
			res.Res.Code = E_TokenInvalid
			res.Res.Error = "putpolicy is invalid"
			res.ResponseErr(wr)
			return
		}
	*/
	repeat_flag = upload_strategy.Upload_isoverwrite(putpolicy.InsertOnly, filename)

	//limit 限制
	if (putpolicy.FsizeLimit != 0 && fsize > putpolicy.FsizeLimit) &&
		(putpolicy.FsizeMin != 0 && fsize < putpolicy.FsizeMin) {
		res.Res.Code = E_Limit
		res.Res.Error = "file size limit"
		res.ResponseErr(wr)
		return
	}

	uretcode, urbody, errstring = compressupload.Cupload(bucket, filename,
		repeat_flag, replication, putpolicy.DeleteAfterDays, req.FileBinaryData, s.efs)
	if uretcode != 200 {
		log.Errorf("compress upload faild")
		res.Res.Code = uretcode
		res.Res.Error = "compress upload failed"
		Responsecbk(wr, uretcode, urbody)
		return
	}

	//persistentOps
	var pTaskId string
	pOps = putpolicy.PersistentOps
	pNotifyUrl = putpolicy.PersistentNotifyUrl
	pPipeLine = putpolicy.PersistentPipeline
	if pOps != "" {
		if pTaskId, retcode, err = s.p.Add(bucket, key, pOps, pNotifyUrl, pPipeLine); err != nil {
			res.Res.Code = retcode
			res.Res.Error = err.Error()
			res.ResponseErr(wr)
			return
		}

		v.PersistentId = pTaskId
	}

	//do returnurl
	retcode, errstring, rbody = upload_strategy.Upload_returnurl(wr, r, putpolicy.ReturnUrl, putpolicy.ReturnBody, v)
	if retcode == 401 {
		res.Res.Code = retcode
		res.Res.Error = errstring
		res.ResponseErr(wr)
		return
	}
	if retcode == 200 {
		//retrunurl is nil return returnbody
		Responsecbk(wr, retcode, rbody)
		return
	}

	if retcode == 303 {
		// return 303
		return
	}

	//variable set
	if req.Key == "" {
		v.Key = filename
	} else {
		v.Key = req.Key
	}

	v.Mimetype = ctype
	v.Bucket = bucket
	v.Fname = req.File
	v.Customvariable = make(map[string]string)
	for vkey, vvalue := range req.XVariable {
		v.Customvariable[vkey] = vvalue
	}

	retcode, errstring, rbody, cbkerres = upload_strategy.Upload_callbak(putpolicy.CallbackUrl,
		putpolicy.CallbackHost, putpolicy.CallbackBody, putpolicy.CallbackBodyType, v, ak, sk)
	if retcode == 400 {
		res.Res.Code = retcode
		res.Res.Error = errstring
		res.ResponseErr(wr)
		return
	} else if retcode == 579 {
		res.Res.Code = retcode
		res.Res.Error = errstring
		cbkerres.Hash = hash
		cbkerres.Key = key
		tmpb := make(map[string]upload_strategy.ResCallbakErr)
		tmpb["error"] = cbkerres
		rbody, err = json.Marshal(tmpb)
		if err != nil {
			log.Errorf("callbak body marshal json error(%v)", err)
		}
		Responsecbk(wr, retcode, rbody)
		return
	} else if retcode == 200 {
		Responsecbk(wr, retcode, rbody)
		return
	} else {
		Responsecbk(wr, uretcode, urbody)
		return
	}

	return

}

func (s *server) mkblk(wr http.ResponseWriter, r *http.Request) {
	var (
		start                     = time.Now()
		retcode, replication, uid int
		err                       error
		rbody                     []byte
		//	status                                 = http.StatusOK
		fsize                             int64
		errstring, bucket, filename, mime string
		req                               = &Mkblock_req{}
		res                               = &Mkblock_res{}
		proxyres                          *efs.Mkblk_res
		putpolicy                         *auth.PutPolicy
	)
	oldsize := int64(0)
	overwriteflag := 0
	defer statislog("/r/mkblk", &bucket, &filename, &overwriteflag, &oldsize, &fsize, start, &res.Res.Code, &res.Res.Error)
	res.Res.Code = 200
	if r.Method == "OPTIONS" {
		res.Res.Code = 200
		res.Cross_domain(wr)
		return
	}
	if r.Method != "POST" && r.Method != "post" {
		res.Res.Code = 400
		res.Res.Error = "http method " + r.Method + " not post"
		res.ResponseErr(wr)
		return
	}
	req.R = r
	retcode, errstring = req.Parms(s.c, r.URL.Path)
	if retcode != 200 {
		res.Res.Code = retcode
		res.Res.Error = errstring
		res.ResponseErr(wr)
		return
	}
	fsize = int64(len(req.Body))

	putpolicy, uid, replication, _, _, err, retcode = s.Auth.UploadAuthorize(req.Token)
	if err != nil {
		res.Res.Code = retcode
		res.Res.Error = err.Error()
		res.ResponseErr(wr)
		return
	}

	//文件名优先顺序，scope 的 filenmae > form 中的key > file hash值
	ekey := strings.Split(putpolicy.Scope, ":")
	if len(ekey) == 1 {
		bucket = ekey[0]
		filename = ""
	} else {
		bucket = ekey[0]
		filename = ekey[1]
	}
	bucket = meta.Get_hbase_bucketname(uid, bucket) // uid_bucketname

	//file type
	if mime = mimetype.SuffixMime(filename); mime == "" {
		if mime = mimetype.FileHeaderMime(req.Body[:10]); mime == "" { // use 5 head byte
			mime = "unknow"
		}
	}

	proxyres, retcode, errstring = s.efs.Multipart_mkblk(bucket, filename, mime, req.Body, req.Blocksize, replication, 0)
	if retcode != 200 {
		res.Res.Code = retcode
		res.Res.Error = errstring
		res.ResponseErr(wr)
		return
	}

	rbody, err = json.Marshal(proxyres)
	if err != nil {
		log.Errorf("callbak body marshal json error(%v)", err)
	}

	Responsecbk(wr, retcode, rbody)
	return
}

func (s *server) bput(wr http.ResponseWriter, r *http.Request) {
	var (
		start                     = time.Now()
		retcode, replication, uid int
		err                       error
		rbody                     []byte
		//	status                                 = http.StatusOK
		fsize                       int64
		errstring, bucket, filename string
		req                         = &Bput_req{}
		res                         = &Bput_res{}
		proxyres                    *efs.Bput_res
		putpolicy                   *auth.PutPolicy
	)
	oldsize := int64(0)
	overwriteflag := 0
	defer statislog("/r/bput", &bucket, &filename, &overwriteflag, &oldsize, &fsize, start, &res.Res.Code, &res.Res.Error)
	res.Res.Code = 200
	if r.Method == "OPTIONS" {
		res.Res.Code = 200
		res.Cross_domain(wr)
		return
	}
	if r.Method != "POST" && r.Method != "post" {
		res.Res.Code = 400
		res.Res.Error = "http method " + r.Method + " not post"
		res.ResponseErr(wr)
		return
	}
	req.R = r
	retcode, errstring = req.Parms(s.c, r.URL.Path)
	if retcode != 200 {
		res.Res.Code = retcode
		res.Res.Error = errstring
		res.ResponseErr(wr)
		return
	}

	fsize = int64(len(req.Body))

	putpolicy, uid, replication, _, _, err, retcode = s.Auth.UploadAuthorize(req.Token)
	if err != nil {
		res.Res.Code = retcode
		res.Res.Error = err.Error()
		res.ResponseErr(wr)
		return
	}

	//文件名优先顺序，scope 的 filenmae > form 中的key > file hash值
	ekey := strings.Split(putpolicy.Scope, ":")
	if len(ekey) == 1 {
		bucket = ekey[0]
		filename = ""
	} else {
		bucket = ekey[0]
		filename = ekey[1]
	}
	bucket = meta.Get_hbase_bucketname(uid, bucket) // uid_bucketname

	proxyres, retcode, errstring = s.efs.Multipart_bput(bucket, filename, req.Ctx, req.Nextchuckoffset, req.Body, replication, 0)
	if retcode != 200 {
		res.Res.Code = retcode
		res.Res.Error = errstring
		res.ResponseErr(wr)
		return
	}

	rbody, err = json.Marshal(proxyres)
	if err != nil {
		log.Errorf("callbak body marshal json error(%v)", err)
	}
	Responsecbk(wr, retcode, rbody)
	return
}

func (s *server) mkfile(wr http.ResponseWriter, r *http.Request) {
	var (
		start                                        = time.Now()
		retcode, repeat_flag, replication, uid, ctxs int
		err                                          error
		rbody                                        []byte
		//	status                                 = http.StatusOK
		fsize, oldsize                                 int64
		errstring, bucket, filename, key, hash, ak, sk string
		pOps, pNotifyUrl, pPipeLine                    string
		req                                            = &Mkfile_req{}
		res                                            = &Mkfile_res{}
		//	proxyres                                       *efs.Bput_res
		//	rbody                                                 []byte
		cbkerres  upload_strategy.ResCallbakErr
		v         = &variable.Variable{}
		ctype     string
		putpolicy *auth.PutPolicy
	)
	defer statislog("/r/mkfile", &bucket, &filename, &repeat_flag, &oldsize, &fsize, start, &res.Res.Code, &res.Res.Error)
	res.Res.Code = 200
	if r.Method == "OPTIONS" {
		res.Res.Code = 200
		res.Cross_domain(wr)
		return
	}
	if r.Method != "POST" && r.Method != "post" {
		res.Res.Code = 400
		res.Res.Error = "http method " + r.Method + " not post"
		res.ResponseErr(wr)
		return
	}
	req.R = r
	retcode, errstring = req.Parms(s.c, r.URL.Path)
	if retcode != 200 {
		res.Res.Code = retcode
		res.Res.Error = errstring
		res.ResponseErr(wr)
		return
	}

	putpolicy, uid, replication, ak, sk, err, retcode = s.Auth.UploadAuthorize(req.Token)
	if err != nil {
		res.Res.Code = retcode
		res.Res.Error = err.Error()
		res.ResponseErr(wr)
		return
	}

	//文件名优先顺序，scope 的 filenmae > form 中的key > file hash值
	ekey := strings.Split(putpolicy.Scope, ":")
	if len(ekey) == 1 {
		bucket = ekey[0]
		filename = ""
	} else {
		bucket = ekey[0]
		filename = ekey[1]
	}
	bucket = meta.Get_hbase_bucketname(uid, bucket) // uid_bucketname

	//文件名优先顺序，scope 的 filenmae > form 中的key > file hash值
	if filename == "" {
		filename = req.Key
	}

	//确定覆盖上传
	repeat_flag = upload_strategy.Upload_isoverwrite(putpolicy.InsertOnly, filename)

	fsize = req.Filesize

	/*
		1.获取文件类型 优先 文件名 其次 文件内容前5字节，最后unknown
		2.首先根据form上传文件判断，其次根据key 判断
	*/

	ctype = mimetype.Check_uploadfile_type("", filename, nil, Unknow_File_Type)

	//limit 限制
	if (putpolicy.FsizeLimit != 0 && fsize > putpolicy.FsizeLimit) &&
		(putpolicy.FsizeMin != 0 && fsize < putpolicy.FsizeMin) {
		res.Res.Code = E_Limit
		res.Res.Error = "file size limit"
		res.ResponseErr(wr)
		return
	}

	// TODO fsizeMin, fsizeLimit,detectMime,mimeLimit,deleteAfterDays
	hash, key, retcode, errstring, oldsize, ctxs = s.efs.Multipart_mkfile(bucket, filename,
		ctype, string(req.Body), req.Filesize, repeat_flag, replication, putpolicy.DeleteAfterDays, req.Callbakurl)
	if retcode != 200 {
		res.Res.Code = retcode
		res.Res.Error = errstring
		res.ResponseErr(wr)
		return
	} else {
		res.Ures.Hash = hash
		res.Ures.Key = key
		//if putpolicy.DeleteAfterDays != 0 {
		res.Res.Error = fmt.Sprintf("%d", putpolicy.DeleteAfterDays)
		//}
		if ctxs > s.c.Maxctxs {
			res.Ures.Needcallbak = true
			res.Upload_response(wr)
			return
		}
	}

	if filename == "" {
		filename = hash
	}

	//variable set
	v.Fsize = fmt.Sprintf("%d", fsize)
	v.Etag = hash
	v.Key = filename
	v.Mimetype = ctype
	v.Bucket = bucket
	v.Fname = ""
	v.Customvariable = req.Valiable

	//persistentOps
	var pTaskId string
	pOps = putpolicy.PersistentOps
	pNotifyUrl = putpolicy.PersistentNotifyUrl
	pPipeLine = putpolicy.PersistentPipeline
	if pOps != "" {
		if pTaskId, retcode, err = s.p.Add(bucket, key, pOps, pNotifyUrl, pPipeLine); err != nil {
			res.Res.Code = retcode
			res.Res.Error = err.Error()
			res.ResponseErr(wr)
			return
		}

		v.PersistentId = pTaskId
	}

	//-----------callbak
	if putpolicy.CallbackUrl != "" && putpolicy.CallbackBody != "" {
		if strings.Contains(putpolicy.CallbackBody, "imageInfo") {
			if v.ImageInfo.Size, v.ImageInfo.Format, v.ImageInfo.ColorModel,
				v.ImageInfo.Width, v.ImageInfo.Height, err =
				s.dp.GetImageInfo(v.Bucket, v.Key); err != nil {

				log.Errorf("getImageInfo bucket:%s key:%s err:%s\n", v.Bucket, v.Key, err)
				res.Res.Code, res.Res.Error = E_ServerFail, "get image info err"
				res.ResponseErr(wr)
				return
			}
		}

		if strings.Contains(putpolicy.CallbackBody, "avinfo") {
			//get audio video info
		}

		retcode, errstring, rbody, cbkerres = upload_strategy.Upload_callbak(putpolicy.CallbackUrl,
			putpolicy.CallbackHost, putpolicy.CallbackBody, putpolicy.CallbackBodyType, v, ak, sk)
		if retcode == 579 {
			res.Res.Code = retcode
			res.Res.Error = errstring
			cbkerres.Hash = hash
			cbkerres.Key = key
			tmpb := make(map[string]upload_strategy.ResCallbakErr)
			tmpb["error"] = cbkerres
			rbody, err = json.Marshal(tmpb)
			if err != nil {
				log.Errorf("callbak body marshal json error(%v)", err)
			}
			Responsecbk(wr, retcode, rbody)
			return
		} else {
			Responsecbk(wr, retcode, rbody)
			return
		}
	}

	//--------- return body
	if putpolicy.ReturnBody != "" {
		if strings.Contains(putpolicy.ReturnBody, "imageInfo") {
			if v.ImageInfo.Size, v.ImageInfo.Format, v.ImageInfo.ColorModel,
				v.ImageInfo.Width, v.ImageInfo.Height, err =
				s.dp.GetImageInfo(v.Bucket, v.Key); err != nil {

				log.Errorf("getImageInfo bucket:%s key:%s err:%s\n", v.Bucket, v.Key, err)
				res.Res.Code, res.Res.Error = E_ServerFail, "get image info err"
				res.ResponseErr(wr)
				return
			}
		}

		if strings.Contains(putpolicy.ReturnBody, "avinfo") {
			//TODO
		}

		//do returnurl
		retcode, errstring, rbody = upload_strategy.Upload_returnurl(wr, r,
			putpolicy.ReturnUrl, putpolicy.ReturnBody, v)
		if retcode == E_BadMessage {
			res.Res.Code = retcode
			res.Res.Error = errstring
			res.ResponseErr(wr)
			return
		}
		if retcode == http.StatusSeeOther {
			return
		}
		if retcode == http.StatusOK {
			Responsecbk(wr, retcode, rbody)
			return
		}
	}

	//normal return
	res.Upload_response(wr)
	return
}

// modify callback,ready to del
func (s *server) mkfile_del(wr http.ResponseWriter, r *http.Request) {
	var (
		start                                        = time.Now()
		retcode, repeat_flag, replication, uid, ctxs int
		err                                          error
		rbody                                        []byte
		//	status                                 = http.StatusOK
		fsize, oldsize                                 int64
		errstring, bucket, filename, key, hash, ak, sk string
		pOps, pNotifyUrl, pPipeLine                    string
		req                                            = &Mkfile_req{}
		res                                            = &Mkfile_res{}
		//	proxyres                                       *efs.Bput_res
		//	rbody                                                 []byte
		cbkerres  upload_strategy.ResCallbakErr
		v         = &variable.Variable{}
		ctype     string
		putpolicy *auth.PutPolicy
	)
	defer statislog("/r/mkfile", &bucket, &filename, &repeat_flag, &oldsize, &fsize, start, &res.Res.Code, &res.Res.Error)
	res.Res.Code = 200
	if r.Method != "POST" && r.Method != "post" {
		res.Res.Code = 400
		res.Res.Error = "http method " + r.Method + " not post"
		res.ResponseErr(wr)
		return
	}
	req.R = r
	retcode, errstring = req.Parms(s.c, r.URL.Path)
	if retcode != 200 {
		res.Res.Code = retcode
		res.Res.Error = errstring
		res.ResponseErr(wr)
		return
	}

	putpolicy, uid, replication, ak, sk, err, retcode = s.Auth.UploadAuthorize(req.Token)
	if err != nil {
		res.Res.Code = retcode
		res.Res.Error = err.Error()
		res.ResponseErr(wr)
		return
	}

	//文件名优先顺序，scope 的 filenmae > form 中的key > file hash值
	ekey := strings.Split(putpolicy.Scope, ":")
	if len(ekey) == 1 {
		bucket = ekey[0]
		filename = ""
	} else {
		bucket = ekey[0]
		filename = ekey[1]
	}
	bucket = meta.Get_hbase_bucketname(uid, bucket) // uid_bucketname

	//文件名优先顺序，scope 的 filenmae > form 中的key > file hash值
	if filename == "" {
		filename = req.Key
	}

	//确定覆盖上传
	repeat_flag = upload_strategy.Upload_isoverwrite(putpolicy.InsertOnly, filename)

	fsize = req.Filesize

	/*
		1.获取文件类型 优先 文件名 其次 文件内容前5字节，最后unknown
		2.首先根据form上传文件判断，其次根据key 判断
	*/

	ctype = mimetype.Check_uploadfile_type("", filename, nil, Unknow_File_Type)

	//limit 限制
	if (putpolicy.FsizeLimit != 0 && fsize > putpolicy.FsizeLimit) &&
		(putpolicy.FsizeMin != 0 && fsize < putpolicy.FsizeMin) {
		res.Res.Code = E_Limit
		res.Res.Error = "file size limit"
		res.ResponseErr(wr)
		return
	}

	// TODO fsizeMin, fsizeLimit,detectMime,mimeLimit,deleteAfterDays
	hash, key, retcode, errstring, oldsize, ctxs = s.efs.Multipart_mkfile(bucket, filename,
		ctype, string(req.Body), req.Filesize, repeat_flag, replication, putpolicy.DeleteAfterDays, req.Callbakurl)
	if retcode != 200 {
		res.Res.Code = retcode
		res.Res.Error = errstring
		res.ResponseErr(wr)
		return
	} else {
		res.Ures.Hash = hash
		res.Ures.Key = key
		//if putpolicy.DeleteAfterDays != 0 {
		res.Res.Error = fmt.Sprintf("%d", putpolicy.DeleteAfterDays)
		//}
		if ctxs > s.c.Maxctxs {
			res.Ures.Needcallbak = true
			res.Upload_response(wr)
			return
		}
	}

	if filename == "" {
		filename = hash
	}

	//variable set
	v.Fsize = fmt.Sprintf("%d", fsize)
	v.Key = filename
	v.Mimetype = ctype
	v.Bucket = bucket
	v.Fname = ""
	v.Customvariable = req.Valiable

	//persistentOps
	var pTaskId string
	pOps = putpolicy.PersistentOps
	pNotifyUrl = putpolicy.PersistentNotifyUrl
	pPipeLine = putpolicy.PersistentPipeline
	if pOps != "" {
		if pTaskId, retcode, err = s.p.Add(bucket, key, pOps, pNotifyUrl, pPipeLine); err != nil {
			res.Res.Code = retcode
			res.Res.Error = err.Error()
			res.ResponseErr(wr)
			return
		}

		v.PersistentId = pTaskId
	}

	//do returnurl
	retcode, errstring, rbody = upload_strategy.Upload_returnurl(wr, r, putpolicy.ReturnUrl, putpolicy.ReturnBody, v)
	if retcode == 401 {
		res.Res.Code = retcode
		res.Res.Error = errstring
		res.ResponseErr(wr)
		return
	}
	if retcode == 200 {
		//retrunurl is nil return returnbody
		Responsecbk(wr, retcode, rbody)
		return
	}

	if retcode == 303 {
		// return 303
		return
	}

	retcode, errstring, rbody, cbkerres = upload_strategy.Upload_callbak(putpolicy.CallbackUrl,
		putpolicy.CallbackHost, putpolicy.CallbackBody, putpolicy.CallbackBodyType, v, ak, sk)
	if retcode == 400 {
		res.Res.Code = retcode
		res.Res.Error = errstring
		res.ResponseErr(wr)
		return
	} else if retcode == 579 {
		res.Res.Code = retcode
		res.Res.Error = errstring
		cbkerres.Hash = hash
		cbkerres.Key = key
		tmpb := make(map[string]upload_strategy.ResCallbakErr)
		tmpb["error"] = cbkerres
		rbody, err = json.Marshal(tmpb)
		if err != nil {
			log.Errorf("callbak body marshal json error(%v)", err)
		}
		Responsecbk(wr, retcode, rbody)
		return
	} else if retcode == 200 {
		Responsecbk(wr, retcode, rbody)
		return
	} else {
		res.Upload_response(wr)
		return
	}

}

// StartApi init the http module.
func StartManagerApi(c *conf.Config) (err error) {
	var (
		s         *server
		multipart *multipartupload.Multipart
	)

	s = &server{}
	s.c = c
	multipart, err = multipartupload.Multipart_init(c)
	if err != nil {
		log.Errorf("init multipartupload faild %v", err)
		return
	}
	s.efs = efs.New(c, multipart)
	if s.Auth, err = auth.New(c); err != nil {
		return
	}
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 100

	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/", s.do)

		server := &http.Server{
			Addr:         c.HttpAddr,
			Handler:      mux,
			ReadTimeout:  _httpServerReadTimeout,
			WriteTimeout: _httpServerWriteTimeout,
		}
		if err := server.ListenAndServe(); err != nil {
			return
		}
	}()
	return
}

func (s *server) do(wr http.ResponseWriter, r *http.Request) {
	type router struct {
		pattern string
		f       func(wr http.ResponseWriter, r *http.Request)
	}

	mRouter := []router{
		router{pattern: "/stat", f: s.stat},
		router{pattern: "/chgm", f: s.chgm},
		router{pattern: "/move", f: s.move},
		router{pattern: "/copy", f: s.resourceCopy},
		router{pattern: "/deleteAfterDays", f: s.deleteAfter},
		router{pattern: "/delete", f: s.resourceDelete},
		router{pattern: "/list", f: s.list},
		router{pattern: "/hash", f: s.hash},
		router{pattern: "/fetch", f: s.fetch},
		router{pattern: "/batch", f: s.batch},
		router{pattern: "/prefetch", f: s.prefetch},
	}

	for _, rt := range mRouter {
		if strings.HasPrefix(r.URL.Path, rt.pattern) {
			rt.f(wr, r)
			return
		}
	}

	log.Errorf("manager request bad url:%s\n", r.URL.Path)
	http.Error(wr, "bad request url", http.StatusBadRequest)
	return
}

func (s *server) stat(wr http.ResponseWriter, r *http.Request) {
	var (
		req                    = &StatReq{R: r}
		resp                   = &StatResp{}
		start                  = time.Now()
		size, putTime          int64
		code                   = http.StatusOK
		hash, mimeType, errMsg string
		bucket, filename       string
		deleteAfterDays        int
	)
	oldsize := int64(0)
	overwriteflag := 0
	defer statislog("/r/stat", &bucket, &filename, &overwriteflag,
		&oldsize, &size, start, &code, &resp.ErrData.Msg)

	if code, errMsg = req.Parse(); code != http.StatusOK {
		resp.ErrData.Code, resp.ErrData.Msg = code, errMsg
		resp.ErrorResp(wr)
		return
	}

	_, uid, ok := s.Auth.ManagerAuthorize(req.Data.Token,
		req.R.URL.Path+"\n", req.Data.Bucket)
	if !ok {
		code = http.StatusUnauthorized
		resp.ErrData.Code, resp.ErrData.Msg = http.StatusUnauthorized, "bad request token"
		resp.ErrorResp(wr)
		return
	}

	bucket = meta.Get_hbase_bucketname(uid, req.Data.Bucket)
	filename = req.Data.Key

	if hash, mimeType, putTime, size, deleteAfterDays, code, errMsg = s.efs.Stat(bucket,
		filename); code != http.StatusOK {
		resp.ErrData.Code = code
		resp.ErrData.Msg = errMsg
		resp.ErrorResp(wr)
		return
	}

	resp.Data.PutTime = putTime
	resp.Data.Hash = hash
	resp.Data.MimeType = mimeType
	resp.Data.FSize = size
	resp.Data.DeleteAfterDays = deleteAfterDays
	resp.OKResp(wr)
	return
}

func (s *server) chgm(wr http.ResponseWriter, r *http.Request) {
	var (
		start            = time.Now()
		req              = &ChgmReq{R: r}
		resp             = &ChgmResp{}
		code             = http.StatusOK
		errMsg           string
		bucket, filename string
	)
	fsize := int64(0)
	oldsize := int64(0)
	overwriteflag := 0
	defer statislog("/r/chgm", &bucket, &filename,
		&overwriteflag, &oldsize, &fsize, start, &code, &req.Data.MimeType)

	if code, errMsg = req.Parse(); code != http.StatusOK {
		resp.ErrData.Code, resp.ErrData.Msg = code, errMsg
		resp.ErrorResp(wr)
		return
	}

	_, uid, ok := s.Auth.ManagerAuthorize(req.Data.Token,
		req.R.URL.Path+"\n", req.Data.Bucket)
	if !ok {
		code = http.StatusUnauthorized
		resp.ErrData.Code, resp.ErrData.Msg = http.StatusUnauthorized, "bad request token"
		resp.ErrorResp(wr)
		return
	}
	bucket = meta.Get_hbase_bucketname(uid, req.Data.Bucket)
	filename = req.Data.Key

	if code, errMsg = s.efs.Chgm(bucket,
		filename, req.Data.MimeType); code != http.StatusOK {
		resp.ErrData.Code, resp.ErrData.Msg = code, errMsg
		resp.ErrorResp(wr)
		return
	}

	resp.OKResp(wr)
	return
}

func (s *server) move(wr http.ResponseWriter, r *http.Request) {
	var (
		start                                            = time.Now()
		req                                              = &MoveReq{R: r}
		resp                                             = &MoveResp{}
		code                                             = http.StatusOK
		errMsg, srcekey, destekey                        string
		srcbucket, srcfilename, destbucket, destfilename string
	)
	fsize := int64(0)
	oldsize := int64(0)
	overwriteflag := 0
	defer statislog("/r/move", &srcekey, &destekey, &overwriteflag,
		&oldsize, &fsize, start, &code, &resp.ErrData.Msg)

	if code, errMsg = req.Parse(); code != http.StatusOK {
		resp.ErrData.Code, resp.ErrData.Msg = code, errMsg
		resp.ErrorResp(wr)
		return
	}

	_, uid, ok := s.Auth.ManagerAuthorize(req.Data.Token,
		req.R.URL.Path+"\n", req.Data.DestBucket)
	if !ok {
		code = http.StatusUnauthorized
		resp.ErrData.Code, resp.ErrData.Msg = http.StatusUnauthorized, "bad request token"
		resp.ErrorResp(wr)
		return
	}
	srcbucket = meta.Get_hbase_bucketname(uid, req.Data.SrcBucket)
	destbucket = meta.Get_hbase_bucketname(uid, req.Data.DestBucket)
	srcfilename = req.Data.SrcKey
	destfilename = req.Data.DestKey
	srcekey = b64.URLEncoding.EncodeToString([]byte(srcbucket + ":" + srcfilename))
	destekey = destbucket + ":" + destfilename

	if fsize, oldsize, code, errMsg = s.efs.Move(srcbucket, srcfilename,
		destbucket, destfilename, req.Data.IsForce); code != http.StatusOK {

		resp.ErrData.Code, resp.ErrData.Msg = code, errMsg
		resp.ErrorResp(wr)
		return
	}

	resp.OKResp(wr)
	return
}

func (s *server) resourceCopy(wr http.ResponseWriter, r *http.Request) {
	var (
		start                                            = time.Now()
		req                                              = &CopyReq{R: r}
		resp                                             = &CopyResp{}
		code                                             = http.StatusOK
		errMsg, srcekey, destekey                        string
		srcbucket, srcfilename, destbucket, destfilename string
	)
	fsize := int64(0)
	oldsize := int64(0)
	overwriteflag := 0
	defer statislog("/r/copy", &srcekey, &destekey, &overwriteflag,
		&oldsize, &fsize, start, &code, &resp.ErrData.Msg)

	if code, errMsg = req.Parse(); code != http.StatusOK {
		resp.ErrData.Code, resp.ErrData.Msg = code, errMsg
		resp.ErrorResp(wr)
		return
	}

	_, uid, ok := s.Auth.ManagerAuthorize(req.Data.Token,
		req.R.URL.Path+"\n", req.Data.DestBucket)
	if !ok {
		code = http.StatusUnauthorized
		resp.ErrData.Code, resp.ErrData.Msg = http.StatusUnauthorized, "bad request token"
		resp.ErrorResp(wr)
		return
	}

	srcbucket = meta.Get_hbase_bucketname(uid, req.Data.SrcBucket)
	destbucket = meta.Get_hbase_bucketname(uid, req.Data.DestBucket)
	srcfilename = req.Data.SrcKey
	destfilename = req.Data.DestKey
	srcekey = b64.URLEncoding.EncodeToString([]byte(srcbucket + ":" + srcfilename))
	destekey = destbucket + ":" + destfilename

	if fsize, oldsize, code, errMsg = s.efs.Copy(srcbucket, srcfilename,
		destbucket, destfilename, req.Data.IsForce); code != http.StatusOK {

		resp.ErrData.Code, resp.ErrData.Msg = code, errMsg
		resp.ErrorResp(wr)
		return
	}

	resp.OKResp(wr)
	return
}

func (s *server) resourceDelete(wr http.ResponseWriter, r *http.Request) {
	var (
		//	start  = time.Now()
		req              = &DeleteReq{R: r}
		resp             = &DeleteResp{}
		code             = http.StatusOK
		start            = time.Now()
		errMsg           string
		bucket, filename string
	)
	fsize := int64(0)
	overwriteflag := 0
	defer statislog("/r/delete", &bucket, &filename, &overwriteflag,
		&fsize, &fsize, start, &code, &resp.ErrData.Msg)
	if code, errMsg = req.Parse(); code != http.StatusOK {
		resp.ErrData.Code, resp.ErrData.Msg = code, errMsg
		resp.ErrorResp(wr)
		return
	}

	_, uid, ok := s.Auth.ManagerAuthorize(req.Data.Token, req.R.URL.Path+"\n", req.Data.Bucket)
	if !ok {
		code = http.StatusUnauthorized
		resp.ErrData.Code, resp.ErrData.Msg = http.StatusUnauthorized, "bad request token"
		resp.ErrorResp(wr)
		return
	}
	bucket = meta.Get_hbase_bucketname(uid, req.Data.Bucket)
	filename = req.Data.Key

	if fsize, code, errMsg = s.efs.Delete(bucket, filename); code != http.StatusOK {
		resp.ErrData.Code, resp.ErrData.Msg = code, errMsg
		resp.ErrorResp(wr)
		return
	}

	resp.OKResp(wr)
	return
}

func (s *server) list(wr http.ResponseWriter, r *http.Request) {
	var (
		start          = time.Now()
		req            = &ListReq{R: r}
		resp           = &ListResp{}
		code           = http.StatusOK
		errMsg, bucket string
	)
	fsize := int64(0)
	key := "-"
	oldsize := int64(0)
	overwriteflag := 0
	defer statislog("/r/list", &bucket, &key, &overwriteflag, &oldsize, &fsize, start, &code, &resp.ErrData.Msg)

	if code, errMsg = req.Parse(); code != http.StatusOK {
		resp.ErrData.Code, resp.ErrData.Msg = code, errMsg
		resp.ErrorResp(wr)
		return
	}

	_, uid, ok := s.Auth.ManagerAuthorize(req.Data.Token, req.R.URL.Path+"?"+
		req.R.URL.RawQuery+"\n", req.Data.Bucket)
	if !ok {
		code = http.StatusUnauthorized
		resp.ErrData.Code, resp.ErrData.Msg = http.StatusUnauthorized, "bad request token"
		resp.ErrorResp(wr)
		return
	}

	bucket = meta.Get_hbase_bucketname(uid, req.Data.Bucket)

	if resp.Data, code, errMsg = s.efs.List(bucket, req.Data.Marker, req.Data.Limit,
		req.Data.Prefix, req.Data.Delimiter); code != http.StatusOK {
		resp.ErrData.Code, resp.ErrData.Msg = code, errMsg
		resp.ErrorResp(wr)
		return
	}

	resp.OKResp(wr)
	return
}

func (s *server) hash(wr http.ResponseWriter, r *http.Request) {
	var (
		start                     = time.Now()
		ekey, bucket, file, token string
		size                      int64
		code                      int
		hashType, errMsg, hash    string
		hr                        *HashRespData
	)

	fsize := int64(0)
	key := "-"
	oldsize := int64(0)
	overwriteflag := 0
	defer statislog("/r/hash", &bucket, &key, &overwriteflag, &oldsize,
		&fsize, start, &code, &errMsg)

	hr = new(HashRespData)

	if hashType, bucket, file, token, code, errMsg =
		HashParse(r.Header.Get("Authorization"), r.URL.Path); code != http.StatusOK {

		hr.Code = code
		hr.Msg = errMsg
		HashResp(wr, hr)
		return
	}

	_, uid, ok := s.Auth.ManagerAuthorize(token,
		r.URL.Path+"\n", bucket)
	if !ok {

		hr.Code, hr.Msg = http.StatusUnauthorized, "bad request token"
		code = http.StatusUnauthorized
		HashResp(wr, hr)
		return
	}
	bucket = meta.Get_hbase_bucketname(uid, bucket)
	ekey = b64.URLEncoding.EncodeToString([]byte(bucket + ":" + file))

	if _, _, _, size, _, code, errMsg = s.efs.Stat(bucket,
		file); code != http.StatusOK {
		hr.Code = code
		hr.Msg = errMsg
		HashResp(wr, hr)
		return
	}

	const DIRECT_SIZE = 500 * 1024 * 1024
	if size <= DIRECT_SIZE {
		code, errMsg, hash = s.directHash(ekey, hashType)
	} else {
		code, errMsg, hash = s.streamHash(ekey, hashType, size)
	}

	if code != http.StatusOK {
		hr.Code = code
		hr.Msg = errMsg
		HashResp(wr, hr)
	}
	hr.Code = http.StatusOK
	hr.Msg = ""
	hr.Hash = hash
	hr.FSize = size
	HashResp(wr, hr)
}

// direct hash
func (s *server) directHash(ekey, hashType string) (code int, errMsg, hash string) {
	var (
		data []byte
	)

	if data, code, errMsg = s.efs.DownloadDirect(ekey); code != http.StatusOK {
		log.Errorf("downloaddirect code wrong:%d  err:%s", code, errMsg)
		return
	}

	switch hashType {
	case "sha1":
		code = http.StatusOK
		hash = fmt.Sprintf("%x", sha1.Sum(data))
	case "md5":
		code = http.StatusOK
		hash = fmt.Sprintf("%x", md5.Sum(data))
	default:
		code, errMsg = http.StatusBadRequest, "hash type error"
	}

	return
}

// stream hash
func (s *server) streamHash(ekey, hashType string, size int64) (code int, errMsg, hashStr string) {
	const BLOCK_SIZE = 4 * 1024 * 1024
	var (
		data          []byte
		start, end, i int64
		hs            hash.Hash
	)

	switch hashType {
	case "sha1":
		hs = sha1.New()
	case "md5":
		hs = md5.New()
	default:
		code, errMsg = http.StatusBadRequest, "hash type error"
	}

	for i = 1; i <= size/BLOCK_SIZE; i++ {
		start = (i - 1) * BLOCK_SIZE
		end = i*BLOCK_SIZE - 1
		if data, code, errMsg = s.efs.DownloadSlice(ekey, start, end); code != http.StatusOK {
			return
		}
		if _, err := io.Copy(hs, bytes.NewReader(data)); err != nil {
			code, errMsg = http.StatusInternalServerError, "write data to hash error"
			return
		}
	}

	if size%BLOCK_SIZE != 0 {
		start = (i - 1) * BLOCK_SIZE
		end = size - 1
		if data, code, errMsg = s.efs.DownloadSlice(ekey, start, end); code != http.StatusOK {
			return
		}
		if _, err := io.Copy(hs, bytes.NewReader(data)); err != nil {
			code, errMsg = http.StatusInternalServerError, "write data to hash error"
			return
		}
	}

	hashStr = fmt.Sprintf("%x", hs.Sum(nil))
	code, errMsg = http.StatusOK, ""
	return
}

func (s *server) fetch(wr http.ResponseWriter, r *http.Request) {
	var (
		start                       = time.Now()
		req                         = &FetchReq{R: r}
		resp                        = &FetchResp{}
		size, oldsize               int64
		replication, uid            int
		ok                          bool
		code                        = http.StatusOK
		err                         error
		hash, key, mimeType, errMsg string
		bucket, filename            string
	)
	overwriteflag := 1
	defer statislog("/r/fetch", &bucket, &filename, &overwriteflag,
		&oldsize, &size, start, &code, &resp.ErrData.Msg)

	if code, errMsg = req.Parse(); code != http.StatusOK {
		resp.ErrData.Code = code
		resp.ErrData.Msg = errMsg
		resp.ErrorResp(wr)
		return
	}

	if replication, uid, ok = s.Auth.ManagerAuthorize(req.Data.Token,
		req.R.URL.Path+"\n", req.Data.Bucket); !ok {
		code = http.StatusUnauthorized
		resp.ErrData.Code = http.StatusUnauthorized
		resp.ErrData.Msg = "bad request token"
		resp.ErrorResp(wr)
		return
	}

	bucket = meta.Get_hbase_bucketname(uid, req.Data.Bucket)
	filename = req.Data.Key

	if size, err = fetch.FileSize(req.Data.FetchURL); err != nil {
		resp.ErrData.Code = http.StatusInternalServerError
		code = http.StatusInternalServerError
		resp.ErrData.Msg = "fetch data error"
		resp.ErrorResp(wr)
		return
	}

	if hash, key, mimeType, code, errMsg, oldsize = s.efs.FetchUpload(bucket,
		filename, req.Data.FetchURL, size, replication); code != http.StatusOK {
		resp.ErrData.Code = code
		resp.ErrData.Msg = errMsg
		resp.ErrorResp(wr)
		return
	}

	resp.Data.Key = key
	resp.Data.Hash = hash
	resp.Data.MimeType = mimeType
	resp.Data.FSize = size
	resp.OKResp(wr)
	return
}

func (s *server) batch(wr http.ResponseWriter, r *http.Request) {
	var (
		start  = time.Now()
		req    = &BatchReq{R: r}
		resp   = &BatchResp{}
		bucket string
		ok     bool
		err    error
		errStr string
		code   = http.StatusOK

		uid int
	)
	tBucket := "-"
	key := "-"
	fsize := int64(0)
	oldsize := int64(0)
	overwriteflag := 0
	defer infolog("batch", &tBucket, &key, &overwriteflag, &oldsize, &fsize, start, &code, &resp.ErrData.Msg)

	if bucket, code, errStr = req.Parse(); code != http.StatusOK {
		resp.ErrData.Code, resp.ErrData.Msg = code, errStr
		resp.ErrorResp(wr)
		return
	}

	if _, uid, ok = s.Auth.ManagerAuthorize(req.Data.Token,
		req.R.URL.Path+"\n"+req.Data.BodyForm, bucket); !ok {
		code = http.StatusUnauthorized
		resp.ErrData.Code, resp.ErrData.Msg = http.StatusUnauthorized, "bad request token"
		resp.ErrorResp(wr)
		return
	}

	var (
		respItems                                []*meta.PFBatchItem
		respItem                                 *meta.PFBatchItem
		errMsg                                   map[string]string
		b64Data                                  []byte
		srcArr, destArr                          []string
		srcBucket, destBucket, srcFile, destFile string
		containFalse                             bool
		deleteAfterDays                          int
	)

	for _, op := range req.Data.Ops {
		respItem = new(meta.PFBatchItem)
		errMsg = make(map[string]string)

		opKey := op["op"]
		switch opKey {
		case "stat":
			var (
				start1        = time.Now()
				hash, mime    string
				putTime, size int64
				respStat      meta.PFStatRetOK
				errstring     string
			)
			b64Data, _ = b64.URLEncoding.DecodeString(op["src"])
			srcArr = strings.Split(string(b64Data), KEY_DELIMITER)
			if len(srcArr) == 2 {
				srcBucket = meta.Get_hbase_bucketname(uid, srcArr[0])
				srcFile = srcArr[1]
			}
			hash, mime, putTime, size, deleteAfterDays, code, errMsg["error"] = s.efs.Stat(srcBucket, srcFile)
			respItem.Code = code
			if code != http.StatusOK {
				containFalse = true
				respItem.Data = errMsg
			} else {
				respStat.FSize = size
				respStat.Hash = hash
				respStat.MimeType = mime
				respStat.PutTime = putTime
				respStat.DeleteAfterDays = deleteAfterDays
				respItem.Data = respStat
			}
			errstring = errMsg["error"]
			statislog("/r/stat", &srcBucket, &srcFile, &overwriteflag, &oldsize,
				&size, start1, &code, &errstring)
		case "copy":
			var (
				srcekey, destekey string
				start1            = time.Now()
				size              int64
				errstring         string
			)
			b64Data, _ = b64.URLEncoding.DecodeString(op["src"])
			srcArr = strings.Split(string(b64Data), KEY_DELIMITER)
			if len(srcArr) == 2 {
				srcBucket = meta.Get_hbase_bucketname(uid, srcArr[0])
				srcFile = srcArr[1]
			}
			b64Data, _ = b64.URLEncoding.DecodeString(op["dest"])
			destArr = strings.Split(string(b64Data), KEY_DELIMITER)
			if len(destArr) == 2 {
				destBucket = meta.Get_hbase_bucketname(uid, destArr[0])
				destFile = destArr[1]
			}
			size, oldsize, code, errMsg["error"] = s.efs.Copy(srcBucket, srcFile,
				destBucket, destFile, op["isforce"] == "true")
			respItem.Code = code
			if code != http.StatusOK {
				containFalse = true
				respItem.Data = errMsg
			}
			errstring = errMsg["error"]
			srcekey = b64.URLEncoding.EncodeToString([]byte(srcBucket + ":" + srcFile))
			destekey = destBucket + ":" + destFile
			statislog("/r/copy", &srcekey, &destekey, &overwriteflag, &oldsize,
				&size, start1, &code, &errstring)
		case "move":
			var (
				size              int64
				start1            = time.Now()
				srcekey, destekey string
				errstring         string
			)
			b64Data, _ = b64.URLEncoding.DecodeString(op["src"])
			srcArr = strings.Split(string(b64Data), KEY_DELIMITER)
			if len(srcArr) == 2 {
				srcBucket = meta.Get_hbase_bucketname(uid, srcArr[0])
				srcFile = srcArr[1]
			}
			b64Data, _ = b64.URLEncoding.DecodeString(op["dest"])
			destArr = strings.Split(string(b64Data), KEY_DELIMITER)
			if len(destArr) == 2 {
				destBucket = meta.Get_hbase_bucketname(uid, destArr[0])
				destFile = destArr[1]
			}
			size, oldsize, code, errMsg["error"] = s.efs.Move(srcBucket, srcFile,
				destBucket, destFile, op["isforce"] == "true")
			respItem.Code = code
			if code != http.StatusOK {
				containFalse = true
				respItem.Data = errMsg
			}
			errstring = errMsg["error"]
			srcekey = b64.URLEncoding.EncodeToString([]byte(srcBucket + ":" + srcFile))
			destekey = destBucket + ":" + destFile
			statislog("/r/move", &srcekey, &destekey, &overwriteflag, &oldsize,
				&size, start1, &code, &errstring)

		case "delete":
			var (
				size      int64
				start1    = time.Now()
				errstring string
			)
			b64Data, _ = b64.URLEncoding.DecodeString(op["src"])
			srcArr = strings.Split(string(b64Data), KEY_DELIMITER)
			if len(srcArr) == 2 {
				srcBucket = meta.Get_hbase_bucketname(uid, srcArr[0])
				srcFile = srcArr[1]
			}
			size, code, errMsg["error"] = s.efs.Delete(srcBucket, srcFile)
			respItem.Code = code
			if code != http.StatusOK {
				containFalse = true
				respItem.Data = errMsg
			}
			errstring = errMsg["error"]
			statislog("/r/delete", &srcBucket, &srcFile, &overwriteflag, &oldsize,
				&size, start1, &code, &errstring)
		default:
			containFalse = true
			code = errors.RetUrlBad
			errMsg["error"] = "op error,op: " + op["op"]
			respItem.Code = code
			respItem.Data = errMsg
		}

		respItems = append(respItems, respItem)
	}

	/*
		if resp.Data, code, errMsg = s.efs.Batch(req.Data.Ops); code != http.StatusOK &&
			code != errors.RetPartialFailed {
			resp.ErrData.Code, resp.ErrData.Msg = code, errMsg
			resp.ErrorResp(wr)
			return
		}
	*/

	if resp.Data, err = json.Marshal(respItems); err != nil {
		resp.ErrData.Code, resp.ErrData.Msg = http.StatusInternalServerError, "json encode error"
		resp.ErrorResp(wr)
		code = http.StatusInternalServerError
		return
	}

	if containFalse {
		code = errors.RetPartialFailed
	}

	resp.OKResp(wr, code)
	return
}

func (s *server) prefetch(wr http.ResponseWriter, r *http.Request) {
	var (
		start            = time.Now()
		req              = &PreFetchReq{R: r}
		resp             = &PreFetchResp{}
		size, oldsize    int64
		replication, uid int
		code             = http.StatusOK
		err              error
		errMsg           string
		imgSource        string
		ok               bool
		bucket, filename string
	)
	overwriteflag := 1
	defer statislog("/r/prefetch", &bucket, &filename, &overwriteflag,
		&oldsize, &size, start, &code, &resp.ErrData.Msg)

	if code, errMsg = req.Parse(); code != http.StatusOK {
		resp.ErrData.Code = code
		resp.ErrData.Msg = errMsg
		resp.ErrorResp(wr)
		return
	}

	if imgSource, replication, uid, ok = s.Auth.ManagerPreFetchAuthorize(req.Data.Token,
		req.R.URL.Path+"\n", req.Data.Bucket); !ok {
		code = http.StatusUnauthorized
		resp.ErrData.Code = http.StatusUnauthorized
		resp.ErrData.Msg = "bad request token"
		resp.ErrorResp(wr)
		return
	}
	bucket = meta.Get_hbase_bucketname(uid, req.Data.Bucket)
	filename = req.Data.Key

	req.Data.PreFetchURL = imgSource + "/" + req.Data.Key

	if size, err = fetch.FileSize(req.Data.PreFetchURL); err != nil {
		code = http.StatusInternalServerError
		resp.ErrData.Code = http.StatusInternalServerError
		resp.ErrData.Msg = "fetch data error"
		resp.ErrorResp(wr)
		return
	}

	if _, _, _, code, errMsg, oldsize = s.efs.FetchUpload(bucket,
		filename, req.Data.PreFetchURL, size, replication); code != http.StatusOK {
		resp.ErrData.Code = code
		resp.ErrData.Msg = errMsg
		resp.ErrorResp(wr)
		return
	}

	resp.OKResp(wr)
	return
}

func (s *server) deleteAfter(wr http.ResponseWriter, r *http.Request) {
	var (
		bucket, file, token, errMsg string
		days                        int
		code                        = http.StatusOK
		start                       = time.Now()
	)
	fsize := int64(0)
	oldsize := int64(0)
	//overwriteflag := 0
	defer statislog("/r/deleteAfter", &bucket, &file, &days,
		&oldsize, &fsize, start, &code, &errMsg)

	bucket, file, days, token, code, errMsg = parseDeleteAfterDays(r)
	if code != http.StatusOK {
		respDeleteAfterDays(wr, code, errMsg)
		return
	}

	_, uid, ok := s.Auth.ManagerAuthorize(token,
		r.URL.Path+"\n", bucket)
	if !ok {

		code, errMsg = http.StatusUnauthorized, "bad request token"
		respDeleteAfterDays(wr, code, errMsg)
		return
	}
	bucket = meta.Get_hbase_bucketname(uid, bucket)

	if code, errMsg = s.efs.DeleteAfterDays(bucket, file, days); code != http.StatusOK {
		respDeleteAfterDays(wr, code, errMsg)
		return
	}

	respDeleteAfterDays(wr, code, errMsg)
	return
}

func parseDeleteAfterDays(req *http.Request) (bucket, file string, days int,
	token string, code int, errMsg string) {
	code = http.StatusOK

	pathArr := strings.Split(strings.Trim(req.URL.Path, "/"), "/")
	if len(pathArr) != 3 || pathArr[1] == "" || pathArr[2] == "" {
		code, errMsg = http.StatusBadRequest, "bad request url params"
		return
	}

	data, err := b64.URLEncoding.DecodeString(pathArr[1])
	if err != nil {
		code, errMsg = http.StatusBadRequest, "bad request url base64"
		return
	}
	keyArr := strings.Split(string(data), ":")
	if len(keyArr) != 2 || keyArr[0] == "" || keyArr[1] == "" {
		code, errMsg = http.StatusBadRequest, "bad request url ekey"
		return
	}
	bucket = keyArr[0]
	file = keyArr[1]

	days, err = strconv.Atoi(pathArr[2])
	if err != nil {
		code, errMsg = http.StatusBadRequest, "bad request url days"
		return
	}

	tokenArr := strings.Split(strings.TrimSpace(req.Header.Get("Authorization")), " ")
	if len(tokenArr) != 2 || tokenArr[1] == "" {
		code, errMsg = http.StatusUnauthorized, "bad token request"
	}
	token = tokenArr[1]

	return
}

func respDeleteAfterDays(wr http.ResponseWriter, code int, errMsg string) {
	wr.WriteHeader(code)
	if code == http.StatusOK {
		return
	}

	tMap := make(map[string]string, 1)
	tMap["error"] = errMsg
	data, _ := json.Marshal(tMap)
	wr.Write(data)
}
