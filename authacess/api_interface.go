package main

import (
	"bytes"
	"efs/authacess/conf"
	"efs/libs/meta"
	log "efs/log/glog"
	"encoding/base64"
	"encoding/json"
	//"fmt"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"os"
	"strconv"
	"strings"
)

const (
	KEY_DELIMITER        = ":"
	TOKEN_DELIMITER      = " "
	ResOk                = 200
	E_BadMessage         = 400
	E_TokenInvalid       = 401
	E_DataToolong        = 413
	E_Limit              = 413
	E_CallbackFail       = 579
	E_ServerFail         = 599
	E_DestSourceExists   = 614
	_upload_content_type = "multipart/form-data"
	_redirect_url        = "%s?upload_ret=%s"
)

type Err_res struct {
	Code  int    `json:"code"`
	Error string `json:"error"`
}

type HttpRe struct {
	R *http.Request
}

//upload
type Upload_req struct {
	HttpRe
	Host           string //hostname
	Content_Type   string //http message type
	Content_Length string //mutilpart data len
	Token          string
	XVariable      map[string]string //Custom variable
	File           string            //file name
	FileBinaryData multipart.File    //file data
	Key            string
	Cr32           string //data cr32
	Accept         string //http header content type
	Size           int64
}

type sizer interface {
	Size() int64
}

type Upload_body_res struct {
	Hash string `json:hash`
	Key  string `json:"key"`
}

type Upload_res struct {
	Res  Err_res
	Ures Upload_body_res
}

// checkFileSize get multipart.File size
func checkFileSize(file multipart.File) (size int64, err error) {
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

func interfacetostring(m map[string]interface{}, key string) (r string) {
	var (
		ok             bool
		interfacevalue interface{}
	)
	interfacevalue, ok = m[key]
	if !ok {
		log.Errorf("map have no this key:%s", key)
		return ""
	}
	r, ok = interfacevalue.(string)
	if !ok {
		log.Errorf("this key %s is not string type ", key)
		return ""
	}
	return r

}

func (ures *Upload_res) ResponseErr(wr http.ResponseWriter) {
	retJson, err := json.Marshal(ures.Res)
	if err != nil {
		log.Errorf("upload response marshal json error(%v)", err)
	}
	wr.Header().Set("Access-Control-Allow-Origin", "*")
	wr.Header().Add("Access-Control-Allow-Headers", "Content-Type")
	wr.Header().Add("Access-Control-Allow-Headers", "enctype")
	wr.Header().Add("Access-Control-Allow-Headers", "Authorization")
	wr.Header().Set("Access-Control-Allow-Methods", "OPTIONS, HEAD, POST")
	wr.Header().Set("Content-Type", "application/json")
	wr.Header().Set("Cache-Control", "no-store")
	wr.WriteHeader(ures.Res.Code)
	wr.Write(retJson)
}

func Responsecbk(wr http.ResponseWriter, retcode int, rbody []byte) {

	wr.Header().Set("Access-Control-Allow-Origin", "*")
	wr.Header().Add("Access-Control-Allow-Headers", "Content-Type")
	wr.Header().Add("Access-Control-Allow-Headers", "enctype")
	wr.Header().Add("Access-Control-Allow-Headers", "Authorization")
	wr.Header().Set("Access-Control-Allow-Methods", "OPTIONS, HEAD, POST")
	wr.Header().Set("Content-Type", "application/json")
	wr.Header().Set("Cache-Control", "no-store")
	wr.WriteHeader(retcode)
	wr.Write(rbody)
}

func (ures *Upload_res) Upload_response(wr http.ResponseWriter) {
	retJson, err := json.Marshal(ures.Ures)
	if err != nil {
		log.Errorf("upload response marshal json error(%v)", err)
	}
	wr.Header().Set("Access-Control-Allow-Origin", "*")
	wr.Header().Add("Access-Control-Allow-Headers", "Content-Type")
	wr.Header().Add("Access-Control-Allow-Headers", "enctype")
	wr.Header().Add("Access-Control-Allow-Headers", "Authorization")
	wr.Header().Set("Access-Control-Allow-Methods", "OPTIONS, HEAD, POST")
	wr.Header().Set("Content-Type", "application/json")
	wr.Header().Set("Cache-Control", "no-store")
	wr.WriteHeader(ures.Res.Code)
	wr.Write(retJson)
}

func (ureq *Upload_req) Parms(c *conf.Config) (code int, errstring string) {
	var (
		err error

		r          = ureq.R
		fileheader *multipart.FileHeader
	)
	code = ResOk
	ureq.Host = r.Host
	if ureq.Host == "" {
		log.Errorf("upload api req Host is null")
		code = E_BadMessage
		errstring = "Host is null"
		return
	}

	ureq.Content_Type = r.Header.Get("Content-Type")
	if ureq.Content_Type == "" || !strings.HasPrefix(ureq.Content_Type, _upload_content_type) {
		log.Errorf("upload api req content type %s wrongful", ureq.Content_Type)
		code = E_BadMessage
		errstring = "Content-Type not multipart/form-data"
		return
	}

	ureq.Content_Length = r.Header.Get("Content-Length")

	if ureq.Token = r.FormValue("token"); ureq.Token == "" {
		log.Errorf("upload api req get token failed")
		code = E_TokenInvalid
		errstring = "token is invalid"
		return
	}

	//ureq.XVariableName = r.Form["xVariableName"]
	//ureq.XVariableValue = r.Form["xVariableValue"]

	if ureq.FileBinaryData, fileheader, err = r.FormFile("file"); err != nil {
		log.Errorf("upload api req get filebinarydata failed err:%s", err.Error())
		code = E_BadMessage
		errstring = "FileBinaryData is invalid"
		return
	}
	ureq.File = fileheader.Filename
	//fmt.Println("lala=======", fileheader.Filename)

	ureq.Key = r.FormValue("key")

	ureq.Cr32 = r.FormValue("crc32")
	ureq.Accept = r.FormValue("accept")
	ureq.Size, err = checkFileSize(ureq.FileBinaryData)
	if err != nil {
		log.Errorf("upload api req get filesize failed")
		code = E_BadMessage
		errstring = "FileBinaryData is invalid"
		return
	}
	ureq.XVariable = make(map[string]string)
	//获取自定义参数名 以x：为前缀的自定义变量key
	for vkey, _ := range r.Form {
		if strings.HasPrefix(vkey, "x:") {
			ureq.XVariable[vkey] = r.FormValue(vkey)
		}
	}

	return

}

func (ureq *Upload_req) FileClose() {
	ureq.FileBinaryData.Close()
}

//mkblock
type Mkblock_req struct {
	HttpRe
	Host           string
	Content_Type   string
	Content_Length string
	Token          string
	Blocksize      int64
	Body           []byte
}

type Mkblock_body_res struct {
	Ctx      string `json:"ctx"`
	Checksum string `json:"checksum"`
	Id       string `json:"id"`
	Cr32     int64  `json:"cr32"`
	Offset   int64  `json:"offset"`
	Host     string `json:"host"`
}

type Mkblock_res struct {
	Res  Err_res
	Ures Mkblock_body_res
}

func (ures *Mkblock_res) ResponseErr(wr http.ResponseWriter) {
	retJson, err := json.Marshal(ures.Res)
	if err != nil {
		log.Errorf("upload response marshal json error(%v)", err)
	}
	wr.Header().Set("Content-Type", "application/json")
	wr.Header().Set("Cache-Control", "no-store")
	wr.WriteHeader(ures.Res.Code)
	wr.Write(retJson)
}
func (ures *Mkblock_res) Cross_domain(wr http.ResponseWriter) {
	wr.Header().Set("Access-Control-Allow-Origin", "*")
	wr.Header().Add("Access-Control-Allow-Headers", "Content-Type")
	wr.Header().Add("Access-Control-Allow-Headers", "enctype")
	wr.Header().Add("Access-Control-Allow-Headers", "Authorization")
	wr.Header().Set("Access-Control-Allow-Methods", "OPTIONS, HEAD, POST")
	wr.Header().Set("Content-Type", "application/json")
	wr.Header().Set("Cache-Control", "no-store")
	wr.WriteHeader(ures.Res.Code)
}

type Bput_body_res struct {
	Ctx      string `json:"ctx"`
	Checksum string `json:"checksum"`
	Id       string `json:"id"`
	Cr32     int64  `json:"cr32"`
	Offset   int64  `json:"offset"`
	Host     string `json:"host"`
}
type Bput_req struct {
	HttpRe
	Host            string
	Content_Type    string
	Content_Length  string
	Token           string
	Ctx             string
	Id              string
	Nextchuckoffset int64
	Body            []byte
}

type Bput_res struct {
	Res  Err_res
	Ures Bput_body_res
}

func (ures *Bput_res) ResponseErr(wr http.ResponseWriter) {
	retJson, err := json.Marshal(ures.Res)
	if err != nil {
		log.Errorf("upload response marshal json error(%v)", err)
	}
	wr.Header().Set("Access-Control-Allow-Origin", "*")
	wr.Header().Add("Access-Control-Allow-Headers", "Content-Type")
	wr.Header().Add("Access-Control-Allow-Headers", "enctype")
	wr.Header().Add("Access-Control-Allow-Headers", "Authorization")
	wr.Header().Set("Access-Control-Allow-Methods", "OPTIONS, HEAD, POST")
	wr.Header().Set("Cache-Control", "no-store")
	wr.Header().Set("Content-Type", "application/json")
	wr.Header().Set("Cache-Control", "no-store")

	wr.WriteHeader(ures.Res.Code)
	wr.Write(retJson)
}

func (ures *Bput_res) Cross_domain(wr http.ResponseWriter) {
	wr.Header().Set("Access-Control-Allow-Origin", "*")
	wr.Header().Add("Access-Control-Allow-Headers", "Content-Type")
	wr.Header().Add("Access-Control-Allow-Headers", "enctype")
	wr.Header().Add("Access-Control-Allow-Headers", "Authorization")
	wr.Header().Set("Access-Control-Allow-Methods", "OPTIONS, HEAD, POST")
	wr.Header().Set("Content-Type", "application/json")
	wr.Header().Set("Cache-Control", "no-store")
	wr.WriteHeader(ures.Res.Code)
}

type Mkfile_req struct {
	HttpRe
	Host           string
	Content_Type   string
	Content_Length string
	Token          string
	Id             string
	Filesize       int64
	Key            string
	Mimetype       string
	Callbakurl     string
	Body           []byte
	Valiable       map[string]string
}
type Mkfile_body_res struct {
	Needcallbak bool   `json:needcallbak`
	Hash        string `json:hash`
	Key         string `json:"key"`
}

type Mkfile_res struct {
	Res  Err_res
	Ures Mkfile_body_res
}

func (ures *Mkfile_res) Upload_response(wr http.ResponseWriter) {
	retJson, err := json.Marshal(ures.Ures)
	if err != nil {
		log.Errorf("upload response marshal json error(%v)", err)
	}
	wr.Header().Set("Access-Control-Allow-Origin", "*")
	wr.Header().Add("Access-Control-Allow-Headers", "Content-Type")
	wr.Header().Add("Access-Control-Allow-Headers", "enctype")
	wr.Header().Add("Access-Control-Allow-Headers", "Authorization")
	wr.Header().Set("Access-Control-Allow-Methods", "OPTIONS, HEAD, POST")
	wr.Header().Set("Cache-Control", "no-store")
	wr.Header().Set("Content-Type", "application/json")
	wr.Header().Set("Content-Type", "application/json")
	wr.Header().Set("Cache-Control", "no-store")
	wr.WriteHeader(ures.Res.Code)
	wr.Write(retJson)
}
func (ures *Mkfile_res) Cross_domain(wr http.ResponseWriter) {
	wr.Header().Set("Access-Control-Allow-Origin", "*")
	wr.Header().Add("Access-Control-Allow-Headers", "Content-Type")
	wr.Header().Add("Access-Control-Allow-Headers", "enctype")
	wr.Header().Add("Access-Control-Allow-Headers", "Authorization")
	wr.Header().Set("Access-Control-Allow-Methods", "OPTIONS, HEAD, POST")
	wr.Header().Set("Content-Type", "application/json")
	wr.Header().Set("Cache-Control", "no-store")
	wr.WriteHeader(ures.Res.Code)
}

func (ures *Mkfile_res) ResponseErr(wr http.ResponseWriter) {
	retJson, err := json.Marshal(ures.Res)
	if err != nil {
		log.Errorf("upload response marshal json error(%v)", err)
	}
	wr.Header().Set("Access-Control-Allow-Origin", "*")
	wr.Header().Add("Access-Control-Allow-Headers", "Content-Type")
	wr.Header().Add("Access-Control-Allow-Headers", "enctype")
	wr.Header().Add("Access-Control-Allow-Headers", "Authorization")
	wr.Header().Set("Access-Control-Allow-Methods", "OPTIONS, HEAD, POST")
	wr.Header().Set("Cache-Control", "no-store")
	wr.Header().Set("Content-Type", "application/json")
	wr.Header().Set("Cache-Control", "no-store")
	wr.WriteHeader(ures.Res.Code)
	wr.Write(retJson)
}

func (mbk *Mkblock_req) Parms(c *conf.Config, urlpath string) (code int, errstring string) {
	var (
		//	datalen int64
		err error

		r = mbk.R
	)
	code = ResOk
	mbk.Host = r.Host
	if mbk.Host == "" {
		log.Errorf("upload api req Host is null")
		code = E_BadMessage
		errstring = "Host is null"
		return
	}

	mbk.Content_Length = r.Header.Get("Content-Length")

	if mbk.Token = r.Header.Get("Authorization"); mbk.Token == "" {
		log.Errorf("upload api req get token failed")
		code = E_TokenInvalid
		errstring = "token is invalid"
		return
	}
	ppath := urlpath[1:]
	pathparms := strings.Split(ppath, "/")
	if len(pathparms) != 2 {
		log.Errorf("mkblock url path invalid")
		code = E_BadMessage
		errstring = "urlpath is invalid"
		return
	}
	mbk.Blocksize, err = strconv.ParseInt(pathparms[1], 10, 64)
	if err != nil {
		log.Errorf("blocksize invalid")
		code = E_BadMessage
		errstring = "urlpath is invalid"
		return
	}
	if mbk.Blocksize > c.SliceFileSize {
		log.Errorf("upload api req content-length %ld is too long", mbk.Content_Length)
		code = E_DataToolong
		errstring = "data length is too long"
		return
	}
	mbk.Content_Type = r.Header.Get("Content-Type")
	/*only for web mkblk upload */
	webtypeheader := r.Header.Get("enctype")
	if webtypeheader == "multipart/form-data" {
		var FileBinaryData multipart.File
		if mbk.Content_Type == "" || !strings.HasPrefix(mbk.Content_Type, "multipart/form-data") {
			log.Errorf("upload api req content type %s wrongful", mbk.Content_Type)
			code = E_BadMessage
			errstring = "Content-Type not multipart/form-data"
			return
		}
		if FileBinaryData, _, err = r.FormFile("file"); err != nil {
			log.Errorf("web mkblk req get filebinarydata failed err:%s", err.Error())
			code = E_BadMessage
			errstring = "FileBinaryData is invalid"
			return
		}
		defer FileBinaryData.Close()
		if mbk.Body, err = ioutil.ReadAll(FileBinaryData); err != nil {
			log.Errorf("mkblock read from body %v", err)
			code = E_BadMessage
			errstring = "data is invalid"
			return
		}
		/*only for web mkblk upload */
	} else {
		/*
				datalen, err = strconv.ParseInt(mbk.Content_Length, 10, 64)
				if err != nil {
					log.Errorf("upload api req content-length %s is wrongful", mbk.Content_Length)
					code = E_BadMessage
					errstring = "Content-Length Wrongful"
					return
				}
				if datalen > c.SliceFileSize {
					code = E_DataToolong
					errstring = "data length is too long"
					return
				}

			if mbk.Content_Type == "" || !strings.HasPrefix(mbk.Content_Type, "application/octet-stream") {
				log.Errorf("upload api req content type %s wrongful", mbk.Content_Type)
				code = E_BadMessage
				errstring = "Content-Type not multipart/form-data"
				return
			}
		*/
		defer r.Body.Close()
		if mbk.Body, err = ioutil.ReadAll(r.Body); err != nil {
			log.Errorf("mkblock read body %v", err)
			code = E_BadMessage
			errstring = "data is invalid"
			return
		}
	}

	if int64(len(mbk.Body)) > mbk.Blocksize {
		log.Errorf("bodysize %d big %d", len(mbk.Body), mbk.Blocksize)
		code = E_DataToolong
		errstring = "data length is too long and big input block size"
		return
	}

	return

}

func (bput *Bput_req) Parms(c *conf.Config, urlpath string) (code int, errstring string) {
	var (
		datalen int64
		err     error

		r = bput.R
	)
	code = ResOk
	bput.Host = r.Host
	if bput.Host == "" {
		log.Errorf("upload api req Host is null")
		code = E_BadMessage
		errstring = "Host is null"
		return
	}
	/*
		bput.Content_Type = r.Header.Get("Content-Type")
		if bput.Content_Type == "" || !strings.HasPrefix(bput.Content_Type, "application/octet-stream") {
			log.Errorf("upload api req content type %s wrongful", bput.Content_Type)
			code = E_BadMessage
			errstring = "Content-Type not multipart/form-data"
			return
		}
	*/
	bput.Content_Length = r.Header.Get("Content-Length")
	datalen, err = strconv.ParseInt(bput.Content_Length, 10, 64)
	if err != nil {
		log.Errorf("upload api req content-length %s is wrongful", bput.Content_Length)
		code = E_BadMessage
		errstring = "Content-Length Wrongful"
		return
	}
	if datalen > c.SliceFileSize {
		log.Errorf("upload api req content-length %ld is too long", bput.Content_Length)
		code = E_DataToolong
		errstring = "data length is too long"
		return
	}

	if bput.Token = r.Header.Get("Authorization"); bput.Token == "" {
		log.Errorf("upload api req get token failed")
		code = E_TokenInvalid
		errstring = "token is invalid"
		return
	}

	ppath := urlpath[1:]
	pathparms := strings.Split(ppath, "/")
	if len(pathparms) != 3 {
		log.Errorf("mkblock url path invalid")
		code = E_BadMessage
		errstring = "urlpath is invalid"
		return
	}
	bput.Ctx = pathparms[1]
	bput.Nextchuckoffset, err = strconv.ParseInt(pathparms[2], 10, 64)
	if err != nil {
		log.Errorf("nextchuckoffset invalid")
		code = E_BadMessage
		errstring = "nextchuckoffset is invalid"
		return
	}
	/*
		if bput.Id = pathparms[3]; bput.Id == "" {
			log.Errorf("upload api req get id failed")
			code = E_BadMessage
			errstring = "id is invalid"
			return
		}
	*/
	defer r.Body.Close()
	if bput.Body, err = ioutil.ReadAll(r.Body); err != nil {
		log.Errorf("mkblock read body %v", err)
		code = E_BadMessage
		errstring = "data is invalid"
		return
	}

	return
}

func (mkf *Mkfile_req) Parms(c *conf.Config, urlpath string) (code int, errstring string) {
	var (
		//datalen             int64
		err                              error
		tkey, tmimetype, tcallbakurl, tv []byte
		r                                = mkf.R
	)
	code = ResOk
	mkf.Host = r.Host
	if mkf.Host == "" {
		log.Errorf("upload api req Host is null")
		code = E_BadMessage
		errstring = "Host is null"
		return
	}
	/*
		mkf.Content_Type = r.Header.Get("Content-Type")
		if mkf.Content_Type == "" || !strings.HasPrefix(mkf.Content_Type, "text/plain") {
			log.Errorf("upload api req content type %s wrongful", mkf.Content_Type)
			code = E_BadMessage
			errstring = "Content-Type not text/plain"
			return
		}
	*/
	mkf.Content_Length = r.Header.Get("Content-Length")
	/*
		datalen, err = strconv.ParseInt(mkf.Content_Length, 10, 64)
		if err != nil {
			log.Errorf("upload api req content-length %s is wrongful", mkf.Content_Length)
			code = E_BadMessage
			errstring = "Content-Length Wrongful"
			return
		}

			if datalen > c.SliceFileSize {
				log.Errorf("upload api req content-length %ld is too long", mkf.Content_Length)
				code = E_DataToolong
				errstring = "data length is too long"
				return
			}
	*/
	if mkf.Token = r.Header.Get("Authorization"); mkf.Token == "" {
		log.Errorf("upload api req get token failed")
		code = E_TokenInvalid
		errstring = "token is invalid"
		return
	}

	ppath := urlpath[1:]
	pathparms := strings.Split(ppath, "/")
	parms_len := len(pathparms)
	if parms_len < 2 {
		log.Errorf("mkblock url path invalid")
		code = E_BadMessage
		errstring = "urlpath is invalid"
		return
	}

	mkf.Filesize, err = strconv.ParseInt(pathparms[1], 10, 64)
	if err != nil {
		log.Errorf("filesize invalid")
		code = E_BadMessage
		errstring = "filesize is invalid"
		return
	}

	if parms_len == 4 || parms_len > 4 {
		if tkey, err = base64.URLEncoding.DecodeString(pathparms[3]); err != nil {
			log.Errorf("decode key failed")
			code = E_BadMessage
			errstring = "urlpath invaild"
			return
		}

		mkf.Key = string(tkey)
	}
	if parms_len == 6 || parms_len > 6 {
		if tmimetype, err = base64.URLEncoding.DecodeString(pathparms[5]); err != nil {
			log.Errorf("decode mimetype failed")
			code = E_BadMessage
			errstring = "urlpath invaild"
			return
		}
		mkf.Mimetype = string(tmimetype)
	}
	if parms_len == 8 || parms_len > 8 {
		if tcallbakurl, err = base64.URLEncoding.DecodeString(pathparms[7]); err != nil {
			log.Errorf("decode callbak failed")
			code = E_BadMessage
			errstring = "urlpath invaild"
			return
		}
		mkf.Callbakurl = string(tcallbakurl)
	}
	if parms_len > 8 {
		if (len(pathparms)-8)%2 != 0 {
			log.Errorf("variable is invaild")
			code = E_BadMessage
			errstring = "urlpath invaild"
			return
		}
		mkf.Valiable = make(map[string]string)
		for i := 8; i < len(pathparms); i = i + 2 {
			if tv, err = base64.URLEncoding.DecodeString(pathparms[i+1]); err != nil {
				log.Errorf("decode valiable failed")
				code = E_BadMessage
				errstring = "urlpath invaild"
				return
			}
			mkf.Valiable[pathparms[i]] = string(tv)
		}
	}
	//	fmt.Println("==========", pathparms)

	defer r.Body.Close()
	if mkf.Body, err = ioutil.ReadAll(r.Body); err != nil {
		log.Errorf("mkblock read body %v", err)
		code = E_BadMessage
		errstring = "data is invalid"
		return
	}

	return
}

//fetch
type FetchReq struct {
	R    *http.Request
	Data FetchReqData
}
type FetchReqData struct {
	Bucket   string `json:"bucket"`
	Key      string `json:"key"`
	FetchURL string `json:"fetchurl"`
	Token    string `json:"token"`
}

func (req *FetchReq) Parse() (code int, errMsg string) {
	var (
		data []byte
		err  error
	)

	pathArr := strings.Split(strings.Trim(req.R.URL.Path, "/"), "/")
	if len(pathArr) != 4 || pathArr[1] == "" || pathArr[3] == "" {
		code, errMsg = http.StatusBadRequest, "bad request url"
		return
	}

	if data, err = base64.URLEncoding.DecodeString(pathArr[1]); err != nil {
		code, errMsg = http.StatusBadRequest, "base64 decode fetch url error"
		return
	}
	req.Data.FetchURL = string(data)

	if data, err = base64.URLEncoding.DecodeString(pathArr[3]); err != nil || len(data) == 0 {
		code, errMsg = http.StatusBadRequest, "base64 decode entry error"
		return
	}
	if bytes.Contains(data, []byte(KEY_DELIMITER)) {
		keyArr := strings.Split(string(data), KEY_DELIMITER)
		if len(keyArr) < 1 || keyArr[0] == "" {
			code, errMsg = http.StatusBadRequest, "bad entry request"
			return
		}
		req.Data.Bucket = keyArr[0]
		if len(keyArr) == 2 {
			req.Data.Key = keyArr[1]
		}
	} else {
		req.Data.Bucket = string(data)
	}

	tokenArr := strings.Split(strings.TrimSpace(req.R.Header.Get("Authorization")), " ")
	if len(tokenArr) != 2 || tokenArr[1] == "" {
		code, errMsg = http.StatusUnauthorized, "bad token request"
		return
	}
	req.Data.Token = tokenArr[1]

	code = http.StatusOK
	return
}

type FetchResp struct {
	ErrData ErrRespData
	Data    FetchRespData
}
type FetchRespData struct {
	FSize    int64  `json:"fsize"`
	MimeType string `json:"mimetype"`
	Key      string `json:"key"`
	Hash     string `json:"hash"`
}

func (resp *FetchResp) ErrorResp(wr http.ResponseWriter) {
	retJson, err := json.Marshal(resp.ErrData)
	if err != nil {
		log.Errorf("fetch error response marshal json error(%s)", err.Error())
	}
	wr.Header().Set("Content-Type", "application/json")
	wr.Header().Set("Cache-Control", "no-store")
	wr.WriteHeader(resp.ErrData.Code)
	wr.Write(retJson)
}

func (resp *FetchResp) OKResp(wr http.ResponseWriter) {
	retJson, err := json.Marshal(resp.Data)
	if err != nil {
		log.Errorf("fetch ok response marshal json error(%s)", err.Error())
	}
	wr.Header().Set("Content-Type", "application/json")
	wr.Header().Set("Cache-Control", "no-store")
	wr.WriteHeader(http.StatusOK)
	wr.Write(retJson)
}

//prefetch
type PreFetchReq struct {
	R    *http.Request
	Data PreFetchReqData
}
type PreFetchReqData struct {
	Bucket      string `json:"bucket"`
	Key         string `json:"key"`
	PreFetchURL string `json:"prefetchurl"`
	Token       string `json:"token"`
}

func (req *PreFetchReq) Parse() (code int, errMsg string) {
	var (
		data []byte
		err  error
	)

	pathArr := strings.Split(strings.Trim(req.R.URL.Path, "/"), "/")
	if len(pathArr) != 2 || pathArr[1] == "" {
		code, errMsg = http.StatusBadRequest, "bad request url"
		return
	}

	if data, err = base64.URLEncoding.DecodeString(pathArr[1]); err != nil {
		code, errMsg = http.StatusBadRequest, "base64 decode entry error"
		return
	}

	keyArr := strings.Split(string(data), KEY_DELIMITER)
	if len(keyArr) != 2 || keyArr[0] == "" || keyArr[1] == "" {
		code, errMsg = http.StatusBadRequest, "bad entry request"
		return
	}
	req.Data.Bucket = keyArr[0]
	req.Data.Key = keyArr[1]

	tokenArr := strings.Split(strings.TrimSpace(req.R.Header.Get("Authorization")), " ")
	if len(tokenArr) != 2 || tokenArr[1] == "" {
		code, errMsg = http.StatusUnauthorized, "bad token request"
		return
	}
	req.Data.Token = tokenArr[1]

	code = http.StatusOK
	return
}

type PreFetchResp struct {
	ErrData ErrRespData
}

func (resp *PreFetchResp) ErrorResp(wr http.ResponseWriter) {
	retJson, err := json.Marshal(resp.ErrData)
	if err != nil {
		log.Errorf("fetch error response marshal json error(%s)", err.Error())
	}
	wr.Header().Set("Content-Type", "application/json")
	wr.Header().Set("Cache-Control", "no-store")
	wr.WriteHeader(resp.ErrData.Code)
	wr.Write(retJson)
}

func (resp *PreFetchResp) OKResp(wr http.ResponseWriter) {
	wr.Header().Set("Cache-Control", "no-store")
	wr.WriteHeader(http.StatusOK)
}

//stat
type StatReq struct {
	R    *http.Request
	Data StatReqData
}
type StatReqData struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
	Token  string `json:"token"`
}

func (req *StatReq) Parse() (code int, errMsg string) {
	if req.Data.Bucket, req.Data.Key, code, errMsg = parseStat(req.R.URL.Path); code != http.StatusOK {
		return
	}

	tokenArr := strings.Split(strings.TrimSpace(req.R.Header.Get("Authorization")), TOKEN_DELIMITER)
	if len(tokenArr) != 2 || tokenArr[1] == "" {
		code, errMsg = http.StatusUnauthorized, "bad token request"
		return
	}
	req.Data.Token = tokenArr[1]

	return
}

func parseStat(url string) (bucket, key string, code int, errMsg string) {
	var (
		data []byte
		err  error
	)

	pathArr := strings.Split(strings.Trim(url, "/"), "/")
	if len(pathArr) != 2 || pathArr[1] == "" {
		code, errMsg = http.StatusBadRequest, "bad request url"
		return
	}

	if data, err = base64.URLEncoding.DecodeString(pathArr[1]); err != nil {
		code, errMsg = http.StatusBadRequest, "base64 decode entry error"
		return
	}

	keyArr := strings.Split(string(data), KEY_DELIMITER)
	if len(keyArr) != 2 || keyArr[0] == "" || keyArr[1] == "" {
		code, errMsg = http.StatusBadRequest, "bad entry request"
		return
	}

	bucket = keyArr[0]
	key = keyArr[1]
	code = http.StatusOK
	return
}

type StatResp struct {
	ErrData ErrRespData
	Data    StatRespData
}
type StatRespData struct {
	FSize           int64  `json:"fsize"`
	MimeType        string `json:"mimetype"`
	PutTime         int64  `json:"puttime"`
	Hash            string `json:"hash"`
	DeleteAfterDays int    `json:"deleteAfterDays"`
}

func (resp *StatResp) ErrorResp(wr http.ResponseWriter) {
	retJson, err := json.Marshal(resp.ErrData)
	if err != nil {
		log.Errorf("stat error response marshal json error(%s)", err.Error())
	}
	wr.Header().Set("Content-Type", "application/json")
	wr.Header().Set("Cache-Control", "no-store")
	wr.WriteHeader(resp.ErrData.Code)
	wr.Write(retJson)
}

func (resp *StatResp) OKResp(wr http.ResponseWriter) {
	retJson, err := json.Marshal(resp.Data)
	if err != nil {
		log.Errorf("stat ok response marshal json error(%s)", err.Error())
	}
	wr.Header().Set("Content-Type", "application/json")
	wr.Header().Set("Cache-Control", "no-store")
	wr.WriteHeader(http.StatusOK)
	wr.Write(retJson)
}

//chgm
type ChgmReq struct {
	R    *http.Request
	Data ChgmReqData
}
type ChgmReqData struct {
	Bucket   string `json:"bucket"`
	Key      string `json:"key"`
	MimeType string `json:"mimetype"`
	Token    string `json:"token"`
}

func (req *ChgmReq) Parse() (code int, errMsg string) {
	if req.Data.Bucket, req.Data.Key, req.Data.MimeType,
		code, errMsg = parseChgm(req.R.URL.Path); code != http.StatusOK {
		return
	}

	tokenArr := strings.Split(strings.TrimSpace(req.R.Header.Get("Authorization")), TOKEN_DELIMITER)
	if len(tokenArr) != 2 || tokenArr[1] == "" {
		code, errMsg = http.StatusUnauthorized, "bad token request"
		return
	}
	req.Data.Token = tokenArr[1]

	return
}

func parseChgm(url string) (bucket, key, mime string, code int, errMsg string) {
	var (
		data []byte
		err  error
	)

	pathArr := strings.Split(strings.Trim(url, "/"), "/")
	if len(pathArr) < 3 || pathArr[1] == "" || (len(pathArr) >= 4 && len(pathArr[3]) > 200) {
		code, errMsg = http.StatusBadRequest, "bad request url"
		return
	}

	if data, err = base64.URLEncoding.DecodeString(pathArr[1]); err != nil {
		code, errMsg = http.StatusBadRequest, "base64 decode entry error"
		return
	}
	keyArr := strings.Split(string(data), KEY_DELIMITER)
	if len(keyArr) != 2 || keyArr[0] == "" || keyArr[1] == "" {
		code, errMsg = http.StatusBadRequest, "bad entry request"
		return
	}

	if len(pathArr) >= 4 && pathArr[3] != "" {
		if data, err = base64.URLEncoding.DecodeString(pathArr[3]); err != nil {
			code, errMsg = http.StatusBadRequest, "base64 decode mime error"
			return
		}
		mime = string(data)
	} else {
		mime = ""
	}

	bucket = keyArr[0]
	key = keyArr[1]
	code = http.StatusOK
	return
}

type ChgmResp struct {
	ErrData ErrRespData
}

func (resp *ChgmResp) OKResp(wr http.ResponseWriter) {
	wr.Header().Set("Cache-Control", "no-store")
	wr.WriteHeader(http.StatusOK)
}

func (resp *ChgmResp) ErrorResp(wr http.ResponseWriter) {
	retJson, err := json.Marshal(resp.ErrData)
	if err != nil {
		log.Errorf("chgm error response marshal json error(%s)", err.Error())
	}
	wr.Header().Set("Content-Type", "application/json")
	wr.Header().Set("Cache-Control", "no-store")
	wr.WriteHeader(resp.ErrData.Code)
	wr.Write(retJson)
}

//move
type MoveReq struct {
	R    *http.Request
	Data MoveReqData
}
type MoveReqData struct {
	SrcBucket  string `json:"srcbucket"`
	SrcKey     string `json:"srckey"`
	DestBucket string `json:"destbucket"`
	DestKey    string `json:"destkey"`
	IsForce    bool   `json:"isforce"`
	Token      string `json:"token"`
}

func (req *MoveReq) Parse() (code int, errMsg string) {
	if req.Data.SrcBucket, req.Data.SrcKey, req.Data.DestBucket, req.Data.DestKey, req.Data.IsForce,
		code, errMsg = parseMove(req.R.URL.Path); code != http.StatusOK {
		return
	}

	tokenArr := strings.Split(strings.TrimSpace(req.R.Header.Get("Authorization")), TOKEN_DELIMITER)
	if len(tokenArr) != 2 || tokenArr[1] == "" {
		code, errMsg = http.StatusUnauthorized, "bad token request"
		return
	}
	req.Data.Token = tokenArr[1]

	return
}

func parseMove(url string) (srcBucket, srcKey, destBucket, destKey string, isForce bool, code int, errMsg string) {
	var (
		data []byte
		err  error
	)

	pathArr := strings.Split(strings.Trim(url, "/"), "/")
	if len(pathArr) == 3 {
		pathArr = append(pathArr, "force")
		pathArr = append(pathArr, "false")
	}
	if len(pathArr) != 5 || pathArr[1] == "" || pathArr[2] == "" || pathArr[4] == "" {
		code, errMsg = http.StatusBadRequest, "bad request url"
		return
	}

	if data, err = base64.URLEncoding.DecodeString(pathArr[1]); err != nil {
		code, errMsg = http.StatusBadRequest, "base64 decode entry error"
		return
	}
	keyArr := strings.Split(string(data), KEY_DELIMITER)
	if len(keyArr) != 2 || keyArr[0] == "" || keyArr[1] == "" {
		code, errMsg = http.StatusBadRequest, "bad entry request"
		return
	}
	srcBucket = keyArr[0]
	srcKey = keyArr[1]

	if data, err = base64.URLEncoding.DecodeString(pathArr[2]); err != nil {
		code, errMsg = http.StatusBadRequest, "base64 decode entry error"
		return
	}
	keyArr = strings.Split(string(data), KEY_DELIMITER)
	if len(keyArr) != 2 || keyArr[0] == "" || keyArr[1] == "" {
		code, errMsg = http.StatusBadRequest, "bad entry request"
		return
	}
	destBucket = keyArr[0]
	destKey = keyArr[1]

	isForce = (string(pathArr[4]) == "true")

	code = http.StatusOK
	return
}

type MoveResp struct {
	ErrData ErrRespData
}

func (resp *MoveResp) OKResp(wr http.ResponseWriter) {
	wr.Header().Set("Cache-Control", "no-store")
	wr.WriteHeader(http.StatusOK)
}

func (resp *MoveResp) ErrorResp(wr http.ResponseWriter) {
	retJson, err := json.Marshal(resp.ErrData)
	if err != nil {
		log.Errorf("move error response marshal json error(%s)", err.Error())
	}
	wr.Header().Set("Content-Type", "application/json")
	wr.Header().Set("Cache-Control", "no-store")
	wr.WriteHeader(resp.ErrData.Code)
	wr.Write(retJson)
}

//copy
type CopyReq struct {
	R    *http.Request
	Data CopyReqData
}
type CopyReqData struct {
	SrcBucket  string `json:"srcbucket"`
	SrcKey     string `json:"srckey"`
	DestBucket string `json:"destbucket"`
	DestKey    string `json:"destkey"`
	IsForce    bool   `json:"isforce"`
	Token      string `json:"token"`
}

func (req *CopyReq) Parse() (code int, errMsg string) {
	if req.Data.SrcBucket, req.Data.SrcKey, req.Data.DestBucket, req.Data.DestKey, req.Data.IsForce,
		code, errMsg = parseCopy(req.R.URL.Path); code != http.StatusOK {
		return
	}

	tokenArr := strings.Split(strings.TrimSpace(req.R.Header.Get("Authorization")), TOKEN_DELIMITER)
	if len(tokenArr) != 2 || tokenArr[1] == "" {
		code, errMsg = http.StatusUnauthorized, "bad token request"
		return
	}
	req.Data.Token = tokenArr[1]

	return
}

func parseCopy(url string) (srcBucket, srcKey, destBucket, destKey string, isForce bool, code int, errMsg string) {
	var (
		data []byte
		err  error
	)

	pathArr := strings.Split(strings.Trim(url, "/"), "/")
	if len(pathArr) == 3 {
		pathArr = append(pathArr, "force")
		pathArr = append(pathArr, "false")
	}
	if len(pathArr) != 5 || pathArr[1] == "" || pathArr[2] == "" || pathArr[4] == "" {
		code, errMsg = http.StatusBadRequest, "bad request url"
		return
	}

	if data, err = base64.URLEncoding.DecodeString(pathArr[1]); err != nil {
		code, errMsg = http.StatusBadRequest, "base64 decode entry error"
		return
	}
	keyArr := strings.Split(string(data), KEY_DELIMITER)
	if len(keyArr) != 2 || keyArr[0] == "" || keyArr[1] == "" {
		code, errMsg = http.StatusBadRequest, "bad entry request"
		return
	}
	srcBucket = keyArr[0]
	srcKey = keyArr[1]

	if data, err = base64.URLEncoding.DecodeString(pathArr[2]); err != nil {
		code, errMsg = http.StatusBadRequest, "base64 decode entry error"
		return
	}
	keyArr = strings.Split(string(data), KEY_DELIMITER)
	if len(keyArr) != 2 || keyArr[0] == "" || keyArr[1] == "" {
		code, errMsg = http.StatusBadRequest, "bad entry request"
		return
	}
	destBucket = keyArr[0]
	destKey = keyArr[1]

	isForce = (string(pathArr[4]) == "true")

	code = http.StatusOK
	return
}

type CopyResp struct {
	ErrData ErrRespData
}

func (resp *CopyResp) ErrorResp(wr http.ResponseWriter) {
	retJson, err := json.Marshal(resp.ErrData)
	if err != nil {
		log.Errorf("copy error response marshal json error(%s)", err.Error())
	}
	wr.Header().Set("Content-Type", "application/json")
	wr.Header().Set("Cache-Control", "no-store")
	wr.WriteHeader(resp.ErrData.Code)
	wr.Write(retJson)
}

func (resp *CopyResp) OKResp(wr http.ResponseWriter) {
	wr.Header().Set("Cache-Control", "no-store")
	wr.WriteHeader(http.StatusOK)
}

//delete
type DeleteReq struct {
	R    *http.Request
	Data DeleteReqData
}
type DeleteReqData struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
	Token  string `json:"token"`
}

func (req *DeleteReq) Parse() (code int, errMsg string) {
	if req.Data.Bucket, req.Data.Key, code, errMsg = parseDelete(req.R.URL.Path); code != http.StatusOK {
		return
	}

	tokenArr := strings.Split(strings.TrimSpace(req.R.Header.Get("Authorization")), TOKEN_DELIMITER)
	if len(tokenArr) != 2 || tokenArr[1] == "" {
		code, errMsg = http.StatusUnauthorized, "bad token request"
		return
	}
	req.Data.Token = tokenArr[1]

	return
}

func parseDelete(url string) (bucket, key string, code int, errMsg string) {
	var (
		data []byte
		err  error
	)

	pathArr := strings.Split(strings.Trim(url, "/"), "/")
	if len(pathArr) != 2 || pathArr[1] == "" {
		code, errMsg = http.StatusBadRequest, "bad request url"
		return
	}

	if data, err = base64.URLEncoding.DecodeString(pathArr[1]); err != nil {
		code, errMsg = http.StatusBadRequest, "base64 decode entry error"
		return
	}
	keyArr := strings.Split(string(data), KEY_DELIMITER)
	if len(keyArr) != 2 || keyArr[0] == "" || keyArr[1] == "" {
		code, errMsg = http.StatusBadRequest, "bad entry request"
		return
	}
	bucket = keyArr[0]
	key = keyArr[1]

	code = http.StatusOK
	return
}

type DeleteResp struct {
	ErrData ErrRespData
}

func (resp *DeleteResp) ErrorResp(wr http.ResponseWriter) {
	retJson, err := json.Marshal(resp.ErrData)
	if err != nil {
		log.Errorf("delete error response marshal json error(%s)", err.Error())
	}
	wr.Header().Set("Content-Type", "application/json")
	wr.Header().Set("Cache-Control", "no-store")
	wr.WriteHeader(resp.ErrData.Code)
	wr.Write(retJson)
}

func (resp *DeleteResp) OKResp(wr http.ResponseWriter) {
	wr.Header().Set("Cache-Control", "no-store")
	wr.WriteHeader(http.StatusOK)
}

//list
type ListReq struct {
	R    *http.Request
	Data ListReqData
}
type ListReqData struct {
	Bucket    string `json:"bucket"`
	Marker    string `json:"marker"`
	Limit     int    `json:"limit"`
	Prefix    string `json:"prefix"`
	Delimiter string `json:"delimiter"`
	Token     string `json:"token"`
}

func (req *ListReq) Parse() (code int, errMsg string) {
	var (
		err error
	)

	req.Data.Bucket = req.R.FormValue("bucket")
	req.Data.Marker = req.R.FormValue("marker")
	limitStr := req.R.FormValue("limit")
	if req.Data.Limit, err = strconv.Atoi(limitStr); err != nil || limitStr == "" {
		req.Data.Limit = 1000
	}
	req.Data.Prefix = req.R.FormValue("prefix")
	req.Data.Delimiter = req.R.FormValue("delimiter")

	tokenArr := strings.Split(strings.TrimSpace(req.R.Header.Get("Authorization")), TOKEN_DELIMITER)
	if len(tokenArr) != 2 || tokenArr[1] == "" {
		code, errMsg = http.StatusUnauthorized, "bad token request"
		return
	}
	req.Data.Token = tokenArr[1]
	code = http.StatusOK
	return
}

type ListResp struct {
	ErrData ErrRespData
	Data    *meta.PFListRetOK
}

func (resp *ListResp) ErrorResp(wr http.ResponseWriter) {
	retJson, err := json.Marshal(resp.ErrData)
	if err != nil {
		log.Errorf("list error response marshal json error(%s)", err.Error())
	}
	wr.Header().Set("Content-Type", "application/json")
	wr.Header().Set("Cache-Control", "no-store")
	wr.WriteHeader(resp.ErrData.Code)
	wr.Write(retJson)
}

func (resp *ListResp) OKResp(wr http.ResponseWriter) {
	retJson, err := json.Marshal(resp.Data)
	if err != nil {
		log.Errorf("list ok response marshal json error(%s)", err.Error())
	}
	wr.Header().Set("Content-Type", "application/json")
	wr.Header().Set("Cache-Control", "no-store")
	wr.WriteHeader(http.StatusOK)
	wr.Write(retJson)
}

//hash
func HashParse(authorization, urlStr string) (hashType, bucket, file, token string, code int, errMsg string) {
	var (
		data []byte
		err  error
		strs []string
		ekey string
	)

	tokenArr := strings.Split(strings.TrimSpace(authorization), TOKEN_DELIMITER)
	if len(tokenArr) != 2 || tokenArr[1] == "" {
		code, errMsg = http.StatusUnauthorized, "bad token request"
		return
	}
	token = tokenArr[1]

	pathArr := strings.Split(strings.Trim(urlStr, "/"), "/")
	if len(pathArr) != 3 || pathArr[1] == "" || pathArr[2] == "" {
		code, errMsg = http.StatusBadRequest, "bad request url"
		return
	}
	hashType = pathArr[2]
	ekey = pathArr[1]

	if data, err = base64.URLEncoding.DecodeString(ekey); err != nil {
		code = http.StatusBadRequest
		errMsg = "ekey base64 decode wrong"
		return
	}

	strs = strings.Split(string(data), ":")
	if len(strs) != 2 {
		code = http.StatusBadRequest
		errMsg = "request bad ekey"
		return
	}

	bucket, file = strs[0], strs[1]
	code, errMsg = http.StatusOK, ""
	return
}

type HashRespData struct {
	Code  int    `json:"code"`
	Msg   string `json:"msg"`
	Hash  string `json:"hash"`
	FSize int64  `json:"fsize"`
}

func HashResp(wr http.ResponseWriter, hr *HashRespData) {
	retJson, err := json.Marshal(hr)
	if err != nil {
		log.Errorf("hash response json ecode error(%s)", err.Error())
	}
	wr.Header().Set("Content-Type", "application/json")
	wr.Header().Set("Cache-Control", "no-store")
	wr.WriteHeader(hr.Code)
	wr.Write(retJson)
}

//batch
type BatchReq struct {
	R    *http.Request
	Data BatchReqData
}
type BatchReqData struct {
	Ops      []map[string]string
	BodyForm string `json:"bodyform"`
	Token    string `json:"token"`
}

func (req *BatchReq) Parse() (bucket string, code int, errMsg string) {
	var (
		data    []byte
		err     error
		ops     []string
		pathArr []string
		opsStr  string
	)

	req.R.ParseForm()
	if ops = req.R.Form["op"]; len(ops) == 0 {
		code, errMsg = http.StatusBadRequest, "op is empty"
		return
	}

	for _, op := range ops {
		opsStr = opsStr + "&op=" + op

		opPrefix := op[:5]

		switch opPrefix {
		case "/stat":
			pathArr = strings.Split(strings.Trim(op, "/"), "/")
			if len(pathArr) != 2 || pathArr[1] == "" {
				code, errMsg = http.StatusBadRequest, "bad request url"
				return
			}
			req.Data.Ops = append(req.Data.Ops, map[string]string{"op": "stat", "src": pathArr[1]})
		case "/copy":
			pathArr = strings.Split(strings.Trim(op, "/"), "/")
			if len(pathArr) == 3 {
				pathArr = append(pathArr, "force")
				pathArr = append(pathArr, "false")
			}
			if len(pathArr) != 5 || pathArr[1] == "" {
				code, errMsg = http.StatusBadRequest, "bad request url"
				return
			}
			req.Data.Ops = append(req.Data.Ops, map[string]string{"op": "copy",
				"src": pathArr[1], "dest": pathArr[2], "isforce": pathArr[4]})
		case "/move":
			pathArr = strings.Split(strings.Trim(op, "/"), "/")
			if len(pathArr) == 3 {
				pathArr = append(pathArr, "force")
				pathArr = append(pathArr, "false")
			}
			if len(pathArr) != 5 || pathArr[1] == "" {
				code, errMsg = http.StatusBadRequest, "bad request url"
				return
			}
			req.Data.Ops = append(req.Data.Ops, map[string]string{"op": "move",
				"src": pathArr[1], "dest": pathArr[2], "isforce": pathArr[4]})
		case "/dele":
			pathArr = strings.Split(strings.Trim(op, "/"), "/")
			if len(pathArr) != 2 || pathArr[1] == "" {
				code, errMsg = http.StatusBadRequest, "bad request url"
				return
			}
			req.Data.Ops = append(req.Data.Ops, map[string]string{"op": "delete", "src": pathArr[1]})
		default:
			code, errMsg = http.StatusBadRequest, "bad op request: "+op
			return
		}
	}

	req.Data.BodyForm = strings.Trim(opsStr, "&")

	//get bucket for secret key
	if data, err = base64.URLEncoding.DecodeString(req.Data.Ops[0]["src"]); err != nil {
		code, errMsg = http.StatusBadRequest, "base64 decode entry error"
		return
	}
	keyArr := strings.Split(string(data), KEY_DELIMITER)
	if len(keyArr) != 2 || keyArr[0] == "" || keyArr[1] == "" {
		code, errMsg = http.StatusBadRequest, "bad entry request"
		return
	}
	bucket = keyArr[0]

	tokenArr := strings.Split(strings.TrimSpace(req.R.Header.Get("Authorization")), TOKEN_DELIMITER)
	if len(tokenArr) != 2 || tokenArr[1] == "" {
		code, errMsg = http.StatusUnauthorized, "bad token request"
		return
	}

	req.Data.Token = tokenArr[1]
	code = http.StatusOK
	return
}

type BatchResp struct {
	ErrData ErrRespData
	Data    []byte
}

func (resp *BatchResp) ErrorResp(wr http.ResponseWriter) {
	retJson, err := json.Marshal(resp.ErrData)
	if err != nil {
		log.Errorf("list error response marshal json error(%s)", err.Error())
	}
	wr.Header().Set("Content-Type", "application/json")
	wr.Header().Set("Cache-Control", "no-store")
	wr.WriteHeader(resp.ErrData.Code)
	wr.Write(retJson)
}

func (resp *BatchResp) OKResp(wr http.ResponseWriter, code int) {
	wr.Header().Set("Content-Type", "application/json")
	wr.Header().Set("Cache-Control", "no-store")
	wr.WriteHeader(http.StatusOK)
	wr.Write(resp.Data)
}

//---
type ErrRespData struct {
	Msg  string `json:"error"`
	Code int    `json:"code"`
}
