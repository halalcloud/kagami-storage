package main

import (
	"crypto/sha1"
	"efs/libs/errors"
	"efs/libs/meta"
	"efs/proxy/bucket"
	"efs/proxy/conf"
	"efs/proxy/efs"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"math"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	log "efs/log/glog"
)

const (
	_httpServerReadTimeout  = 50 * time.Second
	_httpServerWriteTimeout = 50 * time.Second

	UPLOAD_STORE_RETRY = 2
	LIST_LIMIT         = 1000
	NO_OVER_WRITE      = 0
)

type server struct {
	efs     *efs.Efs
	ibucket *bucket.BucketInfo
	c       *conf.Config
}

// StartApi init the http module.
func StartApi(c *conf.Config) (err error) {
	var s = &server{}
	s.c = c
	s.efs = efs.New(c)
	s.ibucket = bucket.Bucket_init(c)

	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/r/get", s.download)
		mux.HandleFunc("/r/list", s.list)
		mux.HandleFunc("/r/chgm", s.chgm)
		mux.HandleFunc("/r/copy", s.copy)
		mux.HandleFunc("/r/move", s.move)
		mux.HandleFunc("/r/stat", s.stat)
		mux.HandleFunc("/r/upload", s.upload)
		mux.HandleFunc("/r/mkblk", s.mkblk)
		mux.HandleFunc("/r/bput", s.bput)
		mux.HandleFunc("/r/mkfile", s.mkfile)
		mux.HandleFunc("/r/delete", s.delete)
		mux.HandleFunc("/r/batch", s.batch)
		mux.HandleFunc("/b/create", s.bcreate)
		mux.HandleFunc("/b/rename", s.brename)
		mux.HandleFunc("/b/delete", s.bdelete)
		mux.HandleFunc("/b/list", s.blist)
		mux.HandleFunc("/b/stat", s.bstat)
		//mux.HandleFunc("/ping", s.ping)
		server := &http.Server{
			Addr:    c.HttpAddr,
			Handler: mux,
			//	ReadTimeout:  _httpServerReadTimeout,
			//WriteTimeout: _httpServerWriteTimeout,
		}
		if err := server.ListenAndServe(); err != nil {
			return
		}
	}()
	return
}

// flag 1->fileoverwirte
func httpLog(uri string, bucket, file *string, flag int, osize *int64, size *int64, start time.Time, status *int, err *error) {
	if *bucket == "" {
		*bucket = "-"
	}
	if *file == "" {
		*file = "-"
	}
	fname := base64.URLEncoding.EncodeToString([]byte(*file))
	log.Statisf("%s	%s	%s	%d	%d	%d	%f	%d	error(%v)",
		uri, *bucket, fname, flag, *osize, *size, time.Now().Sub(start).Seconds(), *status, *err)
}

func (s *server) download(wr http.ResponseWriter, r *http.Request) {
	var (
		ekey   string
		ranges string
		arr    []string
		bucket string
		file   string
		data   []byte
		err    error
		status = http.StatusOK
	)
	//	fmt.Println(r.Header)
	if ekey = r.Header.Get("ekey"); ekey == "" {
		status = errors.RetUrlBad
		wr.WriteHeader(status)
		log.Errorf("download method have not header ekey")
		return
	}
	if data, err = base64.URLEncoding.DecodeString(ekey); err != nil {
		status = errors.RetUrlBad
		wr.WriteHeader(status)
		log.Errorf("base64 urlencoding decode string(%s) failed(%v)", ekey, err)
		return
	}
	arr = strings.Split(string(data), ":")
	if len(arr) != 2 {
		status = errors.RetUrlBad
		wr.WriteHeader(status)
		log.Errorf("ekey(%s) is not bucket:filename", string(data))
		return
	}
	bucket = arr[0]
	file = arr[1]

	if bucket == "" || file == "" {
		status = errors.RetUrlBad
		wr.WriteHeader(status)
		log.Errorf("bucket(%s) or filename(%s) is null", bucket, file)
		return
	}

	if ranges = r.Header.Get("range"); ranges != "" {
		//log.Errorf("range %s", ranges)
		s.downloadSlice(ekey, bucket, file, wr, r)
	} else {
		s.downloadDirect(ekey, bucket, file, wr, r)
	}
}

// download.
func (s *server) downloadDirect(ekey, bucket, file string, wr http.ResponseWriter, r *http.Request) {
	var (
		ctlen     int64
		oFileSize int64

		start = time.Now()

		status = http.StatusOK
		err    error
	)

	defer httpLog(r.URL.Path, &bucket, &file, 0, &oFileSize, &ctlen, start, &status, &err)
	//proxy efs module done
	if ctlen, err = s.efs.Get_stream(ekey, wr, r); err != nil {
		if err == errors.ErrNeedleNotExist {
			status = errors.RetResNoExist
		} else if err == errors.ErrSrcBucketNoExist {
			status = errors.RetResNoExist
		} else {
			status = errors.RetServerFailed
		}
		log.Errorf("get stream failed %v", err)
		if status == errors.RetResNoExist {
			status = http.StatusNotFound
		}
		wr.WriteHeader(status)
		return
	}
	return
}

//
func (s *server) downloadSlice(ekey, bucket, file string, wr http.ResponseWriter, r *http.Request) {
	var (
		ranges string
		arr    []string
		start  = time.Now()
		status = http.StatusPartialContent
		//	status                                  = http.StatusOK
		err                                     error
		rangeStart, rangeEnd, reqLen, oFileSize int64
	)
	defer httpLog(r.URL.Path, &bucket, &file, 0, &oFileSize, &reqLen, start, &status, &err)
	if ranges = r.Header.Get("range"); ranges == "" {
		log.Errorf("ranges is null")
		status = http.StatusBadRequest
		wr.WriteHeader(status)
		return
	}

	arr = strings.Split(strings.TrimLeft(ranges, "bytes="), "-")
	if len(arr) != 2 {
		log.Errorf("have range %s style is invalid", ranges)
		status = http.StatusBadRequest
		wr.WriteHeader(status)
		return
	}
	if arr[0] == "" {
		arr[0] = "0"
	}
	if arr[1] == "" {
		arr[1] = strconv.FormatInt(math.MaxInt64, 10)
	}

	rangeStart, err = strconv.ParseInt(arr[0], 10, 64)
	rangeEnd, err = strconv.ParseInt(arr[1], 10, 64)
	if err != nil {
		log.Errorf("range end %s is invalid", arr[1])
		status = errors.RetUrlBad
		wr.WriteHeader(status)
		return
	}
	if rangeStart < 0 || rangeEnd < 0 {
		log.Errorf("range end %s start %s is invalid", arr[1], arr[0])
		status = errors.RetUrlBad
		wr.WriteHeader(status)
		return
	}

	if err = s.efs.GetRangeStream(ekey, rangeStart, &rangeEnd, wr, r); err != nil {
		if err == errors.ErrNeedleNotExist {
			status = errors.RetResNoExist
		} else if err == errors.ErrSrcBucketNoExist {
			status = errors.RetResNoExist
		} else if err == errors.ErrParam {
			status = errors.RetUrlBad
		} else {
			status = errors.RetServerFailed
		}
		log.Errorf("get rangestream failed %v", err)
		if status == errors.RetResNoExist {
			status = http.StatusNotFound
		}
		wr.WriteHeader(status)
		return
	}

	reqLen = rangeEnd - rangeStart + 1
	return
}

func (s *server) upload(wr http.ResponseWriter, r *http.Request) {
	var (
		ekey          string
		overWriteFlag int
		replication   int
		arr           []string
		bucket        string
		filename      string
		data          []byte
		err           error
		status        = http.StatusOK
	)
	if ekey = r.Header.Get("ekey"); ekey == "" {
		status = errors.RetUrlBad
		rUploadErrResp(wr, status, status, "ekey is empty string")
		return
	}
	if data, err = base64.URLEncoding.DecodeString(ekey); err != nil {
		status = errors.RetUrlBad
		rUploadErrResp(wr, status, status, "ekey b64 decode error")
		return
	}
	arr = strings.Split(string(data), ":")
	if len(arr) != 2 {
		status = errors.RetUrlBad
		rUploadErrResp(wr, status, status, "ekey error")
		return
	}
	bucket = arr[0]
	filename = arr[1]
	if bucket == "" || filename == "" {
		status = errors.RetUrlBad
		rUploadErrResp(wr, status, status, "bucket or file is empty string")
		return
	}

	if overWriteFlag, err = strconv.Atoi(r.Header.Get("overwrite")); err != nil {
		overWriteFlag = NO_OVER_WRITE
	}

	_, _, _, _, replication, err, _ = s.ibucket.Getkey(bucket)
	if err != nil {
		status = errors.RetUrlBad
		rUploadErrResp(wr, status, status, "bucket info get error")
		return
	}

	if r.ContentLength > int64(s.c.MaxFileSize) {
		s.uploadSlice(ekey, bucket, filename, overWriteFlag, replication, wr, r)
	} else {
		s.uploadDirect(ekey, bucket, filename, overWriteFlag, replication, wr, r)
	}
}

//upload dispatcher
func (s *server) uploadDispatcher(wr http.ResponseWriter, ekey string, lastVid int32,
	overWriteFlag, replication int) (res *meta.Response, err error) {
	var (
		status  int
		errCode int
		derr    errors.Error
		ok      bool
	)

	if res, err = s.efs.Dispatcher(ekey, lastVid, overWriteFlag, replication); err != nil {
		if err == errors.ErrNeedleExist {
			status = errors.RetResExist
			errCode = errors.RetNeedleExist
			rUploadErrResp(wr, status, errCode, errors.ErrNeedleExist.Error())
		} else if err == errors.ErrDestBucketNoExist {
			status = errors.RetResNoExist
			errCode = errors.RetDestBucketNoExist
			rUploadErrResp(wr, status, errCode, errors.ErrDestBucketNoExist.Error())
		} else {
			if derr, ok = (err).(errors.Error); ok {
				errCode = int(derr)
			} else {
				errCode = errors.RetServerFailed
			}
			status = errors.RetServerFailed
			rUploadErrResp(wr, status, errCode, errors.ErrServerFailed.Error())
		}

		return
	}

	return
}

//directly upload file.
func (s *server) uploadDirect(ekey, bucket, filename string, overWriteFlag, replication int, wr http.ResponseWriter, r *http.Request) {
	var (
		ok        bool
		body      []byte
		reqLen    int64
		oFileSize int64
		mime      string
		sha1sum   string
		filePart  multipart.File
		ext       string
		sha       [sha1.Size]byte
		err       error
		derr      errors.Error
		errCode   int
		data      []byte
		status    = http.StatusOK
		start     = time.Now()
		respOK    meta.PFUploadRetOK
		res       *meta.Response
	)
	defer httpLog(r.URL.Path, &bucket, &filename, overWriteFlag, &oFileSize, &reqLen, start, &status, &err)

	if res, err = s.uploadDispatcher(wr, ekey, -1, overWriteFlag, replication); err != nil {
		//dispatcher failed
		return
	}

	if mime = r.Header.Get("Content-Type"); mime == "" {
		mime = "multipart/form-data"
	}
	if ext = path.Base(mime); ext == "jpeg" {
		ext = "jpg"
	}
	if filePart, _, err = r.FormFile("file"); err != nil {
		status = errors.RetUrlBad
		rUploadErrResp(wr, status, status, "get formfile error")
		return
	}
	defer filePart.Close()

	if body, err = ioutil.ReadAll(filePart); err != nil {
		status = errors.RetUrlBad
		rUploadErrResp(wr, status, status, "read formfile error")
		return
	}
	r.Body.Close()
	reqLen = int64(len(body))
	if len(body) > s.c.MaxFileSize {
		status = errors.RetFileTooLarge
		rUploadErrResp(wr, status, status, "file is bigger than max file size")
		return
	}
	if len(body) <= 0 {
		status = errors.RetUrlBad
		rUploadErrResp(wr, status, status, "body data is empty")
		return
	}

	sha = sha1.Sum(body)
	sha1sum = hex.EncodeToString(sha[:])

	//n retry upload store
	for i := 1; i <= UPLOAD_STORE_RETRY; i++ {

		if err = s.efs.UploadStore(res, body); err == nil {
			break //success
		}

		if i == UPLOAD_STORE_RETRY {
			if derr, ok = (err).(errors.Error); ok {
				errCode = int(derr)
			} else {
				errCode = errors.RetServerFailed
			}
			status = errors.RetServerFailed
			rUploadErrResp(wr, status, errCode, errors.ErrServerFailed.Error())
			return
		}

		if res, err = s.uploadDispatcher(wr, ekey, res.Vid, overWriteFlag, replication); err != nil {
			//dispatcher failed
			return
		}
	}

	if oFileSize, err = s.efs.UploadDirectory(ekey, mime, sha1sum, int(reqLen),
		res.Key, res.Vid, res.Cookie, overWriteFlag); err != nil {

		//delete store file
		for _, host := range res.Stores {
			s.efs.StoreFileDel(host, res.Key, res.Cookie, res.Vid)
		}

		if derr, ok = (err).(errors.Error); ok {
			errCode = int(derr)
		} else {
			errCode = errors.RetServerFailed
		}
		status = errors.RetServerFailed
		rUploadErrResp(wr, status, errCode, errors.ErrServerFailed.Error())
		return
	}

	respOK.Hash = sha1sum
	respOK.Key = filename
	if data, err = json.Marshal(respOK); err != nil {
		wr.WriteHeader(http.StatusInternalServerError)
		log.Errorf("uploadDirect respOK marshal json error(%v)", err)
		return
	}

	wr.WriteHeader(status)
	wr.Header().Set("Content-Type", "application/json; charset=utf-8")
	wr.Write(data)
	return
}

//upload slice file
func (s *server) uploadSlice(ekey, bucket, filename string, overWriteFlag, replication int, wr http.ResponseWriter, r *http.Request) {
	var (
		mime      string
		filePart  multipart.File
		err       error
		index     int
		ctx, id   string
		offset    int64
		oFileSize int64
		finish    bool
		start     = time.Now()
		status    = http.StatusOK
		ctxList   string
	)

	defer httpLog(r.URL.Path, &bucket, &filename, 0, &oFileSize, &offset, start, &status, &err)
	defer r.Body.Close()

	if mime = r.Header.Get("Content-Type"); mime == "" {
		mime = "multipart/form-data"
	}

	if filePart, _, err = r.FormFile("file"); err != nil {
		status = errors.RetUrlBad
		rUploadErrResp(wr, status, status, "get formfile error")
		return
	}
	defer filePart.Close()

	for {
		if index == 0 {
			if ctx, id, offset, status, finish, err = s.sliceMkblk(ekey, mime, overWriteFlag, replication, wr, r, filePart); err != nil {
				return
			}
			ctxList = ctx + ","
		} else {
			if ctx, id, offset, status, finish, err = s.sliceMkblk(ekey, mime, overWriteFlag, replication, wr, r, filePart); err != nil {
				return
			}
			ctxList += ctx
			if finish {
				break
			}
			ctxList += ","
		}

		index++
	}

	status, _ = s.sliceMkfile(ekey, bucket, filename, &oFileSize, mime, id, offset, ctxList, overWriteFlag, wr, r)
	return
}

//slice mkblk
func (s *server) sliceMkblk(ekey, mime string, overWriteFlag, replication int, wr http.ResponseWriter,
	r *http.Request, file io.Reader) (ctx, id string,
	offset int64, status int, finish bool, err error) {
	var (
		derr    errors.Error
		errCode int
		readNum int
		ok      bool
		data    []byte
		reqLen  int64
		sha1sum string
		sha     [sha1.Size]byte
		respOK  *meta.PMkblkRetOK
		res     *meta.Response
	)
	data = make([]byte, int(s.c.SliceFileSize))

	if res, err = s.uploadDispatcher(wr, ekey, -1, overWriteFlag, replication); err != nil {
		return
	}

	if readNum, err = io.ReadFull(file, data); err != nil {
		if err == io.EOF {
			finish = true
			err = nil
			return
		} else if err == io.ErrUnexpectedEOF {
			data = data[:readNum]
			finish = true
			err = nil
		} else {
			status = errors.RetUrlBad
			rUploadErrResp(wr, status, status, "request body read data error")
			return
		}
	}

	reqLen = int64(len(data))
	sha = sha1.Sum(data)
	sha1sum = hex.EncodeToString(sha[:])

	//n retry upload store
	for i := 1; i <= UPLOAD_STORE_RETRY; i++ {
		//new mutilpart upload
		ctx = strconv.FormatInt(res.Key, 10)
		if err = s.efs.UploadStore(res, data); err == nil {
			break //success
		}

		if i == UPLOAD_STORE_RETRY {
			if derr, ok = (err).(errors.Error); ok {
				errCode = int(derr)
			} else {
				errCode = errors.RetServerFailed
			}
			status = errors.RetServerFailed
			rUploadErrResp(wr, status, errCode, errors.ErrServerFailed.Error())
			return
		}

		if res, err = s.uploadDispatcher(wr, ekey, res.Vid, overWriteFlag, replication); err != nil {
			//dispatcher failed
			return
		}
	}

	if respOK, err = s.efs.Mkblk(ekey, mime, sha1sum, int(reqLen), res.Key, res.Vid, res.Cookie); err != nil {
		//delete store file
		for _, host := range res.Stores {
			s.efs.StoreFileDel(host, res.Key, res.Cookie, res.Vid)
		}

		if derr, ok = (err).(errors.Error); ok {
			errCode = int(derr)
		} else {
			errCode = errors.RetServerFailed
		}
		status = errors.RetServerFailed
		rUploadErrResp(wr, status, errCode, errors.ErrServerFailed.Error())

		return
	}

	respOK.Host = r.Host
	respOK.Crc32 = int64(crc32.ChecksumIEEE(data))
	respOK.Checksum = fmt.Sprintf("%d", respOK.Crc32)

	//	ctx = respOK.Ctx
	id = respOK.Id
	offset = respOK.Offset

	return
}

//slice bput
func (s *server) sliceBput(ekey, ctx, id, mime string, offset int64, overWriteFlag, replication int,
	wr http.ResponseWriter, r *http.Request, file io.Reader) (nctx string, noffset int64, finish bool, status int, err error) {
	var (
		ok      bool
		reqLen  int64
		readNum int
		data    []byte
		derr    errors.Error
		errCode int
		sha1sum string
		respOK  meta.PBputRetOK
		res     *meta.Response
		sha     [sha1.Size]byte
	)
	data = make([]byte, s.c.SliceFileSize)

	if res, err = s.uploadDispatcher(wr, ekey, -1, overWriteFlag, replication); err != nil {
		return
	}

	if ctx == "" || id == "" || offset == 0 {
		status = errors.RetUrlBad
		rUploadErrResp(wr, status, status, "ctx or id or offset is error")
		return
	}

	if readNum, err = io.ReadFull(file, data); err != nil {
		if err == io.EOF {
			finish = true
			err = nil
			return
		} else if err == io.ErrUnexpectedEOF {
			data = data[:readNum]
			finish = true
			err = nil
		} else {
			status = errors.RetUrlBad
			rUploadErrResp(wr, status, status, "request body read data error")
			return
		}
	}
	reqLen = int64(len(data))
	sha = sha1.Sum(data)
	sha1sum = hex.EncodeToString(sha[:])

	//n retry upload store
	for i := 1; i <= UPLOAD_STORE_RETRY; i++ {

		if err = s.efs.UploadStore(res, data); err == nil {
			break //success
		}

		if i == UPLOAD_STORE_RETRY {
			if derr, ok = (err).(errors.Error); ok {
				errCode = int(derr)
			} else {
				errCode = errors.RetServerFailed
			}
			status = errors.RetServerFailed
			rUploadErrResp(wr, status, errCode, errors.ErrServerFailed.Error())
			return
		}

		if res, err = s.uploadDispatcher(wr, ekey, res.Vid, overWriteFlag, replication); err != nil {
			//dispatcher failed
			return
		}
	}

	if respOK, err = s.efs.Bput(ekey, ctx, id, offset, mime, sha1sum,
		reqLen, res.Key, res.Vid, res.Cookie); err != nil {

		//delete store file
		for _, host := range res.Stores {
			s.efs.StoreFileDel(host, res.Key, res.Cookie, res.Vid)
		}

		if derr, ok = (err).(errors.Error); ok {
			errCode = int(derr)
		} else {
			errCode = errors.RetServerFailed
		}
		status = errors.RetServerFailed
		rBlkSliceErrResp(wr, status, errCode, errors.ErrServerFailed.Error())
		return
	}

	respOK.Host = r.Host
	respOK.Crc32 = int64(crc32.ChecksumIEEE(data))
	respOK.Checksum = fmt.Sprintf("%d", respOK.Crc32)

	nctx = respOK.Ctx
	noffset = respOK.Offset
	return
}

//slice mkfile
func (s *server) sliceMkfile(ekey, bucket, file string, oFileSize *int64, mime, id string, filesize int64, buf string,
	overWriteFlag int, wr http.ResponseWriter, r *http.Request) (status int, err error) {
	var (
		ok      bool
		derr    errors.Error
		errCode int
		retJson []byte
		respOK  meta.PMkfileRetOK
	)

	if id == "" || mime == "" || buf == "" || filesize == 0 {
		status = errors.RetUrlBad
		rUploadErrResp(wr, status, status, "id or mime or buf or filesize error")
		return
	}

	if respOK, *oFileSize, err = s.efs.Mkfile(ekey, bucket, file, overWriteFlag, id, filesize, mime, buf); err != nil {
		if err == errors.ErrNeedleExist {
			status = errors.RetResExist
			errCode = errors.RetNeedleExist
			rUploadErrResp(wr, status, errCode, errors.ErrNeedleExist.Error())
		} else if err == errors.ErrNeedleNotExist {
			status = errors.RetResNoExist
			errCode = errors.RetNeedleNotExist
			rUploadErrResp(wr, status, errCode, errors.ErrNeedleNotExist.Error())
		} else {
			if derr, ok = (err).(errors.Error); ok {
				errCode = int(derr)
			} else {
				errCode = errors.RetServerFailed
			}
			status = errors.RetServerFailed
			rUploadErrResp(wr, status, errCode, errors.ErrServerFailed.Error())
		}

		return
	}

	if retJson, err = json.Marshal(respOK); err != nil {
		wr.WriteHeader(http.StatusInternalServerError)
		log.Errorf("mkblk rOK marshal json Failed data(%v)", respOK)
		return
	}
	status = http.StatusOK
	wr.WriteHeader(status)
	wr.Write(retJson)

	return
}

// mkblk file.
func (s *server) mkblk(wr http.ResponseWriter, r *http.Request) {
	var (
		arr           []string
		ekey          string
		bucket        string
		file          string
		overWriteFlag int
		replication   int
		err           error
		derr          errors.Error
		errCode       int
		ok            bool
		body          []byte
		reqLen        int64
		sha1sum       string
		mime          string
		sha           [sha1.Size]byte
		respOK        *meta.PMkblkRetOK
		res           *meta.Response
		data          []byte
		start         = time.Now()
		status        = http.StatusOK
		oFileSize     int64
	)

	defer httpLog(r.URL.Path, &bucket, &file, 0, &oFileSize, &reqLen, start, &status, &err)

	if ekey = r.Header.Get("ekey"); ekey == "" {
		status = errors.RetUrlBad
		rMakeBlkErrResp(wr, status, status, "ekey is emtpy string")
		return
	}
	if data, err = base64.URLEncoding.DecodeString(ekey); err != nil {
		status = errors.RetUrlBad
		rMakeBlkErrResp(wr, status, status, "ekey b64 debode error")
		return
	}
	arr = strings.Split(string(data), ":")
	bucket = arr[0]
	file = arr[1]
	if len(arr) != 2 {
		status = errors.RetUrlBad
		rMakeBlkErrResp(wr, status, status, "ekey b64 error")
		return
	}
	if bucket == "" || file == "" {
		status = errors.RetUrlBad
		rMakeBlkErrResp(wr, status, status, "bucket or file is emtpy")
		return
	}
	if _, _, _, _, replication, err, _ = s.ibucket.Getkey(bucket); err != nil {
		status = errors.RetUrlBad
		rMakeBlkErrResp(wr, status, status, "bucket info get error")
		return
	}

	if overWriteFlag, err = strconv.Atoi(r.Header.Get("overwrite")); err != nil {
		overWriteFlag = NO_OVER_WRITE
	}

	if res, err = s.uploadDispatcher(wr, ekey, -1, overWriteFlag, replication); err != nil {
		return
	}

	if mime = r.Header.Get("Content-Type"); mime != "application/octet-stream" {
		status = errors.RetUrlBad
		rMakeBlkErrResp(wr, status, status, "request content-type error")
		return
	}

	if body, err = ioutil.ReadAll(r.Body); err != nil || int64(len(body)) != s.c.SliceFileSize {
		status = errors.RetUrlBad
		rMakeBlkErrResp(wr, status, status, "request body read data error")
		return
	}
	r.Body.Close()
	reqLen = int64(len(body))
	sha = sha1.Sum(body)
	sha1sum = hex.EncodeToString(sha[:])

	//n retry upload store
	for i := 1; i <= UPLOAD_STORE_RETRY; i++ {

		if err = s.efs.UploadStore(res, body); err == nil {
			break //success
		}

		if i == UPLOAD_STORE_RETRY {
			if derr, ok = (err).(errors.Error); ok {
				errCode = int(derr)
			} else {
				errCode = errors.RetServerFailed
			}
			status = errors.RetServerFailed
			rMakeBlkErrResp(wr, status, errCode, errors.ErrServerFailed.Error())
			return
		}

		if res, err = s.uploadDispatcher(wr, ekey, res.Vid, overWriteFlag, replication); err != nil {
			//dispatcher failed
			return
		}
	}

	if respOK, err = s.efs.Mkblk(ekey, mime, sha1sum, int(reqLen), res.Key, res.Vid, res.Cookie); err != nil {
		//delete store file
		for _, host := range res.Stores {
			s.efs.StoreFileDel(host, res.Key, res.Cookie, res.Vid)
		}

		if derr, ok = (err).(errors.Error); ok {
			errCode = int(derr)
		} else {
			errCode = errors.RetServerFailed
		}
		status = errors.RetServerFailed
		rMakeBlkErrResp(wr, status, errCode, errors.ErrServerFailed.Error())

		return
	}

	respOK.Host = r.Host
	respOK.Crc32 = int64(crc32.ChecksumIEEE(body))
	respOK.Checksum = fmt.Sprintf("%d", respOK.Crc32)
	if data, err = json.Marshal(respOK); err != nil {
		wr.WriteHeader(http.StatusInternalServerError)
		log.Errorf("mkblk respOK marshal json Failed")
		return
	}

	wr.WriteHeader(status)
	wr.Header().Set("ETag", sha1sum)
	wr.Write(data)
	return
}

//bput file.
func (s *server) bput(wr http.ResponseWriter, r *http.Request) {
	var (
		ekey          string
		arr           []string
		ok            bool
		body          []byte
		reqLen        int64
		data          []byte
		bucket        string
		file          string
		err           error
		derr          errors.Error
		errCode       int
		retJson       []byte
		sha1sum       string
		respOK        meta.PBputRetOK
		res           *meta.Response
		ctx           string
		offset        int64
		mime          string
		sha           [sha1.Size]byte
		start         = time.Now()
		status        = http.StatusOK
		id            string
		oFileSize     int64
		overWriteFlag int
		replication   int
	)
	defer httpLog(r.URL.Path, &bucket, &file, 0, &oFileSize, &reqLen, start, &status, &err)

	if ekey = r.Header.Get("ekey"); ekey == "" {
		status = errors.RetUrlBad
		rBlkSliceErrResp(wr, status, status, "ekey is empty string")
		return
	}
	if data, err = base64.URLEncoding.DecodeString(ekey); err != nil {
		status = errors.RetUrlBad
		rBlkSliceErrResp(wr, status, status, "ekey b64 decode error")
		return
	}
	arr = strings.Split(string(data), ":")
	bucket = arr[0]
	file = arr[1]
	if len(arr) != 2 {
		status = errors.RetUrlBad
		rBlkSliceErrResp(wr, status, status, "ekey b64 error")
		return
	}

	if overWriteFlag, err = strconv.Atoi(r.Header.Get("overwrite")); err != nil {
		overWriteFlag = NO_OVER_WRITE
	}
	if _, _, _, _, replication, err, _ = s.ibucket.Getkey(bucket); err != nil {
		status = errors.RetUrlBad
		rBlkSliceErrResp(wr, status, status, "bucket info get error")
		return
	}

	if res, err = s.uploadDispatcher(wr, ekey, -1, overWriteFlag, replication); err != nil {
		return
	}

	ctx = r.Header.Get("ctx")
	id = r.Header.Get("id")
	offset, _ = strconv.ParseInt(r.Header.Get("offset"), 10, 64)
	if ctx == "" || id == "" || offset == 0 {
		status = errors.RetUrlBad
		rBlkSliceErrResp(wr, status, status, "ctx or id or offset is error")
		return
	}

	if mime = r.Header.Get("Content-Type"); mime != "application/octet-stream" {
		status = errors.RetUrlBad
		rBlkSliceErrResp(wr, status, status, "request content-type error")
		return
	}

	if body, err = ioutil.ReadAll(r.Body); err != nil {
		status = errors.RetUrlBad
		rBlkSliceErrResp(wr, status, status, "request body read data error")
		return
	}
	r.Body.Close()
	reqLen = int64(len(body))

	if int64(len(body)) > s.c.SliceFileSize {
		status = errors.RetFileTooLarge
		rBlkSliceErrResp(wr, status, status, "slice file size is too big")
		return
	}

	if len(body) <= 0 {
		status = errors.RetUrlBad
		rBlkSliceErrResp(wr, status, status, "slice file size is too small")
	}

	sha = sha1.Sum(body)
	sha1sum = hex.EncodeToString(sha[:])

	//n retry upload store
	for i := 1; i <= UPLOAD_STORE_RETRY; i++ {

		if err = s.efs.UploadStore(res, body); err == nil {
			break //success
		}

		if i == UPLOAD_STORE_RETRY {
			if derr, ok = (err).(errors.Error); ok {
				errCode = int(derr)
			} else {
				errCode = errors.RetServerFailed
			}
			status = errors.RetServerFailed
			rBlkSliceErrResp(wr, status, errCode, errors.ErrServerFailed.Error())
			return
		}

		if res, err = s.uploadDispatcher(wr, ekey, res.Vid, overWriteFlag, replication); err != nil {
			//dispatcher failed
			return
		}
	}

	if respOK, err = s.efs.Bput(ekey, ctx, id, offset, mime, sha1sum,
		reqLen, res.Key, res.Vid, res.Cookie); err != nil {
		//delete store file
		for _, host := range res.Stores {
			s.efs.StoreFileDel(host, res.Key, res.Cookie, res.Vid)
		}

		if derr, ok = (err).(errors.Error); ok {
			errCode = int(derr)
		} else {
			errCode = errors.RetServerFailed
		}
		status = errors.RetServerFailed
		rBlkSliceErrResp(wr, status, errCode, errors.ErrServerFailed.Error())
		return
	}

	respOK.Host = r.Host
	respOK.Crc32 = int64(crc32.ChecksumIEEE(body))
	respOK.Checksum = fmt.Sprintf("%d", respOK.Crc32)

	if retJson, err = json.Marshal(respOK); err != nil {
		wr.WriteHeader(http.StatusInternalServerError)
		log.Errorf("mkblk rOK marshal json Failed data(%v)", respOK)
		return
	}

	wr.Header().Set("ETag", sha1sum)
	wr.Write(retJson)
	return
}

//make file.
func (s *server) mkfile(wr http.ResponseWriter, r *http.Request) {
	var (
		ekey          string
		arr           []string
		buf           string
		ok            bool
		m             map[string]interface{}
		data          []byte
		bucket        string
		file          string
		overWriteFlag int
		err           error
		derr          errors.Error
		errCode       int
		retJson       []byte
		respOK        meta.PMkfileRetOK
		mime          string
		filesize      int64
		start         = time.Now()
		status        = http.StatusOK
		id            string
		oFileSize     int64
	)
	m = make(map[string]interface{})

	defer httpLog(r.URL.Path, &bucket, &file, 0, &oFileSize, &filesize, start, &status, &err)

	if ekey = r.Header.Get("ekey"); ekey == "" {
		status = errors.RetUrlBad
		rMakeFileErrResp(wr, status, status, "ekey is empty string")
		return
	}
	if data, err = base64.URLEncoding.DecodeString(ekey); err != nil {
		status = errors.RetUrlBad
		rMakeFileErrResp(wr, status, status, "ekey b64 decode error")
		return
	}
	arr = strings.Split(string(data), ":")
	bucket = arr[0]
	file = arr[1]
	if len(arr) != 2 {
		status = errors.RetUrlBad
		rMakeFileErrResp(wr, status, status, "ekey b64 error")
		return
	}

	if overWriteFlag, err = strconv.Atoi(r.Header.Get("overwrite")); err != nil {
		overWriteFlag = NO_OVER_WRITE
	}

	if data, err = ioutil.ReadAll(r.Body); err != nil {
		status = errors.RetUrlBad
		rMakeFileErrResp(wr, status, status, "request body read data error")
		return
	}
	r.Body.Close()
	if err = json.Unmarshal(data, &m); err != nil {
		status = errors.RetUrlBad
		rMakeFileErrResp(wr, status, status, "request body data json decode error")
		return
	}
	id = m["id"].(string)
	mime = m["mime"].(string)
	buf = m["buf"].(string)
	filesize = int64(m["filesize"].(float64))
	if id == "" || mime == "" || buf == "" || filesize == 0 {
		status = errors.RetUrlBad
		rMakeFileErrResp(wr, status, status, "id or mime or buf or filesize error")
		return
	}

	if respOK, oFileSize, err = s.efs.Mkfile(ekey, bucket, file, overWriteFlag, id, filesize, mime, buf); err != nil {
		if err == errors.ErrNeedleExist {
			status = errors.RetResExist
			errCode = errors.RetNeedleExist
			rMakeFileErrResp(wr, status, errCode, errors.ErrNeedleExist.Error())
		} else if err == errors.ErrNeedleNotExist {
			status = errors.RetResNoExist
			errCode = errors.RetNeedleNotExist
			rMakeFileErrResp(wr, status, errCode, errors.ErrNeedleNotExist.Error())
		} else {
			if derr, ok = (err).(errors.Error); ok {
				errCode = int(derr)
			} else {
				errCode = errors.RetServerFailed
			}
			status = errors.RetServerFailed
			rMakeFileErrResp(wr, status, errCode, errors.ErrServerFailed.Error())
		}

		return
	}

	if retJson, err = json.Marshal(respOK); err != nil {
		wr.WriteHeader(http.StatusInternalServerError)
		log.Errorf("mkblk rOK marshal json Failed data(%v)", respOK)
		return
	}
	wr.WriteHeader(status)
	wr.Write(retJson)
	return
}

// delete
func (s *server) delete(wr http.ResponseWriter, r *http.Request) {
	var (
		arr       []string
		ekey      string
		data      []byte
		bucket    string
		file      string
		ok        bool
		err       error
		derr      errors.Error
		status    = http.StatusOK
		start     = time.Now()
		oFileSize int64
	)

	defer httpLog(r.URL.Path, &bucket, &file, 0, &oFileSize, &oFileSize, start, &status, &err)

	if ekey = r.Header.Get("ekey"); ekey == "" {
		status = errors.RetUrlBad
		rDelErrResp(wr, status, status, "ekey is empty string")
		return
	}
	if data, err = base64.URLEncoding.DecodeString(ekey); err != nil {
		status = errors.RetUrlBad
		rDelErrResp(wr, status, status, "ekey b64 decode error")
		return
	}
	arr = strings.Split(string(data), ":")
	if len(arr) != 2 {
		status = errors.RetUrlBad
		rDelErrResp(wr, status, status, "ekey b64 error")
		return
	}
	bucket = arr[0]
	file = arr[1]

	if bucket == "" || file == "" {
		status = errors.RetUrlBad
		rDelErrResp(wr, status, status, "bucket or file is empty string")
		return
	}

	if oFileSize, err = s.efs.Delete(ekey, bucket, file); err != nil {
		if err == errors.ErrNeedleNotExist {
			status = errors.RetResNoExist
			rDelErrResp(wr, status, errors.RetNeedleNotExist, errors.ErrNeedleNotExist.Error())
		} else if err == errors.ErrSrcBucketNoExist {
			status = errors.RetResNoExist
			rDelErrResp(wr, status, errors.RetSrcBucketNoExist, errors.ErrSrcBucketNoExist.Error())
		} else {
			if derr, ok = (err).(errors.Error); ok {
				status = errors.RetServerFailed
				errCode := int(derr)
				rDelErrResp(wr, status, errCode, errors.ErrServerFailed.Error())
			} else {
				status = errors.RetServerFailed
				rDelErrResp(wr, status, status, errors.ErrServerFailed.Error())
			}
		}

		return
	}

	wr.WriteHeader(status)
	wr.Header().Set("Content-Type", "application/json; charset=utf-8")
	return
}

func (s *server) move(wr http.ResponseWriter, r *http.Request) {
	var (
		arr                []string
		data               []byte
		ekey               string
		m                  map[string]string
		bucketSrc          string
		fileSrc            string
		bucketDest         string
		fileDest           string
		logbucket, logfile string
		overWriteFlag      int
		err                error
		derr               errors.Error
		errCode            int
		ok                 bool
		mtime              int64
		start              = time.Now()
		status             = http.StatusOK
		reqLen             int64
		oFileSize          int64
	)
	m = make(map[string]string)

	defer httpLog(r.URL.Path, &logbucket, &logfile, 0, &oFileSize, &reqLen, start, &status, &err)

	if ekey = r.Header.Get("ekey"); ekey == "" {
		status = errors.RetUrlBad
		rMoveErrResp(wr, status, status, "ekey is empty string")
		return
	}
	if data, err = base64.URLEncoding.DecodeString(ekey); err != nil {
		status = errors.RetUrlBad
		rMoveErrResp(wr, status, status, "ekey b64 decode error")
		return
	}
	arr = strings.Split(string(data), ":")
	if len(arr) != 2 {
		status = errors.RetUrlBad
		rMoveErrResp(wr, status, status, "ekey decode error")
		return
	}
	bucketSrc = arr[0]
	fileSrc = arr[1]
	logbucket = ekey //src ekey

	if data, err = ioutil.ReadAll(r.Body); err != nil {
		status = errors.RetUrlBad
		rMoveErrResp(wr, status, status, "get request body error")
		return
	}
	if err = json.Unmarshal(data, &m); err != nil {
		status = errors.RetUrlBad
		rMoveErrResp(wr, status, status, "body json Unmarshal error")
		return
	}

	if overWriteFlag, err = strconv.Atoi(m["overwrite"]); err != nil {
		status = errors.RetUrlBad
		rMoveErrResp(wr, status, status, "bad overwrite flag")
		return
	}

	if data, err = base64.URLEncoding.DecodeString(m["dest"]); err != nil {
		status = errors.RetUrlBad
		rMoveErrResp(wr, status, status, "dest b64 decode error")
		return
	}
	arr = strings.Split(string(data), ":")
	bucketDest = arr[0]
	fileDest = arr[1]
	logfile = bucketDest + ":" + fileDest //destekey

	if (bucketSrc == bucketDest && fileSrc == fileDest) ||
		bucketSrc == "" || fileSrc == "" ||
		bucketDest == "" || fileDest == "" {

		status = errors.RetUrlBad
		rMoveErrResp(wr, status, status, "bucket or file is empty string")
		return
	}

	if err = s.efs.Move(ekey, m["dest"], overWriteFlag); err != nil {
		if err == errors.ErrNeedleNotExist {
			status = errors.RetResNoExist
			errCode = errors.RetNeedleNotExist
			rMoveErrResp(wr, status, errCode, errors.ErrNeedleNotExist.Error())
		} else if err == errors.ErrNeedleExist {
			status = errors.RetResExist
			errCode = errors.RetNeedleExist
			rMoveErrResp(wr, status, errCode, errors.ErrNeedleExist.Error())
		} else if err == errors.ErrSrcBucketNoExist {
			status = errors.RetResNoExist
			errCode = errors.RetSrcBucketNoExist
			rMoveErrResp(wr, status, errCode, errors.ErrSrcBucketNoExist.Error())
		} else if err == errors.ErrDestBucketNoExist {
			status = errors.RetResNoExist
			errCode = errors.RetDestBucketNoExist
			rMoveErrResp(wr, status, errCode, errors.ErrDestBucketNoExist.Error())
		} else {
			if derr, ok = (err).(errors.Error); ok {
				errCode = int(derr)
			} else {
				errCode = errors.RetServerFailed
			}
			status = errors.RetServerFailed
			rMoveErrResp(wr, status, errCode, errors.ErrServerFailed.Error())
		}

		return
	}

	wr.WriteHeader(status)
	wr.Header().Set("Content-Length", strconv.Itoa(0))
	wr.Header().Set("Content-Type", "application/json")
	wr.Header().Set("Server", "efs")
	wr.Header().Set("Last-Modified", time.Unix(0, mtime).Format(http.TimeFormat))
	return
}

func (s *server) copy(wr http.ResponseWriter, r *http.Request) {
	var (
		arr                []string
		data               []byte
		ekey               string
		m                  map[string]string
		bucketSrc          string
		fileSrc            string
		bucketDest         string
		fileDest           string
		logbucket, logfile string
		overWriteFlag      int
		err                error
		ok                 bool
		derr               errors.Error
		errCode            int
		mtime              int64
		start              = time.Now()
		status             = http.StatusOK
		reqLen             int64
		oFileSize          int64
	)
	m = make(map[string]string)

	defer httpLog(r.URL.Path, &logbucket, &logfile, 0, &oFileSize, &reqLen, start, &status, &err)

	if ekey = r.Header.Get("ekey"); ekey == "" {
		status = errors.RetUrlBad
		rCopyErrResp(wr, status, status, "ekey is empty string")
		return
	}
	if data, err = base64.URLEncoding.DecodeString(ekey); err != nil {
		status = errors.RetUrlBad
		rCopyErrResp(wr, status, status, "ekey b64 decode error")
		return
	}
	arr = strings.Split(string(data), ":")
	if len(arr) != 2 {
		status = errors.RetUrlBad
		rCopyErrResp(wr, status, status, "ekey b64 decode error")
		return
	}
	bucketSrc = arr[0]
	fileSrc = arr[1]
	logbucket = ekey //src ekey

	if data, err = ioutil.ReadAll(r.Body); err != nil {
		status = errors.RetUrlBad
		rCopyErrResp(wr, status, status, "get request body error")
		return
	}
	if err = json.Unmarshal(data, &m); err != nil {
		status = errors.RetUrlBad
		rCopyErrResp(wr, status, status, "request body json Unmarshal error")
		return
	}

	if overWriteFlag, err = strconv.Atoi(m["overwrite"]); err != nil {
		status = errors.RetUrlBad
		rCopyErrResp(wr, status, status, "bad overwrite flag")
		return
	}

	if data, err = base64.URLEncoding.DecodeString(m["dest"]); err != nil {
		status = errors.RetUrlBad
		rCopyErrResp(wr, status, status, "dest b64 decode error")
		return
	}
	arr = strings.Split(string(data), ":")
	bucketDest = arr[0]
	fileDest = arr[1]
	logfile = m["dest"] //dest ekey

	if (bucketSrc == bucketDest && fileSrc == fileDest) ||
		bucketSrc == "" || fileSrc == "" ||
		bucketDest == "" || fileDest == "" {

		status = errors.RetUrlBad
		rCopyErrResp(wr, status, status, "bucket or file is empty string")
		return
	}

	if err = s.efs.Copy(ekey, m["dest"], overWriteFlag); err != nil {
		if err == errors.ErrNeedleNotExist {
			status = errors.RetResNoExist
			errCode = errors.RetNeedleNotExist
			rCopyErrResp(wr, status, errCode, errors.ErrNeedleNotExist.Error())
			return
		} else if err == errors.ErrSrcBucketNoExist {
			status = errors.RetResNoExist
			errCode = errors.RetSrcBucketNoExist
			rCopyErrResp(wr, status, errCode, errors.ErrSrcBucketNoExist.Error())
			return
		} else if err == errors.ErrNeedleExist {
			status = errors.RetResExist
			errCode = errors.RetNeedleExist
			rCopyErrResp(wr, status, errCode, errors.ErrNeedleExist.Error())
			return
		} else if err == errors.ErrDestBucketNoExist {
			status = errors.RetResNoExist
			errCode = errors.RetDestBucketNoExist
			rCopyErrResp(wr, status, errCode, errors.ErrDestBucketNoExist.Error())
			return
		} else {
			if derr, ok = (err).(errors.Error); ok {
				errCode = int(derr)
			} else {
				errCode = errors.RetServerFailed
			}
			status = errors.RetServerFailed
			rCopyErrResp(wr, status, errCode, errors.ErrServerFailed.Error())
		}
		return
	}

	wr.WriteHeader(status)
	wr.Header().Set("Content-Length", strconv.Itoa(0))
	wr.Header().Set("Content-Type", "application/json")
	wr.Header().Set("Server", "efs")
	wr.Header().Set("Last-Modified", time.Unix(0, mtime).Format(http.TimeFormat))
	return
}

func (s *server) chgm(wr http.ResponseWriter, r *http.Request) {
	var (
		strArr    []string
		ekey      string
		bucket    string
		file      string
		err       error
		derr      errors.Error
		errCode   int
		data      []byte
		mime      string
		ok        bool
		m         map[string]string
		mtime     int64
		start     = time.Now()
		status    = http.StatusOK
		reqLen    int64
		oFileSize int64
	)
	m = make(map[string]string)
	defer httpLog(r.URL.Path, &bucket, &file, 0, &oFileSize, &reqLen, start, &status, &err)

	if ekey = r.Header.Get("ekey"); ekey == "" {
		status = errors.RetUrlBad
		rMetaModifyErrResp(wr, status, status, "ekey is empty string")
		return
	}
	if data, err = base64.URLEncoding.DecodeString(ekey); err != nil {
		status = errors.RetUrlBad
		rMetaModifyErrResp(wr, status, status, "ekey b64 decode error")
		return
	}
	strArr = strings.Split(string(data), ":")
	if len(strArr) != 2 {
		status = errors.RetUrlBad
		rMetaModifyErrResp(wr, status, status, "ekey b64  error")
		return
	}
	bucket = strArr[0]
	file = strArr[1]
	if bucket == "" || file == "" {
		status = errors.RetUrlBad
		rMetaModifyErrResp(wr, status, status, "bucket or file is empty string")
		return
	}

	if data, err = ioutil.ReadAll(r.Body); err != nil {
		status = errors.RetUrlBad
		rMetaModifyErrResp(wr, status, status, "get request body error")
		return
	}
	if err = json.Unmarshal(data, &m); err != nil {
		status = errors.RetUrlBad
		rMetaModifyErrResp(wr, status, status, "request data json Unmarshal error")
		return
	}

	if mime, ok = m["mime"]; !ok {
		status = errors.RetUrlBad
		rMetaModifyErrResp(wr, status, status, "request data, get mime error")
		return
	}

	if err = s.efs.Chgm(ekey, mime); err != nil {
		if err == errors.ErrNeedleNotExist {
			status = errors.RetResNoExist
			errCode = errors.RetNeedleNotExist
			rMetaModifyErrResp(wr, status, errCode, errors.ErrNeedleNotExist.Error())
		} else if err == errors.ErrSrcBucketNoExist {
			status = errors.RetResNoExist
			errCode = errors.RetSrcBucketNoExist
			rMetaModifyErrResp(wr, status, errCode, errors.ErrSrcBucketNoExist.Error())
		} else {
			if derr, ok = (err).(errors.Error); ok {
				errCode = int(derr)
			} else {
				errCode = errors.RetServerFailed
			}
			status = errors.RetServerFailed
			rMetaModifyErrResp(wr, status, errCode, errors.ErrServerFailed.Error())
		}

		return
	}

	wr.WriteHeader(status)
	wr.Header().Set("Content-Length", strconv.Itoa(0))
	wr.Header().Set("Content-Type", "application/json")
	wr.Header().Set("Last-Modified", time.Unix(0, mtime).Format(http.TimeFormat))
	return
}

func (s *server) stat(wr http.ResponseWriter, r *http.Request) {
	var (
		arr       []string
		ekey      string
		bucket    string
		file      string
		err       error
		derr      errors.Error
		errCode   int
		ok        bool
		mime      string
		sha1      string
		mtime     int64
		data      []byte
		fsize     string
		start     = time.Now()
		status    = http.StatusOK
		respOK    meta.PFStatRetOK
		reqLen    int64
		oFileSize int64
	)

	defer httpLog(r.URL.Path, &bucket, &file, 0, &oFileSize, &reqLen, start, &status, &err)

	if ekey = r.Header.Get("ekey"); ekey == "" {
		status = errors.RetUrlBad
		rStatErrResp(wr, status, status, "ekey is empty string")
		return
	}
	if data, err = base64.URLEncoding.DecodeString(ekey); err != nil {
		status = errors.RetUrlBad
		rStatErrResp(wr, status, status, "ekey b64 decode error")
		return
	}
	arr = strings.Split(string(data), ":")
	bucket = arr[0]
	file = arr[1]
	if len(arr) != 2 {
		status = errors.RetUrlBad
		rStatErrResp(wr, status, status, "ekey  error")
		return
	}
	if bucket == "" || file == "" {
		status = errors.RetUrlBad
		rStatErrResp(wr, status, status, "bucket or file is empty")
		return
	}

	if fsize, mtime, sha1, mime, err = s.efs.Stat(ekey, bucket, file); err != nil {
		if err == errors.ErrNeedleNotExist {
			status = errors.RetResNoExist
			errCode = errors.RetNeedleNotExist
			rStatErrResp(wr, status, errCode, errors.ErrNeedleNotExist.Error())
		} else if err == errors.ErrSrcBucketNoExist {
			status = errors.RetResNoExist
			errCode = errors.RetSrcBucketNoExist
			rStatErrResp(wr, status, errCode, errors.ErrSrcBucketNoExist.Error())
		} else {
			if derr, ok = (err).(errors.Error); ok {
				errCode = int(derr)
			} else {
				errCode = errors.RetServerFailed
			}
			status = errors.RetServerFailed
			rStatErrResp(wr, status, errCode, errors.ErrServerFailed.Error())
		}

		return
	}

	tfsize, err := strconv.ParseInt(fsize, 10, 64)
	if err != nil {
		status = errors.RetServerFailed
		rStatErrResp(wr, status, status, "fsize parse int error")
		return
	}

	respOK.FSize = int64(tfsize)
	respOK.Hash = sha1
	respOK.MimeType = mime
	respOK.PutTime = mtime
	if data, err = json.Marshal(respOK); err != nil {
		status = errors.RetServerFailed
		rStatErrResp(wr, status, status, "response data json encode error")
		return
	}

	wr.WriteHeader(status)
	wr.Header().Set("Content-Length", strconv.Itoa(len(data)))
	wr.Header().Set("Content-Type", "application/json")
	wr.Header().Set("Server", "efs")
	wr.Header().Set("Last-Modified", time.Unix(0, mtime).Format(http.TimeFormat))
	wr.Header().Set("Etag", sha1)
	wr.Write(data)
	return
}

func (s *server) list(wr http.ResponseWriter, r *http.Request) {
	var (
		bucket    string
		ekey      string
		data      []byte
		limit     string
		delimiter string
		marker    string
		prefix    string
		start     = time.Now()
		status    = http.StatusOK
		err       error
		ok        bool
		derr      errors.Error
		errCode   int
		flist     *meta.PFListRetOK
		reqLen    int64
		oFileSize int64
		file      string
	)
	defer httpLog(r.URL.Path, &bucket, &file, 0, &oFileSize, &reqLen, start, &status, &err)

	if ekey = r.Header.Get("ekey"); ekey == "" {
		status = errors.RetUrlBad
		rListErrResp(wr, status, status, "ekey is empty string")
		return
	}
	if data, err = base64.URLEncoding.DecodeString(ekey); err != nil {
		status = errors.RetUrlBad
		rListErrResp(wr, status, status, "ekey base64 decode error")
		return
	}
	bucket = string(data)

	limit = r.FormValue("limit")
	prefix = r.FormValue("prefix")
	delimiter = r.FormValue("delimiter")
	marker = r.FormValue("marker")

	if limit == "" {
		limit = strconv.Itoa(LIST_LIMIT)
	}
	var limitNum int64
	if limitNum, err = strconv.ParseInt(limit, 10, 32); err != nil {
		status = errors.RetUrlBad
		rListErrResp(wr, status, status, "parse limit error")
		return
	}
	if limitNum < 0 || limitNum > LIST_LIMIT {
		status = errors.RetUrlBad
		rListErrResp(wr, status, status, "limit can't big than 1000")
		return
	}
	if limitNum == 0 {
		limitNum = LIST_LIMIT
	}

	if flist, err = s.efs.List(ekey, strconv.FormatInt(limitNum, 10), prefix, delimiter, marker); err != nil {
		if err == errors.ErrSrcBucketNoExist {
			status = errors.RetResNoExist
			errCode = errors.RetSrcBucketNoExist
			rListErrResp(wr, status, errCode, errors.ErrSrcBucketNoExist.Error())
		} else {
			if derr, ok = (err).(errors.Error); ok {
				errCode = int(derr)
			} else {
				errCode = errors.RetServerFailed
			}
			status = errors.RetServerFailed
			rListErrResp(wr, status, errCode, errors.ErrServerFailed.Error())
		}

		return
	}

	if data, err = json.Marshal(flist); err != nil {
		log.Errorf("downloadSlice() flist(%v) json marshal error", flist)
		status = errors.RetServerFailed
		rListErrResp(wr, status, status, "json marshal error")
		return
	}
	wr.WriteHeader(status)
	wr.Header().Set("Content-Type", "application/json")
	wr.Header().Set("Content-Length", strconv.Itoa(len(data)))
	wr.Write(data)
	return
}

// monitorPing sure program now runs correctly, when return http status 200.
func (s *server) ping(wr http.ResponseWriter, r *http.Request) {
	var (
		byteJson []byte
		f        *os.File
		res      = map[string]interface{}{"code": 0}
		err      error
	)
	if f, err = os.Open("/tmp/proxy.ping"); err == nil {
		// ping check
		res["code"] = http.StatusInternalServerError
		f.Close()
	}
	if err = s.efs.Ping(); err != nil {
		http.Error(wr, "", http.StatusInternalServerError)
		res["code"] = http.StatusInternalServerError
	}
	if byteJson, err = json.Marshal(res); err != nil {
		return
	}
	wr.Header().Set("Content-Type", "application/json;charset=utf-8")
	if _, err = wr.Write(byteJson); err != nil {
		return
	}
	return
}

func (s *server) bcreate(wr http.ResponseWriter, r *http.Request) {
	var (
		ekey      string
		data      []byte
		m         map[string]string
		ok        bool
		bucket    string
		families  string
		start     = time.Now()
		err       error
		derr      errors.Error
		status    = http.StatusOK
		reqLen    int64
		oFileSize int64
		file      string
	)
	m = make(map[string]string)

	defer httpLog(r.URL.Path, &bucket, &file, 0, &oFileSize, &reqLen, start, &status, &err)
	if ekey = r.Header.Get("ekey"); ekey == "" {
		status = errors.RetUrlBad
		buktCrtErrResp(wr, status, status, "ekey is empty string")
		return
	}
	if data, err = base64.URLEncoding.DecodeString(ekey); err != nil {
		status = errors.RetUrlBad
		buktCrtErrResp(wr, status, status, "ekey b64 decode error")
		return
	}
	bucket = string(data)

	if data, err = ioutil.ReadAll(r.Body); err != nil {
		status = errors.RetUrlBad
		buktCrtErrResp(wr, status, status, "request body data get error")
		return
	}
	if err = json.Unmarshal(data, &m); err != nil {
		status = errors.RetUrlBad
		buktCrtErrResp(wr, status, status, "request body data json decode error")
		return
	}
	if families, ok = m["families"]; !ok || families == "" {
		status = errors.RetUrlBad
		buktCrtErrResp(wr, status, status, "families is emtpy string")
		return
	}

	if err = s.efs.BucketCreate(ekey, families); err == nil {
		wr.WriteHeader(status)
		wr.Header().Set("Content-Type", "application/json")
		return
	} else {
		if err == errors.ErrNeedleExist {
			status = errors.RetResExist
			buktCrtErrResp(wr, status, errors.RetNeedleExist, errors.ErrNeedleExist.Error())
		} else {
			if derr, ok = (err).(errors.Error); ok {
				status = errors.RetServerFailed
				errCode := int(derr)
				buktCrtErrResp(wr, status, errCode, errors.ErrServerFailed.Error())
			} else {
				status = errors.RetServerFailed
				buktCrtErrResp(wr, status, status, errors.ErrServerFailed.Error())
			}
		}
		return
	}
}

//rename bucket
func (s *server) brename(wr http.ResponseWriter, r *http.Request) {
	var (
		bucket_src string
		bucket_dst string
		status     = http.StatusOK
		data       []byte
		m          map[string]string
		ekey       string
		start      = time.Now()
		err        error
		derr       errors.Error
		ok         bool
		reqLen     int64
		oFileSize  int64
	)
	m = make(map[string]string)

	defer httpLog(r.URL.Path, &bucket_src, &bucket_dst, 0, &oFileSize, &reqLen, start, &status, &err)

	if ekey = r.Header.Get("ekey"); ekey == "" {
		status = errors.RetUrlBad
		buktRnmErrResp(wr, status, status, "ekey is empty string")
		return
	}
	if data, err = base64.URLEncoding.DecodeString(ekey); err != nil {
		status = errors.RetUrlBad
		buktRnmErrResp(wr, status, status, "ekey b64 decode error")
		return
	}
	bucket_src = string(data)

	if data, err = ioutil.ReadAll(r.Body); err != nil {
		status = errors.RetUrlBad
		buktRnmErrResp(wr, status, status, "request body data get error")
		return
	}
	if err = json.Unmarshal(data, &m); err != nil {
		status = errors.RetUrlBad
		buktRnmErrResp(wr, status, status, "request body data json decode error")
		return
	}
	if data, err = base64.URLEncoding.DecodeString(m["dest"]); err != nil {
		status = errors.RetUrlBad
		buktRnmErrResp(wr, status, status, "dest b64 decode error")
		return
	}
	bucket_dst = string(data)

	if err = s.efs.BucketRename(ekey, m["dest"]); err == nil {
		wr.WriteHeader(status)
		wr.Header().Set("Content-Type", "application/json")
		return
	} else {
		if err == errors.ErrNeedleNotExist {
			status = errors.RetResNoExist
			buktRnmErrResp(wr, status, errors.RetNeedleNotExist, errors.ErrNeedleNotExist.Error())
		} else {
			if derr, ok = (err).(errors.Error); ok {
				status = errors.RetServerFailed
				errCode := int(derr)
				buktRnmErrResp(wr, status, errCode, errors.ErrServerFailed.Error())
			} else {
				status = errors.RetServerFailed
				buktRnmErrResp(wr, status, status, errors.ErrServerFailed.Error())
			}
		}
		return
	}
}

//delete bucket
func (s *server) bdelete(wr http.ResponseWriter, r *http.Request) {
	var (
		bucket    string
		ekey      string
		data      []byte
		status    = http.StatusOK
		start     = time.Now()
		err       error
		derr      errors.Error
		ok        bool
		reqLen    int64
		oFileSize int64
		file      string
	)

	defer httpLog(r.URL.Path, &bucket, &file, 0, &oFileSize, &reqLen, start, &status, &err)

	if ekey = r.Header.Get("ekey"); ekey == "" {
		status = errors.RetUrlBad
		buktDelErrResp(wr, status, status, "ekey is empty string")
		return
	}
	if data, err = base64.URLEncoding.DecodeString(ekey); err != nil {
		status = errors.RetUrlBad
		buktDelErrResp(wr, status, status, "ekey b64 decode error")
		return
	}
	bucket = string(data)

	if err = s.efs.BucketDelete(ekey); err == nil {
		wr.WriteHeader(status)
		wr.Header().Set("Content-Type", "application/json")
		return
	} else {
		if err == errors.ErrSrcBucketNoExist {
			status = errors.RetResNoExist
			buktDelErrResp(wr, status, errors.RetSrcBucketNoExist, errors.ErrSrcBucketNoExist.Error())
		} else {
			if derr, ok = (err).(errors.Error); ok {
				status = errors.RetServerFailed
				errCode := int(derr)
				buktDelErrResp(wr, status, errCode, errors.ErrServerFailed.Error())
			} else {
				status = errors.RetServerFailed
				buktDelErrResp(wr, status, status, errors.ErrServerFailed.Error())
			}
		}
		return
	}
}

//list bucket
func (s *server) blist(wr http.ResponseWriter, r *http.Request) {
	var (
		status    = http.StatusOK
		start     = time.Now()
		err       error
		derr      errors.Error
		data      []byte
		m         map[string]string
		ok        bool
		respOK    meta.PBListRetOK
		regular   string
		reqLen    int64
		oFileSize int64
		file      string
	)
	m = make(map[string]string)

	defer httpLog(r.URL.Path, &file, &file, 0, &oFileSize, &reqLen, start, &status, &err)

	if data, err = ioutil.ReadAll(r.Body); err != nil {
		status = errors.RetUrlBad
		buktListErrResp(wr, status, status, "request data get error")
		return
	}
	if err = json.Unmarshal(data, &m); err != nil {
		status = errors.RetUrlBad
		buktListErrResp(wr, status, status, "request data json decode error")
		return
	}
	if regular, ok = m["regular"]; !ok || regular == "" {
		status = errors.RetUrlBad
		buktListErrResp(wr, status, status, "regular get error")
		return
	}

	if respOK.Buckets, err = s.efs.BucketList(regular); err == nil {
		if data, err = json.Marshal(respOK); err == nil {
			wr.WriteHeader(status)
			wr.Header().Set("Content-Type", "application/json")
			wr.Header().Set("Content-Length", strconv.Itoa(len(data)))
			wr.Write(data)
		} else {
			wr.WriteHeader(http.StatusInternalServerError)
			log.Errorf("bukt list json.Marshal() error(%v)", err)
		}
		return
	} else {
		if derr, ok = (err).(errors.Error); ok {
			status = errors.RetServerFailed
			errCode := int(derr)
			buktListErrResp(wr, status, errCode, errors.ErrServerFailed.Error())
		} else {
			status = errors.RetServerFailed
			buktListErrResp(wr, status, status, errors.ErrServerFailed.Error())
		}
		return
	}
}

//bucket stat
func (s *server) bstat(wr http.ResponseWriter, r *http.Request) {
	var (
		bucket    string
		ekey      string
		data      []byte
		status    = http.StatusOK
		start     = time.Now()
		err       error
		derr      errors.Error
		ok        bool
		respOK    meta.PBStatRetOK
		reqLen    int64
		oFileSize int64
		file      string
	)

	defer httpLog(r.URL.Path, &bucket, &file, 0, &oFileSize, &reqLen, start, &status, &err)

	if ekey = r.Header.Get("ekey"); ekey == "" {
		status = errors.RetUrlBad
		buktDelErrResp(wr, status, status, "ekey is empty string")
		return
	}
	if data, err = base64.URLEncoding.DecodeString(ekey); err != nil {
		status = errors.RetUrlBad
		buktDelErrResp(wr, status, status, "ekey b64 decode error")
		return
	}
	bucket = string(data)

	if respOK.Exist, err = s.efs.BucketStat(ekey); err == nil {
		if data, err = json.Marshal(respOK); err == nil {
			wr.WriteHeader(status)
			wr.Header().Set("Content-Type", "application/json")
			wr.Write(data)
		} else {
			wr.WriteHeader(http.StatusInternalServerError)
			log.Errorf("bukt stat json.Marshal() error(%v)", err)
		}
		return
	} else {

		if derr, ok = (err).(errors.Error); ok {
			status = errors.RetServerFailed
			errCode := int(derr)
			buktStatErrResp(wr, status, errCode, errors.ErrServerFailed.Error())
		} else {
			status = errors.RetServerFailed
			buktStatErrResp(wr, status, status, errors.ErrServerFailed.Error())
		}
		return
	}
}

//batch
func (s *server) batch(wr http.ResponseWriter, r *http.Request) {
	const DLMT = ":"
	var (
		failureExist bool
		status       int
		errMsg       map[string]string
		data         []byte
		ops          []map[string]string
		err          error
		derr         errors.Error
		ok           bool
		respItem     *meta.PFBatchItem
		respItems    []*meta.PFBatchItem
		src          []byte
		srcBucket    string
		start        = time.Now()
		srcFile      string
		dest         []byte
		destBucket   string
		destFile     string
		ctlen        int64
		oFileSize    int64
		b_file       string
	)
	ops = make([]map[string]string, 0)

	defer httpLog(r.URL.Path, &b_file, &b_file, 0, &oFileSize, &ctlen, start, &status, &err)

	if data, err = ioutil.ReadAll(r.Body); err != nil {
		wr.WriteHeader(errors.RetUrlBad)
		return
	}

	if err = json.Unmarshal(data, &ops); err != nil {
		wr.WriteHeader(errors.RetUrlBad)
		return
	}

	for _, op := range ops {
		respItem = new(meta.PFBatchItem)
		errMsg = make(map[string]string)

		opKey := op["op"]
		switch opKey {
		case "stat":
			var (
				fsize    string
				tfsize   int64
				mtime    int64
				sha1     string
				mime     string
				respStat meta.PFStatRetOK
			)

			srcB64 := op["src"]
			if src, err = base64.URLEncoding.DecodeString(srcB64); err != nil || srcB64 == "" {
				failureExist = true
				status = errors.RetUrlBad
				errMsg["error"] = "stat err, src b64 decode err, op: " + op["op"] + ", src: " + op["src"] + ", dest: " + op["dest"]
				respItem.Code = status
				respItem.Data = errMsg
				httpLog("/r/stat", &srcBucket, &srcFile, 0, &oFileSize, &ctlen, start, &status, &err)
				goto addItem
			}
			srcArr := strings.Split(string(src), DLMT)
			if len(srcArr) == 2 {
				srcBucket = srcArr[0]
				srcFile = srcArr[1]
			}
			if fsize, mtime, sha1, mime, err = s.efs.Stat(srcB64, srcBucket, srcFile); err != nil {
				failureExist = true
				if err == errors.ErrNeedleNotExist {
					status = errors.RetResNoExist
					errMsg["error"] = "stat err, " + strconv.Itoa(errors.RetNeedleNotExist) + ": res no exist, op: " + op["op"] + ", src: " + op["src"] + ", dest: " + op["dest"]
				} else if err == errors.ErrSrcBucketNoExist {
					status = errors.RetResNoExist
					errMsg["error"] = "stat err, " + strconv.Itoa(errors.RetSrcBucketNoExist) + ": res no exist, op: " + op["op"] + ", src: " + op["src"] + ", dest: " + op["dest"]
				} else if err == errors.ErrStoreNotAvailable {
					status = errors.RetServerFailed
					errMsg["error"] = "stat err," + strconv.Itoa(errors.RetStoreNotAvailable) + ": serv failed, op: " + op["op"] + ", src: " + op["src"] + ", dest: " + op["dest"]
				} else {
					status = errors.RetServerFailed
					errMsg["error"] = "stat err, " + strconv.Itoa(status) + ": serv err, op: " + op["op"] + ", src: " + op["src"] + ", dest: " + op["dest"]
				}
				respItem.Code = status
				respItem.Data = errMsg
				httpLog("/r/stat", &srcBucket, &srcFile, 0, &oFileSize, &ctlen, start, &status, &err)
				goto addItem
			}

			if tfsize, err = strconv.ParseInt(fsize, 10, 64); err != nil {
				failureExist = true
				status = errors.RetServerFailed
				errMsg["error"] = "stat err, parse int fsize err, op: " + op["op"] + ", src: " + op["src"] + ", dest: " + op["dest"]
				respItem.Code = status
				respItem.Data = errMsg
				httpLog("/r/stat", &srcBucket, &srcFile, 0, &oFileSize, &ctlen, start, &status, &err)
				goto addItem
			}

			respStat.FSize = tfsize
			respStat.Hash = sha1
			respStat.MimeType = mime
			respStat.PutTime = mtime
			//respItem
			status = http.StatusOK
			respItem.Code = status
			respItem.Data = respStat

			httpLog("/r/stat", &srcBucket, &srcFile, 0, &oFileSize, &ctlen, start, &status, &err)
		case "copy":
			srcB64 := op["src"]
			if src, err = base64.URLEncoding.DecodeString(srcB64); err != nil || srcB64 == "" {
				failureExist = true
				status = errors.RetUrlBad
				errMsg["error"] = "copy err, src b64 decode err, op: " + op["op"] + ", src: " + op["src"] + ", dest: " + op["dest"]
				respItem.Code = status
				respItem.Data = errMsg
				httpLog("/r/copy", &srcBucket, &srcFile, 0, &oFileSize, &ctlen, start, &status, &err)
				goto addItem
			}
			srcArr := strings.Split(string(src), DLMT)
			if len(srcArr) == 2 {
				srcBucket = srcArr[0]
				srcFile = srcArr[1]
			}

			destB64 := op["dest"]
			if dest, err = base64.URLEncoding.DecodeString(destB64); err != nil || destB64 == "" {
				failureExist = true
				status = errors.RetUrlBad
				respItem.Code = status
				errMsg["error"] = "copy err, dest b64 decode err, op: " + op["op"] + ", src: " + op["src"] + ", dest: " + op["dest"]
				respItem.Data = errMsg
				httpLog("/r/copy", &srcBucket, &srcFile, 0, &oFileSize, &ctlen, start, &status, &err)
				goto addItem
			}
			destArr := strings.Split(string(dest), DLMT)
			if len(destArr) == 2 {
				destBucket = destArr[0]
				destFile = destArr[1]
			}

			if srcBucket == "" || srcFile == "" || destBucket == "" || destFile == "" || srcFile == destFile {
				failureExist = true
				status = errors.RetUrlBad
				respItem.Code = status
				errMsg["error"] = "copy err, src or dest name is empty, op: " + op["op"] + ", src: " + op["src"] + ", dest: " + op["dest"]
				respItem.Data = errMsg
				httpLog("/r/copy", &srcBucket, &srcFile, 0, &oFileSize, &ctlen, start, &status, &err)
				goto addItem
			}

			if err = s.efs.Copy(srcB64, destB64, 0); err != nil {
				failureExist = true
				if err == errors.ErrNeedleNotExist {
					status = errors.RetResNoExist
					errMsg["error"] = "copy err, " + strconv.Itoa(errors.RetNeedleNotExist) + ": res no exist , op: " + op["op"] + ", src: " + op["src"] + ", dest: " + op["dest"]
				} else if err == errors.ErrStoreNotAvailable {
					status = errors.RetServerFailed
					errMsg["error"] = "copy err, " + strconv.Itoa(errors.RetStoreNotAvailable) + ": srv err, op: " + op["op"] + ", src: " + op["src"] + ", dest: " + op["dest"]
				} else {
					status = errors.RetServerFailed
					errMsg["error"] = "copy err, " + strconv.Itoa(status) + ": srv err, op: " + op["op"] + ", src: " + op["src"] + ", dest: " + op["dest"]
				}
				respItem.Code = status
				respItem.Data = errMsg
				httpLog("/r/copy", &srcBucket, &srcFile, 0, &oFileSize, &ctlen, start, &status, &err)
				goto addItem
			}
			status = http.StatusOK
			respItem.Code = status
			httpLog("/r/copy", &srcBucket, &srcFile, 0, &oFileSize, &ctlen, start, &status, &err)

		case "move":
			srcB64 := op["src"]
			if src, err = base64.URLEncoding.DecodeString(srcB64); err != nil || srcB64 == "" {
				failureExist = true
				status = errors.RetUrlBad
				errMsg["error"] = "move err, src b64 decode err, op: " + op["op"] + ", src: " + op["src"] + ", dest: " + op["dest"]
				respItem.Code = status
				respItem.Data = errMsg
				httpLog("/r/move", &srcBucket, &srcFile, 0, &oFileSize, &ctlen, start, &status, &err)
				goto addItem
			}
			srcArr := strings.Split(string(src), DLMT)
			if len(srcArr) == 2 {
				srcBucket = srcArr[0]
				srcFile = srcArr[1]
			}

			destB64 := op["dest"]
			if dest, err = base64.URLEncoding.DecodeString(destB64); err != nil || destB64 == "" {
				failureExist = true
				status = errors.RetUrlBad
				errMsg["error"] = "move err, dest b64 decode err, op: " + op["op"] + ", src: " + op["src"] + ", dest: " + op["dest"]
				respItem.Code = status
				respItem.Data = errMsg
				httpLog("/r/move", &srcBucket, &srcFile, 0, &oFileSize, &ctlen, start, &status, &err)
				goto addItem
			}
			destArr := strings.Split(string(dest), DLMT)
			if len(destArr) == 2 {
				destBucket = destArr[0]
				destFile = destArr[1]
			}

			if srcBucket == "" || srcFile == "" || destBucket == "" || destFile == "" || srcFile == destFile {
				failureExist = true
				status = errors.RetUrlBad
				respItem.Code = status
				errMsg["error"] = "copy err, src or dest name is empty, op: " + op["op"] + ", src: " + op["src"] + ", dest: " + op["dest"]
				respItem.Data = errMsg
				httpLog("/r/move", &srcBucket, &srcFile, 0, &oFileSize, &ctlen, start, &status, &err)
				goto addItem
			}

			if err = s.efs.Move(srcB64, destB64, 0); err != nil {
				failureExist = true
				if err == errors.ErrNeedleNotExist {
					status = errors.RetResNoExist
					errMsg["error"] = "move err," + strconv.Itoa(errors.RetNeedleNotExist) + ": res no exist , op: " + op["op"] + ", src: " + op["src"] + ", dest: " + op["dest"]
				} else if err == errors.ErrStoreNotAvailable {
					status = errors.RetServerFailed
					errMsg["error"] = "move err," + strconv.Itoa(errors.RetStoreNotAvailable) + ": srv failed, op: " + op["op"] + ", src: " + op["src"] + ", dest: " + op["dest"]
				} else {
					status = errors.RetServerFailed
					errMsg["error"] = "move err," + strconv.Itoa(status) + ":  srv failed, op: " + op["op"] + ", src: " + op["src"] + ", dest: " + op["dest"]
				}
				respItem.Code = status
				respItem.Data = errMsg
				httpLog("/r/move", &srcBucket, &srcFile, 0, &oFileSize, &ctlen, start, &status, &err)
				goto addItem
			}

			status = http.StatusOK
			respItem.Code = status
			httpLog("/r/move", &srcBucket, &srcFile, 0, &oFileSize, &ctlen, start, &status, &err)

		case "delete":
			status = http.StatusOK
			errMsg = make(map[string]string)
			srcB64 := op["src"]
			if src, err = base64.URLEncoding.DecodeString(srcB64); err != nil || srcB64 == "" {
				failureExist = true
				status = errors.RetUrlBad
				errMsg["error"] = "delete err, src b64 decode err, op: " + op["op"] + ", src: " + op["src"] + ", dest: " + op["dest"]
				respItem.Code = status
				respItem.Data = errMsg
				httpLog("/r/delete", &srcBucket, &srcFile, 0, &oFileSize, &oFileSize, start, &status, &err)
				goto addItem
			}
			srcArr := strings.Split(string(src), DLMT)
			if len(srcArr) == 2 {
				srcBucket = srcArr[0]
				srcFile = srcArr[1]
			}

			if oFileSize, err = s.efs.Delete(srcB64, srcBucket, srcFile); err != nil {
				if err == errors.ErrNeedleNotExist {
					status = errors.RetResNoExist
					errMsg["error"] = "delete err, " + strconv.Itoa(status) + ": resource no exist, op: " +
						op["op"] + ", src: " + op["src"] + ", dest: " + op["dest"]
				} else if err == errors.ErrSrcBucketNoExist {
					status = errors.RetResNoExist
					errMsg["error"] = "delete err, " + strconv.Itoa(status) + ": bucket no exist, op: " +
						op["op"] + ", src: " + op["src"] + ", dest: " + op["dest"]
				} else {
					if derr, ok = (err).(errors.Error); ok {
						status = int(derr)
						errMsg["error"] = "delete err, " + strconv.Itoa(status) + ": srv failed, op: " +
							op["op"] + ", src: " + op["src"] + ", dest: " + op["dest"]
					} else {
						status = errors.RetServerFailed
						errMsg["error"] = "delete err, " + strconv.Itoa(status) + ": srv failed, op: " +
							op["op"] + ", src: " + op["src"] + ", dest: " + op["dest"]
					}
				}
				failureExist = true

				respItem.Code = status
				respItem.Data = errMsg
				httpLog("/r/delete", &srcBucket, &srcFile, 0, &oFileSize, &oFileSize, start, &status, &err)
				goto addItem
			}

			status = http.StatusOK
			respItem.Code = status
			httpLog("/r/delete", &srcBucket, &srcFile, 0, &oFileSize, &oFileSize, start, &status, &err)

		default:
			failureExist = true
			status = errors.RetUrlBad
			errMsg["error"] = "op error, op: " + op["op"] + ", src: " + op["src"] + ", dest: " + op["dest"]
			respItem.Code = status
			respItem.Data = errMsg
		}

	addItem:
		respItems = append(respItems, respItem)
	}

	if data, err = json.Marshal(respItems); err != nil {
		status = errors.RetServerFailed
		wr.WriteHeader(status)
		return
	}

	if failureExist {
		status = errors.RetPartialFailed
	}

	wr.WriteHeader(status)
	wr.Header().Set("Content-Type", "application/json; charset=utf-8")
	wr.Write(data)
}

//bucket create error response
func buktCrtErrResp(wr http.ResponseWriter, status int, errCode int, errMsg string) {
	var (
		respErr meta.PBCreateRetFailed
		retJson []byte
		err     error
	)

	respErr.Error = strconv.Itoa(errCode) + ": " + errMsg
	if retJson, err = json.Marshal(respErr); err != nil {
		wr.WriteHeader(http.StatusInternalServerError)
		log.Errorf("buktCrtErrResp marshal json error(%v)", err)
		return
	}

	wr.Header().Set("Content-Type", "application/json; charset=utf-8")
	wr.WriteHeader(status)
	wr.Write(retJson)
	return
}

//bucket rname error response
func buktRnmErrResp(wr http.ResponseWriter, status int, errCode int, errMsg string) {
	var (
		respErr meta.PBRenameRetFailed
		retJson []byte
		err     error
	)

	respErr.Error = strconv.Itoa(errCode) + ": " + errMsg
	if retJson, err = json.Marshal(respErr); err != nil {
		wr.WriteHeader(http.StatusInternalServerError)
		log.Errorf("buktRnmErrResp marshal json error(%v)", err)
		return
	}

	wr.Header().Set("Content-Type", "application/json; charset=utf-8")
	wr.WriteHeader(status)
	wr.Write(retJson)
	return
}

//bucket delete error response
func buktDelErrResp(wr http.ResponseWriter, status int, errCode int, errMsg string) {
	var (
		respErr meta.PBDeleteRetFailed
		retJson []byte
		err     error
	)

	respErr.Error = strconv.Itoa(errCode) + ": " + errMsg
	if retJson, err = json.Marshal(respErr); err != nil {
		wr.WriteHeader(http.StatusInternalServerError)
		log.Errorf("buktDelErrResp marshal json error(%v)", err)
		return
	}

	wr.Header().Set("Content-Type", "application/json; charset=utf-8")
	wr.WriteHeader(status)
	wr.Write(retJson)
	return
}

//bucket list error response
func buktListErrResp(wr http.ResponseWriter, status int, errCode int, errMsg string) {
	var (
		respErr meta.PBListRetFailed
		retJson []byte
		err     error
	)

	respErr.Error = strconv.Itoa(errCode) + ": " + errMsg
	if retJson, err = json.Marshal(respErr); err != nil {
		wr.WriteHeader(http.StatusInternalServerError)
		log.Errorf("buktListErrResp marshal json error(%v)", err)
		return
	}

	wr.Header().Set("Content-Type", "application/json; charset=utf-8")
	wr.WriteHeader(status)
	wr.Write(retJson)
	return
}

//bucket stat error response
func buktStatErrResp(wr http.ResponseWriter, status int, errCode int, errMsg string) {
	var (
		respErr meta.PBStatRetFailed
		retJson []byte
		err     error
	)

	respErr.Error = strconv.Itoa(errCode) + ": " + errMsg
	if retJson, err = json.Marshal(respErr); err != nil {
		wr.WriteHeader(http.StatusInternalServerError)
		log.Errorf("buktStatErrResp marshal json error(%v)", err)
		return
	}

	wr.Header().Set("Content-Type", "application/json; charset=utf-8")
	wr.WriteHeader(status)
	wr.Write(retJson)
	return
}

//resource list error response
func rListErrResp(wr http.ResponseWriter, status int, errCode int, errMsg string) {
	var (
		respErr meta.PFListFailed
		retJson []byte
		err     error
	)

	respErr.Error = strconv.Itoa(errCode) + ": " + errMsg
	if retJson, err = json.Marshal(respErr); err != nil {
		wr.WriteHeader(http.StatusInternalServerError)
		log.Errorf("rListErrResp marshal json error(%v)", err)
		return
	}

	wr.Header().Set("Content-Type", "application/json; charset=utf-8")
	wr.WriteHeader(status)
	wr.Write(retJson)
	return
}

//resource metadata modify error response
func rMetaModifyErrResp(wr http.ResponseWriter, status int, errCode int, errMsg string) {
	var (
		respErr meta.PFStatChgFailed
		retJson []byte
		err     error
	)

	respErr.Error = strconv.Itoa(errCode) + ": " + errMsg
	if retJson, err = json.Marshal(respErr); err != nil {
		wr.WriteHeader(http.StatusInternalServerError)
		log.Errorf("rMetaModifyErrResp marshal json error(%v)", err)
		return
	}

	wr.Header().Set("Content-Type", "application/json; charset=utf-8")
	wr.WriteHeader(status)
	wr.Write(retJson)
	return
}

//resource copy error response
func rCopyErrResp(wr http.ResponseWriter, status int, errCode int, errMsg string) {
	var (
		respErr meta.PFCopyFailed
		retJson []byte
		err     error
	)

	respErr.Error = strconv.Itoa(errCode) + ": " + errMsg
	if retJson, err = json.Marshal(respErr); err != nil {
		wr.WriteHeader(http.StatusInternalServerError)
		log.Errorf("rCopyErrResp marshal json error(%v)", err)
		return
	}

	wr.Header().Set("Content-Type", "application/json; charset=utf-8")
	wr.WriteHeader(status)
	wr.Write(retJson)
	return
}

//resource move error response
func rMoveErrResp(wr http.ResponseWriter, status int, errCode int, errMsg string) {
	var (
		respErr meta.PFMvFailed
		retJson []byte
		err     error
	)

	respErr.Error = strconv.Itoa(errCode) + ": " + errMsg
	if retJson, err = json.Marshal(respErr); err != nil {
		wr.WriteHeader(http.StatusInternalServerError)
		log.Errorf("rMoveErrResp marshal json error(%v)", err)
		return
	}

	wr.Header().Set("Content-Type", "application/json; charset=utf-8")
	wr.WriteHeader(status)
	wr.Write(retJson)
	return
}

//resource stat error response
func rStatErrResp(wr http.ResponseWriter, status int, errCode int, errMsg string) {
	var (
		respErr meta.PFStatFailed
		retJson []byte
		err     error
	)

	respErr.Error = strconv.Itoa(errCode) + ": " + errMsg
	if retJson, err = json.Marshal(respErr); err != nil {
		wr.WriteHeader(http.StatusInternalServerError)
		log.Errorf("rStatErrResp marshal json error(%v)", err)
		return
	}

	wr.Header().Set("Content-Type", "application/json; charset=utf-8")
	wr.WriteHeader(status)
	wr.Write(retJson)
	return
}

//resource delete error response
func rDelErrResp(wr http.ResponseWriter, status int, errCode int, errMsg string) {
	var (
		respErr meta.PFDelFailed
		retJson []byte
		err     error
	)

	respErr.Error = strconv.Itoa(errCode) + ": " + errMsg
	if retJson, err = json.Marshal(respErr); err != nil {
		wr.WriteHeader(http.StatusInternalServerError)
		log.Errorf("rDelErrResp marshal json error(%v)", err)
		return
	}

	wr.Header().Set("Content-Type", "application/json; charset=utf-8")
	wr.WriteHeader(status)
	wr.Write(retJson)
	return
}

//resource upload make block error response
func rMakeBlkErrResp(wr http.ResponseWriter, status int, errCode int, errMsg string) {
	var (
		respErr meta.PMkblkRetFailed
		retJson []byte
		err     error
	)

	respErr.Code = status
	respErr.Error = strconv.Itoa(errCode) + ": " + errMsg
	if retJson, err = json.Marshal(respErr); err != nil {
		wr.WriteHeader(http.StatusInternalServerError)
		log.Errorf("rMakeBlkErrResp marshal json error(%v)", err)
		return
	}

	wr.Header().Set("Content-Type", "application/json; charset=utf-8")
	wr.WriteHeader(status)
	wr.Write(retJson)
	return
}

//resource upload  block slice error response
func rBlkSliceErrResp(wr http.ResponseWriter, status int, errCode int, errMsg string) {
	var (
		respErr meta.PBputRetFailed
		retJson []byte
		err     error
	)
	respErr.Code = status
	respErr.Error = strconv.Itoa(errCode) + ": " + errMsg
	if retJson, err = json.Marshal(respErr); err != nil {
		wr.WriteHeader(http.StatusInternalServerError)
		log.Errorf("rBlkSliceErrResp marshal json error(%v)", err)
		return
	}

	wr.Header().Set("Content-Type", "application/json; charset=utf-8")
	wr.WriteHeader(status)
	wr.Write(retJson)
	return
}

//resource upload make file error response
func rMakeFileErrResp(wr http.ResponseWriter, status int, errCode int, errMsg string) {
	var (
		respErr meta.PMkfileRetFailed
		retJson []byte
		err     error
	)

	respErr.Code = status
	respErr.Error = strconv.Itoa(errCode) + ": " + errMsg
	if retJson, err = json.Marshal(respErr); err != nil {
		wr.WriteHeader(http.StatusInternalServerError)
		log.Errorf("rMakeFileErrResp marshal json error(%v)", err)
		return
	}

	wr.Header().Set("Content-Type", "application/json; charset=utf-8")
	wr.WriteHeader(status)
	wr.Write(retJson)
	return
}

//resource upload error response
func rUploadErrResp(wr http.ResponseWriter, status int, errCode int, errMsg string) {
	var (
		respErr meta.PFUploadFailed
		retJson []byte
		err     error
	)

	respErr.Code = status
	respErr.Error = strconv.Itoa(errCode) + ": " + errMsg
	if retJson, err = json.Marshal(respErr); err != nil {
		wr.WriteHeader(http.StatusInternalServerError)
		log.Errorf("rUploadErrResp marshal json error(%v)", err)
		return
	}

	wr.Header().Set("Content-Type", "application/json; charset=utf-8")
	wr.WriteHeader(status)
	wr.Write(retJson)
	return
}
