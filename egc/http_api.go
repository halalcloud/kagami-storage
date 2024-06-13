package main

import (
	// "bytes"
	// "crypto/sha1"
	"encoding/base64"
	"kagamistoreage/egc/conf"
	"kagamistoreage/egc/efs"
	"kagamistoreage/libs/errors"
	"kagamistoreage/libs/meta"

	// "encoding/hex"
	// "encoding/json"
	// "hash/crc32"
	// "io"
	// "io/ioutil"
	"net/http"
	// "os"
	// "path"
	"strconv"
	// "strings"
	log "kagamistoreage/log/glog"
	//"fmt"
	"time"
)

const (
	_httpServerReadTimeout  = 50 * time.Second
	_httpServerWriteTimeout = 50 * time.Second

	LIST_LIMIT = "1000"
)

type server struct {
	efs *efs.Efs
	c   *conf.Config
}

// StartApi init the http module.
func StartApi(c *conf.Config) (err error) {
	var s = &server{}
	s.c = c
	s.efs = efs.New(c)

	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/dealtmp", s.DeleteTmp)
		mux.HandleFunc("/dealtrash", s.DeleteTrash)
		mux.HandleFunc("/dealbucket", s.DeleteTrashBucket)
		mux.HandleFunc("/dealexpire", s.DeleteExpire)

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

func httpLog(uri string, bucket *string, start time.Time, status *int, err *error) {
	log.Statisf("%s	%s %f %d error(%v)", uri, *bucket, time.Now().Sub(start).Seconds(), *status, *err)
}

// set reponse header.
func setCode(wr http.ResponseWriter, status *int) {
	wr.Header().Set("Code", strconv.Itoa(*status))
}

// delete bucket_tmp
func (s *server) DeleteTmp(wr http.ResponseWriter, r *http.Request) {
	var (
		bucket string
		err    error
		status = http.StatusOK
		start  = time.Now()
	)
	defer httpLog(r.URL.Path, &bucket, start, &status, &err)

	bucket = "bucket_tmp"
	ekey := base64.URLEncoding.EncodeToString([]byte(bucket))

	go DestoryBucketfile(s, ekey, bucket, "tmp")

	wr.WriteHeader(status)
	wr.Header().Set("Content-Type", "application/json; charset=utf-8")
	return
}

// delete bucket_trash
func (s *server) DeleteTrash(wr http.ResponseWriter, r *http.Request) {
	var (
		bucket string
		err    error
		status = http.StatusOK
		start  = time.Now()
	)
	bucket = "bucket_trash"
	ekey := base64.URLEncoding.EncodeToString([]byte(bucket))

	defer httpLog(r.URL.Path, &bucket, start, &status, &err)

	go DestoryBucketfile(s, ekey, bucket, "trash")

	wr.WriteHeader(status)
	wr.Header().Set("Content-Type", "application/json; charset=utf-8")
	return
}

// delete bucket_trash_bucket
func (s *server) DeleteTrashBucket(wr http.ResponseWriter, r *http.Request) {
	var (
		regular     string
		err         error
		status      = http.StatusOK
		start       = time.Now()
		bucketnames []string
	)

	defer httpLog(r.URL.Path, &regular, start, &status, &err)
	go func() {
		regular = "bucket_trash_.*"
		if bucketnames, err = s.efs.BucketList(regular); err == nil {
			for _, bn := range bucketnames {
				//fmt.Println("delete bucket:", bn)
				tempekey := base64.URLEncoding.EncodeToString([]byte(bn))
				DestoryBucketfile(s, tempekey, bn, "bucket")
			}
		}
	}()
	wr.WriteHeader(status)
	wr.Header().Set("Content-Type", "application/json; charset=utf-8")
	return
}

func (s *server) destory_needle_to_store(dfile *meta.DestroyFile) (continu bool) {
	var (
		dterr error
		res   meta.Response
	)
	for _, host := range dfile.FileNeedle.Stores {
		if dterr = s.efs.DeleteTmp(strconv.FormatInt(dfile.FileNeedle.Key, 10),
			strconv.FormatInt(int64(dfile.FileNeedle.Vid), 10), host); dterr != nil {
			if dterr == errors.ErrNeedleDeleted || dterr == errors.ErrNeedleNotExist {
				//nothing needle already delete
			} else {
				log.Errorf("Efs DestoryBucketfile() DeleteTmp err! vid:(%d) fname:(%s) fkey:(%d) host:(%s)",
					dfile.FileNeedle.Vid, dfile.FileName, dfile.FileNeedle.Key, host)
				continu = true
				return
			}

		}
	}
	keylen := len(dfile.Keys)
	for i := 1; i < keylen; i++ {
		res, dterr = s.efs.Getkeyaddr(dfile.Keys[i])
		if dterr != nil {
			continu = true
			return
		}
		for _, host := range res.Stores {
			if dterr = s.efs.DeleteTmp(strconv.FormatInt(res.Key, 10),
				strconv.FormatInt(int64(res.Vid), 10), host); dterr != nil {
				if dterr == errors.ErrNeedleDeleted || dterr == errors.ErrNeedleNotExist {
					//nothing needle already delete
				} else {
					log.Errorf("Efs DestoryBucketfile() DeleteTmp err! vid:(%d) fname:(%s) fkey:(%d) host:(%s)",
						res.Vid, dfile.FileName, res.Key, host)
					continu = true
					return
				}

			}
		}

	}
	continu = false
	return

}

// encapsulation func
func DestoryBucketfile(s *server, ekey, bucket, bucketType string) (err error) {
	var (
		marker string
		gcres  *meta.GCDestoryListRetOK
		dlerr  error
		dferr  error
		//	dterr  error
		dberr error
	)

	//tempcount := 0
	for marker != "end" {
		//tempcount++
		if gcres, dlerr = s.efs.DestoryList(ekey, LIST_LIMIT, marker); dlerr != nil {
			log.Errorf("Efs DestoryBucketfile() DestoryList err! ekey:(%s) marker:(%s)", ekey, marker)
			break
		}
		//fmt.Println("list")
		//fmt.Println(gcres)
		if gcres != nil {
			marker = gcres.Marker
			for _, df := range gcres.DList {
				fname := df.FileName
				tempekey := base64.URLEncoding.EncodeToString([]byte(bucket + ":" + fname))
				//fmt.Println("=======", fname)
				// DestoryStorefile
				c := s.destory_needle_to_store(df)
				if c {
					continue
				}

				// DestoryDirfile
				if dferr = s.efs.Destoryfile(tempekey); dferr != nil {
					log.Recoverf("Efs DestoryBucketfile() Destoryfile err! fname:(%s) bucket:(%s)", fname, bucket)
					continue
				}
				//fmt.Println("==222222222222over=====", fname)
			}

			//delete bucket
			if (len(gcres.DList) == 0) && (bucketType != "tmp") && (bucketType != "trash") && gcres.Trash_flag {
				if dberr = s.efs.BucketDelete(ekey); dberr != nil {
					log.Recoverf("Efs DestoryBucketfile() bucketDelete err! ekey:(%s)", ekey)
				}
			}
		}

		if marker == "" {
			break
		}

	}
	return
}

// delete bucket_expire
func (s *server) DeleteExpire(wr http.ResponseWriter, r *http.Request) {
	var (
		err    error
		status = http.StatusOK
		start  = time.Now()
		bucket = "expire"
	)

	defer httpLog(r.URL.Path, &bucket, start, &status, &err)
	go func() {
		s.efs.DeleteExpire()
	}()
	wr.WriteHeader(status)
	wr.Header().Set("Content-Type", "application/json; charset=utf-8")
	return
}
