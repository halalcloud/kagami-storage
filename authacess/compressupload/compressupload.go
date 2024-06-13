package compressupload

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"efs/authacess/efs"
	log "efs/log/glog"
	b64 "encoding/base64"
	"encoding/json"
	"io"
	"mime/multipart"
	"strings"

	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/dsnet/compress/bzip2"
	"github.com/nwaples/rardecode"
	"github.com/ulikunitz/xz"
)

type Err_res struct {
	Filename string `json:"filename"`
	Code     int    `json:"code"`
	Error    string `json:"error"`
}

type Sucess_file struct {
	Filename string `json:"filename"`
	Key      string `json:"key"`
	Hash     string `json:"hash"`
}

type Cupload_res struct {
	Code         int           `json:"code"`
	Error        string        `json:"error"`
	Sucess_files []Sucess_file `json:"sucess_files"`
	Error_files  []Err_res     `json:"error_files"`
}

type sizer interface {
	Size() int64
}

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
			return
		}
		size = fi.Size()
	}
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

func Cupload(bucket, fname string, rflag, replication, deleteAfterDays int, f multipart.File, efs *efs.Efs) (retcode int, rbody []byte, errstring string) {
	var (
		res Cupload_res
		err error
	)

	if strings.HasSuffix(fname, ".tar") {
		retcode, rbody, errstring = tarcompress(bucket, rflag, replication, deleteAfterDays, f, efs)
		return
	} else if strings.HasSuffix(fname, ".tar.gz") || strings.HasSuffix(fname, ".tgz") {
		retcode, rbody, errstring = targzcompress(bucket, rflag, replication, deleteAfterDays, f, efs)
		return
	} else if strings.HasSuffix(fname, ".tar.bz2") || strings.HasSuffix(fname, ".tbz2") {
		retcode, rbody, errstring = tarbzcompress(bucket, rflag, replication, deleteAfterDays, f, efs)
		return
	} else if strings.HasSuffix(fname, ".tar.xz") || strings.HasSuffix(fname, ".txz") {
		retcode, rbody, errstring = tarxzcompress(bucket, rflag, replication, deleteAfterDays, f, efs)
		return
	} else if strings.HasSuffix(fname, ".rar") {
		retcode, rbody, errstring = rarcompress(bucket, rflag, replication, deleteAfterDays, f, efs)
		return
	} else if strings.HasSuffix(fname, ".zip") {
		retcode, rbody, errstring = zipcompress(bucket, rflag, replication, deleteAfterDays, f, efs)
		return
		//retcode, rbody, errstring = zipcompress(bucket, rflag, f, efs)
	} else {
		retcode = 400
		errstring = "have no this compress file"
		res.Code = retcode
		res.Error = errstring
		rbody, err = json.Marshal(res)
		if err != nil {
			log.Errorf("upload res body marshal json error(%v)", err)
		}

		return
		//return err not support this compress file
	}
	return
}

func tarcompress(bucket string, rflag, replication, deleteAfterDays int, f multipart.File, efs *efs.Efs) (retcode int, rbody []byte, errstring string) {
	var (
		tr                *tar.Reader
		err               error
		fname, hash, key  string
		res               Cupload_res
		errorf            *Err_res
		sucessf           *Sucess_file
		filesize, oldsize int64
		body              []byte
	)
	tr = tar.NewReader(f)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			retcode = 400
			errstring = "file compress failed"
			res.Code = retcode
			res.Error = errstring
			goto RET
		}
		fname = header.Name
		ctype := "todo"
		start := time.Now()
		if body, err = ioutil.ReadAll(tr); err != nil {
			retcode = 400
			errstring = "file read error"
			res.Code = retcode
			res.Error = errstring
			goto RET
		}
		hash, key, retcode, errstring, oldsize, filesize = efs.Upload(bucket, fname, ctype,
			rflag, replication, deleteAfterDays, body)
		if retcode == 200 {
			sucessf = new(Sucess_file)
			sucessf.Filename = fname
			sucessf.Key = key
			sucessf.Hash = hash
			res.Sucess_files = append(res.Sucess_files, *sucessf)
			//if deleteAfterDays != 0 {
			errstring = fmt.Sprintf("%d", deleteAfterDays)
			//}
		} else {
			errorf = new(Err_res)
			errorf.Filename = fname
			errorf.Code = retcode
			errorf.Error = errstring
			res.Error_files = append(res.Error_files, *errorf)
		}
		statislog("/r/upload", &bucket, &fname, &rflag, &oldsize, &filesize, start, &retcode, &errstring)
	}
	if len(res.Error_files) != 0 {
		retcode = 298
		res.Code = 298
		errstring = "some file upload failed"
	} else {
		retcode = 200
		res.Code = 200
	}
RET:
	rbody, err = json.Marshal(res)
	if err != nil {
		log.Errorf("upload res body marshal json error(%v)", err)
	}
	return
}

func targzcompress(bucket string, rflag, replication, deleteAfterDays int, f multipart.File, efs *efs.Efs) (retcode int, rbody []byte, errstring string) {
	var (
		tr                *tar.Reader
		err               error
		fname, hash, key  string
		res               Cupload_res
		errorf            *Err_res
		sucessf           *Sucess_file
		gzr               *gzip.Reader
		filesize, oldsize int64
		body              []byte
	)
	gzr, err = gzip.NewReader(f)
	if err != nil {
		retcode = 400
		errstring = "file compress failed"
		res.Code = retcode
		res.Error = errstring
		goto RET
	}
	defer gzr.Close()
	tr = tar.NewReader(gzr)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			retcode = 400
			errstring = "file compress failed"
			return
		}
		fname = header.Name
		ctype := "todo"
		start := time.Now()
		if body, err = ioutil.ReadAll(tr); err != nil {
			retcode = 400
			errstring = "file read error"
			res.Code = retcode
			res.Error = errstring
			goto RET
		}
		hash, key, retcode, errstring, oldsize, filesize = efs.Upload(bucket, fname,
			ctype, rflag, replication, deleteAfterDays, body)
		if retcode == 200 {
			sucessf = new(Sucess_file)
			sucessf.Filename = fname
			sucessf.Key = key
			sucessf.Hash = hash
			res.Sucess_files = append(res.Sucess_files, *sucessf)
			//if deleteAfterDays != 0 {
			errstring = fmt.Sprintf("%d", deleteAfterDays)
			//}
		} else {
			errorf = new(Err_res)
			errorf.Filename = fname
			errorf.Code = retcode
			errorf.Error = errstring
			res.Error_files = append(res.Error_files, *errorf)
		}
		statislog("/r/upload", &bucket, &fname, &rflag, &oldsize, &filesize, start, &retcode, &errstring)
	}
	if len(res.Error_files) != 0 {
		retcode = 298
		res.Code = 298
		errstring = "some file upload failed"
	} else {
		retcode = 200
		res.Code = 200
	}
RET:
	rbody, err = json.Marshal(res)
	if err != nil {
		log.Errorf("upload res body marshal json error(%v)", err)
	}
	return
}

func tarbzcompress(bucket string, rflag, replication, deleteAfterDays int, f multipart.File, efs *efs.Efs) (retcode int, rbody []byte, errstring string) {
	var (
		tr                *tar.Reader
		err               error
		fname, hash, key  string
		res               Cupload_res
		errorf            *Err_res
		sucessf           *Sucess_file
		gzr               *bzip2.Reader
		filesize, oldsize int64
		body              []byte
	)
	gzr, err = bzip2.NewReader(f, nil)
	if err != nil {
		retcode = 400
		errstring = "file compress failed"
		res.Code = retcode
		res.Error = errstring
		goto RET
	}
	defer gzr.Close()
	tr = tar.NewReader(gzr)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			retcode = 400
			errstring = "file compress failed"
			res.Code = retcode
			res.Error = errstring
			goto RET
		}
		fname = header.Name
		ctype := "todo"
		start := time.Now()
		if body, err = ioutil.ReadAll(tr); err != nil {
			retcode = 400
			errstring = "file read error"
			res.Code = retcode
			res.Error = errstring
			goto RET
		}
		hash, key, retcode, errstring, oldsize, filesize = efs.Upload(bucket, fname, ctype,
			rflag, replication, deleteAfterDays, body)
		if retcode == 200 {
			sucessf = new(Sucess_file)
			sucessf.Filename = fname
			sucessf.Key = key
			sucessf.Hash = hash
			res.Sucess_files = append(res.Sucess_files, *sucessf)
			//if deleteAfterDays != 0 {
			errstring = fmt.Sprintf("%d", deleteAfterDays)
			//}
		} else {
			errorf = new(Err_res)
			errorf.Filename = fname
			errorf.Code = retcode
			errorf.Error = errstring
			res.Error_files = append(res.Error_files, *errorf)
		}
		statislog("/r/upload", &bucket, &fname, &rflag, &oldsize, &filesize, start, &retcode, &errstring)
	}
	if len(res.Error_files) != 0 {
		retcode = 298
		res.Code = 298
		errstring = "some file upload failed"
	} else {
		retcode = 200
		res.Code = 200
	}
RET:
	rbody, err = json.Marshal(res)
	if err != nil {
		log.Errorf("upload res body marshal json error(%v)", err)
	}
	return
}

func tarxzcompress(bucket string, rflag, replication, deleteAfterDays int, f multipart.File, efs *efs.Efs) (retcode int, rbody []byte, errstring string) {
	var (
		tr                *tar.Reader
		err               error
		fname, hash, key  string
		res               Cupload_res
		errorf            *Err_res
		sucessf           *Sucess_file
		gzr               *xz.Reader
		filesize, oldsize int64
		body              []byte
	)
	gzr, err = xz.NewReader(f)
	if err != nil {
		retcode = 400
		errstring = "file compress failed"
		res.Code = retcode
		res.Error = errstring
		goto RET
	}

	tr = tar.NewReader(gzr)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			retcode = 400
			errstring = "file compress failed"
			res.Code = retcode
			res.Error = errstring
			goto RET
		}
		fname = header.Name
		ctype := "todo"
		start := time.Now()
		if body, err = ioutil.ReadAll(tr); err != nil {
			retcode = 400
			errstring = "file read error"
			res.Code = retcode
			res.Error = errstring
			goto RET
		}
		hash, key, retcode, errstring, oldsize, filesize = efs.Upload(bucket, fname, ctype,
			rflag, replication, deleteAfterDays, body)
		if retcode == 200 {
			sucessf = new(Sucess_file)
			sucessf.Filename = fname
			sucessf.Key = key
			sucessf.Hash = hash
			res.Sucess_files = append(res.Sucess_files, *sucessf)
			//	if deleteAfterDays != 0 {
			errstring = fmt.Sprintf("%d", deleteAfterDays)
			//}
		} else {
			errorf = new(Err_res)
			errorf.Filename = fname
			errorf.Code = retcode
			errorf.Error = errstring
			res.Error_files = append(res.Error_files, *errorf)
		}
		statislog("/r/upload", &bucket, &fname, &rflag, &oldsize, &filesize, start, &retcode, &errstring)
	}
	if len(res.Error_files) != 0 {
		retcode = 298
		res.Code = 298
		errstring = "some file upload failed"
	} else {
		retcode = 200
		res.Code = 200
	}
RET:
	rbody, err = json.Marshal(res)
	if err != nil {
		log.Errorf("upload res body marshal json error(%v)", err)
	}
	return
}

func rarcompress(bucket string, rflag, replication, deleteAfterDays int, f multipart.File, efs *efs.Efs) (retcode int, rbody []byte, errstring string) {
	var (
		tr                *rardecode.Reader
		err               error
		fname, hash, key  string
		res               Cupload_res
		errorf            *Err_res
		sucessf           *Sucess_file
		filesize, oldsize int64
		body              []byte
	)
	tr, err = rardecode.NewReader(f, "")
	if err != nil {
		retcode = 400
		errstring = "file compress failed"
		res.Code = retcode
		res.Error = errstring
		goto RET
	}
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			retcode = 400
			errstring = "file compress failed"
			res.Code = retcode
			res.Error = errstring
			goto RET
		}
		fname = header.Name
		ctype := "todo"
		start := time.Now()
		if body, err = ioutil.ReadAll(tr); err != nil {
			retcode = 400
			errstring = "file read error"
			res.Code = retcode
			res.Error = errstring
			goto RET
		}
		hash, key, retcode, errstring, oldsize, filesize = efs.Upload(bucket, fname, ctype,
			rflag, replication, deleteAfterDays, body)
		if retcode == 200 {
			sucessf = new(Sucess_file)
			sucessf.Filename = fname
			sucessf.Key = key
			sucessf.Hash = hash
			res.Sucess_files = append(res.Sucess_files, *sucessf)
			//if deleteAfterDays != 0 {
			errstring = fmt.Sprintf("%d", deleteAfterDays)
			//}
		} else {
			errorf = new(Err_res)
			errorf.Filename = fname
			errorf.Code = retcode
			errorf.Error = errstring
			res.Error_files = append(res.Error_files, *errorf)
		}
		statislog("/r/upload", &bucket, &fname, &rflag, &oldsize, &filesize, start, &retcode, &errstring)
	}
	if len(res.Error_files) != 0 {
		retcode = 298
		res.Code = 298
		errstring = "some file upload failed"
	} else {
		retcode = 200
		res.Code = 200
	}
RET:
	rbody, err = json.Marshal(res)
	if err != nil {
		log.Errorf("upload res body marshal json error(%v)", err)
	}
	return
}

func zipcompress(bucket string, rflag, replication, deleteAfterDays int, f multipart.File, efs *efs.Efs) (retcode int, rbody []byte, errstring string) {
	var (
		tr                *zip.Reader
		err               error
		fname, hash, key  string
		res               Cupload_res
		errorf            *Err_res
		sucessf           *Sucess_file
		size              int64
		filesize, oldsize int64
		body              []byte
	)

	size, err = checkFileSize(f)
	if err != nil {
		retcode = 400
		errstring = "file compress failed"
		res.Code = retcode
		res.Error = errstring
		goto RET
	}

	tr, err = zip.NewReader(f, size)
	if err != nil {
		retcode = 400
		errstring = "file compress failed"
		res.Code = retcode
		res.Error = errstring
		goto RET
	}
	for _, zf := range tr.File {
		rc, err := zf.Open()
		if err != nil {
			retcode = 400
			errstring = "file compress failed"
			res.Code = retcode
			res.Error = errstring
			goto RET
		}
		fname = zf.Name
		ctype := "todo"
		start := time.Now()
		if body, err = ioutil.ReadAll(rc); err != nil {
			retcode = 400
			errstring = "file read error"
			res.Code = retcode
			res.Error = errstring
			goto RET
		}
		hash, key, retcode, errstring, oldsize, filesize = efs.Upload(bucket, fname, ctype,
			rflag, replication, deleteAfterDays, body)
		if retcode == 200 {
			sucessf = new(Sucess_file)
			sucessf.Filename = fname
			sucessf.Key = key
			sucessf.Hash = hash
			res.Sucess_files = append(res.Sucess_files, *sucessf)
			//	if deleteAfterDays != 0 {
			errstring = fmt.Sprintf("%d", deleteAfterDays)
			//}
		} else {
			errorf = new(Err_res)
			errorf.Filename = fname
			errorf.Code = retcode
			errorf.Error = errstring
			res.Error_files = append(res.Error_files, *errorf)
		}

		statislog("/r/upload", &bucket, &fname, &rflag, &oldsize, &filesize, start, &retcode, &errstring)
		rc.Close()
	}
	if len(res.Error_files) != 0 {
		retcode = 298
		res.Code = 298
		errstring = "some file upload failed"
	} else {
		retcode = 200
		res.Code = 200
	}
RET:
	rbody, err = json.Marshal(res)
	if err != nil {
		log.Errorf("upload res body marshal json error(%v)", err)
	}
	return
}
