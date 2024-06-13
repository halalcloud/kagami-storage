package hbase

import (
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"kagamistoreage/directory/hbase/hbasethrift"
	"kagamistoreage/libs/errors"
	"kagamistoreage/libs/meta"
	"kagamistoreage/libs/types"
	"reflect"
	"strconv"
	"strings"
	"time"

	log "kagamistoreage/log/glog"
)

const (
	_prefix        = "bucket_"
	_prefix_tmp    = "bucket_tmp"
	_prefix_trash  = "bucket_trash"
	_prefix_expire = "bucket_expire"
)

const (
	_flag_tmp_file   = 1001
	_flag_trash_file = 1002
	_flag_real_file  = 1003
	_block_size      = 4 * 1024 * 1024
)

var (
	_table           = []byte("efsmeta")         // default bucket
	_bigfiletmptable = []byte("bigfiletmptable") // for big file slice storge

	_familyBasic      = []byte("basic") // basic store info column family
	_columnVid        = []byte("vid")
	_columnCookie     = []byte("cookie")
	_columnUpdateTime = []byte("update_time")
	_columnLink       = []byte("link")

	_familyFile            = []byte("efsfile") // file info column family
	_columnKey             = []byte("key")
	_columnSha1            = []byte("sha1")
	_columnMine            = []byte("mime")
	_columnStatus          = []byte("status")
	_columnFilesize        = []byte("filesize") // add filesize
	_columnDeleteAfterDays = []byte("deleteAfterDays")
	// _columnUpdateTime = []byte("update_time")

	_familyExpire = []byte("info")
	_columnExpire = []byte("expire")
)

type HBaseClient struct {
}

// NewHBaseClient
func NewHBaseClient() *HBaseClient {
	return &HBaseClient{}
}

// Get get needle from hbase
func (h *HBaseClient) Get(bucket, filename string) (f *meta.File, err error) {
	//var index int
	if f, err = h.GetFile(bucket, filename); err != nil {
		return
	}
	/*
		n = make([]*meta.Needle, len(f.Key))
		for index, _ = range f.Key {
			//log.Errorf("key ===%d", f.Key[index])
			if n[index], err = h.getNeedle(f.Key[index]); err == errors.ErrNeedleNotExist {
				log.Errorf("table not match: bucket: %s  filename: %s key %d", bucket, filename, f.Key[index])
				return
			}
			if err != nil {
				log.Errorf("get needle key %d failed %v", f.Key[index], err)
				break
			}
		}
		if index == 0 && err == errors.ErrNeedleNotExist {
			h.delFile(bucket, filename)
		}
	*/
	return
}

// Get get destroy needle from hbase
func (h *HBaseClient) GetDestroy(bucket, filename string) (n []*meta.Needle, f *meta.File, err error) {
	var index int
	if f, err = h.getDestroyFile(bucket, filename); err != nil {
		return
	}
	//TODO slice upload
	n = make([]*meta.Needle, len(f.Key))
	for index, _ = range f.Key {
		if n[index], err = h.GetNeedle(f.Key[index]); err == errors.ErrNeedleNotExist {
			log.Warningf("table not match: bucket: %s  filename: %s", bucket, filename)
		}
	}
	if index == 0 && err == errors.ErrNeedleNotExist {
		h.delFile(bucket, filename)
	}
	return
}

// GetTmpFile get needle from hbase
func (h *HBaseClient) GetTmpFile(bucket, filename string, id string) (n []*meta.Needle, f *meta.File, err error) {
	var index int
	if f, err = h.getTmpFile(h.setTmpFileName(bucket, filename, id)); err != nil {
		return
	}
	//TODO slice upload
	n = make([]*meta.Needle, len(f.Key))
	for index, _ = range f.Key {
		if n[index], err = h.GetNeedle(f.Key[index]); err == errors.ErrNeedleNotExist {
			log.Warningf("table not match: bucket: %s  filename: %s", bucket, filename)
		}
	}
	if index == 0 && err == errors.ErrNeedleNotExist {
		if err = h.delTmpFile(bucket, h.setTmpFileName(bucket, filename, id)); err != nil {
			return
		}
	}

	return
}

// Get File stat
func (h *HBaseClient) GetFileStat(bucket, filename string) (f *meta.File, err error) {
	if f, err = h.GetFile(bucket, filename); err != nil {
		log.Warningf("Directory GetFileStat Failed")
		return
	}
	return
}

// Uptate file mime into hbase
func (h *HBaseClient) UpdateMime(bucket string, f *meta.File) (err error) {
	if err = h.updateFileMime(bucket, f); err != nil {
		return
	}
	return
}

// Uptate file expire into hbase
func (h *HBaseClient) UpdateExp(bucket string, f *meta.File) (err error) {
	if err = h.updateFileExp(bucket, f); err != nil {
		return
	}
	return
}

// List row in expire table
func (h *HBaseClient) ListExpire(limit, marker string) (el *meta.ExpireList, err error) {
	var (
		c         *hbasethrift.THBaseServiceClient
		r         []*hbasethrift.TResult_
		tr        *hbasethrift.TResult_
		cv        *hbasethrift.TColumnValue
		scan      *hbasethrift.TScan
		scanId    int32
		retMarker string
		rlimit    int
	)
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}

	scan = new(hbasethrift.TScan)
	scan.Columns = make([]*hbasethrift.TColumn, 0)
	scan.Columns = append(scan.Columns,
		&hbasethrift.TColumn{
			Family:    _familyExpire,
			Qualifier: _columnExpire,
		},
	)

	if marker != "" {
		var (
			m_b []byte
			ok  error
		)
		if m_b, ok = base64.URLEncoding.DecodeString(marker); ok != nil {
			log.Errorf("getExpireList Parser marker (%s) Failed", marker)
			hbasePool.Put(c, true)
			return
		}
		s_t := strings.Split(string(m_b), ":")
		var tmp int
		if tmp, err = strconv.Atoi(s_t[0]); err != nil {
			log.Errorf("getExpireList Parser marker (%s) Failed", marker)
			hbasePool.Put(c, true)
			return
		}
		scanId = int32(tmp)
		ScanHLUpdata(scanId)
	} else {
		if scanId, err = c.OpenScanner([]byte(_prefix_expire), scan); err != nil {
			log.Errorf("getExpireList OpenScanner Failed: err=%v", err)
			hbasePool.Put(c, true)
			return
		}
	}

	if limit == "" {
		rlimit = 5
	} else {
		rlimit, _ = strconv.Atoi(limit)
	}
	if r, err = c.GetScannerRows(scanId, int32(rlimit)); err != nil {
		log.Errorf("getExpireList GetScannerRows Failed %d", rlimit)
		hbasePool.Put(c, true)
		return
	}

	hbasePool.Put(c, false)
	if len(r) < rlimit {
		retMarker = ""
	} else {
		retMarker = base64.URLEncoding.EncodeToString([]byte(fmt.Sprintf("%d:%d", scanId, time.Now().Unix())))
	}
	el = new(meta.ExpireList)
	el.Items = make([]meta.ExpireItem, 0)
	if len(r) == 0 {
		log.Infof("getExpireList GetScannerRows no list")
		return el, nil
	}

	el.Marker = retMarker
	for _, tr = range r {
		var (
			deleteaftertime int64
		)
		for _, cv = range tr.ColumnValues {
			if cv == nil {
				continue
			}
			if bytes.Equal(cv.Family, _familyExpire) &&
				bytes.Equal(cv.Qualifier, _columnExpire) {

				deleteaftertime = int64(binary.BigEndian.Uint64(cv.Value))
			}
		}

		item := new(meta.ExpireItem)
		item.Key = string(tr.Row)
		item.Expire = deleteaftertime

		el.Items = append(el.Items, *item)
	}
	ScanHLPut(scanId)

	return el, nil
}

// Put file expire into hbase expire table
func (h *HBaseClient) PutExpire(filename string, expire int64) (err error) {
	var (
		ks       []byte
		exist    bool
		existErr error
		c        *hbasethrift.THBaseServiceClient
	)

	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}

	ks = []byte(filename)
	ebuf := make([]byte, 8)
	binary.BigEndian.PutUint64(ebuf, uint64(expire))
	err = c.Put([]byte(_prefix_expire), &hbasethrift.TPut{
		Row: ks,
		ColumnValues: []*hbasethrift.TColumnValue{
			&hbasethrift.TColumnValue{
				Family:    _familyExpire,
				Qualifier: _columnExpire,
				Value:     ebuf,
			},
		},
	})

	if err != nil {
		log.Errorf("hbasePool updatefileexp (%s) failed(%v)!", filename, err)

		if exist, existErr = h.isBucketExist([]byte(_prefix_expire)); existErr == nil && !exist {
			log.Errorf("hbasePool updatefileexp bucket no exist")
			hbasePool.Put(c, false)
			err = errors.ErrSrcBucketNoExist
			return
		}

		hbasePool.Put(c, true)
		return
	}

	hbasePool.Put(c, false)

	return
}

// delete expire in hbase expire table
func (h *HBaseClient) DeleteExpire(filename string) (err error) {
	var (
		ks []byte
		c  *hbasethrift.THBaseServiceClient
	)
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}
	ks = []byte(filename)
	if err = c.DeleteSingle([]byte(_prefix_expire), &hbasethrift.TDelete{
		Row:        ks,
		DeleteType: hbasethrift.TDeleteType_DELETE_COLUMNS,
		Columns: []*hbasethrift.TColumn{
			&hbasethrift.TColumn{
				Family:    _familyExpire,
				Qualifier: _columnExpire,
			},
		},
	}); err != nil {
		hbasePool.Put(c, true)
		return
	}

	hbasePool.Put(c, false)
	return
}

// UpdataNeedle
func (h *HBaseClient) UpdataNeedle(n *meta.Needle) (err error) {
	err = h.updataNeedleAll(n)
	return
}

// Copy
func (h *HBaseClient) Copy(destbucket string, f *meta.File, n *meta.Needle) (err error) {
	if err = h.putFile(destbucket, f); err != nil {
		return
	}
	if err = h.updataNeedleAll(n); err != nil {
		return
	}
	return
}

// Move
func (h *HBaseClient) Move(bucket string, filename string, destbucket string, f *meta.File) (err error) {
	if err = h.putFile(destbucket, f); err != nil {
		return
	}
	if err = h.delFile(bucket, filename); err != nil {
		return
	}
	return
}

// List file list from hbase
func (h *HBaseClient) List(bucket, limit, prefix, delimiter, marker string) (f *meta.FileList, err error) {
	if f, err = h.getFileList(bucket, limit, prefix, delimiter, marker); err != nil {
		return
	}
	return
}

// List file list destroy file from hbase
func (h *HBaseClient) DestroyList(bucket, limit, prefix, delimiter,
	marker string) (f *meta.FileList, err error) {
	if f, err = h.GetDestroyFileList(bucket, limit, prefix, delimiter, marker); err != nil {
		return
	}
	return
}

// Put put file and needle into hbase
func (h *HBaseClient) Put(bucket string, f *meta.File, n *meta.Needle) (err error) {

	if err = h.putFile(bucket, f); err != nil {
		log.Errorf("Put putfile error bucket(%s)  filename(%v)", bucket, f.Filename)
		return
	}
	if err = h.putNeedle(n); err != nil && err != errors.ErrNeedleExist {
		log.Errorf("table not match: bucket: %s  filename: %s", bucket, f.Filename)
		h.delFile(bucket, f.Filename)
	}
	return
}

// PutMkblk put file and needle into hbase
func (h *HBaseClient) PutMkblk(bucket string, f *meta.File, id string, n *meta.Needle) (err error) {
	if err = h.putMkblkFile(bucket, f, id); err != nil {
		log.Errorf("PutMkblk() error(%v)\n", err)
		return
	}
	if err = h.putNeedle(n); err != errors.ErrNeedleExist && err != nil {
		log.Warningf("table not match: bucket: %s  filename: %s", bucket, f.Filename)
		if err = h.delTmpFile(bucket, h.setTmpFileName(bucket, f.Filename, id)); err != nil {
			return
		}
	}
	return
}

// PutBput put file and needle into hbase
func (h *HBaseClient) PutBput(bucket string, f *meta.File, ctx string, id string, offset int64, n *meta.Needle) (retoffset int64, err error) {
	var (
		ft      *meta.File
		file    = new(meta.File)
		bsize   int64
		oldsize int64
		newsize int64
		ctxi    int64
	)

	if ft, err = h.getTmpFile(h.setTmpFileName(bucket, f.Filename, id)); err != nil {
		log.Errorf("gettmpfile failed bucket= %s,filename=%s,id=%s", bucket, f.Filename, id)
		return
	}
	if ctxi, err = strconv.ParseInt(ctx, 10, 64); err != nil {
		log.Errorf("parseint failed ctx=%s", ctx)
		return
	}
	if bsize, err = strconv.ParseInt(f.Filesize, 10, 64); err != nil {
		log.Errorf("parseint failed f.Filesize= %s", f.Filesize)
		return
	}
	if oldsize, err = strconv.ParseInt(ft.Filesize, 10, 64); err != nil {
		log.Errorf("parseint failed ft.filesize = %s", ft.Filesize)
		return
	}

	//must series upload
	if offset > oldsize {
		log.Errorf("Directory MkFile offset=%d > oldsize=%d, Failed", offset, oldsize)
		err = errors.ErrParameterFailed
		return
	}
	if ctxi != ft.Key[offset/_block_size-1] {
		log.Errorf("Directory MkFile ctx != hbase ctx, Failed")
		err = errors.ErrParameterFailed
		return
	}

	// a new slice
	if offset == oldsize {
		newsize = oldsize + bsize
		file.Filesize = strconv.FormatInt(int64(newsize), 10)

		file.Key = make([]int64, len(ft.Key))
		copy(file.Key, ft.Key)
		file.Key = append(file.Key, f.Key[0])

		sha := sha1.Sum([]byte(ft.Sha1 + f.Sha1))
		file.Sha1 = hex.EncodeToString(sha[:])
	} else { // a pre slice
		newsize = oldsize
		file.Filesize = strconv.FormatInt(int64(newsize), 10)

		file.Key = make([]int64, len(ft.Key))
		copy(file.Key, ft.Key)

		file.Sha1 = ft.Sha1
	}

	file.Mine = ft.Mine
	file.Filename = f.Filename
	file.Status = f.Status
	file.MTime = f.MTime

	if err = h.putBputFile(bucket, file, id); err != nil {
		log.Errorf("putbputfile failed bucket=%s,file=%v,id=%s", bucket, file, id)
		return
	}
	if err = h.putNeedle(n); err != errors.ErrNeedleExist && err != nil {
		log.Warningf("table not match: bucket: %s  filename: %s", bucket, f.Filename)
		if err = h.delTmpFile(bucket, h.setTmpFileName(bucket, f.Filename, id)); err != nil {
			log.Errorf("del tmpfile failed err(%v)", err)
			return
		}
	}
	retoffset = newsize
	return
}

// PutMkfile put file and needle into hbase
func (h *HBaseClient) PutMkfile(bucket string, f *meta.File) (err error) {
	if err = h.putFile(bucket, f); err != nil {
		log.Errorf("Directory Hbase PutMkfile putfile failed (%s)", err.Error())
		return
	}

	return
}

func (h *HBaseClient) DelMkfileBlocks(bucket string, tfs []*meta.File) {
	for _, tf := range tfs {
		//if err := h.delTmpFile(bucket, h.setTmpFileName(bucket, tf.Filename, tf.Filename)); err != nil {
		if err := h.delTmpFile(bucket, tf.Filename); err != nil {
			log.Recoverf("deltmpFile bucket %s filename %s failed(%v)",
				bucket, tf.Filename, err)
		}
	}
}

// Del del file and needle from hbase
func (h *HBaseClient) Del(bucket, filename string, link int32) (err error) {
	var (
		f *meta.File
	)
	if f, err = h.GetFile(bucket, filename); err != nil {
		return
	}
	if err = h.delFile(bucket, filename); err != nil {
		return
	}
	if link == 0 {
		//slice upload
		for index, _ := range f.Key {
			if err = h.delNeedle(f.Key[index]); err != nil {
				return
			}
		}
	}
	return
}

// Del destroy file and needle from hbase
func (h *HBaseClient) DelDestroy(bucket, filename string, link int32) (err error) {
	var (
		f           *meta.File
		tkeys       []int64
		tkey, fsize int64
	)
	if f, err = h.getDestroyFile(bucket, filename); err != nil {
		return
	}

	if err = h.delDestroyFile(bucket, filename); err != nil {
		return
	}
	if link == 0 {
		if fsize, err = strconv.ParseInt(f.Filesize, 10, 64); err != nil {
			log.Errorf("bucket %s filename %s filesize is invalid", bucket, filename)
			return
		}
		if int(fsize/_block_size) > len(f.Key) {
			for _, tkey = range f.Key {
				tkeys, err = h.Getbigfiletmpkey(tkey)
				if err != nil {
					log.Errorf("get bigfiletmpkey %d failed %v", tkey, err)
					return
				}
				for _, tkey1 := range tkeys {
					if err = h.delNeedle(tkey1); err != nil {
						return
					}
				}
				err = h.Delbigfiletmpkey(tkey)
				if err != nil {
					log.Recoverf("delete bigfiletmpkey %d failed %v", err)
				}

			}
		} else {
			for index, _ := range f.Key {
				if err = h.delNeedle(f.Key[index]); err != nil {
					return
				}
			}
		}

	}
	return
}

// delete file only delete file
func (h *HBaseClient) DelFile(bucket, filename string) (err error) {
	if err = h.delFile(bucket, filename); err != nil {
		return
	}

	return
}

// delete destroy file only delete file
func (h *HBaseClient) DelDestroyFile(bucket, filename string) (err error) {
	if err = h.delDestroyFile(bucket, filename); err != nil {
		return
	}

	return
}

// DelTmpFile del half-baked file meta
func (h *HBaseClient) DelTmpFile(bucket, filename string, id string) (err error) {
	var (
		f *meta.File
	)
	if f, err = h.getTmpFile(h.setTmpFileName(bucket, filename, id)); err != nil {
		return
	}
	if err = h.delTmpFile(bucket, h.setTmpFileName(bucket, filename, id)); err != nil {
		return
	}
	//slice upload
	for index, _ := range f.Key {
		if err = h.delNeedle(f.Key[index]); err != nil {
			return
		}
	}
	return
}

// DelOneSlice del file key meta, and needle meta
func (h *HBaseClient) DelOneSlice(bucket, filename string, id string) (err error) {
	var (
		f    *meta.File
		ukey []int64
		key  int64
	)
	if f, err = h.getTmpFile(h.setTmpFileName(bucket, filename, id)); err != nil {
		return
	}
	if len(f.Key) <= 0 {
		err = errors.ErrNeedleNotExist
		return
	}
	key = f.Key[len(f.Key)-1]
	ukey = make([]int64, len(f.Key)-1)
	ukey = f.Key[:len(f.Key)-1]
	f.Key = ukey
	if err = h.updateTmpFileKey(bucket, f); err != nil {
		return
	}
	if err = h.delNeedle(key); err != nil {
		return
	}
	return
}

func (h *HBaseClient) IsBucketExist(bucket string) (exist bool, err error) {
	exist, err = h.isBucketExist(h.tableName(bucket))
	return
}

func (h *HBaseClient) IsFileExist(bucket, filename string) (exist bool, err error) {
	exist, err = h.isFileExist(h.tableName(bucket), []byte(filename))
	return
}

// getNeedle get meta data from hbase.efsmeta
func (h *HBaseClient) GetNeedle(key int64) (n *meta.Needle, err error) {
	var (
		ks []byte
		c  *hbasethrift.THBaseServiceClient
		r  *hbasethrift.TResult_
		cv *hbasethrift.TColumnValue
	)
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}
	ks = h.key(key)
	if r, err = c.Get(_table, &hbasethrift.TGet{Row: ks}); err != nil {
		hbasePool.Put(c, true)
		return
	}
	hbasePool.Put(c, false)
	if len(r.ColumnValues) == 0 {
		err = errors.ErrNeedleNotExist
		return
	}
	n = new(meta.Needle)
	n.Key = key
	for _, cv = range r.ColumnValues {
		if cv == nil {
			continue
		}
		if bytes.Equal(cv.Family, _familyBasic) {
			if bytes.Equal(cv.Qualifier, _columnVid) {
				n.Vid = int32(binary.BigEndian.Uint32(cv.Value))
			} else if bytes.Equal(cv.Qualifier, _columnCookie) {
				n.Cookie = int32(binary.BigEndian.Uint32(cv.Value))
			} else if bytes.Equal(cv.Qualifier, _columnUpdateTime) {
				n.MTime = int64(binary.BigEndian.Uint64(cv.Value))
			} else if bytes.Equal(cv.Qualifier, _columnLink) {
				n.Link = int32(binary.BigEndian.Uint32(cv.Value))
			}
		}
	}
	return
}

// updataNeedleAll
func (h *HBaseClient) updataNeedleAll(n *meta.Needle) (err error) {
	var (
		ks   []byte
		vbuf = make([]byte, 4)
		cbuf = make([]byte, 4)
		ubuf = make([]byte, 8)
		lbuf = make([]byte, 4)
		c    *hbasethrift.THBaseServiceClient
	)
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}

	ks = h.key(n.Key)
	if _, err = c.Exists(_table, &hbasethrift.TGet{Row: ks}); err != nil {
		hbasePool.Put(c, true)
		return
	}
	binary.BigEndian.PutUint32(vbuf, uint32(n.Vid))
	binary.BigEndian.PutUint32(cbuf, uint32(n.Cookie))
	binary.BigEndian.PutUint64(ubuf, uint64(time.Now().UnixNano()))
	binary.BigEndian.PutUint32(lbuf, uint32(n.Link))

	if err = c.Put(_table, &hbasethrift.TPut{
		Row: ks,
		ColumnValues: []*hbasethrift.TColumnValue{
			&hbasethrift.TColumnValue{
				Family:    _familyBasic,
				Qualifier: _columnVid,
				Value:     vbuf,
			},
			&hbasethrift.TColumnValue{
				Family:    _familyBasic,
				Qualifier: _columnCookie,
				Value:     cbuf,
			},
			&hbasethrift.TColumnValue{
				Family:    _familyBasic,
				Qualifier: _columnUpdateTime,
				Value:     ubuf,
			},
			&hbasethrift.TColumnValue{
				Family:    _familyBasic,
				Qualifier: _columnLink,
				Value:     lbuf,
			},
		},
	}); err != nil {
		hbasePool.Put(c, true)
		return
	}
	hbasePool.Put(c, false)
	return
}

func (h *HBaseClient) Putbigfiletmpkey(tkeys []int64) (err error) {
	var (
		ks   []byte
		kbuf = make([]byte, 8)
		c    *hbasethrift.THBaseServiceClient
	)

	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}
	log.Errorf("put hbase key %d ", tkeys[0])
	ks = h.key(tkeys[0])
	kbuf = types.ByteSlice(tkeys)

	if err = c.Put(_bigfiletmptable, &hbasethrift.TPut{
		Row: ks,
		ColumnValues: []*hbasethrift.TColumnValue{
			&hbasethrift.TColumnValue{
				Family:    _familyFile,
				Qualifier: _columnKey,
				Value:     kbuf,
			},
		},
	}); err != nil {
		hbasePool.Put(c, true)
		return
	}
	hbasePool.Put(c, false)
	return

}

func (h *HBaseClient) Getbigfiletmpkey(tkey int64) (tkeys []int64, err error) {
	var (
		ks []byte
		c  *hbasethrift.THBaseServiceClient
		r  *hbasethrift.TResult_
		cv *hbasethrift.TColumnValue
	)

	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}

	//ks = []byte(fmt.Sprintf("%d", tkey))
	ks = h.key(tkey)
	if r, err = c.Get(_bigfiletmptable, &hbasethrift.TGet{Row: ks}); err != nil {
		hbasePool.Put(c, true)
		return
	}
	hbasePool.Put(c, false)
	if len(r.ColumnValues) == 0 {
		err = errors.ErrNeedleNotExist
		return
	}
	for _, cv = range r.ColumnValues {
		if cv == nil {
			continue
		}
		if bytes.Equal(cv.Family, _familyFile) {
			if bytes.Equal(cv.Qualifier, _columnKey) {
				//slice upload
				//f.Key = int64(binary.BigEndian.Uint64(cv.Value))
				tkeys = types.Slice(cv.Value, reflect.TypeOf([]int64(nil))).([]int64)
			}
		}
	}

	return
}

// putNeedle overwriting is bug,  banned
func (h *HBaseClient) putNeedle(n *meta.Needle) (err error) {
	var (
		ks   []byte
		vbuf = make([]byte, 4)
		cbuf = make([]byte, 4)
		ubuf = make([]byte, 8)
		lbuf = make([]byte, 4)
		//exist bool
		c *hbasethrift.THBaseServiceClient
	)
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}
	ks = h.key(n.Key)
	//如果gosnowflake 不重复id 这里不应该有重复的 key，所以不必要检查
	/*
		if exist, err = c.Exists(_table, &hbasethrift.TGet{Row: ks}); err != nil {
			log.Errorf("Directory hbase table exists failed, table=%s, ks=%d, err=%s", _table, n.Key, err.Error())
			hbasePool.Put(c, true)
			return
		}
		if exist {
			hbasePool.Put(c, false)
			log.Errorf("Directory hbase efsmeta needle key exists")
			return errors.ErrNeedleExist
		}
	*/
	if n.Vid == 0 {
		log.Recoverf("vid is 0 keyid =%d", n.Key)
	}
	binary.BigEndian.PutUint32(vbuf, uint32(n.Vid))
	binary.BigEndian.PutUint32(cbuf, uint32(n.Cookie))
	binary.BigEndian.PutUint64(ubuf, uint64(time.Now().UnixNano()))
	binary.BigEndian.PutUint32(lbuf, uint32(n.Link))
	if err = c.Put(_table, &hbasethrift.TPut{
		Row: ks,
		ColumnValues: []*hbasethrift.TColumnValue{
			&hbasethrift.TColumnValue{
				Family:    _familyBasic,
				Qualifier: _columnVid,
				Value:     vbuf,
			},
			&hbasethrift.TColumnValue{
				Family:    _familyBasic,
				Qualifier: _columnCookie,
				Value:     cbuf,
			},
			&hbasethrift.TColumnValue{
				Family:    _familyBasic,
				Qualifier: _columnUpdateTime,
				Value:     ubuf,
			},
			&hbasethrift.TColumnValue{
				Family:    _familyBasic,
				Qualifier: _columnLink,
				Value:     lbuf,
			},
		},
	}); err != nil {
		log.Errorf("Directory hbase put file failed err=%s", err.Error())
		hbasePool.Put(c, true)
		return
	}
	hbasePool.Put(c, false)
	return
}

// delNeedle delete the hbase.efsmeta colume vid and cookie by the key.
func (h *HBaseClient) delNeedle(key int64) (err error) {
	var (
		ks []byte
		c  *hbasethrift.THBaseServiceClient
	)
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}
	ks = h.key(key)
	if err = c.DeleteSingle(_table, &hbasethrift.TDelete{
		Row: ks,
		Columns: []*hbasethrift.TColumn{
			&hbasethrift.TColumn{
				Family:    _familyBasic,
				Qualifier: _columnVid,
			},
			&hbasethrift.TColumn{
				Family:    _familyBasic,
				Qualifier: _columnCookie,
			},
			&hbasethrift.TColumn{
				Family:    _familyBasic,
				Qualifier: _columnUpdateTime,
			},
			&hbasethrift.TColumn{
				Family:    _familyBasic,
				Qualifier: _columnLink,
			},
		},
	}); err != nil {
		hbasePool.Put(c, true)
		return
	}
	hbasePool.Put(c, false)
	return
}

// getFile get file data from hbase.bucket_xxx.
func (h *HBaseClient) GetFile(bucket, filename string) (f *meta.File, err error) {
	var (
		ks       []byte
		c        *hbasethrift.THBaseServiceClient
		r        *hbasethrift.TResult_
		cv       *hbasethrift.TColumnValue
		exist    bool
		existErr error
	)

	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}

	ks = []byte(filename)
	if r, err = c.Get(h.tableName(bucket), &hbasethrift.TGet{Row: ks}); err != nil {
		log.Errorf("Directory getFile Get bucket(%s),file(%s) Failed (%s)", bucket, filename, err)
		// is bucket no exist
		if exist, existErr = h.isBucketExist(h.tableName(bucket)); existErr == nil && !exist {
			log.Errorf("Directory getFile  TableIsExists no exist")
			err = errors.ErrSrcBucketNoExist
			hbasePool.Put(c, false)
			return
		}

		hbasePool.Put(c, true)
		return
	}
	hbasePool.Put(c, false)
	if len(r.ColumnValues) == 0 {
		//log.Errorf("columnvalues len is 0,bucket = %s filename =%s", bucket, filename)
		err = errors.ErrNeedleNotExist
		return
	}
	f = new(meta.File)
	f.Filename = filename
	for _, cv = range r.ColumnValues {
		if cv == nil {
			continue
		}
		if bytes.Equal(cv.Family, _familyFile) {
			if bytes.Equal(cv.Qualifier, _columnKey) {
				//slice upload
				//f.Key = int64(binary.BigEndian.Uint64(cv.Value))
				f.Key = types.Slice(cv.Value, reflect.TypeOf([]int64(nil))).([]int64)
			} else if bytes.Equal(cv.Qualifier, _columnSha1) {
				f.Sha1 = string(cv.GetValue())
			} else if bytes.Equal(cv.Qualifier, _columnMine) {
				f.Mine = string(cv.GetValue())
			} else if bytes.Equal(cv.Qualifier, _columnStatus) {
				f.Status = int32(binary.BigEndian.Uint32(cv.Value))
			} else if bytes.Equal(cv.Qualifier, _columnUpdateTime) {
				f.MTime = int64(binary.BigEndian.Uint64(cv.Value))
			} else if bytes.Equal(cv.Qualifier, _columnFilesize) {
				f.Filesize = string(cv.GetValue())
			} else if bytes.Equal(cv.Qualifier, _columnDeleteAfterDays) {
				f.DeleteAftertime = int64(binary.BigEndian.Uint64(cv.Value))
			}
		}
	}

	return
}

// is bucket exist
func (h *HBaseClient) isBucketExist(bucket []byte) (exist bool, err error) {
	var (
		c *hbasethrift.THBaseServiceClient
	)

	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("isBucketExist hbasePool.Get() error(%v)", err)
		return
	}
	if exist, err = c.TableIsExists(bucket); err != nil {
		log.Errorf("isBucketExist Failed (%s)", err.Error())
		hbasePool.Put(c, true)
		return
	}

	hbasePool.Put(c, false)
	return
}

// is file exist
func (h *HBaseClient) isFileExist(bucket []byte, filename []byte) (exist bool, err error) {
	var (
		c *hbasethrift.THBaseServiceClient
	)

	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("isFileExist hbasePool.Get() error(%v)", err)
		return
	}

	if exist, err = c.Exists(bucket, &hbasethrift.TGet{Row: filename}); err != nil {
		log.Errorf("isFileExist Exists error(%v)", err)
		hbasePool.Put(c, true)
		return
	}

	hbasePool.Put(c, false)
	return
}

// getFile get Destroy file data from hbase.bucket_xxx.
func (h *HBaseClient) getDestroyFile(bucket, filename string) (f *meta.File, err error) {
	var (
		ks []byte
		c  *hbasethrift.THBaseServiceClient
		r  *hbasethrift.TResult_
		cv *hbasethrift.TColumnValue
	)
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}
	ks = []byte(filename)
	if r, err = c.Get([]byte(bucket), &hbasethrift.TGet{Row: ks}); err != nil {
		log.Errorf("Directory getDestroyFile Get Failed (%s)", err.Error())
		hbasePool.Put(c, true)
		return
	}
	hbasePool.Put(c, false)
	if len(r.ColumnValues) == 0 {
		log.Errorf("Directory getDestroyFile Get no column Failed ")
		err = errors.ErrNeedleNotExist
		return
	}
	f = new(meta.File)
	f.Filename = filename
	for _, cv = range r.ColumnValues {
		if cv == nil {
			continue
		}
		if bytes.Equal(cv.Family, _familyFile) {
			if bytes.Equal(cv.Qualifier, _columnKey) {
				//slice upload
				//f.Key = int64(binary.BigEndian.Uint64(cv.Value))
				f.Key = types.Slice(cv.Value, reflect.TypeOf([]int64(nil))).([]int64)
			} else if bytes.Equal(cv.Qualifier, _columnSha1) {
				f.Sha1 = string(cv.GetValue())
			} else if bytes.Equal(cv.Qualifier, _columnMine) {
				f.Mine = string(cv.GetValue())
			} else if bytes.Equal(cv.Qualifier, _columnStatus) {
				f.Status = int32(binary.BigEndian.Uint32(cv.Value))
			} else if bytes.Equal(cv.Qualifier, _columnUpdateTime) {
				f.MTime = int64(binary.BigEndian.Uint64(cv.Value))
			} else if bytes.Equal(cv.Qualifier, _columnFilesize) {
				f.Filesize = string(cv.GetValue())
			}
		}
	}

	return
}

// getTmpFile get file data from hbase.bucket_xxx.
func (h *HBaseClient) getTmpFile(filename string) (f *meta.File, err error) {
	var (
		ks []byte
		c  *hbasethrift.THBaseServiceClient
		r  *hbasethrift.TResult_
		cv *hbasethrift.TColumnValue
	)
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}
	ks = []byte(filename)
	if r, err = c.Get(h.tableTmpName(), &hbasethrift.TGet{Row: ks}); err != nil {
		hbasePool.Put(c, true)
		return
	}
	hbasePool.Put(c, false)
	if len(r.ColumnValues) == 0 {
		err = errors.ErrNeedleNotExist
		return
	}
	f = new(meta.File)
	f.Filename = filename
	for _, cv = range r.ColumnValues {
		if cv == nil {
			continue
		}
		if bytes.Equal(cv.Family, _familyFile) {
			if bytes.Equal(cv.Qualifier, _columnKey) {
				f.Key = types.Slice(cv.Value, reflect.TypeOf([]int64(nil))).([]int64)
			} else if bytes.Equal(cv.Qualifier, _columnSha1) {
				f.Sha1 = string(cv.GetValue())
			} else if bytes.Equal(cv.Qualifier, _columnMine) {
				f.Mine = string(cv.GetValue())
			} else if bytes.Equal(cv.Qualifier, _columnStatus) {
				f.Status = int32(binary.BigEndian.Uint32(cv.Value))
			} else if bytes.Equal(cv.Qualifier, _columnUpdateTime) {
				f.MTime = int64(binary.BigEndian.Uint64(cv.Value))
			} else if bytes.Equal(cv.Qualifier, _columnFilesize) {
				f.Filesize = string(cv.GetValue())
			}
		}
	}

	return
}

// getFileList get file data from hbase.bucket_xxx.
func (h *HBaseClient) getFileList(bucket, limit, prefix, delimiter, marker string) (f *meta.FileList, err error) {
	var (
		c         *hbasethrift.THBaseServiceClient
		r         []*hbasethrift.TResult_
		tr        *hbasethrift.TResult_
		cv        *hbasethrift.TColumnValue
		scan      *hbasethrift.TScan
		scanId    int32
		retMarker string
		rlimit    int
		fl        *meta.FileList
		exist     bool
		existErr  error
		filterstr string
	)
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}

	//( RowFilter (=, 'regexstring:^%s[^%s]*%s$') )   prefix, delimiter, delimiter
	//1 "( RowFilter (=, 'regexstring:^%s[^%s]*%s?[^%s]*%s$') ) OR ( RowFilter (=, 'regexstring:^%s[^%s]*%s{1}[^%s]+$') )",
	//2 "( RowFilter (=, 'regexstring:^%s[^%s]*%s$') ) OR ( RowFilter (=, 'regexstring:^[^%s]+$') )",
	scan = new(hbasethrift.TScan)
	if delimiter != "" {
		if prefix != "" {
			filterstr = fmt.Sprintf(
				"( RowFilter (=, 'regexstring:^%s[^(%s)]*((%s)([^(%s)]+))*%s$') ) OR ( RowFilter (=, 'regexstring:^%s[^%s]*%s{1}[^%s]+$') )",
				prefix, delimiter, delimiter, delimiter, delimiter, prefix, delimiter, delimiter, delimiter)
		} else {
			filterstr = fmt.Sprintf(
				"( RowFilter (=, 'regexstring:^%s[^(%s)]*%s$') ) OR ( RowFilter (=, 'regexstring:^[^%s]+$') )",
				prefix, delimiter, delimiter, delimiter)
		}
	} else {
		filterstr = fmt.Sprintf("PrefixFilter ('%s')", prefix)
	}
	scan.FilterString = []byte(filterstr)
	scan.Columns = make([]*hbasethrift.TColumn, 0)
	scan.Columns = append(scan.Columns,
		&hbasethrift.TColumn{
			Family:    _familyFile,
			Qualifier: _columnKey,
		},
		&hbasethrift.TColumn{
			Family:    _familyFile,
			Qualifier: _columnSha1,
		},
		&hbasethrift.TColumn{
			Family:    _familyFile,
			Qualifier: _columnMine,
		},
		&hbasethrift.TColumn{
			Family:    _familyFile,
			Qualifier: _columnStatus,
		},
		&hbasethrift.TColumn{
			Family:    _familyFile,
			Qualifier: _columnUpdateTime,
		},
		&hbasethrift.TColumn{ // add filesize
			Family:    _familyFile,
			Qualifier: _columnFilesize,
		},
		&hbasethrift.TColumn{ // add filesize
			Family:    _familyFile,
			Qualifier: _columnDeleteAfterDays,
		},
	)

	if marker != "" {
		var (
			m_b []byte
			ok  error
		)
		if m_b, ok = base64.URLEncoding.DecodeString(marker); ok != nil {
			log.Errorf("getFileList Parser marker (%s) Failed", marker)
			hbasePool.Put(c, true)
			return
		}
		s_t := strings.Split(string(m_b), ":")
		var tmp int
		if tmp, err = strconv.Atoi(s_t[0]); err != nil {
			log.Errorf("getFileList Parser marker (%s) Failed", marker)
			hbasePool.Put(c, true)
			return
		}
		scanId = int32(tmp)
		ScanHLUpdata(scanId)
	} else {
		if scanId, err = c.OpenScanner(h.tableName(bucket), scan); err != nil {
			log.Errorf("getFileList OpenScanner (%s) Failed: err=%v", bucket, err)
			// is bucket no exist
			if exist, existErr = h.isBucketExist(h.tableName(bucket)); existErr == nil && !exist {
				log.Errorf("getFileList OpenScanner  TableIsExists no exist")
				err = errors.ErrSrcBucketNoExist
				hbasePool.Put(c, false)
				return
			}

			hbasePool.Put(c, true)
			return
		}
	}

	if limit == "" {
		rlimit = 5
	} else {
		rlimit, _ = strconv.Atoi(limit)
	}
	if r, err = c.GetScannerRows(scanId, int32(rlimit)); err != nil {
		log.Errorf("getFileList GetScannerRows scanId:%d Failed %d err:%s \n", scanId, rlimit, err.Error())
		hbasePool.Put(c, true)
		return
	}

	hbasePool.Put(c, false)
	if len(r) < rlimit {
		retMarker = ""
	} else {
		retMarker = base64.URLEncoding.EncodeToString([]byte(fmt.Sprintf("%d:%d", scanId, time.Now().Unix())))
	}
	fl = new(meta.FileList)
	fl.CommonPrefixes = make([]string, 0)
	fl.Items = make([]meta.Item, 0)
	if len(r) == 0 {
		log.Infof("getFileList GetScannerRows no list")
		return fl, nil
	}

	fl.Marker = retMarker
	for _, tr = range r {
		var (
			//slice upload
			key             []int64
			sha1            string
			mime            string
			mtime           int64
			filesize        string
			deleteaftertime int64
		)
		for _, cv = range tr.ColumnValues {
			if cv == nil {
				continue
			}
			if bytes.Equal(cv.Family, _familyFile) {
				if bytes.Equal(cv.Qualifier, _columnKey) {
					//slice upload
					//key = int64(binary.BigEndian.Uint64(cv.Value))
					key = types.Slice(cv.Value, reflect.TypeOf([]int64(nil))).([]int64)
				} else if bytes.Equal(cv.Qualifier, _columnSha1) {
					sha1 = string(cv.GetValue())
				} else if bytes.Equal(cv.Qualifier, _columnMine) {
					mime = string(cv.GetValue())
				} else if bytes.Equal(cv.Qualifier, _columnUpdateTime) {
					mtime = int64(binary.BigEndian.Uint64(cv.Value))
				} else if bytes.Equal(cv.Qualifier, _columnFilesize) {
					filesize = string(cv.GetValue())
				} else if bytes.Equal(cv.Qualifier, _columnDeleteAfterDays) {
					deleteaftertime = int64(binary.BigEndian.Uint64(cv.Value))
				}
			}
		}

		if delimiter != "" && string(tr.Row[len(tr.Row)-len(delimiter):]) == delimiter {
			fl.CommonPrefixes = append(fl.CommonPrefixes, string(tr.Row))
		} else {
			if deleteaftertime != 0 && time.Now().Unix() > deleteaftertime {
				log.Infof("deleteaftertime timeout continue") // goto 补上continue 的个数
				continue
			}
			item := new(meta.Item)
			item.Key = string(tr.Row)
			item.PutTime = mtime
			item.Hash = sha1
			item.MimeType = mime
			//slice upload
			item.Customer = fmt.Sprintf("%d", key[0])
			item.Fsize, _ = strconv.ParseInt(filesize, 10, 64)

			fl.Items = append(fl.Items, *item)
		}
	}
	ScanHLPut(scanId)
	return fl, nil
}

// getDestroyFileList get file data from hbase.bucket_xxx.
func (h *HBaseClient) GetDestroyFileList(bucket, limit, prefix, delimiter,
	marker string) (f *meta.FileList, err error) {
	var (
		c         *hbasethrift.THBaseServiceClient
		r         []*hbasethrift.TResult_
		tr        *hbasethrift.TResult_
		cv        *hbasethrift.TColumnValue
		scan      *hbasethrift.TScan
		scanId    int32
		retMarker string
		rlimit    int
		index     int
		fl        *meta.FileList
	)
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}

	scan = new(hbasethrift.TScan)
	//filterstr := fmt.Sprintf("(PrefixFilter('%s')", prefix)
	//scan.FilterString = []byte(filterstr)

	scan.Columns = make([]*hbasethrift.TColumn, 0)
	scan.Columns = append(scan.Columns,
		&hbasethrift.TColumn{
			Family:    _familyFile,
			Qualifier: _columnKey,
		},
		&hbasethrift.TColumn{
			Family:    _familyFile,
			Qualifier: _columnSha1,
		},
		&hbasethrift.TColumn{
			Family:    _familyFile,
			Qualifier: _columnMine,
		},
		&hbasethrift.TColumn{
			Family:    _familyFile,
			Qualifier: _columnStatus,
		},
		&hbasethrift.TColumn{
			Family:    _familyFile,
			Qualifier: _columnUpdateTime,
		},
		&hbasethrift.TColumn{ // add filesize
			Family:    _familyFile,
			Qualifier: _columnFilesize,
		},
		&hbasethrift.TColumn{ // add filesize
			Family:    _familyFile,
			Qualifier: _columnDeleteAfterDays,
		},
	)

	if marker != "" {
		var (
			m_b []byte
			ok  error
		)
		if m_b, ok = base64.URLEncoding.DecodeString(marker); ok != nil {
			log.Errorf("getFileList Parser marker (%s) Failed", marker)
			hbasePool.Put(c, true)
			return
		}
		s_t := strings.Split(string(m_b), ":")
		var tmp int
		if tmp, err = strconv.Atoi(s_t[0]); err != nil {
			log.Errorf("getFileList Parser marker (%s) Failed", marker)
			hbasePool.Put(c, true)
			return
		}
		scanId = int32(tmp)
		ScanHLUpdata(scanId)
	} else {
		if scanId, err = c.OpenScanner([]byte(bucket), scan); err != nil {
			log.Errorf("getFileList OpenScanner (%s) Failed: err=%v", bucket, err)
			hbasePool.Put(c, true)
			return
		}
	}
	retMarker = base64.URLEncoding.EncodeToString([]byte(fmt.Sprintf("%d:%d", scanId, time.Now().Unix())))
	if limit == "" {
		rlimit = 5
	} else {
		rlimit, _ = strconv.Atoi(limit)
	}
	if r, err = c.GetScannerRows(scanId, int32(rlimit)); err != nil {
		log.Errorf("getFileList GetScannerRows Failed %d", rlimit)
		hbasePool.Put(c, true)
		return
	}

	hbasePool.Put(c, false)

	fl = new(meta.FileList)
	fl.CommonPrefixes = make([]string, 0)
	fl.Items = make([]meta.Item, len(r))
	if len(r) == 0 {
		log.Infof("getFileList GetScannerRows no list")
		//	log.Errorf("get scanner rows no list")
		return fl, nil
	}

	fl.Marker = retMarker
	for index, tr = range r {
		var (
			//slice upload
			key             []int64
			sha1            string
			mime            string
			mtime           int64
			filesize        string
			deleteaftertime int64
		)
		for _, cv = range tr.ColumnValues {
			if cv == nil {
				log.Errorf("list cv is nil")
				continue
			}
			if bytes.Equal(cv.Family, _familyFile) {
				if bytes.Equal(cv.Qualifier, _columnKey) {
					//slice upload
					//key = int64(binary.BigEndian.Uint64(cv.Value))
					key = types.Slice(cv.Value, reflect.TypeOf([]int64(nil))).([]int64)
				} else if bytes.Equal(cv.Qualifier, _columnSha1) {
					sha1 = string(cv.GetValue())
				} else if bytes.Equal(cv.Qualifier, _columnMine) {
					mime = string(cv.GetValue())
				} else if bytes.Equal(cv.Qualifier, _columnUpdateTime) {
					mtime = int64(binary.BigEndian.Uint64(cv.Value))
				} else if bytes.Equal(cv.Qualifier, _columnFilesize) {
					filesize = string(cv.GetValue())
				} else if bytes.Equal(cv.Qualifier, _columnDeleteAfterDays) {
					deleteaftertime = int64(binary.BigEndian.Uint64(cv.Value))
				}
			}
		}
		//log.Errorf("bucket %s filename %s keylen %d", bucket, string(tr.Row), len(key))
		item := new(meta.Item)
		item.Key = string(tr.Row)
		item.Keys = key
		item.PutTime = mtime
		item.Hash = sha1
		item.MimeType = mime
		//slice upload
		if len(key) == 0 {
			log.Recoverf("bucket %s filename %s key len is 0", bucket, item.Key)
			continue
		}
		item.Customer = fmt.Sprintf("%d", key[0])
		item.Fsize, _ = strconv.ParseInt(filesize, 10, 64)
		if bucket != "_prefix_tmp" {
			var (
				tkeys, tkeys1 []int64
				tkey          int64
			)
			//log.Errorf("fsize %d filesize %d lenkey %d", item.Fsize, item.Fsize/_block_size, len(key))
			if int(item.Fsize/_block_size) > len(key) {
				for _, tkey = range key {
					tkeys1, err = h.Getbigfiletmpkey(tkey)
					if err != nil {
						log.Recoverf("file %s get bigfiletmpkey %d failed %v", item.Key, tkey, err)
						continue
					}
					for _, tkey = range tkeys1 {
						tkeys = append(tkeys, tkey)
					}

				}
				item.Keys = tkeys
			}
		}
		item.Deleteaftertime = deleteaftertime
		//log.Errorf("1111111bucket %s filename %s keylen %d", bucket, item.Key, len(item.Keys))
		fl.Items[index] = *item

	}
	ScanHLPut(scanId)
	return fl, nil
}

// updateFileMime
func (h *HBaseClient) updateFileMime(bucket string, f *meta.File) (err error) {
	var (
		ks       []byte
		exist    bool
		existErr error
		c        *hbasethrift.THBaseServiceClient
	)

	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}

	ks = []byte(f.Filename)
	if exist, err = h.isFileExist(h.tableName(bucket), ks); err == nil && !exist {
		log.Errorf("hbasePool updatefimemime file not exist!")
		hbasePool.Put(c, false)
		err = errors.ErrNeedleNotExist
		return
	}
	err = c.Put(h.tableName(bucket), &hbasethrift.TPut{
		Row: ks,
		ColumnValues: []*hbasethrift.TColumnValue{
			&hbasethrift.TColumnValue{
				Family:    _familyFile,
				Qualifier: _columnMine,
				Value:     []byte(f.Mine),
			},
		},
	})

	if err != nil {
		log.Errorf("hbasePool updatefimemime (%s) failed(%v)!", f.Filename, err)

		if exist, existErr = h.isBucketExist(h.tableName(bucket)); existErr == nil && !exist {
			log.Errorf("hbasePool updatefimemime bucket no exist")
			hbasePool.Put(c, false)
			err = errors.ErrSrcBucketNoExist
			return
		}

		hbasePool.Put(c, true)
		return
	}

	hbasePool.Put(c, false)
	return
}

// updateFileExp
func (h *HBaseClient) updateFileExp(bucket string, f *meta.File) (err error) {
	var (
		ks       []byte
		exist    bool
		existErr error
		c        *hbasethrift.THBaseServiceClient
	)

	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}

	ks = []byte(f.Filename)
	if exist, err = h.isFileExist(h.tableName(bucket), ks); err == nil && !exist {
		log.Errorf("hbasePool updatefile expire file not exist!")
		hbasePool.Put(c, false)
		err = errors.ErrNeedleNotExist
		return
	}

	ebuf := make([]byte, 8)
	binary.BigEndian.PutUint64(ebuf, uint64(f.DeleteAftertime))
	err = c.Put(h.tableName(bucket), &hbasethrift.TPut{
		Row: ks,
		ColumnValues: []*hbasethrift.TColumnValue{
			&hbasethrift.TColumnValue{
				Family:    _familyFile,
				Qualifier: _columnDeleteAfterDays,
				Value:     ebuf,
			},
		},
	})

	if err != nil {
		log.Errorf("hbasePool updatefileexp (%s) failed(%v)!", f.Filename, err)

		if exist, existErr = h.isBucketExist(h.tableName(bucket)); existErr == nil && !exist {
			log.Errorf("hbasePool updatefileexp bucket no exist")
			hbasePool.Put(c, false)
			err = errors.ErrSrcBucketNoExist
			return
		}

		hbasePool.Put(c, true)
		return
	}

	hbasePool.Put(c, false)
	return
}

// updateTmpFileKey
func (h *HBaseClient) updateTmpFileKey(bucket string, f *meta.File) (err error) {
	var (
		ks    []byte
		exist bool
		c     *hbasethrift.THBaseServiceClient
		kbuf  = make([]byte, 8)
	)

	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}
	ks = []byte(f.Filename)
	if exist, err = c.Exists(h.tableName(bucket), &hbasethrift.TGet{Row: ks}); err != nil {
		hbasePool.Put(c, true)
		return
	}
	if exist == false {
		log.Errorf("hbasePool updatefimemime file not exist!")
		hbasePool.Put(c, false)
		return errors.ErrNeedleNotExist
	}
	kbuf = types.ByteSlice(f.Key)
	err = c.Put(h.tableName(bucket), &hbasethrift.TPut{
		Row: ks,
		ColumnValues: []*hbasethrift.TColumnValue{
			&hbasethrift.TColumnValue{
				Family:    _familyFile,
				Qualifier: _columnKey,
				Value:     kbuf,
			},
		},
	})

	if err != nil {
		log.Errorf("hbasePool updateTmpfimeKey (%s) failed!", f.Filename)
		hbasePool.Put(c, true)
		return
	}
	hbasePool.Put(c, true)
	return
}

// fileToTrash send exist file to trash
func (h *HBaseClient) FileToTrash(bucket string, f *meta.File, flag int) (err error) {
	var (
		file *meta.File
	)
	if flag == _flag_tmp_file {
		if file, err = h.getTmpFile(h.setTmpFileName(bucket, f.Filename, strconv.FormatInt(f.Key[0], 10))); err != nil {
			log.Errorf("Directory Hbase fileToTrash getfile failed (%s)", err.Error())
			return
		}
	} else {
		if file, err = h.GetFile(bucket, f.Filename); err != nil {
			log.Errorf("Directory Hbase fileToTrash getfile failed bucket:%s,file:%v,err:%s",
				bucket, f, err.Error())
			return
		}
	}
	file.Filename = h.setTrashFileName(bucket, f.Filename, strconv.FormatInt(file.Key[0], 10))
	if err = h.putTrashFile(_prefix_trash, file); err != nil {
		log.Errorf("Directory Hbase fileToTrash putfile failed")
		return
	}
	if flag == _flag_tmp_file {
		if err = h.delTmpFile(_prefix_tmp, f.Filename); err != nil {
			// file mv to trash and delte file failed
			log.Recoverf("Directory Hbase fileToTrash delFile failed error(%v), bucket=%s,filename=%s", err, _prefix_tmp, f.Filename)
			return
		}
	} else {
		if err = h.delFile(bucket, f.Filename); err != nil {
			// file mv to trash and delte file failed
			log.Recoverf("Directory Hbase fileToTrash delFile failed error(%v),bucket=%s,filename=%s", err, bucket, f.Filename)
			return
		}
	}

	return
}

// putFile overwriting is bug,  banned
func (h *HBaseClient) putFile(bucket string, f *meta.File) (err error) {
	var (
		ks    []byte
		kbuf  = make([]byte, 8)
		stbuf = make([]byte, 4)
		ubuf  = make([]byte, 8)
		dbuf  = make([]byte, 8)
		c     *hbasethrift.THBaseServiceClient
	)
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}

	ks = []byte(f.Filename)
	//谁调用，谁自己判断文件是否存在
	/*
		if exist, err = c.Exists(h.tableName(bucket), &hbasethrift.TGet{Row: ks}); err != nil {
			log.Errorf("Directory hbase table exists failed, table=%s, ks=%d, err=%s", _table, n.Key, err.Error())
			hbasePool.Put(c, true)
			return
		}
		if exist {
			hbasePool.Put(c, false)
			log.Errorf("Directory hbase efsmeta needle key exists")
			err = errors.ErrNeedleExist
			return
		}
	*/
	kbuf = types.ByteSlice(f.Key)
	binary.BigEndian.PutUint32(stbuf, uint32(f.Status))
	binary.BigEndian.PutUint64(ubuf, uint64(time.Now().UnixNano()))
	binary.BigEndian.PutUint64(dbuf, uint64(f.DeleteAftertime))

	if err = c.Put(h.tableName(bucket), &hbasethrift.TPut{
		Row: ks,
		ColumnValues: []*hbasethrift.TColumnValue{
			&hbasethrift.TColumnValue{
				Family:    _familyFile,
				Qualifier: _columnKey,
				Value:     kbuf,
			},
			&hbasethrift.TColumnValue{
				Family:    _familyFile,
				Qualifier: _columnSha1,
				Value:     []byte(f.Sha1),
			},
			&hbasethrift.TColumnValue{
				Family:    _familyFile,
				Qualifier: _columnMine,
				Value:     []byte(f.Mine),
			},
			&hbasethrift.TColumnValue{
				Family:    _familyFile,
				Qualifier: _columnStatus,
				Value:     stbuf,
			},
			&hbasethrift.TColumnValue{
				Family:    _familyFile,
				Qualifier: _columnUpdateTime,
				Value:     ubuf,
			},
			&hbasethrift.TColumnValue{ // add filesize
				Family:    _familyFile,
				Qualifier: _columnFilesize,
				Value:     []byte(f.Filesize),
			},
			&hbasethrift.TColumnValue{ // add DeleteAfterDays
				Family:    _familyFile,
				Qualifier: _columnDeleteAfterDays,
				Value:     dbuf,
			},
		},
	}); err != nil {
		hbasePool.Put(c, true)
		return
	}
	hbasePool.Put(c, false)
	return
}

// putTrashFile overwriting is bug,  banned
func (h *HBaseClient) putTrashFile(bucket string, f *meta.File) (err error) {
	var (
		ks    []byte
		kbuf  = make([]byte, 8)
		stbuf = make([]byte, 4)
		ubuf  = make([]byte, 8)
		//exist bool
		c *hbasethrift.THBaseServiceClient
	)
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}
	/*
		ks = []byte(f.Filename)
		if exist, err = c.Exists(h.tableTrashName(), &hbasethrift.TGet{Row: ks}); err != nil {
			hbasePool.Put(c, true)
			return
		}
		if exist {


				if err = h.FileToTrash(bucket, f, _flag_trash_file); err != nil {
					hbasePool.Put(c, err != nil)
					return errors.ErrNeedleExist
				}

			//if trash have this file , do file
			ks = []byte(f.Filename + f.Key)
		}
	*/
	//to trash table row = filename + "-" + key
	ks = []byte(f.Filename) // + "-" + fmt.Sprintf("%ld", f.Key))
	kbuf = types.ByteSlice(f.Key)
	binary.BigEndian.PutUint32(stbuf, uint32(f.Status))
	binary.BigEndian.PutUint64(ubuf, uint64(time.Now().UnixNano()))

	if err = c.Put(h.tableTrashName(), &hbasethrift.TPut{
		Row: ks,
		ColumnValues: []*hbasethrift.TColumnValue{
			&hbasethrift.TColumnValue{
				Family:    _familyFile,
				Qualifier: _columnKey,
				Value:     kbuf,
			},
			&hbasethrift.TColumnValue{
				Family:    _familyFile,
				Qualifier: _columnSha1,
				Value:     []byte(f.Sha1),
			},
			&hbasethrift.TColumnValue{
				Family:    _familyFile,
				Qualifier: _columnMine,
				Value:     []byte(f.Mine),
			},
			&hbasethrift.TColumnValue{
				Family:    _familyFile,
				Qualifier: _columnStatus,
				Value:     stbuf,
			},
			&hbasethrift.TColumnValue{
				Family:    _familyFile,
				Qualifier: _columnUpdateTime,
				Value:     ubuf,
			},
			&hbasethrift.TColumnValue{ // add filesize
				Family:    _familyFile,
				Qualifier: _columnFilesize,
				Value:     []byte(f.Filesize),
			},
		},
	}); err != nil {
		hbasePool.Put(c, true)
		return
	}
	hbasePool.Put(c, false)
	return
}

// putMkblkFile overwriting is bug,  banned
func (h *HBaseClient) putMkblkFile(bucket string, f *meta.File, id string) (err error) {
	var (
		ks    []byte
		kbuf  = make([]byte, 8)
		stbuf = make([]byte, 4)
		ubuf  = make([]byte, 8)
		c     *hbasethrift.THBaseServiceClient
	)
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}

	ks = []byte(h.setTmpFileName(bucket, f.Filename, id))
	kbuf = types.ByteSlice(f.Key)
	binary.BigEndian.PutUint32(stbuf, uint32(f.Status))
	binary.BigEndian.PutUint64(ubuf, uint64(time.Now().UnixNano()))
	if err = c.Put(h.tableTmpName(), &hbasethrift.TPut{
		Row: ks,
		ColumnValues: []*hbasethrift.TColumnValue{
			&hbasethrift.TColumnValue{
				Family:    _familyFile,
				Qualifier: _columnKey,
				Value:     kbuf,
			},
			&hbasethrift.TColumnValue{
				Family:    _familyFile,
				Qualifier: _columnSha1,
				Value:     []byte(f.Sha1),
			},
			&hbasethrift.TColumnValue{
				Family:    _familyFile,
				Qualifier: _columnMine,
				Value:     []byte(f.Mine),
			},
			&hbasethrift.TColumnValue{
				Family:    _familyFile,
				Qualifier: _columnStatus,
				Value:     stbuf,
			},
			&hbasethrift.TColumnValue{
				Family:    _familyFile,
				Qualifier: _columnUpdateTime,
				Value:     ubuf,
			},
			&hbasethrift.TColumnValue{ // add filesize
				Family:    _familyFile,
				Qualifier: _columnFilesize,
				Value:     []byte(f.Filesize),
			},
		},
	}); err != nil {
		log.Errorf("Directory putMkblkFile  Failed (%s)", err.Error())

		hbasePool.Put(c, true)
		return
	}
	hbasePool.Put(c, false)
	return
}

// putBputFile overwriting is bug,  banned
func (h *HBaseClient) putBputFile(bucket string, f *meta.File, id string) (err error) {
	var (
		ks    []byte
		kbuf  = make([]byte, 8)
		stbuf = make([]byte, 4)
		ubuf  = make([]byte, 8)
		exist bool
		c     *hbasethrift.THBaseServiceClient
	)
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}
	ks = []byte(h.setTmpFileName(bucket, f.Filename, id))
	if exist, err = c.Exists(h.tableTmpName(), &hbasethrift.TGet{Row: ks}); err != nil {
		hbasePool.Put(c, true)
		return
	}
	if !exist {
		hbasePool.Put(c, err != nil)
		return errors.ErrNeedleNotExist
	}
	//slice upload
	//binary.BigEndian.PutUint64(kbuf, uint64(f.Key))
	kbuf = types.ByteSlice(f.Key)
	binary.BigEndian.PutUint32(stbuf, uint32(f.Status))
	binary.BigEndian.PutUint64(ubuf, uint64(time.Now().UnixNano()))

	if err = c.Put(h.tableTmpName(), &hbasethrift.TPut{
		Row: ks,
		ColumnValues: []*hbasethrift.TColumnValue{
			&hbasethrift.TColumnValue{
				Family:    _familyFile,
				Qualifier: _columnKey,
				Value:     kbuf,
			},
			&hbasethrift.TColumnValue{
				Family:    _familyFile,
				Qualifier: _columnSha1,
				Value:     []byte(f.Sha1),
			},
			&hbasethrift.TColumnValue{
				Family:    _familyFile,
				Qualifier: _columnMine,
				Value:     []byte(f.Mine),
			},
			&hbasethrift.TColumnValue{
				Family:    _familyFile,
				Qualifier: _columnStatus,
				Value:     stbuf,
			},
			&hbasethrift.TColumnValue{
				Family:    _familyFile,
				Qualifier: _columnUpdateTime,
				Value:     ubuf,
			},
			&hbasethrift.TColumnValue{ // add filesize
				Family:    _familyFile,
				Qualifier: _columnFilesize,
				Value:     []byte(f.Filesize),
			},
		},
	}); err != nil {
		hbasePool.Put(c, true)
		return
	}
	hbasePool.Put(c, false)
	return
}

// updateFile overwriting is bug,  banned
func (h *HBaseClient) updateFile(c *hbasethrift.THBaseServiceClient, bucket, filename, sha1 string) (err error) {
	var (
		ks   []byte
		ubuf = make([]byte, 8)
	)
	ks = []byte(filename)
	binary.BigEndian.PutUint64(ubuf, uint64(time.Now().UnixNano()))
	err = c.Put(h.tableName(bucket), &hbasethrift.TPut{
		Row: ks,
		ColumnValues: []*hbasethrift.TColumnValue{
			&hbasethrift.TColumnValue{
				Family:    _familyFile,
				Qualifier: _columnSha1,
				Value:     []byte(sha1),
			},
			&hbasethrift.TColumnValue{
				Family:    _familyFile,
				Qualifier: _columnUpdateTime,
				Value:     ubuf,
			},
		},
	})
	return
}

func (h *HBaseClient) Delbigfiletmpkey(tkey int64) (err error) {
	var (
		ks []byte
		c  *hbasethrift.THBaseServiceClient
	)
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}
	//ks = []byte(fmt.Sprintf("%d", tkey))
	ks = h.key(tkey)
	if err = c.DeleteSingle(_bigfiletmptable, &hbasethrift.TDelete{
		Row:        ks,
		DeleteType: hbasethrift.TDeleteType_DELETE_COLUMNS,
		Columns: []*hbasethrift.TColumn{
			&hbasethrift.TColumn{
				Family:    _familyFile,
				Qualifier: _columnKey,
			},
		},
	}); err != nil {
		hbasePool.Put(c, true)
		return
	}
	hbasePool.Put(c, false)
	return
}

// delFile delete file from hbase.bucket_xxx.
func (h *HBaseClient) delFile(bucket, filename string) (err error) {
	var (
		ks []byte
		c  *hbasethrift.THBaseServiceClient
	)
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}
	ks = []byte(filename)
	if err = c.DeleteSingle(h.tableName(bucket), &hbasethrift.TDelete{
		Row:        ks,
		DeleteType: hbasethrift.TDeleteType_DELETE_COLUMNS,
		Columns: []*hbasethrift.TColumn{
			&hbasethrift.TColumn{
				Family:    _familyFile,
				Qualifier: _columnKey,
			},
			&hbasethrift.TColumn{
				Family:    _familyFile,
				Qualifier: _columnSha1,
			},
			&hbasethrift.TColumn{
				Family:    _familyFile,
				Qualifier: _columnMine,
			},
			&hbasethrift.TColumn{
				Family:    _familyFile,
				Qualifier: _columnStatus,
			},
			&hbasethrift.TColumn{
				Family:    _familyFile,
				Qualifier: _columnUpdateTime,
			},
			&hbasethrift.TColumn{
				Family:    _familyFile,
				Qualifier: _columnFilesize,
			},
			&hbasethrift.TColumn{
				Family:    _familyFile,
				Qualifier: _columnDeleteAfterDays,
			},
		},
	}); err != nil {
		hbasePool.Put(c, true)
		return
	}
	hbasePool.Put(c, false)
	return
}

// delFile destroy file from hbase.bucket_xxx.
func (h *HBaseClient) delDestroyFile(bucket, filename string) (err error) {
	var (
		ks []byte
		c  *hbasethrift.THBaseServiceClient
	)
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}
	ks = []byte(filename)
	if err = c.DeleteSingle([]byte(bucket), &hbasethrift.TDelete{
		DeleteType: hbasethrift.TDeleteType_DELETE_COLUMNS,
		Row:        ks,
		Columns: []*hbasethrift.TColumn{
			&hbasethrift.TColumn{
				Family:    _familyFile,
				Qualifier: _columnKey,
			},
			&hbasethrift.TColumn{
				Family:    _familyFile,
				Qualifier: _columnSha1,
			},
			&hbasethrift.TColumn{
				Family:    _familyFile,
				Qualifier: _columnMine,
			},
			&hbasethrift.TColumn{
				Family:    _familyFile,
				Qualifier: _columnStatus,
			},
			&hbasethrift.TColumn{
				Family:    _familyFile,
				Qualifier: _columnUpdateTime,
			},
			&hbasethrift.TColumn{
				Family:    _familyFile,
				Qualifier: _columnFilesize,
			},
		},
	}); err != nil {
		hbasePool.Put(c, true)
		return
	}
	hbasePool.Put(c, false)
	return
}

// delTmpFile delete file from hbase.bucket_xxx.
func (h *HBaseClient) delTmpFile(bucket, filename string) (err error) {
	var (
		ks []byte
		c  *hbasethrift.THBaseServiceClient
	)
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}
	ks = []byte(filename)
	if err = c.DeleteSingle(h.tableTmpName(), &hbasethrift.TDelete{
		Row:        ks,
		DeleteType: hbasethrift.TDeleteType_DELETE_COLUMNS,
	}); err != nil {
		hbasePool.Put(c, true)
		log.Errorf("Directory MkFile del tmpfile failed(%s)", err.Error())
		return
	}
	hbasePool.Put(c, false)
	//log.Errorf("delete filename %s", filename)
	return
}

// key hbase efsmeta
func (h *HBaseClient) key(key int64) []byte {
	var (
		sb [sha1.Size]byte
		b  []byte
	)
	b = make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(key))
	sb = sha1.Sum(b)
	return sb[:]
}

// tableName name of bucket table
func (h *HBaseClient) tableName(bucket string) []byte {
	return []byte(_prefix + bucket)
}

// destroy tableName name of destroy bucket table
func (h *HBaseClient) tableDestroyName(bucket string) []byte {
	return []byte(_prefix_trash + "_" + bucket)
}

// tableName name of bucket delete
func (h *HBaseClient) tableDeleteName(bucket string) []byte {
	return []byte(_prefix_trash + "_" + bucket + fmt.Sprintf("%d", time.Now().Unix()))
}

// tableTmpName name of bucket table
func (h *HBaseClient) tableTmpName() []byte {
	return []byte(_prefix_tmp)
}

// setTmpFileName name of slice tmp file
func (h *HBaseClient) setTmpFileName(bucket string, filename string, id string) (tFilename string) {
	tFilename = bucket + "/" + filename + ":" + id
	return
}

// tableTrashName table name for trash
func (h *HBaseClient) tableTrashName() []byte {
	return []byte(_prefix_trash)
}

// trashFileName set trash file name
func (h *HBaseClient) setTrashFileName(bucket string, filename string, key string) (tFilename string) {
	tFilename = bucket + "/" + filename + "_" + key
	return
}

// create a table
func (h *HBaseClient) CreateTable(table string, families string) (err error) {
	var (
		famil []string
		c     *hbasethrift.THBaseServiceClient
	)

	famil = strings.Split(families, ";")
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}

	if err = c.CreateTable(h.tableName(table), famil); err != nil {
		log.Errorf("Directory CreateTable CreateTable failed")
		hbasePool.Put(c, true)
		return
	}
	hbasePool.Put(c, false)
	return
}

// delete a table
func (h *HBaseClient) DeleteTable(table string) (err error) {
	var (
		c        *hbasethrift.THBaseServiceClient
		exist    bool
		existErr error
	)
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}

	if err = c.RenameTable(h.tableName(table), h.tableDeleteName(table)); err != nil {
		log.Errorf("Directory DeleteTable RenameTable failed(%v)", err)

		if exist, existErr = h.isBucketExist(h.tableName(table)); existErr == nil && !exist {
			err = errors.ErrSrcBucketNoExist
			log.Errorf("Directory DeleteTable  TableIsExists no exist")
			hbasePool.Put(c, false)
			return
		}

		hbasePool.Put(c, true)
		return
	}

	hbasePool.Put(c, false)
	return
}

// destroy a table
func (h *HBaseClient) DestroyTable(table string) (err error) {
	var (
		c *hbasethrift.THBaseServiceClient
	)
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}

	if err = c.DeleteTable([]byte(table)); err != nil {
		log.Errorf("Directory DestroyTable Delete Table failed(%v)", err)
		hbasePool.Put(c, true)
		return
	}
	hbasePool.Put(c, false)
	return
}
func (h *HBaseClient) RenameTable(table_old, table_new string) (err error) {
	var (
		c *hbasethrift.THBaseServiceClient
	)

	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}

	if err = c.RenameTable(h.tableName(table_old), h.tableName(table_new)); err != nil {
		log.Errorf("Directory RenameTable RenameTable failed")
		hbasePool.Put(c, true)
		return
	}
	hbasePool.Put(c, false)
	return
}

func (h *HBaseClient) ListTable(regular string) (list []string, err error) {
	var (
		c *hbasethrift.THBaseServiceClient
	)

	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}
	if list, err = c.ListTableNames(regular); err != nil {
		log.Errorf("Directory ListTable ListTableNames failed, list=%d, err=%s", len(list), err.Error())
		hbasePool.Put(c, true)
		return
	}
	hbasePool.Put(c, false)
	return
}

func (h *HBaseClient) StatTable(table string) (exist bool, err error) {
	var (
		c *hbasethrift.THBaseServiceClient
	)
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}

	if exist, err = c.TableIsExists([]byte(table)); err != nil {
		log.Errorf("Directory TableIsExists  Table failed(%v)", err)
		hbasePool.Put(c, true)
		return
	}

	hbasePool.Put(c, false)
	return
}
