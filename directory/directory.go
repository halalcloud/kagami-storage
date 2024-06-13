package main

import (
	"bytes"
	"crypto/sha1"
	b64 "encoding/base64"
	"encoding/hex"
	"encoding/json"
	"kagamistoreage/directory/conf"
	"kagamistoreage/directory/hbase"
	"kagamistoreage/directory/snowflake"
	myzk "kagamistoreage/directory/zk"
	"kagamistoreage/libs/errors"
	"kagamistoreage/libs/meta"
	"strconv"
	"strings"
	"sync"
	"time"

	log "kagamistoreage/log/glog"

	"github.com/samuel/go-zookeeper/zk"
)

const (
	retrySleep   = time.Second * 1
	ONCE_NEEDS   = 10
	_needle_size = 4 * 1024 * 1024
)

const (
	_prefix       = "bucket_"
	_prefix_tmp   = "bucket_tmp"
	_prefix_trash = "bucket_trash"
)

const (
	_flag_tmp_file   = 1001
	_flag_trash_file = 1002
	_flag_real_file  = 1003
)

// Directory
// id means store serverid; vid means volume id; gid means group id
type Directory struct {
	// STORE
	store       map[string]*meta.Store // store_server_id:store_info
	storeVolume map[string][]int32     // store_server_id:volume_ids

	// GROUP
	storeGroup map[string]int   // store_server_id:group
	group      map[int][]string // group_id:store_servers

	// VOLUME
	volume            map[int32]*meta.VolumeState // volume_id:volume_state
	volumeStore       map[int32][]string          // volume_id:store_server_id
	volumestorestatus map[int32]map[string]int    //volume_id:(storeid:status)

	genkey        *snowflake.Genkey  // snowflake client for gen key
	hBase         *hbase.HBaseClient // hBase client
	dispatcher    *Dispatcher        // dispatch for write or read reqs
	dis_algorithm int                // poling or score

	cache_block      map[string]*meta.File
	cache_block_lock *sync.RWMutex

	config *conf.Config
	zk     *myzk.Zookeeper
}

// NewDirectory
func NewDirectory(config *conf.Config) (d *Directory, err error) {
	d = &Directory{}
	d.config = config
	if d.zk, err = myzk.NewZookeeper(config); err != nil {
		return
	}
	if d.genkey, err = snowflake.NewGenkey(config.Snowflake.ZkAddrs, config.Snowflake.ZkPath, config.Snowflake.ZkTimeout.Duration, config.Snowflake.WorkId); err != nil {
		return
	}
	if err = hbase.Init(config); err != nil {
		return
	}
	d.hBase = hbase.NewHBaseClient()
	d.dispatcher = NewDispatcher()
	d.cache_block = make(map[string]*meta.File)
	d.cache_block_lock = &sync.RWMutex{}
	go d.SyncZookeeper()
	//every 1 hour check timeout 12 hours block, and delete
	go func() {
		for {
			var timeout_blocks []*meta.File
			time.Sleep(time.Hour * 1)
			d.cache_block_lock.RLock()
			for _, tf := range d.cache_block {
				if time.Now().Unix() > tf.MTime+12*3600 {
					timeout_blocks = append(timeout_blocks, tf)
				}
			}
			d.cache_block_lock.RUnlock()
			d.cache_block_lock.Lock()
			for _, tf := range timeout_blocks {
				delete(d.cache_block, tf.Filename)
			}
			d.cache_block_lock.Unlock()
		}

	}()
	return
}

func statislog(method string, bucket, file *string, overwriteflag *int, oldsize *int64, size string, start time.Time, status *int, err *string) {
	if *bucket == "" {
		*bucket = "-"
	}
	if *file == "" {
		*file = "-"
	}
	fname := b64.URLEncoding.EncodeToString([]byte(*file))
	if time.Now().Sub(start).Seconds() > 1.0 {
		log.Statisf("proxymore 1s ============%f", time.Now().Sub(start).Seconds())
	}
	log.Statisf("%s	%s	%s	%d	%d	%s	%f	%d	error(%s)",
		method, *bucket, fname, *overwriteflag, *oldsize, size, time.Now().Sub(start).Seconds(), *status, *err)
}

// Stores get all the store nodes and set a watcher
func (d *Directory) syncStores() (ev <-chan zk.Event, err error) {
	var (
		storeMeta              *meta.Store
		store                  map[string]*meta.Store
		storeVolume            map[string][]int32
		rack, str, volume      string
		racks, stores, volumes []string
		data                   []byte
		vid                    int
	)
	// get all rack
	if racks, ev, err = d.zk.WatchRacks(); err != nil {
		return
	}
	store = make(map[string]*meta.Store)
	storeVolume = make(map[string][]int32)
	for _, rack = range racks {
		// get all stores in the rack
		if stores, err = d.zk.Stores(rack); err != nil {
			return
		}
		for _, str = range stores {
			// get store
			if data, err = d.zk.Store(rack, str); err != nil {
				return
			}
			storeMeta = new(meta.Store)
			if err = json.Unmarshal(data, storeMeta); err != nil {
				log.Errorf("json.Unmarshal() error(%v)", err)
				return
			}
			// get all volumes in the store
			if volumes, err = d.zk.StoreVolumes(rack, str); err != nil {
				return
			}
			storeVolume[storeMeta.Id] = []int32{}
			for _, volume = range volumes {
				if vid, err = strconv.Atoi(volume); err != nil {
					log.Errorf("wrong volume:%s", volume)
					continue
				}
				storeVolume[storeMeta.Id] = append(storeVolume[storeMeta.Id], int32(vid))
			}
			store[storeMeta.Id] = storeMeta
		}
	}
	d.store = store
	d.storeVolume = storeVolume
	return
}

// Volumes get all volumes in zk
func (d *Directory) syncVolumes() (err error) {
	var (
		vid               int
		str               string
		volumes, stores   []string
		data              []byte
		volumeState       *meta.VolumeState
		volume            map[int32]*meta.VolumeState
		volumeStore       map[int32][]string
		volumestorestatus map[int32]map[string]int // vid for echo store vid status
		storestatus       map[string]int
	)
	// get all volumes
	if volumes, err = d.zk.Volumes(); err != nil {
		return
	}
	volume = make(map[int32]*meta.VolumeState)
	volumeStore = make(map[int32][]string)
	volumestorestatus = make(map[int32]map[string]int)
	for _, str = range volumes {
		// get the volume
		if data, err = d.zk.Volume(str); err != nil {
			return
		}
		volumeState = new(meta.VolumeState)
		if err = json.Unmarshal(data, volumeState); err != nil {
			log.Errorf("vid %s json.Unmarshal() error(%v)", str, err)
			return
		}
		if vid, err = strconv.Atoi(str); err != nil {
			log.Errorf("wrong volume:%s", str)
			continue
		}
		volume[int32(vid)] = volumeState
		// get the stores by the volume
		if stores, err = d.zk.VolumeStores(str); err != nil {
			return
		}
		volumeStore[int32(vid)] = stores
		storestatus = make(map[string]int)
		for _, sid := range stores {
			status, err := d.zk.Volumestorestatus(str, sid)
			if err != nil {
				log.Errorf("sync volume store status error(%v)", err)
				status = meta.StoreStatusFail
			}
			storestatus[sid] = status
		}
		if err != nil {
			return
		}
		volumestorestatus[int32(vid)] = storestatus
	}
	d.volume = volume
	d.volumeStore = volumeStore
	d.volumestorestatus = volumestorestatus
	return
}

// syncGroups get all groups and set a watcher.
func (d *Directory) syncGroups() (err error) {
	var (
		gid            int
		str            string
		groups, stores []string
		group          map[int][]string
		storeGroup     map[string]int
	)
	// get all groups
	if groups, err = d.zk.Groups(); err != nil {
		return
	}
	group = make(map[int][]string)
	storeGroup = make(map[string]int)
	for _, str = range groups {
		// get all stores by the group
		if stores, err = d.zk.GroupStores(str); err != nil {
			return
		}
		if gid, err = strconv.Atoi(str); err != nil {
			log.Errorf("wrong group:%s", str)
			continue
		}
		group[gid] = stores
		for _, str = range stores {
			storeGroup[str] = gid
		}
	}
	d.group = group
	d.storeGroup = storeGroup
	return
}

// SyncZookeeper Synchronous zookeeper data to memory
func (d *Directory) SyncZookeeper() {
	var (
		sev <-chan zk.Event
		err error
	)

	d.dis_algorithm, err = d.zk.Initdispatcher()
	if err != nil {
		d.dis_algorithm = meta.Dispatcher_score
		log.Errorf("init dispatcher failed  init score")
	}

	for {
		if sev, err = d.syncStores(); err != nil {
			log.Errorf("syncStores() called error(%v)", err)
			time.Sleep(retrySleep)
			continue
		}
		if err = d.syncGroups(); err != nil {
			log.Errorf("syncGroups() called error(%v)", err)
			time.Sleep(retrySleep)
			continue
		}
		if err = d.syncVolumes(); err != nil {
			log.Errorf("syncVolumes() called error(%v)", err)
			time.Sleep(retrySleep)
			continue
		}
		if err = d.dispatcher.Update(d.group, d.store, d.volume, d.storeVolume, d.volumestorestatus); err != nil {
			log.Errorf("Update() called error(%v)", err)
			time.Sleep(retrySleep)
			continue
		}
		select {
		case <-sev:
			log.Infof("stores status change or new store")
			break
		case <-time.After(d.config.Zookeeper.PullInterval.Duration):
			log.Infof("pull from zk")
			break
		}
	}
}

// TODO move cookie  rand uint16
func (d *Directory) cookie() (cookie int32) {
	return int32(uint16(time.Now().UnixNano())) + 1
}

func (d *Directory) Getneedle(key int64) (n *meta.Needle, addrs []string, err error) {
	var (
		v           *meta.Needle
		store       string
		store_s     meta.StoreS
		svrs        []string
		storeMeta   *meta.Store
		ok          bool
		storestatus map[string]int
		status      int
	)
	v, err = d.hBase.GetNeedle(key)
	if err != nil {
		log.Errorf("get needle key %d failed %v", key, err)
		if err != errors.ErrNeedleNotExist {
			err = errors.ErrHBase
		}
		return
	}

	if svrs, ok = d.volumeStore[v.Vid]; !ok {
		log.Errorf("volume have no this vid %d", v.Vid)
		err = errors.ErrZookeeperDataError
		return
	}

	if storestatus, ok = d.volumestorestatus[v.Vid]; !ok {
		log.Errorf("volume store status have no this vid %d", v.Vid)
		err = errors.ErrZookeeperDataError
		return
	}

	for _, store = range svrs {
		if storeMeta, ok = d.store[store]; !ok {
			log.Errorf("zookeeper have no this store %s", store)
			err = errors.ErrZookeeperDataError
			return
		}

		if status, ok = storestatus[store]; !ok {
			err = errors.ErrZookeeperDataError
			log.Errorf("zookeeper have no storestatus this store %s", store)
			return
		}
		if !d.dispatcher.VolumeCanRead(status) { //filter can not read volume
			log.Errorf("volume bad continue")
			continue
		}

		if !storeMeta.CanRead() {
			log.Errorf("Directory Stat Get Stores(%s) Can not Read\n", storeMeta.Id)
			err = errors.ErrStoreNotAvailable
			continue
		}

		store_s.Stores = append(store_s.Stores, storeMeta.Api)
	}

	if len(store_s.Stores) == 0 {
		log.Errorf("Directory Stat no store can Read vid:%d\n", v.Vid)
		err = errors.ErrStoreNotAvailable
		return
	}
	err = nil
	addrs = store_s.Stores
	n = v
	return

}

// GetStores get readable stores for http get
func (d *Directory) GetStores(bucket, filename string) (n []*meta.Needle, f *meta.File, stores *[]meta.StoreS, err error) {
	var (
		store         string
		svrs          []string
		storeMeta     *meta.Store
		ok            bool
		store_s       []meta.StoreS
		storestatus   map[string]int
		status        int
		fsize, tkey   int64
		tkeys, tkeys1 []int64
	)
	if f, err = d.hBase.Get(bucket, filename); err != nil {
		log.Errorf("hBase.Get error(%v) b:%s f: %s", err, bucket, filename)
		if !(err == errors.ErrNeedleNotExist || err == errors.ErrSrcBucketNoExist) {
			err = errors.ErrHBase
		}
		return
	}

	if fsize, err = strconv.ParseInt(f.Filesize, 10, 64); err != nil {
		log.Errorf("bucket %s filename %s filesize is invalid", bucket, filename)
		return
	}
	if int(fsize/_needle_size) > len(f.Key) {
		for _, tkey = range f.Key {
			tkeys1, err = d.hBase.Getbigfiletmpkey(tkey)
			if err != nil {
				log.Errorf("bucket %s filename %s filesize get bigfiletmpkey %d faild %v", bucket, filename, tkey, err)
				return
			}
			for _, tkey = range tkeys1 {
				tkeys = append(tkeys, tkey)
			}
		}
		f.Key = tkeys
	}

	if f.DeleteAftertime != 0 && time.Now().Unix() > f.DeleteAftertime {
		log.Infof("bucket %s filename %s deletetimeout", bucket, filename)
		err = errors.ErrNeedleNotExist
		return
	}
	if len(f.Key) > ONCE_NEEDS {
		n = make([]*meta.Needle, ONCE_NEEDS)
	} else {
		n = make([]*meta.Needle, len(f.Key))
	}
	for index, _ := range n {
		n[index], err = d.hBase.GetNeedle(f.Key[index])
		if err != nil {
			log.Errorf("bucket %s filename %s get key %d failed %v",
				bucket, filename, f.Key[index], err)
			if err != errors.ErrNeedleNotExist {
				err = errors.ErrHBase
			}
			return
		}
	}

	store_s = make([]meta.StoreS, len(n))
	for index, v := range n {
		if v == nil {
			continue
		}
		if svrs, ok = d.volumeStore[v.Vid]; !ok {
			log.Errorf("volume have no this vid %d", v.Vid)
			err = errors.ErrZookeeperDataError
			return
		}

		if storestatus, ok = d.volumestorestatus[v.Vid]; !ok {
			log.Errorf("volume store status have no this vid %d", v.Vid)
			err = errors.ErrZookeeperDataError
			return
		}

		for _, store = range svrs {
			if storeMeta, ok = d.store[store]; !ok {
				log.Errorf("zookeeper have no this store %s", store)
				err = errors.ErrZookeeperDataError
				return
			}

			if status, ok = storestatus[store]; !ok {
				err = errors.ErrZookeeperDataError
				log.Errorf("zookeeper have no storestatus this store %s", store)
				return
			}
			if !d.dispatcher.VolumeCanRead(status) { //filter can not read volume
				log.Errorf("volume bad continue")
				continue
			}

			if !storeMeta.CanRead() {
				log.Errorf("Directory Stat Get Stores(%s) Can not Read\n", storeMeta.Id)
				err = errors.ErrStoreNotAvailable
				continue
			}

			store_s[index].Stores = append(store_s[index].Stores, storeMeta.Api)
		}

		if len(store_s[index].Stores) == 0 {
			log.Errorf("Directory Stat no store can Read vid:%d\n", v.Vid)
			err = errors.ErrStoreNotAvailable
			return
		}
	}

	stores = &store_s
	if len(*stores) == 0 {
		log.Errorf("Directory Stat Get Stores Failed")
		err = errors.ErrStoreNotAvailable
	} else {
		err = nil
	}
	return
}

// GetStat get
func (d *Directory) GetStat(bucket, filename string) (f *meta.File, err error) {
	if f, err = d.hBase.GetFileStat(bucket, filename); err != nil {
		log.Errorf("hBase.Get error(%v)", err)
		if !(err == errors.ErrNeedleNotExist || err == errors.ErrSrcBucketNoExist) {
			err = errors.ErrHBase
		}
		return
	}

	return
}

// UpdateMetas update metadata for http chgm
func (d *Directory) UpdataMetas(bucket string, f *meta.File) (err error) {

	if err = d.hBase.UpdateMime(bucket, f); err != nil {
		if !(err == errors.ErrNeedleNotExist || err == errors.ErrSrcBucketNoExist) {
			log.Errorf("hBase.UpdataMetas error(%v)", err)
			err = errors.ErrHBase
		}
	}
	return
}

func (d *Directory) getexpfilename(bucket, filename string) string {
	return b64.URLEncoding.EncodeToString([]byte(bucket)) +
		EXPIRE_FILE_DELIMITER +
		b64.URLEncoding.EncodeToString([]byte(filename))
}

// UpdateExp update expire data for http chgexp
func (d *Directory) UpdateExp(bucket string, f *meta.File) (err error) {
	if err = d.hBase.UpdateExp(bucket, f); err != nil {
		if !(err == errors.ErrNeedleNotExist || err == errors.ErrSrcBucketNoExist) {
			log.Errorf("hBase.UpdataExp error(%v)", err)
			err = errors.ErrHBase
		}

		return
	}
	expfilename := d.getexpfilename(bucket, f.Filename)
	if err = d.hBase.PutExpire(expfilename, f.DeleteAftertime); err != nil {
		if !(err == errors.ErrNeedleNotExist || err == errors.ErrSrcBucketNoExist) {
			log.Errorf("hBase.UpdataExp error(%v)", err)
			err = errors.ErrHBase
		}
	}

	return
}

// dispatcher get upload needle(vid)
func (d *Directory) Dispatcher(bucket, filename string, lastVid int32,
	overWriteFlag int, replication int) (n *meta.Needle, stores []string, err error) {
	var (
		key       int64
		vid       int32
		svrs      []string
		store     string
		storeMeta *meta.Store
		ok        bool
		exist     bool
		exclGid   int
		df        *meta.File
	)

	//判断文件是否存在
	df, err = d.hBase.Get(bucket, filename)
	if err == nil && df != nil {
		exist = true
	} else if err != nil && !(err == errors.ErrNeedleNotExist || err == errors.ErrSrcBucketNoExist) {
		err = errors.ErrHBase
		return
	} else {
		exist = false
	}
	if !d.isOverWrite(overWriteFlag) {
		if exist {
			if df.DeleteAftertime != 0 && df.DeleteAftertime < time.Now().Unix() {

			} else {
				err = errors.ErrNeedleExist
				return
			}

		}

	}

	if lastVid < 0 {
		exclGid = -1
	} else {
		tstores := d.volumeStore[int32(lastVid)]
		if len(tstores) == 0 {
			err = errors.ErrZookeeperDataError
			return
		}
		exclGid = d.storeGroup[tstores[0]]
	}

	if vid, err = d.dispatcher.VolumeId(exclGid, d.dis_algorithm, replication); err != nil {
		log.Errorf("dispatcher.VolumeId error(%v)", err)
		err = errors.ErrStoreNotAvailable
		return
	}
	svrs = d.volumeStore[vid]
	stores = make([]string, 0, len(svrs))
	for _, store = range svrs {
		if storeMeta, ok = d.store[store]; !ok {
			err = errors.ErrZookeeperDataError
			return
		}
		stores = append(stores, storeMeta.Api)
	}
	if key, err = d.genkey.Getkey(); err != nil {
		log.Errorf("genkey.Getkey() error(%v)", err)
		err = errors.ErrIdNotAvailable
		return
	}

	n = new(meta.Needle)
	n.Key = key
	n.Vid = vid
	n.Cookie = d.cookie()
	n.Link = 0

	return
}

func (d *Directory) Getkeyid() (key int64, err error) {
	if key, err = d.genkey.Getkey(); err != nil {
		log.Errorf("genkey.Getkey() error(%v)", err)
		err = errors.ErrIdNotAvailable
		return
	}
	return
}

// isOverWrite  is resource over write
func (d *Directory) isOverWrite(overWriteFlag int) bool {
	return overWriteFlag > 0
}

// Upload get writable stores for http upload
func (d *Directory) Upload(bucket string, f *meta.File, n *meta.Needle,
	overWriteFlag, deleteAfterDays int) (oFileSize string, err error) {
	var (
		df            *meta.File
		recover_error error
		filename      string
		exist         bool
	)
	oFileSize = "0"

	filename = f.Filename
	/*
		判断目的文件是否存在
		若覆盖，文件存在：则删除文件
		不覆盖，若文件存在：先判断文件是否过期，过期则删除文件。不过期 则返回文件已经存在
	*/
	df, err = d.hBase.Get(bucket, filename)
	if err == nil && df != nil {
		exist = true
	} else if err != nil && !(err == errors.ErrNeedleNotExist || err == errors.ErrSrcBucketNoExist) {
		err = errors.ErrHBase
		return
	} else {
		exist = false
	}
	if !d.isOverWrite(overWriteFlag) {
		if exist {
			if df.DeleteAftertime != 0 && df.DeleteAftertime < time.Now().Unix() {
				if _, err = d.DelStores(bucket, filename); err != nil && err != errors.ErrNeedleNotExist {
					log.Errorf("delstores bucket:%s, file:%s, err:%s\n", bucket, filename, err)
					return
				}
				oFileSize = df.Filesize
			} else {
				err = errors.ErrNeedleExist
				return
			}

		}

	} else {
		if exist {
			if _, err = d.DelStores(bucket, filename); err != nil && err != errors.ErrNeedleNotExist {
				log.Errorf("delstores bucket:%s, file:%s, err:%s\n", bucket, filename, err)
				return
			}
			oFileSize = df.Filesize
		}

	}

	if deleteAfterDays == 0 {
		f.DeleteAftertime = 0
	} else {
		f.DeleteAftertime = int64(deleteAfterDays)*24*3600 + time.Now().Unix()
	}

	if err = d.hBase.Put(bucket, f, n); err != nil {
		if !(err == errors.ErrNeedleExist) {
			log.Errorf("hBase.Put error(%v)", err)
			err = errors.ErrHBase
		}

		return
	}

	//更新expr hbase表
	if deleteAfterDays != 0 {
		expfilename := d.getexpfilename(bucket, f.Filename)
		if recover_error = d.hBase.PutExpire(expfilename, f.DeleteAftertime); recover_error != nil {
			log.Recoverf("put exper hbase table bucekt %s filename %s afterdaystime %d failed %v",
				bucket, f.Filename, f.DeleteAftertime, recover_error)
		}
	}

	return
}

// MkblkStores get writable stores for http upload
func (d *Directory) Mkblk(bucket string, f *meta.File, n *meta.Needle) (err error) {
	//log.Errorf("filename %s key %s=====", f.Filename, strconv.FormatInt(n.Key, 10))
	if err = d.hBase.PutMkblk(bucket, f, strconv.FormatInt(n.Key, 10), n); err != nil {
		log.Errorf("hBase.PutMkblk error(%v)", err)
		err = errors.ErrHBase
		return
	}
	//cache this block
	/*
		f.MTime = time.Now().Unix()
		d.cache_block_lock.Lock()
		d.cache_block[f.Filename] = f
		d.cache_block_lock.Unlock()
	*/
	return
}

// BputStores get writable stores for http upload
func (d *Directory) Bput(bucket string, f *meta.File, n *meta.Needle, ctx string, id string, offset int64) (retOffset int64, err error) {
	if retOffset, err = d.hBase.PutBput(bucket, f, ctx, id, offset, n); err != nil {
		log.Errorf("hBase.PutBput error(%v)", err)
		err = errors.ErrHBase
	}
	return
}

func (d *Directory) getDeleteAftertime(day int) int64 {
	return int64(day)*24*3600 + time.Now().Unix()
}

// MkfileStores get writable stores for http upload
func (d *Directory) MkfileStores(bucket string, f *meta.File, buf string,
	overWriteFlag, deleteAfterDays int) (oFileSize string, sha1string string, err error) {

	var (
		tf, df        *meta.File
		tfs           []*meta.File
		blockids      []string
		id, allsha1s  string
		exist         bool
		recover_error error
		//	i             int
		sbuf            bytes.Buffer
		datalen, tfsize int64
	)
	oFileSize = "0"
	blockids = strings.Split(buf, ",")
	//d.cache_block_lock.RLock()
	for _, id = range blockids {
		// block filename and id is keyid
		//tf, ok = d.cache_block[id]
		//if !ok {
		//log.Errorf("miss==== %d", i)
		//d.cache_block_lock.RUnlock()
		if _, tf, err = d.hBase.GetTmpFile(bucket, id, id); err != nil {
			log.Errorf("get tmpfile error:%s, when filename is empty", err.Error())
			return
		}
		//d.cache_block_lock.RLock()
		//}
		tfs = append(tfs, tf)
		sbuf.WriteString(tf.Sha1)
		if tfsize, err = strconv.ParseInt(tf.Filesize, 10, 64); err != nil {
			//should not come here,if come here hbase failed bug
			log.Errorf("bucket %s mkfile filename %s filesize is invalid %v", bucket, f.Filename, err)
			return
		}
		datalen += tfsize
		//allsha1s += tf.Sha1
	}
	//check filesize
	if tfsize, err = strconv.ParseInt(f.Filesize, 10, 64); err != nil {
		//should not come here,if come here hbase failed bug
		log.Errorf("bucket %s mkfile filename %s filesize is invalid %v", bucket, f.Filename, err)
		return
	}
	if datalen != tfsize {
		log.Errorf("this mkfile bucket %s filename %s failed req datalen %d but upload data len is %s",
			bucket, f.Filename, f.Filesize, datalen)
		err = errors.ErrMkfileDatalenNotMatch
		return
	}
	//d.cache_block_lock.RUnlock()
	allsha1s = sbuf.String()
	sha := sha1.Sum([]byte(allsha1s))
	sha1string = hex.EncodeToString(sha[:])
	if f.Filename == "" {
		f.Filename = sha1string
	}

	/*
		判断目的文件是否存在
		若覆盖，文件存在：则删除文件
		不覆盖，若文件存在：先判断文件是否过期，过期则删除文件。不过期 则返回文件已经存在
	*/
	df, err = d.hBase.Get(bucket, f.Filename)
	if err == nil && df != nil {
		exist = true
	} else if err != nil && !(err == errors.ErrNeedleNotExist || err == errors.ErrSrcBucketNoExist) {
		err = errors.ErrHBase
		return
	} else {
		exist = false
	}
	if !d.isOverWrite(overWriteFlag) {
		if exist {
			if df.DeleteAftertime != 0 && df.DeleteAftertime < time.Now().Unix() {
				if _, err = d.DelStores(bucket, f.Filename); err != nil && err != errors.ErrNeedleNotExist {
					log.Errorf("delstores bucket:%s, file:%s, err:%s\n", bucket, f.Filename, err)
					return
				}
				oFileSize = df.Filesize
			} else {
				err = errors.ErrNeedleExist
				return
			}

		}

	} else {
		if exist {
			if _, err = d.DelStores(bucket, f.Filename); err != nil && err != errors.ErrNeedleNotExist {
				log.Errorf("delstores bucket:%s, file:%s, err:%s\n", bucket, f.Filename, err)
				return
			}

			oFileSize = df.Filesize

		}

	}

	f.Sha1 = sha1string

	for _, tf = range tfs {
		f.Key = append(f.Key, tf.Key[0])
	}

	if deleteAfterDays == 0 {
		f.DeleteAftertime = 0
	} else {
		f.DeleteAftertime = d.getDeleteAftertime(deleteAfterDays)
	}
	if err = d.hBase.PutMkfile(bucket, f); err != nil {
		if err != errors.ErrNeedleExist {
			log.Errorf("hBase.PutMkfile error(%v)", err)
			err = errors.ErrHBase
		}
		return
	}
	//更新expr hbase表
	if deleteAfterDays != 0 {
		expfilename := d.getexpfilename(bucket, f.Filename)
		if recover_error = d.hBase.PutExpire(expfilename, f.DeleteAftertime); recover_error != nil {
			log.Recoverf("put exper hbase table bucekt %s filename %s afterdaystime %d failed %v",
				bucket, f.Filename, f.DeleteAftertime, recover_error)
		}
	}

	//goroute do clean tmp block an cache block
	//	go func() {
	//log.Errorf("delete tmp ids %s", bucket)
	d.hBase.DelMkfileBlocks(bucket, tfs)
	//	d.cache_block_lock.Lock()
	//for _, id = range blockids {
	//delete(d.cache_block, id)
	//}
	//	d.cache_block_lock.Unlock()
	//}()
	return
}

func (d *Directory) MkbigfileStores(bucket string, f *meta.File, buf string,
	overWriteFlag, deleteAfterDays int) (oFileSize string, sha1string string, err error) {

	var (
		tf, df          *meta.File
		tfs             []*meta.File
		blockids        []string
		id, allsha1s    string
		exist, tflag    bool
		recover_error   error
		i, j            int
		sbuf            bytes.Buffer
		tkey            []int64
		datalen, tfsize int64
	)
	oFileSize = "0"
	blockids = strings.Split(buf, ",")

	for _, id = range blockids {

		if _, tf, err = d.hBase.GetTmpFile(bucket, id, id); err != nil {
			log.Errorf("get tmpfile error:%s, when filename is empty", err.Error())
			return
		}

		tfs = append(tfs, tf)
		sbuf.WriteString(tf.Sha1)
		if tfsize, err = strconv.ParseInt(tf.Filesize, 10, 64); err != nil {
			//should not come here,if come here hbase failed bug
			log.Errorf("bucket %s mkfile filename %s filesize is invalid %v", bucket, f.Filename, err)
			return
		}
		datalen += tfsize

	}

	//check filesize
	if tfsize, err = strconv.ParseInt(f.Filesize, 10, 64); err != nil {
		//should not come here,if come here hbase failed bug
		log.Errorf("bucket %s mkfile filename %s filesize is invalid %v", bucket, f.Filename, err)
		return
	}
	if datalen != tfsize {
		log.Errorf("this mkfile bucket %s filename %s failed req datalen %d but upload data len is %s",
			bucket, f.Filename, f.Filesize, datalen)
		err = errors.ErrMkfileDatalenNotMatch
		return
	}

	allsha1s = sbuf.String()
	sha := sha1.Sum([]byte(allsha1s))
	sha1string = hex.EncodeToString(sha[:])
	if f.Filename == "" {
		f.Filename = sha1string
	}

	/*
		判断目的文件是否存在
		若覆盖，文件存在：则删除文件
		不覆盖，若文件存在：先判断文件是否过期，过期则删除文件。不过期 则返回文件已经存在
	*/
	df, err = d.hBase.Get(bucket, f.Filename)
	if err == nil && df != nil {
		exist = true
	} else if err != nil && !(err == errors.ErrNeedleNotExist || err == errors.ErrSrcBucketNoExist) {
		err = errors.ErrHBase
		return
	} else {
		exist = false
	}
	if !d.isOverWrite(overWriteFlag) {
		if exist {
			if df.DeleteAftertime != 0 && df.DeleteAftertime < time.Now().Unix() {
				if _, err = d.DelStores(bucket, f.Filename); err != nil && err != errors.ErrNeedleNotExist {
					log.Errorf("delstores bucket:%s, file:%s, err:%s\n", bucket, f.Filename, err)
					return
				}
				oFileSize = df.Filesize
			} else {
				err = errors.ErrNeedleExist
				return
			}

		}

	} else {
		if exist {
			if _, err = d.DelStores(bucket, f.Filename); err != nil && err != errors.ErrNeedleNotExist {
				log.Errorf("delstores bucket:%s, file:%s, err:%s\n", bucket, f.Filename, err)
				return
			}

			oFileSize = df.Filesize

		}

	}

	f.Sha1 = sha1string
	i = 0
	klen := len(tfs)
	for _, tf = range tfs {
		if i == 0 || tflag {
			j = i
			log.Errorf("put key %d", tf.Key[0])
			f.Key = append(f.Key, tf.Key[0])
			tflag = false
		}

		tkey = append(tkey, tf.Key[0])
		if i%9999 == 0 || i == klen-1 {
			err = d.hBase.Putbigfiletmpkey(tkey[j:])
			if err != nil {
				log.Errorf("put big file tmp key failed %v", err)
				return
			}

			tflag = true
		}
		i++
	}

	if deleteAfterDays == 0 {
		f.DeleteAftertime = 0
	} else {
		f.DeleteAftertime = d.getDeleteAftertime(deleteAfterDays)
	}
	if err = d.hBase.PutMkfile(bucket, f); err != nil {
		if err != errors.ErrNeedleExist {
			log.Errorf("hBase.PutMkfile error(%v)", err)
			err = errors.ErrHBase
		}
		return
	}
	//更新expr hbase表
	if deleteAfterDays != 0 {
		expfilename := d.getexpfilename(bucket, f.Filename)
		if recover_error = d.hBase.PutExpire(expfilename, f.DeleteAftertime); recover_error != nil {
			log.Recoverf("put exper hbase table bucekt %s filename %s afterdaystime %d failed %v",
				bucket, f.Filename, f.DeleteAftertime, recover_error)
		}
	}
	d.hBase.DelMkfileBlocks(bucket, tfs)
	return
}

// DelStores get delable stores for http del
func (d *Directory) DelStores(bucket, filename string) (f *meta.File, err error) {
	var (
		n *meta.Needle
	)
	if f, err = d.hBase.Get(bucket, filename); err != nil {
		if !(err == errors.ErrNeedleNotExist || err == errors.ErrSrcBucketNoExist) {
			log.Errorf("hBase.Get error(%v)", err)
			err = errors.ErrHBase
		}
		return
	}
	if n, err = d.hBase.GetNeedle(f.Key[0]); err != nil {
		log.Errorf("bucket name %s file %s get needle failed %v", bucket, filename, err)
		if err != errors.ErrNeedleNotExist {
			err = errors.ErrHBase
		}
		return
	}

	//if needle have link,do not delete
	n.Link--
	if err = d.hBase.UpdataNeedle(n); err != nil {
		log.Errorf("hBase.Del updataNeedle faild(%v)", err)
		return
	}

	err = d.hBase.FileToTrash(bucket, f, _flag_real_file)
	if err != nil && err != errors.ErrNeedleNotExist {
		log.Errorf("habse file to trash failed error:%v", err)
		err = errors.ErrHBase
	}

	if f.DeleteAftertime != 0 && f.DeleteAftertime < time.Now().Unix() {
		log.Infof("delete DeleteAftertime file bucket %s filename %s", bucket, filename)
		err = errors.ErrNeedleNotExist
	}
	//删除expr hbase 表数据
	if f.DeleteAftertime != 0 {
		expfilename := d.getexpfilename(bucket, f.Filename)
		if err = d.hBase.DeleteExpire(expfilename); err != nil {
			log.Errorf("hBase.DeleteExpire bucket %s filename %s error:%s", bucket, f.Filename, err.Error())
			log.Recoverf("hBase.DeleteExpire bucket %s filename %s error:%s", bucket, f.Filename, err.Error())
		}
	}
	return
}

// DelTmpStores get delable stores for http del
func (d *Directory) DelTmpStores(bucket, filename string, id string) (n []*meta.Needle, stores *[]meta.StoreS, err error) {
	var (
		ok        bool
		store     string
		svrs      []string
		storeMeta *meta.Store
		store_s   []meta.StoreS
	)
	if n, _, err = d.hBase.GetTmpFile(bucket, filename, id); err != nil {
		log.Errorf("hBase.GetTmpFile error(%v)", err)
		if err != errors.ErrNeedleNotExist {
			err = errors.ErrHBase
		}
		return
	}
	if n == nil {
		err = errors.ErrNeedleNotExist
		return
	}

	store_s = make([]meta.StoreS, len(n))
	for index, v := range n {
		if svrs, ok = d.volumeStore[v.Vid]; !ok {
			err = errors.ErrZookeeperDataError
			return
		}
		for _, store = range svrs {
			if storeMeta, ok = d.store[store]; !ok {
				err = errors.ErrZookeeperDataError
				return
			}
			if !storeMeta.CanWrite() {
				err = errors.ErrStoreNotAvailable
				return
			}
			store_s[index].Stores = append(store_s[index].Stores, storeMeta.Api)
		}
	}

	stores = &store_s
	//if needle have link,do not delete
	if n[0].Link > 0 {
		n[0].Link--
		if err = d.hBase.UpdataNeedle(n[0]); err != nil {
			log.Errorf("hBase.Del updataNeedle faild(%v)", err)
			return
		}
	}
	if err = d.hBase.DelTmpFile(bucket, filename, id); err != nil {
		log.Errorf("hBase.Del error(%v)", err)
		err = errors.ErrHBase
	}
	return
}

func (d *Directory) DestroyStore(bucket, filename string) (err error) {
	if err = d.hBase.DelDestroy(bucket, filename, 0); err != nil {
		log.Errorf("hBase.Del error(%v),bucket(%s), filename(%s)", err, bucket, filename)
	}
	return
}

var IS_DESTROYEXPIRE_RUN = false

func (d *Directory) DestroyExpire() {
	if IS_DESTROYEXPIRE_RUN {
		return
	}
	IS_DESTROYEXPIRE_RUN = true
	defer func() {
		IS_DESTROYEXPIRE_RUN = false
	}()

	// filter all row in expire table
	var (
		el                   *meta.ExpireList
		item                 meta.ExpireItem
		err                  error
		limit                = 1000
		marker               string
		index                int
		arr                  []string
		data, dataFile       []byte
		f                    *meta.File
		bucketname, filename string
	)
	oldsize := int64(0)
	overwriteflag := 0
	code := 200
	errMsg := ""
	start := time.Now()

	for {
		index++
		log.Infof("filter expire table index:%d", index)

		if el, err = d.hBase.ListExpire(strconv.Itoa(limit), marker); err != nil {
			log.Errorf("hBase.ListExpire error:%s", err.Error())
			return
		}

		marker = el.Marker

		for _, item = range el.Items {

			if item.Expire > time.Now().Unix() {
				continue
			}

			//file move to trash
			arr = strings.Split(item.Key, EXPIRE_FILE_DELIMITER)
			if len(arr) != 2 {
				log.Errorf("directory.DestroyExpire key:%s  key bad", item.Key)
				continue
			}
			if data, err = b64.URLEncoding.DecodeString(arr[0]); err != nil {
				log.Errorf("directory.DestroyExpire bucket:%s base64 decode err:%s", arr[0], err.Error())
				continue
			}
			if dataFile, err = b64.URLEncoding.DecodeString(arr[1]); err != nil {
				log.Errorf("directory.DestroyExpire file:%s base64 decode err:%s", arr[1], err.Error())
				continue
			}

			filename = string(dataFile)
			bucketname = string(data)
			//DelStores

			if f, err = d.DelStores(bucketname, filename); err != nil && err != errors.ErrNeedleNotExist {
				log.Errorf("delstores bucket:%s, file:%s, err:%s\n", bucketname, filename, err)
				continue
			}
			/*
				if _, f, err = d.hBase.Get(bucektname, f.Filename); err != nil {
					log.Errorf("directory.DestroyExpire get  bucket %s filename %s failed %v",
						bucektname, f.Filename, err)
					continue
				}

				if err = d.hBase.FileToTrash(bucektname, f, _flag_real_file); err != nil {
					log.Errorf("hBase.FileToTrash bucket:%s file:%s error:%s",
						bucektname, f.Filename, err.Error())
					log.Recoverf("hBase.FileToTrash bucket:%s file:%s error:%s",
						bucektname, f.Filename, err.Error())
				}

				//delete expire table
				if err = d.hBase.DeleteExpire(item.Key); err != nil {
					log.Errorf("hBase.DeleteExpire bucket:%s file:%s error:%s",
						bucektname, f.Filename, err.Error())
					log.Recoverf("hBase.DeleteExpire bucket:%s file:%s error:%s",
						bucektname, f.Filename, err.Error())
				}
			*/

			fsize := f.Filesize
			statislog("/r/deleteAfter", &bucketname, &filename, &overwriteflag,
				&oldsize, fsize, start, &code, &errMsg)

		}

		if marker == "" {
			return
		}
	}
}

// UpdataCopyMetas
func (d *Directory) UpdataCopyMetas(bucket, filename, destbucket, destfname string,
	overWriteFlag int) (fsize, oldsize string, err error) {
	var (
		n     *meta.Needle
		f, df *meta.File
		exist bool
	)
	oldsize = "0"
	if f, err = d.hBase.Get(bucket, filename); err != nil {
		log.Errorf("hBase.Get error(%v)", err)
		if !(err == errors.ErrNeedleNotExist || err == errors.ErrSrcBucketNoExist) {
			err = errors.ErrHBase
		}
		return
	}
	if f == nil {
		err = errors.ErrNeedleNotExist
		return
	}

	fsize = f.Filesize

	if f.DeleteAftertime != 0 && f.DeleteAftertime < time.Now().Unix() {
		log.Infof("bucket %s filename ")
		err = errors.ErrNeedleNotExist
		return
	}

	/*
		判断目的文件是否存在
		若覆盖，文件存在：则删除文件
		不覆盖，若文件存在：先判断文件是否过期，过期则删除文件。不过期 则返回文件已经存在
	*/
	df, err = d.hBase.Get(destbucket, destfname)
	if err == nil && df != nil {
		exist = true
	} else if err != nil && !(err == errors.ErrNeedleNotExist || err == errors.ErrSrcBucketNoExist) {
		err = errors.ErrHBase
		return
	} else {
		exist = false
	}
	if !d.isOverWrite(overWriteFlag) {
		if exist {
			if df.DeleteAftertime != 0 && df.DeleteAftertime < time.Now().Unix() {
				if _, err = d.DelStores(destbucket, destfname); err != nil && err != errors.ErrNeedleNotExist {
					log.Errorf("delstores bucket:%s, file:%s, err:%s\n", destbucket, destfname, err)
					return
				}
				oldsize = df.Filesize
			} else {
				err = errors.ErrNeedleExist
				return
			}

		}

	} else {
		if exist {
			if _, err = d.DelStores(destbucket, destfname); err != nil && err != errors.ErrNeedleNotExist {
				log.Errorf("delstores bucket:%s, file:%s, err:%s\n", destbucket, destfname, err)
				return
			}
			oldsize = df.Filesize
		}

	}
	if n, err = d.hBase.GetNeedle(f.Key[0]); err != nil {
		log.Errorf("bucket name %s file %s get needle failed %v", bucket, filename, err)
		if err != errors.ErrNeedleNotExist {
			err = errors.ErrHBase
		}
		return
	}
	n.Link++
	f.Filename = destfname
	f.DeleteAftertime = 0
	if err = d.hBase.Copy(destbucket, f, n); err != nil {
		log.Errorf("hBase.Copy error(%v)", err)
		if !(err == errors.ErrNeedleExist || err == errors.ErrDestBucketNoExist) {
			err = errors.ErrHBase
		}
	}
	return
}

func (d *Directory) delete_overwrite_exprfile(bucket, filename,
	fsize string) (err error) {
	expfilename := d.getexpfilename(bucket, filename)
	if err = d.hBase.DeleteExpire(expfilename); err != nil {
		log.Errorf("hBase.DeleteExpire bucket:%s file:%s error:%s",
			bucket, filename, err.Error())
		err = errors.ErrHBase
		return
	}
	oldsize := int64(0)
	overwriteflag := 0
	code := 200
	errMsg := ""
	start := time.Now()
	statislog("/r/delete", &bucket, &filename, &overwriteflag,
		&oldsize, fsize, start, &code, &errMsg)

	return
}

// UpdataMoveMetas
func (d *Directory) UpdataMoveMetas(bucket, filename, destbucket,
	destfname string, overWriteFlag int) (fsize, oldsize string, err error) {

	var (
		f, df *meta.File
		exist bool
	)
	oldsize = "0"

	if f, err = d.hBase.Get(bucket, filename); err != nil {
		log.Errorf("hBase.Get error(%v)", err)
		if !(err == errors.ErrNeedleNotExist || err == errors.ErrSrcBucketNoExist) {
			err = errors.ErrHBase
		}
		return
	}
	if f == nil {
		err = errors.ErrNeedleNotExist
		return
	}
	fsize = f.Filesize

	//判断源文件是否过期，过期则返回文件不存在
	if f.DeleteAftertime != 0 && f.DeleteAftertime < time.Now().Unix() {
		log.Errorf("bucket %s filename %s", bucket, filename)
		err = errors.ErrNeedleNotExist
		return
	}

	/*
		判断目的文件是否存在
		若覆盖，文件存在：则删除文件
		不覆盖，若文件存在：先判断文件是否过期，过期则删除文件。不过期 则返回文件已经存在
	*/
	df, err = d.hBase.Get(destbucket, destfname)
	if err == nil && df != nil {
		exist = true
	} else if err != nil && !(err == errors.ErrNeedleNotExist || err == errors.ErrSrcBucketNoExist) {
		err = errors.ErrHBase
		return
	} else {
		exist = false
	}
	if !d.isOverWrite(overWriteFlag) {
		if exist {
			if df.DeleteAftertime != 0 && df.DeleteAftertime < time.Now().Unix() {
				if _, err = d.DelStores(destbucket, destfname); err != nil && err != errors.ErrNeedleNotExist {
					log.Errorf("delstores bucket:%s, file:%s, err:%s\n", destbucket, destfname, err)
					return
				}
				err = d.delete_overwrite_exprfile(destbucket, destfname, df.Filesize)
				if err != nil {
					log.Recoverf("hBase.UpdataMoveMetas delete expr bucket:%s file:%s error:%s",
						destbucket, destfname, err.Error())
				}
				oldsize = df.Filesize
			} else {
				err = errors.ErrNeedleExist
				return
			}

		}

	} else {
		if exist {
			if _, err = d.DelStores(destbucket, destfname); err != nil && err != errors.ErrNeedleNotExist {
				log.Errorf("delstores bucket:%s, file:%s, err:%s\n", destbucket, destfname, err)
				return
			}
			oldsize = df.Filesize
		}

	}

	f.Filename = destfname

	if err = d.hBase.Move(bucket, filename, destbucket, f); err != nil {
		log.Errorf("hBase.Move error(%v)", err)
		if !(err == errors.ErrNeedleExist || err == errors.ErrDestBucketNoExist) {
			err = errors.ErrHBase
		}
	}
	//move expr 表数据
	if f.DeleteAftertime != 0 {
		var expfilename string
		expfilename = d.getexpfilename(bucket, filename)
		if err = d.hBase.DeleteExpire(expfilename); err != nil {
			log.Errorf("hBase.DeleteExpire bucket %s filename %s error:%s", bucket, f.Filename, err.Error())
			log.Recoverf("hBase.DeleteExpire bucket %s filename %s error:%s", bucket, f.Filename, err.Error())
		}
		expfilename = d.getexpfilename(destbucket, destfname)
		if err = d.hBase.PutExpire(expfilename, f.DeleteAftertime); err != nil {
			log.Errorf("hBase.PutExpire bucket %s filename %s error:%s", destbucket, destfname, err.Error())
			log.Recoverf("hBase.PutExpire bucket %s filename %s error:%s", destbucket, destfname, err.Error())
		}
	}

	return
}

// GetFileList
func (d *Directory) GetFileList(bucket, limit, prefix, delimiter, marker string) (l *meta.FileListResponse, err error) {
	var (
		lf *meta.FileList
		lr meta.FileListResponse
	)
	if lf, err = d.hBase.List(bucket, limit, prefix, delimiter, marker); err != nil {
		log.Errorf("hBase.GetFileList error(%v)", err)
		if !(err == errors.ErrSrcBucketNoExist) {
			err = errors.ErrHBase
		}
		return
	}
	lr.Flist = *lf
	l = &lr
	return
}

// destory file list
func (d *Directory) GetDestroyFileList(bucket, limit, marker string) (dlResp *meta.DestroyListResponse, trash_flag bool, err error) {
	var (
		fl       *meta.FileList
		df       *meta.DestroyFile
		fileLink int32
	)
	trash_flag = true //for _trash_xxx delete flag 如果那个表中一部分文件不能删除，那么这个表也不能被删除。
	dlResp = new(meta.DestroyListResponse)
	if fl, err = d.hBase.DestroyList(bucket, limit, "", "", marker); err != nil {
		log.Errorf("hBase.List() error(%v), bucket(%s), limit(%s), marker(%s)", err, bucket, limit, marker)
		if err != errors.ErrNeedleNotExist {
			err = errors.ErrHBase
		}
		return
	}

	if len(fl.Items) == 0 {
		dlResp.Marker = "end"
		return
	}

	dlResp.Marker = fl.Marker
	for _, item := range fl.Items {
		df = new(meta.DestroyFile)
		if bucket == _prefix_tmp {
			//log.Errorf("key==%d", item.Key)
			if time.Duration(time.Now().UnixNano()-item.PutTime) < d.config.TmpTimeout.Duration {
				//	log.Errorf("=========timeout")
				continue
			}
		}

		df.FileName = item.Key
		df.Keys = item.Keys
		if len(df.Keys) == 0 {
			log.Recoverf("bucketname %s filename %s needle len is 0", bucket, df.FileName)
			continue
		}
		if df.FileNeedle, fileLink, err = d.getDestroyFileNeedles(df.Keys[0]); err != nil {
			log.Errorf("getDestroyFileNeedles() error(%v), bucket(%s), filename(%s)", err, bucket, item.Key)
			if err == errors.ErrNeedleNotExist {
				d.hBase.DelDestroyFile(bucket, item.Key)
			}
			err = nil // if some one failed ,other continue
			trash_flag = false
			continue
		}
		// equal 0 indicate have one normal file,so can't delete needle
		if (bucket == "bucket_trash" && fileLink >= 0) || (fileLink >= 1) {
			d.hBase.DelDestroyFile(bucket, item.Key)
			continue
		}
		dlResp.DList = append(dlResp.DList, df)
	}

	return
}

// get Destroy File Needle
func (d *Directory) getDestroyFileNeedles(needlekey int64) (dfn *meta.DFileNeedle, link int32, err error) {
	var (
		n *meta.Needle
		//	dfn         *meta.DFileNeedle
		store       string
		svrs        []string
		storeMeta   *meta.Store
		ok          bool
		storestatus map[string]int
		status      int
	)
	if n, err = d.hBase.GetNeedle(needlekey); err != nil {
		log.Errorf("get needle key %d failed %v", needlekey, err)
		if err != errors.ErrNeedleNotExist {
			err = errors.ErrHBase
		}
		return
	}

	link = n.Link

	if svrs, ok = d.volumeStore[n.Vid]; !ok {
		log.Errorf("needle:%v svrs:%v \n", *n, svrs)
		err = errors.ErrZookeeperDataError
		return
	}
	if storestatus, ok = d.volumestorestatus[n.Vid]; !ok {
		log.Errorf("needle:%v storestatus:%v \n", *n, storestatus)
		err = errors.ErrZookeeperDataError
		return
	}
	dfn = new(meta.DFileNeedle)
	for _, store = range svrs {

		if storeMeta, ok = d.store[store]; !ok {
			log.Errorf("store:%s storeMeta:%v \n", store, storeMeta)
			err = errors.ErrZookeeperDataError
			return
		}
		if status, ok = storestatus[store]; !ok {
			log.Errorf("vid:%d storestatus:%v \n", n.Vid, storestatus)
			err = errors.ErrZookeeperDataError
			return
		}

		if !d.dispatcher.VolumeCanRead(status) {
			log.Errorf("volume can not read needle(%+v) store(%+v) status(%d)\n", n, store, status)
			err = errors.ErrStoreNotAvailable
			return
		}

		if storeMeta.IsFail() {
			log.Errorf("store is fail, needle(%+v) store(%+v) status(%d)\n", n, store, status)
			err = errors.ErrStoreNotAvailable
			return
		}
		dfn.Key = n.Key
		dfn.Vid = n.Vid
		dfn.Stores = append(dfn.Stores, storeMeta.Api)
	}

	if len(dfn.Stores) == 0 {
		log.Errorf("getDestroyFileNeedles() Get dfns Failed")
		err = errors.ErrStoreNotAvailable
	}

	return
}

/*
func (d *Directory) Clean_timeout_file(bucket string, begin, end int64) (failedfiles string, err error) {
	var (
		exist                                  bool
		fl                                     *meta.FileList
		limit, marker, failedname, hbasebucket string
	)
	limit = "1000"
	marker = ""
	failedname = ""
	exist, err = d.hBase.IsBucketExist(bucket)
	if err != nil {
		log.Errorf("is bucket %s exist failed", bucket)
		return
	}
	if !exist {
		err = errors.ErrSrcBucketNoExist
		return
	}
	hbasebucket = "bucket_" + bucket
	for {
		if fl, err = d.hBase.GetDestroyFileList(hbasebucket, limit, "", "", marker, begin, end); err != nil {
			log.Errorf("hBase.List() error(%v), bucket(%s), limit(%s), marker(%s)", err,
				bucket, limit, marker)
			if err != errors.ErrNeedleNotExist {
				err = errors.ErrHBase
			}
			return
		}

		if len(fl.Items) == 0 {
			log.Errorf("over====")
			break
		}

		marker = fl.Marker
		for _, item := range fl.Items {
			log.Errorf("file = %s time =%d", item.Key, item.Deleteaftertime)
			if item.Deleteaftertime != 0 && item.Deleteaftertime < time.Now().Unix() {
				if _, err = d.DelStores(bucket, item.Key); err != nil && err != errors.ErrNeedleNotExist {
					log.Errorf("delstores bucket:%s, file:%s, err:%s\n", bucket, item.Key, err)
					if failedname == "" {
						failedname += item.Key
					} else {
						failedname = failedname + "," + item.Key
					}
					continue
				}
			}
		}

	}
	failedfiles = failedname
	err = nil
	return

}
*/

// Bucket Create do create hbase table
func (d *Directory) BucketCreate(bucket, families string) (err error) {
	err = d.hBase.CreateTable(bucket, families)
	return
}

// Bucket Rename do rename hbase table
func (d *Directory) BucketRename(bucket_src, bucket_dst string) (err error) {
	err = d.hBase.RenameTable(bucket_src, bucket_dst)
	return
}

// Bucket Delete do rename hbase table
func (d *Directory) BucketDelete(bucket string) (err error) {
	err = d.hBase.DeleteTable(bucket)
	return
}

// Bucket Destroy do delete hbase table
func (d *Directory) BucketDestroy(bucket string) (err error) {
	err = d.hBase.DestroyTable(bucket)
	return
}

// Bucket List do list hbase table
func (d *Directory) BucketList(regular string) (list []string, err error) {
	list, err = d.hBase.ListTable(regular)
	return
}

func (d *Directory) BucketStat(bucket string) (exist bool, err error) {
	return d.hBase.StatTable(bucket)
}
