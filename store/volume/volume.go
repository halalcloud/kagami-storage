package volume

import (
	"fmt"
	"io"
	"kagamistoreage/libs/errors"
	"kagamistoreage/libs/stat"
	"kagamistoreage/store/block"
	"kagamistoreage/store/conf"
	"kagamistoreage/store/index"
	"kagamistoreage/store/needle"
	myos "kagamistoreage/store/os"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "kagamistoreage/log/glog"
)

const (
	_finish   = 0
	_ready    = 1
	_movebuff = 4 * 1024 * 1024 //move volume 4MB for each
)

// uint32Slice deleted offset sort.
type uint32Slice []uint32

func (p uint32Slice) Len() int           { return len(p) }
func (p uint32Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p uint32Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// An store server contains many logic Volume, volume is superblock container.
type Volume struct {
	wg   sync.WaitGroup
	lock sync.RWMutex
	// meta
	Id int32 `json:"id"`
	//tsp add delete nums
	Del_numbers int32             `json:"del_numbers"`
	Stats       *stat.Stats       `json:"stats"`
	Block       *block.SuperBlock `json:"block"`
	Indexer     *index.Indexer    `json:"index"`
	// data
	needles map[int64]int64
	ch      chan uint32
	conf    *conf.Config
	// compact
	Compact       bool   `json:"compact"`
	CompactOffset uint32 `json:"compact_offset"`
	CompactTime   int64  `json:"compact_time"`
	compactKeys   []int64

	// move volume
	Moving  bool `json:"moving"`
	Damage  bool `json:"Damage"` //disk bad then reset flag this volume is damage volume is doing recover
	moveref int

	// status
	closed bool
}

// NewVolume new a volume and init it.
func NewVolume(id int32, bfile, ifile string, c *conf.Config) (v *Volume, err error) {
	v = &Volume{}
	v.Id = id
	v.Stats = &stat.Stats{}
	// data
	v.needles = make(map[int64]int64)
	v.ch = make(chan uint32, c.Volume.SyncDelete)
	v.conf = c
	// compact
	v.Compact = false
	v.CompactOffset = 0
	v.CompactTime = 0
	v.compactKeys = []int64{}
	// status
	v.closed = false
	if v.Block, err = block.NewSuperBlock(bfile, c); err != nil {
		return nil, err
	}
	if v.Indexer, err = index.NewIndexer(ifile, c); err != nil {
		v.Close()
		return nil, err
	}
	if err = v.init(); err != nil {
		v.Close()
		return nil, err
	}
	v.wg.Add(1)
	go v.delproc()
	return
}

// init recovery super block from index or super block.
func (v *Volume) init() (err error) {
	var (
		size       int64
		offset     uint32
		lastOffset uint32
	)
	// recovery from index
	if err = v.Indexer.Recovery(func(ix *index.Index) error {
		// must no less than last offset
		if ix.Offset < lastOffset {
			log.Error("recovery index: %s lastoffset: %d error(%v)", ix, lastOffset, errors.ErrIndexOffset)
			return errors.ErrIndexOffset
		}
		// WARN if index's offset more than the block, discard it.
		if size = int64(ix.Size) + needle.BlockOffset(ix.Offset); size > v.Block.Size {
			log.Error("recovery index: %s EOF", ix)
			return errors.ErrIndexEOF
		}
		v.needles[ix.Key] = needle.NewCache(ix.Offset, ix.Size)
		offset = ix.Offset + needle.NeedleOffset(int64(ix.Size))
		lastOffset = ix.Offset
		return nil
	}); err != nil && err != errors.ErrIndexEOF {
		return
	}
	// recovery from super block
	//fmt.Println("del flag ++++++come+++++", offset)
	if err = v.Block.Recovery(offset, func(n *needle.Needle, so, eo uint32) (err1 error) {
		if n.Flag == needle.FlagOK {
			//fmt.Println("del flag +++++++11111111111111111++++++++")
			if err1 = v.Indexer.Write(n.Key, so, n.TotalSize); err1 != nil {
				return
			}
		} else {
			so = needle.CacheDelOffset
			//	fmt.Println("del flag +++++++++++++++")
			//tsp add get delete numbers
			v.Del_numbers = v.Del_numbers + 1
		}
		v.needles[n.Key] = needle.NewCache(so, n.TotalSize)
		return
	}); err != nil {
		return
	}
	// flush index
	err = v.Indexer.Flush()
	return
}

// Meta get index meta data.
func (v *Volume) Meta() []byte {
	var (
		damage int
	)
	if v.Damage {
		damage = 1
	} else {
		damage = 0
	}
	return []byte(fmt.Sprintf("%s,%s,%d,%d,%d", v.Block.File, v.Indexer.File, v.Id, v.Del_numbers, damage))
}

// ParseMeta parse index meta data.
func (v *Volume) ParseMeta(line string) (block, index string, id int32, err error) {
	var (
		vid  int64
		seps []string
	)
	if seps = strings.Split(line, ","); len(seps) != 3 {
		log.Errorf("volume index: \"%s\" format error", line)
		err = errors.ErrStoreVolumeIndex
		return
	}
	block = seps[0]
	index = seps[1]
	if vid, err = strconv.ParseInt(seps[2], 10, 32); err == nil {
		id = int32(vid)
	} else {
		log.Errorf("volume index: \"%s\" format error", line)
	}
	return
}

// IsClosed reports whether the volume is closed.
func (v *Volume) IsClosed() bool {
	return v.closed
}

func (v *Volume) read(n *needle.Needle) (err error) {
	var (
		key  = n.Key
		size = n.TotalSize
		now  = time.Now().UnixNano()
	)
	// pread syscall is atomic, no lock
	if err = v.Block.ReadAt(n); err != nil {
		return
	}
	if n.Key != key {
		return errors.ErrNeedleKey
	}
	if n.TotalSize != size {
		return errors.ErrNeedleSize
	}
	if log.V(1) {
		log.Infof("get needle key: %d, cookie: %d, offset: %d, size: %d", n.Key, n.Cookie, n.Offset, size)
		log.Infof("%v\n", n)
	}
	// needles map may be out-dated, recheck
	if n.Flag == needle.FlagDel {
		v.lock.Lock()
		v.needles[key] = needle.NewCache(needle.CacheDelOffset, size)
		v.lock.Unlock()
		err = errors.ErrNeedleDeleted
	} else {
		atomic.AddUint64(&v.Stats.TotalGetProcessed, 1)
		atomic.AddUint64(&v.Stats.TotalReadBytes, uint64(size))
		atomic.AddUint64(&v.Stats.TotalGetDelay, uint64(time.Now().UnixNano()-now))
	}
	return
}

// Read get a needle by key and cookie and write to wr.
func (v *Volume) Read(key int64, cookie int32) (n *needle.Needle, err error) {
	var (
		ok bool
		nc int64
	)
	v.lock.RLock()
	if nc, ok = v.needles[key]; !ok {
		err = errors.ErrNeedleNotExist
	}
	v.lock.RUnlock()
	if err == nil {
		if n = needle.NewReader(key, nc); n.Offset != needle.CacheDelOffset {
			if err = v.read(n); err == nil {
				if n.Cookie != cookie {
					err = errors.ErrNeedleCookie
				}
			}
		} else {
			err = errors.ErrNeedleDeleted
		}
		if err != nil {
			n.Close()
			n = nil
		}
	}
	return
}

// Probe probe a needle.
func (v *Volume) Probe() (err error) {
	var (
		ok  bool
		nc  int64
		key int64
		n   *needle.Needle
	)
	v.lock.RLock()
	// get a rand key
	for key, _ = range v.needles {
		break
	}
	if nc, ok = v.needles[key]; !ok {
		err = errors.ErrNeedleNotExist
	}
	v.lock.RUnlock()
	if err == nil {
		if n = needle.NewReader(key, nc); n.Offset != needle.CacheDelOffset {
			err = v.read(n)
		} else {
			err = errors.ErrNeedleDeleted
		}
		n.Close()
	}
	return
}

func (v *Volume) Getprobekey() (reskeys []string) {
	var (
		key, nc int64
		i       int
		n       *needle.Needle
		keys    []int64
		ncs     []int64
		err     error
	)
	i = 0
	v.lock.RLock()
	for key, nc = range v.needles {
		if i == 3 {
			break
		}
		keys = append(keys, key)
		ncs = append(ncs, nc)

		//	keys = append(keys, strconv.FormatInt(key, 10))
		i++
	}
	v.lock.RUnlock()
	i = 0

	for i, key = range keys {
		if n = needle.NewReader(key, ncs[i]); n.Offset != needle.CacheDelOffset {
			err = v.read(n)
			if err == nil {
				reskeys = append(reskeys, strconv.FormatInt(key, 10))
			}

		}

		n.Close()

	}

	return
}

func (v *Volume) Probekey(key int64) (err error) {
	var (
		ok bool
		nc int64
		n  *needle.Needle
	)
	v.lock.RLock()
	// get a rand key
	if nc, ok = v.needles[key]; !ok {
		err = errors.ErrNeedleNotExist
		log.Error("needles key %d not exist", key)
	}
	v.lock.RUnlock()
	if err == nil {
		if n = needle.NewReader(key, nc); n.Offset != needle.CacheDelOffset {
			err = v.read(n)
			if err != nil && err == errors.ErrNeedleDeleted {
				err = nil
			}
			if err != nil && err != errors.ErrNeedleDeleted {
				log.Error("read key %d failed error %v", key, err)
			}
		}
		//not check needle deleted
		/*
			else {
				err = errors.ErrNeedleDeleted
			}
		*/
		n.Close()
	}
	return
}

// Write add a needle, if key exists append to super block, then update
// needle cache offset to new offset.
func (v *Volume) Write(n *needle.Needle) (err error) {
	var (
		ok     bool
		nc     int64
		offset uint32
		now    = time.Now().UnixNano()
	)
	v.lock.Lock()
	n.Offset = v.Block.Offset
	if err = v.Block.Write(n); err == nil {
		if err = v.Indexer.Add(n.Key, n.Offset, n.TotalSize); err == nil {
			nc, ok = v.needles[n.Key]
			v.needles[n.Key] = needle.NewCache(n.Offset, n.TotalSize)
		}
	}
	v.lock.Unlock()
	if err == nil {
		if log.V(1) {
			log.Infof("add needle, offset: %d, size: %d", n.Offset, n.TotalSize)
			log.Info(n)
		}
		if ok {
			offset, _ = needle.Cache(nc)
			v.del(offset)
		}
		atomic.AddUint64(&v.Stats.TotalWriteProcessed, 1)
		atomic.AddUint64(&v.Stats.TotalWriteBytes, uint64(n.TotalSize))
		atomic.AddUint64(&v.Stats.TotalWriteDelay, uint64(time.Now().UnixNano()-now))
	}
	return
}

// Writes add needles, if key exists append to super block, then update
// needle cache offset to new offset.
func (v *Volume) Writes(ns *needle.Needles) (err error) {
	var (
		ok     bool
		nc     int64
		ncs    []int64
		offset uint32
		n      *needle.Needle
		now    = time.Now().UnixNano()
	)
	v.lock.Lock()
	for n = ns.Next(); n != nil; n = ns.Next() {
		offset = v.Block.Offset
		if err = v.Block.Write(n); err != nil {
			break
		}
		if err = v.Indexer.Add(n.Key, offset, n.TotalSize); err != nil {
			break
		}
		if nc, ok = v.needles[n.Key]; ok {
			ncs = append(ncs, nc)
		}
		v.needles[n.Key] = needle.NewCache(offset, n.TotalSize)
		if log.V(1) {
			log.Infof("add needle, offset: %d, size: %d", offset, n.TotalSize)
			log.Info(n)
		}
	}
	v.lock.Unlock()
	if err == nil {
		for _, nc = range ncs {
			offset, _ = needle.Cache(nc)
			v.del(offset)
		}
		atomic.AddUint64(&v.Stats.TotalWriteProcessed, uint64(ns.Num))
		atomic.AddUint64(&v.Stats.TotalWriteBytes, uint64(ns.TotalSize))
		atomic.AddUint64(&v.Stats.TotalWriteDelay, uint64(time.Now().UnixNano()-now))
	}
	return
}

// del signal the godel goroutine aync merge all offsets and del.
func (v *Volume) del(offset uint32) (err error) {
	if offset == needle.CacheDelOffset {
		return
	}
	select {
	case v.ch <- offset:
	default:
		log.Errorf("volume: %d send signal failed", v.Id)
		err = errors.ErrVolumeDel
	}
	return
}

// Delete logical delete a needle, update disk needle flag and memory needle
// cache offset to zero.
func (v *Volume) Delete(key int64) (err error) {
	var (
		ok     bool
		nc     int64
		size   int32
		offset uint32
	)
	v.lock.Lock()
	defer v.lock.Unlock()
	if nc, ok = v.needles[key]; ok {
		if offset, size = needle.Cache(nc); offset != needle.CacheDelOffset {
			v.needles[key] = needle.NewCache(needle.CacheDelOffset, size)
			// when in compact, must save all del operations.
			if v.Compact {
				v.compactKeys = append(v.compactKeys, key)
			} else {
				if err = v.Block.Delete(offset); err != nil {
					log.Error("delete vid %d key %d failed err %v", v.Id, key, err)
					return
				}
				//tsp add get delete numbers
				v.Del_numbers = v.Del_numbers + 1
			}
		} else {
			err = errors.ErrNeedleDeleted
		}
	} else {
		err = errors.ErrNeedleNotExist
	}

	/*
		if err == nil {
			err = v.del(offset)
		}
	*/
	return
}

// who use this func who lock volume for compact
func (v *Volume) UnlockDelete(key int64) (err error) {
	var (
		ok     bool
		nc     int64
		size   int32
		offset uint32
	)
	if nc, ok = v.needles[key]; ok {
		if offset, size = needle.Cache(nc); offset != needle.CacheDelOffset {
			v.needles[key] = needle.NewCache(needle.CacheDelOffset, size)
			// when in compact, must save all del operations.
			if err = v.Block.Delete(offset); err != nil {
				log.Error("delete vid %d key %d failed err %v", v.Id, key, err)
				return
			}
			//tsp add get delete numbers
			v.Del_numbers = v.Del_numbers + 1

		} else {
			err = errors.ErrNeedleDeleted
		}
	} else {
		err = errors.ErrNeedleNotExist
	}
	/*
		if err == nil {
			err = v.del(offset)
		}
	*/
	return
}

// del merge from volume signal, then update block needles flag.
func (v *Volume) delproc() {
	var (
		err     error
		now     int64
		exit    bool
		offset  uint32
		offsets []uint32
	)
	log.Infof("volume: %d del job start", v.Id)
	for {
		select {
		case offset = <-v.ch:
			if exit = (offset == _finish); !exit {
				if offsets = append(offsets, offset); len(offsets) < v.conf.Volume.SyncDelete {
					continue
				}
			}
		case <-time.After(v.conf.Volume.SyncDeleteDelay.Duration):
			exit = false
		}
		if len(offsets) > 0 {
			// sort let the disk seqence write
			sort.Sort(uint32Slice(offsets))
			for _, offset = range offsets {
				now = time.Now().UnixNano()
				// NOTE Modify no lock here, canuse only a atomic write
				// operation but when Compact must finish the job, the cached
				// offset is a old block owner.
				if err = v.Block.Delete(offset); err != nil {
					break
				}
				//tsp add get delete numbers
				v.Del_numbers = v.Del_numbers + 1
				atomic.AddUint64(&v.Stats.TotalDelProcessed, 1)
				atomic.AddUint64(&v.Stats.TotalWriteBytes, 1)
				atomic.AddUint64(&v.Stats.TotalDelDelay, uint64(time.Now().UnixNano()-now))
			}
			offsets = offsets[:0]
		}
		// signal exit
		if exit {
			break
		}
	}
	v.wg.Done()
	log.Warningf("volume[%d] del job exit", v.Id)
	return
}

func (v *Volume) Moveref() {
	v.lock.Lock()
	defer v.lock.Unlock()
	v.moveref = v.moveref + 1
	v.Moving = true
}
func (v *Volume) Moveunref() {
	v.lock.Lock()
	defer v.lock.Unlock()
	v.moveref = v.moveref - 1
	if v.moveref == 0 {
		v.Moving = false
	}
}

// if volume is doing recover so it is status is Damage true
func (v *Volume) Modify_doing_recovery_status() {
	v.lock.Lock()
	defer v.lock.Unlock()
	v.Damage = true
}

// move volume to other sotre
func (v *Volume) Movevolume(file string, fn func(data []byte, offset, total int64, end bool) (err error)) (err error) {
	var (
		r  *os.File
		fi os.FileInfo
		//		rd    *bufio.Reader
		data  []byte
		send  int64
		n     int
		total int64
	)

	if r, err = os.OpenFile(file, os.O_RDONLY|myos.O_NOATIME, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\") error(%v)", file, err)
		return
	}

	if fi, err = r.Stat(); err != nil {
		log.Errorf("block: %s Stat() error(%v)", v.Block.File, err)
		r.Close()
		return
	}
	fd := r.Fd()
	total = fi.Size()
	if err = myos.Fadvise(fd, 0, total, myos.POSIX_FADV_SEQUENTIAL); err != nil {
		log.Errorf("block: %s Fadvise() error(%v)", v.Block.File, err)
		r.Close()
		return
	}

	data = make([]byte, _movebuff)
	for {
		if n, err = r.ReadAt(data, send); err != nil {
			if err == io.EOF {
				if err = fn(data[:n], send, total, true); err != nil {
					break
				}
				break
			} else {
				log.Errorf("read file %s error(%v)", v.Block.File, err)
				time.Sleep(time.Second * 1)
				continue
			}
		} else {
			if err = fn(data[:n], send, total, false); err != nil {
				break
			}
		}
		send = send + int64(n)

	}
	r.Close()
	return

}

// compact compact v to new v.
func (v *Volume) compact(nv *Volume) (err error) {
	err = v.Block.Compact(v.CompactOffset, func(n *needle.Needle, so, eo uint32) (err1 error) {
		if n.Flag != needle.FlagDel {
			//	log.Infof("-----------")
			if err1 = nv.Write(n); err1 != nil {
				return
			}
		}
		v.CompactOffset = eo
		return
	})
	return
}

// Compact copy the super block to another space, and drop the "delete"
// needle, so this can reduce disk space cost.
func (v *Volume) StartCompact(nv *Volume) (err error) {
	v.lock.Lock()
	if v.Compact {
		err = errors.ErrVolumeInCompact
	} else {
		v.Compact = true
	}
	v.lock.Unlock()
	if err != nil {
		return
	}
	v.CompactTime = time.Now().UnixNano()
	if err = v.compact(nv); err != nil {
		return
	}
	atomic.AddUint64(&v.Stats.TotalCompactProcessed, 1)
	return
}

// StopCompact try append left block space and deleted needles when
// compacting, then reset compact flag, offset and compactKeys.
// After stop compact, the nv will set to old volume, and old volume will
// update inner block/index/needles pointer.
// if nv is nil, only reset compact status.
func (v *Volume) StopCompact(nv *Volume) (err error) {
	var key int64

	if nv != nil {
		if err = v.compact(nv); err != nil {
			log.Errorf("compact failed")
			goto free
		}
		//	log.Infof("----------------------------")
		/*
			for _, key = range v.compactKeys {
				if err = nv.Delete(key); err != nil {
					if err == errors.ErrNeedleNotExist || err == errors.ErrNeedleDeleted {
						continue
					}
					goto free
				}
			}
		*/
		// NOTE MUST wait old block finish async delete operations.
		v.lock.Lock()
		//	v.ch <- _finish
		//	v.wg.Wait()
		// then replace old & new block/index/needles variables
		v.Block, nv.Block = nv.Block, v.Block
		v.Indexer, nv.Indexer = nv.Indexer, v.Indexer
		v.needles, nv.needles = nv.needles, v.needles
		v.Del_numbers = int32(len(v.compactKeys))
		atomic.AddUint64(&v.Stats.TotalCompactDelay, uint64(time.Now().UnixNano()-v.CompactTime))
		// NOTE MUST restart delproc job
		//v.wg.Add(1)
		//go v.delproc()

		//	log.Infof("compactkery len %d", len(v.compactKeys))
		//wait delproc ok,then compactkeys
		//	time.Sleep(10 * time.Millisecond)
		for _, key = range v.compactKeys {
			if err = v.UnlockDelete(key); err != nil {
				log.Infof("vid %s key %d del failed (%v)", v.Id, key, err)
			}
		}
		err = nil
		v.Compact = false
		v.CompactOffset = 0
		v.CompactTime = 0
		v.compactKeys = v.compactKeys[:0]
		v.lock.Unlock()
		return

	}
free:
	err = nil
	v.lock.Lock()
	v.Compact = false
	v.CompactOffset = 0
	v.CompactTime = 0
	v.compactKeys = v.compactKeys[:0]
	v.lock.Unlock()
	return
}

// Open open the closed volume, must called after NewVolume.
func (v *Volume) Open() (err error) {
	v.lock.Lock()
	defer v.lock.Unlock()
	if !v.closed {
		return
	}
	if err = v.Block.Open(); err != nil {
		v.Close()
		return
	}
	if err = v.Indexer.Open(); err != nil {
		v.Close()
		return
	}
	if err = v.init(); err != nil {
		v.Close()
		return
	}
	v.closed = false
	v.wg.Add(1)
	go v.delproc()
	return
}

func (v *Volume) close() {
	if v.ch != nil {
		v.ch <- _finish
		v.wg.Wait()
	}
	if v.Block != nil {
		v.Block.Close()
	}
	if v.Indexer != nil {
		v.Indexer.Close()
	}
	v.closed = true
}

// Close close the volume.
func (v *Volume) Close() {
	v.lock.Lock()
	defer v.lock.Unlock()
	v.close()
}

// Destroy remove block and index file, must called after Close().
func (v *Volume) Destroy() {
	v.lock.Lock()
	defer v.lock.Unlock()
	if !v.closed {
		v.close()
	}
	if v.Block != nil {
		v.Block.Destroy()
	}
	if v.Indexer != nil {
		v.Indexer.Destroy()
	}
}
