package main

import (
	"efs/libs/errors"
	"efs/libs/meta"
	"efs/store/conf"
	myos "efs/store/os"
	"efs/store/volume"
	myzk "efs/store/zk"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	log "efs/log/glog"
)

// Store get all volume meta data from a index file. index contains volume id,
// volume file path, the super block file index ends with ".idx" if the super
// block is /efs/super_block_1, then the super block index file is
// /efs/super_block_1.idx.
//
// volume index file format:
//  ---------------------------------
// | block_path,index_path,volume_id |
// | /efs/block_1,/efs/block_1.idx\n |
// | /efs/block_2,/efs/block_2.idx\n |
//  ---------------------------------
//
// store -> N volumes
//		 -> volume index -> volume info
//
// volume -> super block -> needle -> photo info
//        -> block index -> needle -> photo info without raw data

const (
	freeVolumePrefix = "_free_block_"
	volumeIndexExt   = ".idx"
	volumeFreeId     = -1
)

var (
	_compactSleep = time.Second * 10
	_backupvolume = time.Minute * 10
)

// Store save volumes.
type Store struct {
	vf                   *os.File
	fvf                  *os.File
	mvf                  *os.File
	dmvf                 *os.File
	rcvf                 *os.File
	FreeId               int32
	Volumes              map[int32]*volume.Volume // split volumes lock
	FreeVolumes          []*volume.Volume
	rebalancevolumes     []*volumerebalance
	rebalancedestvolumes []*volumetmp
	recoveryvolumes      []*volumetmp
	zk                   *myzk.Zookeeper
	conf                 *conf.Config
	flock                sync.Mutex // protect FreeId & saveIndex
	vlock                sync.Mutex // protect Volumes map
	mlock                sync.Mutex //protect move volume
	dmlock               sync.Mutex //protect move dest volume
	rclock               sync.Mutex //protect recovery volume
}

// NewStore
func NewStore(c *conf.Config) (s *Store, err error) {
	s = &Store{}
	if s.zk, err = myzk.NewZookeeper(c); err != nil {
		return
	}
	s.conf = c
	s.FreeId = 0
	s.Volumes = make(map[int32]*volume.Volume)
	if s.vf, err = os.OpenFile(c.Store.VolumeIndex, os.O_RDWR|os.O_CREATE|myos.O_NOATIME, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\") error(%v)", c.Store.VolumeIndex, err)
		s.Close()
		return nil, err
	}
	if s.fvf, err = os.OpenFile(c.Store.FreeVolumeIndex, os.O_RDWR|os.O_CREATE|myos.O_NOATIME, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\") error(%v)", c.Store.FreeVolumeIndex, err)
		s.Close()
		return nil, err
	}

	if s.mvf, err = os.OpenFile(c.Store.RebalanceIndex, os.O_RDWR|os.O_CREATE|myos.O_NOATIME, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\") error(%v)", c.Store.RebalanceIndex, err)
		s.Close()
		return nil, err
	}

	if s.dmvf, err = os.OpenFile(c.Store.RebalanceDestIndex, os.O_RDWR|os.O_CREATE|myos.O_NOATIME, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\") error(%v)", c.Store.RebalanceDestIndex, err)
		s.Close()
		return nil, err
	}

	if s.rcvf, err = os.OpenFile(c.Store.RecoveryIndex, os.O_RDWR|os.O_CREATE|myos.O_NOATIME, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\") error(%v)", c.Store.RecoveryIndex, err)
		s.Close()
		return nil, err
	}

	if err = s.init(); err != nil {
		s.Close()
		return nil, err
	}
	return
}

// init init the store.
func (s *Store) init() (err error) {
	//parse move dest volume index
	if err = s.parseMoveDestVolIndex(); err != nil {
		return
	}

	if err = s.parseFreeVolumeIndex(); err == nil {
		err = s.parseVolumeIndex()
	}

	if err = s.parseRebalanceIndex(); err != nil {
		log.Errorf("parserebalance index failed error %v", err)
		return
	}

	if err = s.parseRecoveryIndex(); err != nil {
		log.Errorf("parserecovery index failed error %v", err)
		return
	}

	return
}

func (s *Store) backupvolume() {
	var err error
	for {
		time.Sleep(_backupvolume)
		s.vlock.Lock()
		if err = s.saveVolumeIndex(); err != nil {
			s.vlock.Unlock()
			log.Recoverf("backup volume info in inde failed %v", err)
			return
		}
		s.vlock.Unlock()
	}

}

func (s *Store) backup_compact_volume() {
	var (
		err           error
		compact_volid []int32
	)
	for {

		for _, v := range s.Volumes {
			if int(v.Del_numbers) > s.conf.CompactNums && !v.Compact {
				compact_volid = append(compact_volid, v.Id)
			}
		}
		for _, vid := range compact_volid {

			log.Infof("compact volume: %d start", vid)
			if err = s.CompactVolume(vid); err != nil {
				log.Errorf("s.CompactVolume() error(%v)", err)
			}
			log.Infof("compact volume: %d stop", vid)

		}
		compact_volid = compact_volid[0:0:0]
		time.Sleep(_backupvolume)

	}
}

func (s *Store) volumeTmpIndex(lines []string) (vdirs, idirs []string, vids []int32, err error) {
	var (
		line string
		idir string
		vdir string
		vid  int64
		seps []string
	)

	for _, line = range lines {
		if len(strings.TrimSpace(line)) == 0 {
			continue
		}
		if seps = strings.Split(line, ","); len(seps) != 3 {
			log.Errorf("volume index: \"%s\" format error", line)
			err = errors.ErrStoreRebalanceIndex
			return
		}

		vdir = seps[0]
		idir = seps[1]
		if vid, err = strconv.ParseInt(seps[2], 10, 32); err != nil {
			log.Errorf("volume index: \"%s\" format error", line)
			return
		}
		vdirs = append(vdirs, vdir)
		idirs = append(idirs, idir)
		vids = append(vids, int32(vid))

	}

	return
}

func (s *Store) rebalanceindex(lines []string) (storeid []string, vids []int32, newvids []int32, err error) {
	//???
	var (
		line   string
		store  string
		vid    int64
		newvid int64
		seps   []string
	)
	for _, line = range lines {
		if len(strings.TrimSpace(line)) == 0 {
			continue
		}
		if seps = strings.Split(line, ","); len(seps) != 3 {
			log.Errorf("volume index: \"%s\" format error", line)
			err = errors.ErrStoreRebalanceIndex
			return
		}

		store = seps[0]
		if vid, err = strconv.ParseInt(seps[1], 10, 32); err != nil {
			log.Errorf("volume index: \"%s\" format error", line)
			return
		}
		if newvid, err = strconv.ParseInt(seps[2], 10, 32); err != nil {
			log.Errorf("volume index: \"%s\" format error", line)
			return
		}

		storeid = append(storeid, store)
		vids = append(vids, int32(vid))
		newvids = append(newvids, int32(newvid))

	}

	return
}

func (s *Store) parseRecoveryIndex() (err error) {
	var (
		i     int
		data  []byte
		lines []string
		vdirs []string
		idirs []string
		vids  []int32
		tmp   *volumetmp
	)
	if data, err = ioutil.ReadAll(s.rcvf); err != nil {
		log.Errorf("ioutil.ReadAll() error(%v)", err)
		return
	}

	lines = strings.Split(string(data), "\n")
	if vdirs, idirs, vids, err = s.volumeTmpIndex(lines); err != nil {
		log.Errorf("parse rebalance data error(%v)", err)
		return
	}

	for i = 0; i < len(vids); i++ {
		tmp = new(volumetmp)
		tmp.vid = vids[i]
		tmp.file_tmpvolume = vdirs[i]
		tmp.file_tmpvolumeindex = idirs[i]
		if tmp.ftvol, err = os.OpenFile(vdirs[i], os.O_WRONLY|os.O_CREATE|myos.O_NOATIME, 0664); err != nil {
			log.Errorf("rebalance dest open file %s failed error(%v)", vdirs[i], err)
			return
		}
		if tmp.ftvolindex, err = os.OpenFile(idirs[i], os.O_WRONLY|os.O_CREATE|myos.O_NOATIME, 0664); err != nil {
			log.Errorf("rebalance dest open file %s failed error(%v)", idirs[i], err)
			return
		}
		log.Infof("get name %s,%s", vdirs[i], idirs[i])
		s.recoveryvolumes = append(s.recoveryvolumes, tmp)

	}
	go s.CleanFailVolmeMove()

	return

}

//parse rebalance movedestVolindex
func (s *Store) parseMoveDestVolIndex() (err error) {
	var (
		i     int
		data  []byte
		lines []string
		vdirs []string
		idirs []string
		vids  []int32
		tmp   *volumetmp
	)

	if data, err = ioutil.ReadAll(s.dmvf); err != nil {
		log.Errorf("ioutil.ReadAll() error(%v)", err)
		return
	}

	lines = strings.Split(string(data), "\n")
	if vdirs, idirs, vids, err = s.volumeTmpIndex(lines); err != nil {
		log.Errorf("parse rebalance data error(%v)", err)
		return
	}

	for i = 0; i < len(vids); i++ {
		tmp = new(volumetmp)
		tmp.vid = vids[i]
		tmp.file_tmpvolume = vdirs[i]
		tmp.file_tmpvolumeindex = idirs[i]
		if tmp.ftvol, err = os.OpenFile(vdirs[i], os.O_WRONLY|os.O_CREATE|myos.O_NOATIME, 0664); err != nil {
			log.Errorf("rebalance dest open file %s failed error(%v)", vdirs[i], err)
			return
		}
		if tmp.ftvolindex, err = os.OpenFile(idirs[i], os.O_WRONLY|os.O_CREATE|myos.O_NOATIME, 0664); err != nil {
			log.Errorf("rebalance dest open file %s failed error(%v)", idirs[i], err)
			return
		}
		s.rebalancedestvolumes = append(s.rebalancedestvolumes, tmp)

	}
	return
}

// parse rebalanceindex parse dest store,vid from local
func (s *Store) parseRebalanceIndex() (err error) {

	var (
		i        int
		data     []byte
		lines    []string
		storeids []string
		vids     []int32
		newvids  []int32
		tmp      *volumerebalance
	)

	if data, err = ioutil.ReadAll(s.mvf); err != nil {
		log.Errorf("ioutil.ReadAll() error(%v)", err)
		return
	}

	lines = strings.Split(string(data), "\n")
	//???
	if storeids, vids, newvids, err = s.rebalanceindex(lines); err != nil {
		log.Errorf("parse rebalance data error(%v)", err)
		return
	}

	for i = 0; i < len(storeids); i++ {
		tmp = new(volumerebalance)
		tmp.vid = vids[i]
		tmp.newvid = newvids[i]
		tmp.deststoreid = storeids[i]

		s.rebalancevolumes = append(s.rebalancevolumes, tmp)
		go s.movevolume(storeids[i], vids[i], newvids[i])
	}
	go func() {
		for {
			s.mlock.Lock()
			for _, tmp = range s.rebalancevolumes {
				if tmp.status {
					go s.movevolume(tmp.deststoreid, tmp.vid, tmp.newvid)
				}
			}
			s.mlock.Unlock()
			time.Sleep(10 * time.Second)
		}
	}()

	return

}

// parseFreeVolumeIndex parse free index from local.
func (s *Store) parseFreeVolumeIndex() (err error) {
	var (
		i     int
		id    int32
		bfile string
		ifile string
		v     *volume.Volume
		data  []byte
		ids   []int32
		lines []string
		efs   []string
		ifs   []string
	)
	if data, err = ioutil.ReadAll(s.fvf); err != nil {
		log.Errorf("ioutil.ReadAll() error(%v)", err)
		return
	}
	lines = strings.Split(string(data), "\n")
	if _, ids, efs, ifs, _, _, err = s.parseIndex(lines); err != nil {
		return
	}
	for i = 0; i < len(efs); i++ {
		id, bfile, ifile = ids[i], efs[i], ifs[i]
		if v, err = newVolume(id, bfile, ifile, s.conf); err != nil {
			return
		}
		v.Close()
		s.FreeVolumes = append(s.FreeVolumes, v)
		if id = s.fileFreeId(bfile); id > s.FreeId {
			s.FreeId = id
		}
	}
	log.V(1).Infof("current max free volume id: %d", s.FreeId)
	return
}

// parseVolumeIndex parse index from local config and zookeeper.
func (s *Store) parseVolumeIndex() (err error) {
	var (
		i                                  int
		ok                                 bool
		id, dn                             int32
		bfile                              string
		ifile                              string
		v                                  *volume.Volume
		data                               []byte
		lids, zids, dnums, zdnums, damages []int32
		lines                              []string
		lefs, lifs                         []string
		zefs, zifs                         []string
		lim, zim                           map[int32]struct{}
	)
	if data, err = ioutil.ReadAll(s.vf); err != nil {
		log.Errorf("ioutil.ReadAll() error(%v)", err)
		return
	}
	lines = strings.Split(string(data), "\n")
	if lim, lids, lefs, lifs, dnums, damages, err = s.parseIndex(lines); err != nil {
		return
	}
	if lines, err = s.zk.Volumes(); err != nil {
		return
	}
	if zim, zids, zefs, zifs, zdnums, _, err = s.parseIndex(lines); err != nil {
		return
	}
	// local index
	for i = 0; i < len(lefs); i++ {
		id, bfile, ifile, dn, _ = lids[i], lefs[i], lifs[i], dnums[i], damages[i]
		if _, ok = s.Volumes[id]; ok {
			continue
		}
		if v, err = newVolume(id, bfile, ifile, s.conf); err != nil {
			return
		}
		v.Del_numbers = dn
		/*
			if damage == 1 {
				v.Damage = true
			}
		*/
		s.Volumes[id] = v
		if _, ok = zim[id]; !ok {
			if err = s.zk.AddVolume(id, v.Meta()); err != nil {
				return
			}
		} else {
			if err = s.zk.SetVolume(id, v.Meta()); err != nil {
				return
			}
		}
	}
	// zk index
	for i = 0; i < len(zefs); i++ {
		id, bfile, ifile, dn = zids[i], zefs[i], zifs[i], zdnums[i]
		if _, ok = s.Volumes[id]; ok {
			continue
		}
		// if not exists in local
		if _, ok = lim[id]; !ok {
			/*
				if !myos.Exist(bfile) || !myos.Exist(ifile) {
					ok = true
				} else {
					ok = false
				}
			*/
			if v, err = newVolume(id, bfile, ifile, s.conf); err != nil {
				return
			}
			v.Del_numbers = dn
			/*
				if ok {
					v.Damage = ok
				}
			*/
			s.Volumes[id] = v
		}
	}
	err = s.saveVolumeIndex()
	return
}

// parseIndex parse volume info from a index file.
func (s *Store) parseIndex(lines []string) (im map[int32]struct{}, ids []int32, efs, ifs []string, dnums, damages []int32, err error) {
	var (
		id, delnums, damage int64
		vid                 int32
		line                string
		bfile               string
		ifile               string
		seps                []string
	)
	im = make(map[int32]struct{})
	for _, line = range lines {
		if len(strings.TrimSpace(line)) == 0 {
			continue
		}
		/*
			if seps = strings.Split(line, ","); len(seps) != 5 {
				log.Errorf("volume index: \"%s\" format error", line)
				err = errors.ErrStoreVolumeIndex
				return
			}
		*/
		seps = strings.Split(line, ",")
		bfile = seps[0]
		ifile = seps[1]
		if id, err = strconv.ParseInt(seps[2], 10, 32); err != nil {
			log.Errorf("volume index: \"%s\" format error", line)
			return
		}
		if len(seps) > 3 {
			if delnums, err = strconv.ParseInt(seps[3], 10, 32); err != nil {
				log.Errorf("volume index: \"%s\" format error", line)
				return
			}
		}
		if len(seps) > 4 {
			if damage, err = strconv.ParseInt(seps[4], 10, 32); err != nil {
				log.Errorf("volume index: \"%s\" format error", line)
				return
			}
		}
		vid = int32(id)
		ids = append(ids, vid)
		efs = append(efs, bfile)
		ifs = append(ifs, ifile)
		dnums = append(dnums, int32(delnums))
		damages = append(damages, int32(damage))
		im[vid] = struct{}{}
		log.V(1).Infof("parse volume index, id: %d, block: %s, index: %s", id, bfile, ifile)
	}
	return
}

// saveFreeVolumeIndex save free volumes index info to disk.
func (s *Store) saveFreeVolumeIndex() (err error) {
	var (
		tn, n int
		v     *volume.Volume
	)
	if _, err = s.fvf.Seek(0, os.SEEK_SET); err != nil {
		log.Errorf("fvf.Seek() error(%v)", err)
		return
	}
	for _, v = range s.FreeVolumes {
		if n, err = s.fvf.WriteString(fmt.Sprintf("%s\n", string(v.Meta()))); err != nil {
			log.Errorf("fvf.WriteString() error(%v)", err)
			return
		}
		tn += n
	}
	if err = s.fvf.Sync(); err != nil {
		log.Errorf("fvf.saveFreeVolumeIndex Sync() error(%v)", err)
		return
	}
	if err = os.Truncate(s.conf.Store.FreeVolumeIndex, int64(tn)); err != nil {
		log.Errorf("os.Truncate() error(%v)", err)
	}
	return
}

// saveVolumeIndex save volumes index info to disk.
func (s *Store) saveVolumeIndex() (err error) {
	var (
		tn, n int
		v     *volume.Volume
	)
	if _, err = s.vf.Seek(0, os.SEEK_SET); err != nil {
		log.Errorf("vf.Seek() error(%v)", err)
		return
	}
	for _, v = range s.Volumes {
		if n, err = s.vf.WriteString(fmt.Sprintf("%s\n", string(v.Meta()))); err != nil {
			log.Errorf("vf.WriteString() error(%v)", err)
			return
		}
		tn += n
	}
	if err = s.vf.Sync(); err != nil {
		log.Errorf("vf.Sync() error(%v)", err)
		return
	}
	if err = os.Truncate(s.conf.Store.VolumeIndex, int64(tn)); err != nil {
		log.Errorf("os.Truncate() error(%v)", err)
	}
	return
}

// SetZookeeper set zookeeper store meta.
func (s *Store) SetZookeeper() (err error) {
	// update zk store meta
	if err = s.zk.SetStore(&meta.Store{
		Stat:      s.conf.StatListen,
		Admin:     s.conf.AdminListen,
		Api:       s.conf.ApiListen,
		Rebalance: s.conf.RebalanceListen,
	}); err != nil {
		log.Errorf("zk.SetStore() error(%v)", err)
		return
	}
	// update zk root
	if err = s.zk.SetRoot(); err != nil {
		log.Errorf("zk.SetRoot() error(%v)", err)
		return
	}
	return
}

// freeFile get volume block & index free file name.
func (s *Store) freeFile(id int32, bdir, idir string) (bfile, ifile string) {
	var file = fmt.Sprintf("%s%d", freeVolumePrefix, id)
	bfile = filepath.Join(bdir, file)
	file = fmt.Sprintf("%s%d%s", freeVolumePrefix, id, volumeIndexExt)
	ifile = filepath.Join(idir, file)
	return
}

// file get volume block & index file name.
func (s *Store) file(id int32, bdir, idir string, i int) (bfile, ifile string) {
	var file = fmt.Sprintf("%d_%d", id, i)
	bfile = filepath.Join(bdir, file)
	file = fmt.Sprintf("%d_%d%s", id, i, volumeIndexExt)
	ifile = filepath.Join(idir, file)
	return
}

// fileFreeId get a file free id.
func (s *Store) fileFreeId(file string) (id int32) {
	var (
		fid    int64
		fidStr = strings.Replace(filepath.Base(file), freeVolumePrefix, "", -1)
	)
	fid, _ = strconv.ParseInt(fidStr, 10, 32)
	id = int32(fid)
	return
}

// AddFreeVolume add free volumes.
func (s *Store) AddFreeVolume(n int, bdir, idir string) (sn int, err error) {
	var (
		i            int
		bfile, ifile string
		v            *volume.Volume
	)

	if !myos.Exist(bdir) {
		err = myos.Mkdir(bdir)
		if err != nil {
			log.Errorf("create bdir %s faild %s", bdir, err.Error())
			return
		}
	}

	if !myos.Exist(idir) {
		err = myos.Mkdir(idir)
		if err != nil {
			log.Errorf("create bdir %s faild %s", idir, err.Error())
			return
		}
	}

	s.flock.Lock()
	for i = 0; i < n; i++ {
		s.FreeId++
		bfile, ifile = s.freeFile(s.FreeId, bdir, idir)
		if myos.Exist(bfile) || myos.Exist(ifile) {
			continue
		}
		if v, err = newVolume(volumeFreeId, bfile, ifile, s.conf); err != nil {
			// if no free space, delete the file
			os.Remove(bfile)
			os.Remove(ifile)
			break
		}
		v.Close()
		s.FreeVolumes = append(s.FreeVolumes, v)
		sn++
	}
	err = s.saveFreeVolumeIndex()
	s.flock.Unlock()
	return
}

func (s *Store) getlessdiskvolume() (v *volume.Volume) {
	var (
		diskvolume map[string][]*volume.Volume
		vol        *volume.Volume
		dir        string
		dirmax     int
		i          int
		vols       []*volume.Volume
		volmax     []*volume.Volume
		ok         bool
	)
	diskvolume = make(map[string][]*volume.Volume)
	if len(s.FreeVolumes) == 0 {
		return
	}
	for _, vol = range s.FreeVolumes {
		//dir = filepath.Dir(vol.Block.File)
		t := strings.Split(vol.Block.File, "/")
		if len(t) < 2 {
			log.Errorf("volume block file path not /")
			return
		}

		dir = t[1]
		if vols, ok = diskvolume[dir]; ok {
			vols = append(vols, vol)
			diskvolume[dir] = vols
		} else {
			diskvolume[dir] = make([]*volume.Volume, 0)
			diskvolume[dir] = append(diskvolume[dir], vol)
		}
	}
	ok = false
	for _, vols = range diskvolume {
		if !ok {
			dirmax = len(vols)
			volmax = vols
			ok = true
		}
		if dirmax < len(vols) {
			volmax = vols
			dirmax = len(vols)
		}
	}
	if len(volmax) == 0 {
		v = nil
	} else {
		v = volmax[0]
	}

	for i, vol = range s.FreeVolumes {
		if vol.Block.File == v.Block.File {
			break
		}
	}

	s.FreeVolumes = append(s.FreeVolumes[:i], s.FreeVolumes[i+1:]...)
	return
}

// freeVolume get a free volume.
func (s *Store) freeVolume(id int32) (v *volume.Volume, err error) {
	var (
		i                                        int
		bfile, nbfile, ifile, nifile, bdir, idir string
	)
	s.flock.Lock()
	defer s.flock.Unlock()
	if len(s.FreeVolumes) == 0 {
		err = errors.ErrStoreNoFreeVolume
		return
	}
	//v = s.FreeVolumes[0]
	//s.FreeVolumes = s.FreeVolumes[1:]
	v = s.getlessdiskvolume()
	v.Id = id
	bfile, ifile = v.Block.File, v.Indexer.File
	bdir, idir = filepath.Dir(bfile), filepath.Dir(ifile)
	for {
		nbfile, nifile = s.file(id, bdir, idir, i)
		if !myos.Exist(nbfile) && !myos.Exist(nifile) {
			break
		}
		i++
	}
	log.Infof("rename block: %s to %s", bfile, nbfile)
	log.Infof("rename index: %s to %s", ifile, nifile)
	if err = os.Rename(ifile, nifile); err != nil {
		log.Errorf("os.Rename(\"%s\", \"%s\") error(%v)", ifile, nifile, err)
		v.Destroy()
		return
	}
	if err = os.Rename(bfile, nbfile); err != nil {
		log.Errorf("os.Rename(\"%s\", \"%s\") error(%v)", bfile, nbfile, err)
		v.Destroy()
		return
	}
	v.Block.File = nbfile
	v.Indexer.File = nifile
	if err = v.Open(); err != nil {
		v.Destroy()
		return
	}
	err = s.saveFreeVolumeIndex()
	return
}

// addVolume atomic add volume by copy-on-write.
func (s *Store) addVolume(id int32, nv *volume.Volume) {
	var (
		vid     int32
		v       *volume.Volume
		volumes = make(map[int32]*volume.Volume, len(s.Volumes)+1)
	)
	for vid, v = range s.Volumes {
		volumes[vid] = v
	}
	volumes[id] = nv
	// goroutine safe replace
	s.Volumes = volumes
}

// AddVolume add a new volume.
func (s *Store) AddVolume(id int32) (v *volume.Volume, err error) {
	var ov *volume.Volume
	// try check exists
	if ov = s.Volumes[id]; ov != nil {
		return nil, errors.ErrVolumeExist
	}
	// find a free volume
	if v, err = s.freeVolume(id); err != nil {
		return
	}
	s.vlock.Lock()
	if ov = s.Volumes[id]; ov == nil {
		s.addVolume(id, v)
		if err = s.saveVolumeIndex(); err == nil {
			err = s.zk.AddVolume(id, v.Meta())
		}
		if err != nil {
			log.Errorf("add volume: %d error(%v), local index or zookeeper index may save failed", id, err)
		}
	} else {
		err = errors.ErrVolumeExist
	}
	s.vlock.Unlock()
	if err == errors.ErrVolumeExist {
		v.Destroy()
	}
	return
}

// delVolume atomic del volume by copy-on-write.
func (s *Store) delVolume(id int32) {
	var (
		vid     int32
		v       *volume.Volume
		volumes = make(map[int32]*volume.Volume, len(s.Volumes)-1)
	)
	for vid, v = range s.Volumes {
		volumes[vid] = v
	}
	delete(volumes, id)
	// goroutine safe replace
	s.Volumes = volumes
}

// DelVolume del the volume by volume id.
func (s *Store) DelVolume(id int32) (err error) {
	var v *volume.Volume
	s.vlock.Lock()
	if v = s.Volumes[id]; v != nil {
		if !v.Compact {
			s.delVolume(id)
			if err = s.saveVolumeIndex(); err == nil {
				err = s.zk.DelVolume(id)
			}
			if err != nil {
				log.Errorf("del volume: %d error(%v), local index or zookeeper index may save failed", id, err)
			}
		} else {
			err = errors.ErrVolumeInCompact
		}
	} else {
		err = errors.ErrVolumeNotExist
	}
	s.vlock.Unlock()
	// if succced or not meta data saved error, close volume
	if err == nil || (err != errors.ErrVolumeInCompact &&
		err != errors.ErrVolumeNotExist) {
		v.Destroy()
	}
	return
}

// BulkVolume copy a super block from another store server add to this server.
func (s *Store) BulkVolume(id int32, bfile, ifile string) (err error) {
	var v, nv *volume.Volume
	// recovery new block
	if nv, err = newVolume(id, bfile, ifile, s.conf); err != nil {
		return
	}
	s.vlock.Lock()
	if v = s.Volumes[id]; v == nil {
		s.addVolume(id, nv)
		if err = s.saveVolumeIndex(); err == nil {
			err = s.zk.AddVolume(id, nv.Meta())
		}
		if err != nil {
			log.Errorf("bulk volume: %d error(%v), local index or zookeeper index may save failed", id, err)
		}
	} else {
		err = errors.ErrVolumeExist
	}
	s.vlock.Unlock()
	return
}

// CompactVolume compact a super block to another file.
func (s *Store) CompactVolume(id int32) (err error) {
	var (
		v, nv      *volume.Volume
		bdir, idir string
	)
	// try check volume
	if v = s.Volumes[id]; v != nil {
		if v.Compact || v.Moving {
			return errors.ErrVolumeInCompact
		}
	} else {
		return errors.ErrVolumeExist
	}
	// find a free volume
	if nv, err = s.freeVolume(id); err != nil {
		return
	}
	log.Infof("start compact volume: (%d) %s to %s", id, v.Block.File, nv.Block.File)
	// no lock here, Compact is no side-effect
	if err = v.StartCompact(nv); err != nil {
		v.StopCompact(nil)
		goto free
	}
	s.vlock.Lock()
	if v = s.Volumes[id]; v != nil {
		log.Infof("stop compact volume: (%d) %s to %s", id, v.Block.File, nv.Block.File)
		if err = v.StopCompact(nv); err == nil {
			// WARN no need update volumes map, use same object, only update
			// zookeeper the local index cause the block and index file changed.
			if err = s.saveVolumeIndex(); err == nil {
				err = s.zk.SetVolume(id, v.Meta())
			}
			if err != nil {
				log.Errorf("compact volume: %d error(%v), local index or zookeeper index may save failed", id, err)
			}
		}
	} else {
		// never happen
		err = errors.ErrVolumeExist
		log.Errorf("compact volume: %d not exist(may bug)", id)
	}
	s.vlock.Unlock()
	// WARN if failed, nv is free volume, if succeed nv replace with v.
	// Sleep untill anyone had old volume variables all processed.
	time.Sleep(_compactSleep)
free:
	nv.Destroy()
	bdir, idir = filepath.Dir(nv.Block.File), filepath.Dir(nv.Indexer.File)
	_, err = s.AddFreeVolume(1, bdir, idir)

	return
}

// Close close the store.
// WARN the global variable store must first set nil and reject any other
// requests then safty close.
func (s *Store) Close() {
	log.Info("store close")
	var v *volume.Volume
	if s.vf != nil {
		s.vf.Close()
	}
	if s.fvf != nil {
		s.fvf.Close()
	}
	for _, v = range s.Volumes {
		log.Infof("volume[%d] close", v.Id)
		v.Close()
	}
	if s.zk != nil {
		s.zk.Close()
	}
	return
}

func newVolume(id int32, bfile, ifile string, c *conf.Config) (v *volume.Volume, err error) {
	if v, err = volume.NewVolume(id, bfile, ifile, c); err != nil {
		log.Errorf("newVolume(%d, %s, %s) error(%v)", id, bfile, ifile, err)
	}
	return
}
