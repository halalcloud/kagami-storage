package zk

import (
	"efs/libs/meta"
	"efs/store/conf"
	"encoding/json"
	"path"
	"strconv"
	"strings"

	log "efs/log/glog"

	myzk "github.com/samuel/go-zookeeper/zk"
)

// zookeeper save the store meta data.
//
//                                 /rack -- rack root path
//									 |
//                   /rack-a -------- --------- /rack-b -- rack node path
//                     |
//         /store-a -------- /store-b -- store node path (data: {"stat":"localhost:6061","admin":"localhost:6063","api":"localhost:6062","status":0})
//            |                 |
// /volume-1 -                   - /volume-4 volume node path (data: /tmp/block_1,/tmp/block_1.idx,1)
// /volume-2 -                   - /volume-5
// /volume-3 -                   - /volume-6

type Zookeeper struct {
	c     *myzk.Conn
	conf  *conf.Config
	fpath string
}

// NewZookeeper new a connection to zookeeper.
func NewZookeeper(c *conf.Config) (z *Zookeeper, err error) {
	var (
		s <-chan myzk.Event
	)
	z = &Zookeeper{}
	z.conf = c
	z.fpath = strings.TrimRight(path.Join(c.Zookeeper.Root, c.Zookeeper.Rack, c.Zookeeper.ServerId), "/")
	if z.c, s, err = myzk.Connect(c.Zookeeper.Addrs, c.Zookeeper.Timeout.Duration); err != nil {
		log.Errorf("zk.Connect(\"%v\") error(%v)", c.Zookeeper.Addrs, err)
		return
	}
	go func() {
		var e myzk.Event
		for {
			if e = <-s; e.Type == 0 {
				return
			}
			log.Infof("zookeeper get a event: %s", e.State.String())
		}
	}()
	err = z.init()
	return
}

// createPath create a zookeeper path.
func (z *Zookeeper) CreatePath(fpath string) (err error) {
	var (
		str   string
		tpath string
	)
	for _, str = range strings.Split(fpath, "/")[1:] {
		tpath = path.Join(tpath, "/", str)
		log.V(1).Infof("create zookeeper path: \"%s\"", tpath)
		if _, err = z.c.Create(tpath, []byte(""), 0, myzk.WorldACL(myzk.PermAll)); err != nil {
			if err != myzk.ErrNodeExists {
				log.Errorf("zk.create(\"%s\") error(%v)", tpath, err)
				return
			} else {
				err = nil
			}
		}
	}
	return
}

// init create /rack/store zk path.
func (z *Zookeeper) init() (err error) {
	err = z.CreatePath(z.fpath)
	return
}

// Volumes get all zk path volumes data.
func (z *Zookeeper) Volumes() (lines []string, err error) {
	var (
		d     []byte
		paths []string
		dpath string
	)
	if paths, _, err = z.c.Children(z.fpath); err != nil {
		log.Errorf("zk.Children(\"%s\") error(%v)", z.fpath, err)
		return
	}
	for _, dpath = range paths {
		if d, _, err = z.c.Get(path.Join(z.fpath, dpath)); err != nil {
			log.Errorf("zk.Get(\"%s\") error(%v)", path.Join(z.fpath, dpath), err)
			return
		}
		lines = append(lines, string(d))
	}
	return
}

func (z *Zookeeper) volumePath(id int32) string {
	return path.Join(z.fpath, strconv.Itoa(int(id)))
}

// AddVolume add a volume data in zk.
func (z *Zookeeper) AddVolume(id int32, data []byte) (err error) {
	var vpath = z.volumePath(id)
	if _, err = z.c.Create(vpath, data, 0, myzk.WorldACL(myzk.PermAll)); err != nil {
		log.Errorf("zk.Create(\"%s\") error(%v)", vpath, err)
	}
	return
}

//Add zookeeper volume/store
func (z *Zookeeper) AddVolStore(vid int32) (err error) {
	var (
		data  []byte
		vpath string
	)
	vpath = path.Join(z.conf.Zookeeper.Volume, strconv.Itoa(int(vid)))
	vpath = path.Join(vpath, z.conf.Zookeeper.ServerId)
	if _, err = z.c.Create(vpath, data, 0, myzk.WorldACL(myzk.PermAll)); err != nil {
		log.Errorf("zk.Create(\"%s\") error(%v)", vpath, err)
	}
	return
}

// DelVolume delete a volume from zk.
func (z *Zookeeper) DelVolume(id int32) (err error) {
	var (
		vpath = z.volumePath(id)
	)

	if err = z.c.Delete(vpath, -1); err != nil {
		log.Errorf("zk.Delete(\"%s\") error(%v)", vpath, err)
	}
	return
}

func (z *Zookeeper) DelVolumeStore(id int32) (err error) {
	var (
		vpath string
	)
	vpath = path.Join(z.conf.Zookeeper.Volume, strconv.Itoa(int(id)))
	vpath = path.Join(vpath, z.conf.Zookeeper.ServerId)

	if err = z.c.Delete(vpath, -1); err != nil {
		log.Errorf("zk.Delete(\"%s\") error(%v)", vpath, err)
	}
	return
}
func (z *Zookeeper) GetRecoveryMovestat(vid int32, deststoreid string) (status *meta.Recoverystat, err error) {
	var (
		vpath string
		data  []byte
		stat  *meta.Recoverystat
	)

	vpath = path.Join(z.conf.Zookeeper.Recovery, strconv.Itoa(int(vid)))
	vpath = path.Join(vpath, deststoreid)
	if data, _, err = z.c.Get(vpath); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", vpath, err)
		return
	}

	stat = new(meta.Recoverystat)
	if len(data) > 0 {
		if err = json.Unmarshal(data, stat); err != nil {
			log.Errorf("json.Unmarshal() error(%v)", err)
			return
		}
	}
	status = stat
	return

}

// set zookeeper /recovery/vid/store
func (z *Zookeeper) SetRecoveryMovestat(vid int32, deststoreid string, status *meta.Recoverystat) (err error) {
	var (
		stat  *myzk.Stat
		vpath string
		data  []byte
	)

	vpath = path.Join(z.conf.Zookeeper.Recovery, strconv.Itoa(int(vid)))
	vpath = path.Join(vpath, deststoreid)
	if _, stat, err = z.c.Get(vpath); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", vpath, err)
		return
	}

	if data, err = json.Marshal(status); err != nil {
		log.Errorf("json.Marshal() error(%v)", err)
		return
	}

	if _, err = z.c.Set(vpath, data, stat.Version); err != nil {
		log.Errorf("zk.Set(\"%s\") error(%v)", vpath, err)
	}
	return
}

//set zookeeper /rebalance/vid/store  move status
func (z *Zookeeper) SetVolMovestat(vid int32, status *meta.StoreInfo) (err error) {
	var (
		stat  *myzk.Stat
		vpath string
		data  []byte
	)

	vpath = path.Join(z.conf.Zookeeper.Rebalance, strconv.Itoa(int(vid)))
	vpath = path.Join(vpath, z.conf.Zookeeper.ServerId)
	if _, stat, err = z.c.Get(vpath); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", vpath, err)
		return
	}

	if data, err = json.Marshal(status); err != nil {
		log.Errorf("json.Marshal() error(%v)", err)
		return
	}

	if _, err = z.c.Set(vpath, data, stat.Version); err != nil {
		log.Errorf("zk.Set(\"%s\") error(%v)", vpath, err)
	}
	return
}

// SetVolume set the data into fpath.
func (z *Zookeeper) SetVolume(id int32, data []byte) (err error) {
	var (
		stat  *myzk.Stat
		vpath = z.volumePath(id)
	)
	if _, stat, err = z.c.Get(vpath); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", vpath, err)
		return
	}

	if _, err = z.c.Set(vpath, data, stat.Version); err != nil {
		log.Errorf("zk.Set(\"%s\") error(%v)", vpath, err)
	}
	return
}

// SetStore set the data into fpath.
func (z *Zookeeper) SetStore(s *meta.Store) (err error) {
	var (
		data []byte
		stat *myzk.Stat
		os   = new(meta.Store)
	)
	s.Id = z.conf.Zookeeper.ServerId
	s.Rack = z.conf.Zookeeper.Rack
	s.Status = meta.StoreStatusInit
	if data, stat, err = z.c.Get(z.fpath); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", z.fpath, err)
		return
	}
	if len(data) > 0 {
		if err = json.Unmarshal(data, os); err != nil {
			log.Errorf("json.Unmarshal() error(%v)", err)
			return
		}
		log.Infof("\nold store meta: %s, \ncurrent store meta: %s", os, s)
		s.Status = os.Status
	}
	// meta.Status not modifify, may update by pitchfork
	if data, err = json.Marshal(s); err != nil {
		log.Errorf("json.Marshal() error(%v)", err)
		return
	}
	if _, err = z.c.Set(z.fpath, data, stat.Version); err != nil {
		log.Errorf("zk.Set(\"%s\") error(%v)", z.fpath, err)
	}
	return
}

// SetRoot update root.
func (z *Zookeeper) SetRoot() (err error) {
	var s *myzk.Stat
	if _, s, err = z.c.Get(z.conf.Zookeeper.Root); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", z.conf.Zookeeper.Root, err)
		return
	}
	if _, err = z.c.Set(z.conf.Zookeeper.Root, []byte(""), s.Version); err != nil {
		log.Errorf("zk.Set(\"%s\") error(%v)", z.conf.Zookeeper.Root, err)
	}
	return
}

func (z *Zookeeper) Getrebalanceapi(storeid string) (api string, err error) {
	var (
		data  []byte
		store meta.Store
	)

	if data, _, err = z.c.Get(path.Join(z.conf.Zookeeper.Root, storeid)); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", storeid, err)
		return
	}
	if err = json.Unmarshal(data, &store); err != nil {
		return
	}

	api = store.Rebalance
	return

}

// Close close the zookeeper connection.
func (z *Zookeeper) Close() {
	z.c.Close()
}
