package zk

import (
	"efs/libs/meta"
	"efs/pitchfork/conf"
	"encoding/json"
	"fmt"
	"path"

	log "efs/log/glog"

	"github.com/samuel/go-zookeeper/zk"
)

type Zookeeper struct {
	c      *zk.Conn
	config *conf.Config
}

// NewZookeeper new a connection to zookeeper.
func NewZookeeper(config *conf.Config) (z *Zookeeper, err error) {
	var (
		s <-chan zk.Event
	)
	z = &Zookeeper{}
	if z.c, s, err = zk.Connect(config.Zookeeper.Addrs, config.Zookeeper.Timeout.Duration); err != nil {
		log.Errorf("zk.Connect(\"%v\") error(%v)", config.Zookeeper.Addrs, err)
		return
	}
	z.config = config
	go func() {
		var e zk.Event
		for {
			if e = <-s; e.Type == 0 {
				return
			}
			log.Infof("zookeeper get a event: %s", e.State.String())
		}
	}()
	return
}

// NewNode create pitchfork node in zk.
func (z *Zookeeper) NewNode(fpath string) (node string, err error) {
	if _, err = z.c.Create(fpath, []byte(""), 0, zk.WorldACL(zk.PermAll)); err != nil {
		if err != zk.ErrNodeExists {
			log.Errorf("zk.create(\"%s\") error(%v)", fpath, err)
			return
		} else {
			err = nil
		}
	}

	if node, err = z.c.Create(path.Join(fpath, "")+"/", []byte(""), int32(zk.FlagEphemeral|zk.FlagSequence), zk.WorldACL(zk.PermAll)); err != nil {
		log.Errorf("zk.Create error(%v)", err)
	} else {
		node = path.Base(node)
	}
	return
}

func (z *Zookeeper) Exist_create(spath string) (err error) {
	var (
		exist bool
	)
	FLAG_PERSISTENT := int32(0)
	if exist, _, err = z.c.Exists(spath); err != nil {
		log.Errorf("zk path %s exist failed %v", spath, err)
		return
	}
	if !exist {
		if _, err = z.c.Create(spath, []byte{}, FLAG_PERSISTENT, zk.WorldACL(zk.PermAll)); err != nil {
			log.Errorf("zk create path %s failed %v", spath, err)
			return
		}
	}
	return
}

//设置需要修复的volume信息
func (z *Zookeeper) SetVolumeRecoverState(vid int32, storeid, srcstoreid string) (err error) {
	var (
		recover_path string
	)

	if err = z.Exist_create(z.config.Zookeeper.ReqrecoverRoot); err != nil {
		return
	}
	recover_path = path.Join(z.config.Zookeeper.ReqrecoverRoot, fmt.Sprintf("%d", vid))
	if err = z.Exist_create(recover_path); err != nil {
		return
	}
	recover_path = path.Join(recover_path, storeid)
	if err = z.Exist_create(recover_path); err != nil {
		return
	}
	data := srcstoreid + "," + storeid
	if _, err = z.c.Set(recover_path, []byte(data), -1); err != nil {
		log.Errorf("zk.Set(\"%s\") error(%v)", recover_path, err)
		return
	}
	return
}

// setRoot update root.
func (z *Zookeeper) setRoot() (err error) {
	if _, err = z.c.Set(z.config.Zookeeper.StoreRoot, []byte(""), -1); err != nil {
		log.Errorf("zk.Set(\"%s\") error(%v)", z.config.Zookeeper.StoreRoot, err)
	}
	return
}

// SetStore update store status.
func (z *Zookeeper) SetStore(s *meta.Store) (err error) {
	var (
		data  []byte
		store = &meta.Store{}
		spath = path.Join(z.config.Zookeeper.StoreRoot, s.Rack, s.Id)
	)
	if data, _, err = z.c.Get(spath); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", spath, err)
		return
	}
	if len(data) > 0 {
		if err = json.Unmarshal(data, store); err != nil {
			log.Errorf("json.Unmarshal() error(%v)", err)
			return
		}
	}
	store.Status = s.Status
	if data, err = json.Marshal(store); err != nil {
		log.Errorf("json.Marshal() error(%v)", err)
		return err
	}
	if _, err = z.c.Set(spath, data, -1); err != nil {
		log.Errorf("zk.Set(\"%s\") error(%v)", spath, err)
		return
	}
	err = z.setRoot()
	return
}

// WatchPitchforks watch pitchfork nodes.
func (z *Zookeeper) WatchPitchforks() (nodes []string, ev <-chan zk.Event, err error) {
	if nodes, _, ev, err = z.c.ChildrenW(z.config.Zookeeper.PitchforkRoot); err != nil {
		log.Errorf("zk.ChildrenW(\"%s\") error(%v)", z.config.Zookeeper.PitchforkRoot, err)
	}
	return
}

// WatchRacks watch the rack nodes.
func (z *Zookeeper) WatchRacks() (nodes []string, ev <-chan zk.Event, err error) {
	if nodes, _, ev, err = z.c.ChildrenW(z.config.Zookeeper.StoreRoot); err != nil {
		log.Errorf("zk.ChildrenW(\"%s\") error(%v)", z.config.Zookeeper.StoreRoot, err)
	}
	return
}

// WatchRacks watch the groups.
func (z *Zookeeper) WatchGroups() (groups []string, ev <-chan zk.Event, err error) {
	if groups, _, ev, err = z.c.ChildrenW(z.config.Zookeeper.GroupRoot); err != nil {
		log.Errorf("zk.ChildrenW(\"%s\") error(%v)", z.config.Zookeeper.GroupRoot, err)
	}
	return
}

// GroupStores get stores of group
func (z *Zookeeper) GroupStores(group string) (nodes []string, err error) {
	var spath = path.Join(z.config.Zookeeper.GroupRoot, group)
	if nodes, _, err = z.c.Children(spath); err != nil {
		log.Errorf("zk.Children(\"%s\") error(%v)", spath, err)
	}
	return
}

// Stores get all stores from a rack.
func (z *Zookeeper) Stores(rack string) (stores []string, err error) {
	var spath = path.Join(z.config.Zookeeper.StoreRoot, rack)
	if stores, _, err = z.c.Children(spath); err != nil {
		log.Errorf("zk.Children(\"%s\") error(%v)", spath, err)
	}
	return
}

// Store get a store node data.
func (z *Zookeeper) Store(rack, store string) (data []byte, err error) {
	var spath = path.Join(z.config.Zookeeper.StoreRoot, rack, store)
	if data, _, err = z.c.Get(spath); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", spath, err)
	}
	return
}

// SetVolumeStat set volume stat
func (z *Zookeeper) SetVolumeState(volume *meta.Volume, storeid string, status int) (err error) {
	var (
		d      []byte
		spath  string
		vstate = &meta.VolumeState{
			TotalWriteProcessed: volume.Stats.TotalWriteProcessed,
			TotalWriteDelay:     volume.Stats.TotalWriteDelay,
		}
	)
	vstate.FreeSpace = volume.Block.FreeSpace()
	spath = path.Join(z.config.Zookeeper.VolumeRoot, fmt.Sprintf("%d", volume.Id))
	if d, err = json.Marshal(vstate); err != nil {
		log.Errorf("json.Marshal() error(%v)", err)
		return
	}
	if _, err = z.c.Set(spath, d, -1); err != nil {
		log.Errorf("zk.Set(\"%s\") error(%v)", spath, err)
	}

	spath = path.Join(spath, fmt.Sprintf("%s", storeid))
	if d, err = json.Marshal(status); err != nil {
		log.Errorf("json.Marshal() error(%v)", err)
		return
	}
	if _, err = z.c.Set(spath, d, -1); err != nil {
		log.Errorf("zk.Set(\"%s\") error(%v)", spath, err)
	}

	return
}

// Close close the zookeeper connection.
func (z *Zookeeper) Close() {
	z.c.Close()
}
