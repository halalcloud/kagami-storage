package zk

import (
	"efs/libs/meta"
	"efs/rebalance/conf"
	"encoding/json"
	"path"

	log "efs/log/glog"

	"github.com/samuel/go-zookeeper/zk"
	//"strconv"
)

const (
	FLAG_PERSISTENT = int32(0)
	FLAG_EPHEMERAL  = int32(1)
)

const (
	_volumeRoot = "/volume"
)

var (
	ACL = zk.WorldACL(zk.PermAll)
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
	z.config = config
	if z.c, s, err = zk.Connect(config.Zookeeper.Addrs, config.Zookeeper.Timeout.Duration); err != nil {
		log.Errorf("zk.Connect(\"%v\") error(%v)", config.Zookeeper.Addrs, err)
		return
	}
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

func (z *Zookeeper) Initdispatcher() {
	var (
		err   error
		exist bool
		d     []byte
	)
	dispatcherpath := z.config.Zookeeper.DispatcherRoot
	if exist, _, err = z.c.Exists(dispatcherpath); err != nil {
		log.Errorf("zk op  error(%v)", err)
		return
	}

	if !exist {
		if d, err = json.Marshal(meta.Dispatcher_score); err != nil {
			log.Errorf("json.Marshal() error(%v)", err)
			return
		}
		if _, err = z.c.Create(dispatcherpath, d, FLAG_PERSISTENT, ACL); err != nil {
			log.Errorf("zk create dispatcherpath   error(%v)", err)
			return
		}
	}

}

// WatchRacks get all racks and watch
func (z *Zookeeper) WatchRacks() (nodes []string, ev <-chan zk.Event, err error) {
	if _, _, ev, err = z.c.GetW(z.config.Zookeeper.StoreRoot); err != nil {
		log.Errorf("zk.GetW(\"%s\") error(%v)", z.config.Zookeeper.StoreRoot, err)
		return
	}
	if nodes, _, err = z.c.Children(z.config.Zookeeper.StoreRoot); err != nil {
		log.Errorf("zk.Children(\"%s\") error(%v)", z.config.Zookeeper.StoreRoot, err)
	}
	return
}

// Stores get all stores
func (z *Zookeeper) Stores(rack string) (nodes []string, err error) {
	var spath = path.Join(z.config.Zookeeper.StoreRoot, rack)
	if nodes, _, err = z.c.Children(spath); err != nil {
		log.Errorf("zk.Children(\"%s\") error(%v)", spath, err)
	}
	return
}

// Store get store node data
func (z *Zookeeper) Store(rack, store string) (data []byte, err error) {
	var spath = path.Join(z.config.Zookeeper.StoreRoot, rack, store)
	if data, _, err = z.c.Get(spath); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", spath, err)
	}
	return
}

// StoreVolumes get volumes of store
func (z *Zookeeper) StoreVolumes(rack, store string) (nodes []string, err error) {
	var spath = path.Join(z.config.Zookeeper.StoreRoot, rack, store)
	if nodes, _, err = z.c.Children(spath); err != nil {
		log.Errorf("zk.Children(\"%s\") error(%v)", spath, err)
	}
	return
}

// StoreVolumes get volumes value of store
func (z *Zookeeper) StoreVolume(rack, store, volume string) (data []byte, err error) {
	var spath = path.Join(z.config.Zookeeper.StoreRoot, rack, store, volume)
	if data, _, err = z.c.Get(spath); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", spath, err)
	}
	return
}

// Volumes get all volumes
func (z *Zookeeper) Volumes() (nodes []string, err error) {
	if nodes, _, err = z.c.Children(z.config.Zookeeper.VolumeRoot); err != nil {
		log.Errorf("zk.Children(\"%s\") error(%v)", z.config.Zookeeper.VolumeRoot, err)
	}
	return
}

// Volume get volume node data
func (z *Zookeeper) Volume(volume string) (data []byte, err error) {
	var spath = path.Join(z.config.Zookeeper.VolumeRoot, volume)
	if data, _, err = z.c.Get(spath); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", spath, err)
	}
	return
}

func (z *Zookeeper) Volumestorestatus(volume string, storeid string) (status int, err error) {
	var (
		spath string
		data  []byte
		s     int
	)
	spath = path.Join(z.config.Zookeeper.VolumeRoot, volume)
	spath = path.Join(spath, storeid)
	if data, _, err = z.c.Get(spath); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", spath, err)
	}
	if err = json.Unmarshal(data, &s); err != nil {
		log.Errorf("json.Unmarshal() error(%v)", err)
		return
	}
	status = s
	return

}

// VolumeStores get stores of volume
func (z *Zookeeper) VolumeStores(volume string) (nodes []string, err error) {
	var spath = path.Join(z.config.Zookeeper.VolumeRoot, volume)
	if nodes, _, err = z.c.Children(spath); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", spath, err)
	}
	return
}

// Groups get all groups and watch
func (z *Zookeeper) Groups() (nodes []string, err error) {
	if nodes, _, err = z.c.Children(z.config.Zookeeper.GroupRoot); err != nil {
		log.Errorf("zk.Children(\"%s\") error(%v)", z.config.Zookeeper.GroupRoot, err)
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

//Get Rebalance children vids
func (z *Zookeeper) Rebalance() (vids []string, err error) {
	if vids, _, err = z.c.Children(z.config.Zookeeper.RebalanceRoot); err != nil {
		log.Errorf("zk.Children(\"%s\") error(%v)", z.config.Zookeeper.RebalanceRoot, err)
	}
	return
}

//Get Rebalance vids children stores
func (z *Zookeeper) RebalanceVid(vid string) (stores []string, err error) {
	var spath = path.Join(z.config.Zookeeper.RebalanceRoot, vid)
	if stores, _, err = z.c.Children(spath); err != nil {
		log.Errorf("zk.Children(\"%s\") error (%v)", spath, err)
	}
	return
}

//Create Rebalance vids node
func (z *Zookeeper) CreateRebalanceVid(vid string) (err error) {
	var (
		exist bool
		spath string
	)

	spath = path.Join(z.config.Zookeeper.RebalanceRoot, vid)
	if exist, _, err = z.c.Exists(spath); err != nil {
		log.Errorf("CreateRebalanceVid: Exists faild (%v)", err)
		return
	}

	if !exist {
		if _, err = z.c.Create(spath, []byte{}, FLAG_PERSISTENT, ACL); err != nil {
			log.Errorf("CreateRebalanceVid: Create path (%s) faild (%v)", spath, err)
			return
		}
	}

	return
}

//Get rebalance vid value
func (z *Zookeeper) GetRebalanceVidValue(vid string) (data []byte, err error) {
	var spath = path.Join(z.config.Zookeeper.RebalanceRoot, vid)
	if data, _, err = z.c.Get(spath); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", spath, err)
	}
	return
}

//Set rebalance vid value
func (z *Zookeeper) SetRebalanceVidValue(vid string, data []byte) (err error) {
	var (
		spath string
		exist bool
	)
	spath = path.Join(z.config.Zookeeper.RebalanceRoot, vid)
	if exist, _, err = z.c.Exists(spath); err != nil {
		log.Errorf("SetRebalanceVidValue: Exists faild (%v)", err)
		return
	}
	if !exist {
		err = zk.ErrNoNode
		log.Errorf("SetRebalanceVidValue: %s is not exist failed", spath)
		return
	}
	if _, err = z.c.Set(spath, data, -1); err != nil {
		log.Errorf("zk.Set(\"%s\") error(%v)", spath, err)
		return
	}
	return
}

//Create rebalance vid store node
func (z *Zookeeper) CreateRebalanceVidStore(vid string, store string) (err error) {
	var (
		exist bool
		spath string
	)

	spath = path.Join(z.config.Zookeeper.RebalanceRoot, vid)
	spath = path.Join(spath, store)
	if exist, _, err = z.c.Exists(spath); err != nil {
		log.Errorf("CreateRebalanceVidStore: Exists faild (%v)", err)
		return
	}

	if !exist {
		if _, err = z.c.Create(spath, []byte{}, FLAG_PERSISTENT, ACL); err != nil {
			log.Errorf("CreateRebalanceVidStore: Create path (%s) faild (%v)", spath, err)
			return
		}
	}

	return
}

//Get rebalance vid store value
func (z *Zookeeper) GetRebalanceVidStoreValue(vid string, store string) (data []byte, err error) {
	var spath = path.Join(z.config.Zookeeper.RebalanceRoot, vid)
	spath = path.Join(spath, store)
	if data, _, err = z.c.Get(spath); err != nil {
		log.Errorf("zk.Get(\"%s\") error (%v)", spath, err)
	}
	return
}

//Set rebalance vid store value
func (z *Zookeeper) SetRebalanceVidStoreValue(vid string, store string, data []byte) (err error) {
	var (
		spath string
		exist bool
	)
	spath = path.Join(z.config.Zookeeper.RebalanceRoot, vid)
	spath = path.Join(spath, store)

	if exist, _, err = z.c.Exists(spath); err != nil {
		log.Errorf("SetRebalanceVidStoreValue: Exists faild (%v)", err)
		return
	}
	if !exist {
		err = zk.ErrNoNode
		log.Errorf("SetRebalanceVidStoreValue: %s is not exist failed", spath)
		return
	}
	if _, err = z.c.Set(spath, data, -1); err != nil {
		log.Errorf("zk.Set(\"%s\") error(%v)", spath, err)
		return
	}
	return
}

//Get Rebalance path value os status
func (z *Zookeeper) GetRebalanceStatusValue() (state string, err error) {
	var (
		exist         bool
		rebalancePath string
		state_t       []byte
	)

	rebalancePath = z.config.Zookeeper.RebalanceRoot
	if exist, _, err = z.c.Exists(rebalancePath); err != nil {
		log.Errorf("GetRebalanceStatusValue: Exists faild (%s)", err.Error())
		return
	}
	if !exist {

		if _, err = z.c.Create(rebalancePath, []byte{}, FLAG_PERSISTENT, ACL); err != nil {
			log.Errorf("zk create dispatcherpath   error(%v)", err)
			return
		}
		//	err = zk.ErrNoNode
		//log.Errorf("GetRebalanceStatusValue: not Exists faild (%s)", err.Error())
		return
	}
	if state_t, _, err = z.c.Get(rebalancePath); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", rebalancePath, err)
		return
	}

	state = string(state_t)
	return
}

//Set Rebalance vid store value of status
func (z *Zookeeper) SetVidStoreRebalanceStatus(vid, storeid string, data []byte) (err error) {
	var (
		exist     bool
		storePath string
		stat      *zk.Stat
	)

	storePath = z.config.Zookeeper.RebalanceRoot
	storePath = path.Join(storePath, vid)
	storePath = path.Join(storePath, storeid)
	if exist, _, err = z.c.Exists(storePath); err != nil {
		log.Errorf("SetVidStoreRebalanceStatus: Exists faild (%s)", err.Error())
		return
	}

	if !exist {
		log.Errorf("SetVidStoreRebalanceStatus: do not exist storePath")
		return
	}

	if _, stat, err = z.c.Get(storePath); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", storePath, err)
		return
	}

	if _, err = z.c.Set(storePath, data, stat.Version); err != nil {
		log.Errorf("zk.Set(\"%s\") error(%v)", storePath, err)
		return
	}

	return
}

// Set Rebalance path value of status
func (z *Zookeeper) SetRebalanceStatusValue(state string) (err error) {
	var (
		exist         bool
		rebalancePath string
		stat          *zk.Stat
	)

	rebalancePath = z.config.Zookeeper.RebalanceRoot
	if exist, _, err = z.c.Exists(rebalancePath); err != nil {
		log.Errorf("SetRebalanceStatusValue: Exists faild (%s)", err.Error())
		return
	}

	if !exist {
		if _, err = z.c.Create(rebalancePath, []byte{}, FLAG_PERSISTENT, ACL); err != nil {
			log.Errorf("SetRebalanceStatusValue: Create path (%s) faild (%s)", rebalancePath, err.Error())
			return
		}
	}

	if _, stat, err = z.c.Get(rebalancePath); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", rebalancePath, err)
		return
	}

	if _, err = z.c.Set(rebalancePath, []byte(state), stat.Version); err != nil {
		log.Errorf("zk.Set(\"%s\") error(%v)", rebalancePath, err)
		return
	}
	return
}

//Set Dispatcher path valume of model
func (z *Zookeeper) SetDispatcherModleValue(model string) (err error) {
	var (
		exist          bool
		dispatcherPath string
		stat           *zk.Stat
	)
	dispatcherPath = z.config.Zookeeper.DispatcherRoot

	if exist, _, err = z.c.Exists(dispatcherPath); err != nil {
		log.Errorf("SetDispatcherModleValue: Exists faild (%s)", err.Error())
		return
	}
	if !exist {
		if _, err = z.c.Create(dispatcherPath, []byte{}, FLAG_PERSISTENT, ACL); err != nil {
			log.Errorf("SetDispatcherModleValue: Create path (%s) faild (%s)", dispatcherPath, err.Error())
			return
		}
	}
	if _, stat, err = z.c.Get(dispatcherPath); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", dispatcherPath, err)
		return
	}
	if _, err = z.c.Set(dispatcherPath, []byte(model), stat.Version); err != nil {
		log.Errorf("zk.Set(\"%s\") error(%v)", dispatcherPath, err)
		return
	}
	return
}

//Delete Rebalance all children nodes
func (z *Zookeeper) CleanRebalanceChildrenNodes() (err error) {
	var (
		exist         bool
		rblPath       string
		nodes, snodes []string
	)

	rblPath = z.config.Zookeeper.RebalanceRoot
	//	rblPath = "/volume"

	if exist, _, err = z.c.Exists(rblPath); err != nil {
		log.Errorf("CleanRebalanceChildrenNodes: Exists faild (%s)", err.Error())
		return
	}

	if !exist {
		if _, err = z.c.Create(rblPath, []byte{}, FLAG_PERSISTENT, ACL); err != nil {
			log.Errorf("CleanRebalanceChildrenNodes: Create path (%s) faild (%s)", rblPath, err.Error())
			return
		}
		return
	}

	if nodes, _, err = z.c.Children(rblPath); err != nil {
		log.Errorf("CleanRebalanceChildrenNodes: zk.Children(\"%s\") error(%v)", rblPath, err)
		return
	}

	for _, v := range nodes {
		/*
			vid, _ := strconv.ParseInt(v, 10, 32)
			if vid < 49 {
				continue
			}
		*/
		nPath := path.Join(rblPath, v)
		if snodes, _, err = z.c.Children(nPath); err != nil {
			log.Errorf("CleanRebalanceChildrenNodes: zk.Children(\"%s\") error(%v)", rblPath, err)
			return
		}
		for _, sv := range snodes {
			/*
				if sv == "47E273ED-CD3A-4D6A-94CE-554BA9B195EC" || sv == "47E273ED-CD3A-4D6A-94CE-554BA9B195A2" {
					continue
				}
			*/
			spath := path.Join(nPath, sv)
			if err = z.c.Delete(spath, -1); err != nil && err != zk.ErrNoNode {
				log.Errorf("CleanRebalanceChildrenNodes: zk.Delete node (%s) failed (%s)", nPath, err.Error())
				return
			}
		}
		if err = z.c.Delete(nPath, -1); err != nil && err != zk.ErrNoNode {
			log.Errorf("CleanRebalanceChildrenNodes: zk.Delete node (%s) failed (%s)", nPath, err.Error())
			return
		}
	}
	return
}

func (z *Zookeeper) AddRebalancedVolume(vid string, storeId string) (err error) {
	var (
		volumePath string
		storePath  string
		exist      bool
	)

	volumePath = _volumeRoot + "/" + vid
	if exist, _, err = z.c.Exists(volumePath); err != nil {
		log.Errorf("AddRebalancedVolume: zk failed (%s)", err.Error())
		return
	}

	if !exist {
		if _, err = z.c.Create(volumePath, []byte{}, FLAG_PERSISTENT, ACL); err != nil {
			log.Errorf("AddRebalancedVolume: zk Create (%s) failed (%s)", volumePath, err.Error())
			return
		}
	}

	storePath = volumePath + "/" + storeId
	if exist, _, _ = z.c.Exists(storePath); err != nil {
		log.Errorf("AddRebalancedVolume: zk failed (%s)", err.Error())
		return
	}

	if !exist {
		if _, err = z.c.Create(storePath, []byte{}, FLAG_PERSISTENT, ACL); err != nil {
			log.Errorf("AddRebalancedVolume: zk Create (%s) failed (%s)", volumePath, err.Error())
			return
		}
	}
	return
}

// Close close the zookeeper connection.
func (z *Zookeeper) Close() {
	z.c.Close()
}
