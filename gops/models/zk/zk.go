package zk

import (
	"encoding/json"
	"kagamistoreage/gops/models/global"
	"kagamistoreage/gops/models/types"
	"kagamistoreage/libs/meta"
	"strconv"
	"strings"
	"time"

	"github.com/astaxie/beego"
	"github.com/samuel/go-zookeeper/zk"
)

const (
	_storeRoot       = "/store"
	_volumeRoot      = "/volume"
	_rackRoot        = "/rack"
	_groupRoot       = "/group"
	_rebalanceRoot   = "/rebalance"
	_recoveryRoot    = "/recovery"
	_recoveryreqRoot = "/recoveryreq"

	FLAG_PERSISTENT = int32(0)
	FLAG_EPHEMERAL  = int32(1)
)

var (
	ACL = zk.WorldACL(zk.PermAll)
)

type ZooKeeper struct {
	c *zk.Conn
}

func New() (z *ZooKeeper, err error) {
	var (
		servers []string
		timeout int64
		s       <-chan zk.Event
	)
	servers = beego.AppConfig.Strings("ZkServers")
	if timeout, err = beego.AppConfig.Int64("ZkTimeout"); err != nil {
		return
	}

	z = new(ZooKeeper)

	z.c, s, err = zk.Connect(servers, time.Duration(timeout)*time.Second)

	go func() {
		var e zk.Event

		for {
			if e = <-s; e.Type == 0 {
				return
			}

			beego.Info("zookeeper get a event:", e.State.String())
		}
	}()

	return
}

type Node struct {
	Path     string                 `json:"-"`
	Name     string                 `json:"name"`
	Data     map[string]interface{} `json:"data"`
	Children []*Node                `json:"children"`
}

func (z *ZooKeeper) WatchRacks() (ev <-chan zk.Event, err error) {
	if _, _, ev, err = z.c.GetW(_rackRoot); err != nil {
		beego.Error("zk.GetW(\"%s\") error(%v)", _rackRoot, err)

	}
	return
}

func (z *ZooKeeper) GetRack() (racks []*types.Rack, err error) {
	var (
		children  []string
		children1 []string
		volumes   []string
		vid       uint64
		rack      *types.Rack
		store     *types.Store
		data      []byte
	)

	if children, _, err = z.c.Children(_rackRoot); err != nil {
		return
	}
	racks = make([]*types.Rack, len(children))
	for i, child := range children {
		rack = new(types.Rack)
		rack.Name = child

		if children1, _, err = z.c.Children(_rackRoot + "/" + child); err != nil {
			return
		}

		rack.Stores = make([]*types.Store, len(children1))
		for j, child1 := range children1 {
			store = new(types.Store)
			if data, _, err = z.c.Get(_rackRoot + "/" + child + "/" + child1); err != nil {
				return
			}

			if err = json.Unmarshal(data, store); err != nil {
				return
			}
			if store.Status == meta.StoreStatusHealth {
				store.Status = global.Statushealth
			} else if store.Status == meta.StoreStatusRead {
				store.Status = global.Statuscanread
			} else if store.Status == meta.StoreStatusWrite {
				store.Status = global.Statushealth
			} else {
				store.Status = global.Statusfail
			}

			if volumes, _, err = z.c.Children(_rackRoot + "/" + child + "/" + child1); err != nil {
				return
			}
			for _, vol := range volumes {
				if vid, err = strconv.ParseUint(vol, 10, 64); err != nil {
					return
				}
				store.Volumes = append(store.Volumes, vid)
			}

			store.Ip = strings.Split(store.Stat, ":")[0]
			rack.Stores[j] = store
		}
		racks[i] = rack
	}

	return
}

func (z *ZooKeeper) GetGroup() (groups []*types.Group, err error) {

	var (
		children []string
		group    *types.Group
		child    string
		i, j     int
		storeId  string
		store    *types.Store
		exist    bool
	)

	if exist, _, err = z.c.Exists(_groupRoot); err != nil {
		beego.Error(err)
		return
	}
	if !exist {
		if _, err = z.c.Create(_groupRoot, []byte{}, FLAG_PERSISTENT, ACL); err != nil {
			beego.Error(err)
			return
		}
	}

	if children, _, err = z.c.Children(_groupRoot); err != nil {
		return
	}

	groups = make([]*types.Group, len(children))
	for i, child = range children {
		group = new(types.Group)
		if group.Id, err = strconv.ParseUint(child, 10, 64); err != nil {
			return
		}

		if group.StoreIds, _, err = z.c.Children(_groupRoot + "/" + child); err != nil {
			return
		}

		group.Stores = make([]*types.Store, len(group.StoreIds))
		global.Store_lock.RLock()
		tStores := global.STORES
		global.Store_lock.RUnlock()
		for j, storeId = range group.StoreIds {
			store = tStores[storeId]
			group.Stores[j] = store
		}
		group.Volumes = store.Volumes

		groups[i] = group
	}

	return
}

func (z *ZooKeeper) GetVolume() (volumes []*types.Volume, err error) {
	var (
		children []string
		volume   *types.Volume
		data     []byte
		exist    bool
	)

	if exist, _, err = z.c.Exists(_volumeRoot); err != nil {
		beego.Error(err)
		return
	}
	if !exist {
		if _, err = z.c.Create(_volumeRoot, []byte{}, FLAG_PERSISTENT, ACL); err != nil {
			beego.Error(err)
			return
		}
	}

	if children, _, err = z.c.Children(_volumeRoot); err != nil {
		return
	}

	volumes = make([]*types.Volume, len(children))
	for i, child := range children {
		volume = new(types.Volume)
		volume.Status = make(map[string]int)

		if data, _, err = z.c.Get(_volumeRoot + "/" + child); err != nil {
			beego.Error("vid:", child, " error")
			return
		}
		if err = json.Unmarshal(data, volume); err != nil {
			beego.Error("vid:", child, " get data error")
			return
		}

		if volume.Id, err = strconv.ParseUint(child, 10, 64); err != nil {
			beego.Error("vid:", child, " parse data error")
			return
		}

		if volume.StoreIds, _, err = z.c.Children(_volumeRoot + "/" + child); err != nil {
			beego.Error("vid:", child, " get child error")
			return
		}
		//volumestore status
		for _, tsid := range volume.StoreIds {
			var tstatus int
			if data, _, err = z.c.Get(_volumeRoot + "/" + child + "/" + tsid); err != nil {
				beego.Error("vid:", child, " sid:", tsid, "get data error")
				return
			}
			if err = json.Unmarshal(data, &tstatus); err != nil { //when new volume create
				beego.Error("vid:", child, " sid:", tsid, "get data error")
				volume.Status[tsid] = global.Statusfail
				continue
			}
			switch tstatus {
			case meta.StoreStatusRead:
				volume.Status[tsid] = global.Statuscanread
			case meta.StoreStatusWrite:
				volume.Status[tsid] = global.Statushealth
			case meta.StoreStatusHealth:
				volume.Status[tsid] = global.Statushealth
			case meta.StoreStatusRecover:
				volume.Status[tsid] = global.Statusrecover
				volume.Badstoreids = append(volume.Badstoreids, tsid)
			case meta.StoreStatusFail:
				volume.Status[tsid] = global.Statusfail
				volume.Badstoreids = append(volume.Badstoreids, tsid)
			}
		}

		volumes[i] = volume
	}

	return
}

func (z *ZooKeeper) CreateGroup(groupId uint64, storeId string) (err error) {

	var (
		exist     bool
		groupPath string
		storePath string
	)

	groupPath = _groupRoot + "/" + strconv.Itoa(int(groupId))
	if exist, _, err = z.c.Exists(groupPath); err != nil {
		beego.Error(err)
		return
	}

	if !exist {
		if _, err = z.c.Create(groupPath, []byte{}, FLAG_PERSISTENT, ACL); err != nil {
			beego.Error(err)
			return
		}
	}

	storePath = groupPath + "/" + storeId
	if exist, _, err = z.c.Exists(storePath); err != nil {
		return
	}

	if !exist {
		if _, err = z.c.Create(storePath, []byte{}, FLAG_PERSISTENT, ACL); err != nil {
			return
		}
	}

	return
}

func (z *ZooKeeper) DelGroup(groupId uint64, storeId string) (err error) {
	var (
		exist  bool
		childs []string
	)

	groupPath := _groupRoot + "/" + strconv.Itoa(int(groupId))
	if exist, _, err = z.c.Exists(groupPath); err != nil {
		return
	}
	if !exist {
		return
	}

	storePath := groupPath + "/" + storeId
	if exist, _, err = z.c.Exists(storePath); err != nil {
		return
	}
	if exist {
		if err = z.c.Delete(storePath, -1); err != nil {
			return
		}
	}

	if childs, _, err = z.c.Children(groupPath); err != nil {
		return
	}
	if len(childs) == 0 {
		if err = z.c.Delete(groupPath, -1); err != nil {
			return
		}
	}

	return
}

func (z *ZooKeeper) AddVolume(vid uint64, storeId string) (err error) {
	var (
		volumePath string
		storePath  string
		exist      bool
	)

	volumePath = _volumeRoot + "/" + strconv.FormatUint(vid, 10)
	if exist, _, err = z.c.Exists(volumePath); err != nil {
		return
	}

	if !exist {
		if _, err = z.c.Create(volumePath, []byte{}, FLAG_PERSISTENT, ACL); err != nil {
			return
		}
	}

	storePath = volumePath + "/" + storeId
	if exist, _, _ = z.c.Exists(storePath); err != nil {
		return
	}

	if !exist {
		if _, err = z.c.Create(storePath, []byte{}, FLAG_PERSISTENT, ACL); err != nil {
			return
		}
	}
	return
}

/********************
	recovery
********************/

func (z *ZooKeeper) RecoverySids(vid string) (sids []string, err error) {
	if sids, _, err = z.c.Children(_recoveryRoot + "/" + vid); err != nil {
		return
	}

	return
}

func (z *ZooKeeper) RecoveryreqVids() (vids []string, err error) {
	var exist bool
	if exist, _, err = z.c.Exists(_recoveryreqRoot); err != nil {
		return
	}
	if !exist {
		if _, err = z.c.Create(_recoveryreqRoot, []byte{}, FLAG_PERSISTENT, ACL); err != nil {
			return
		}
	}

	if vids, _, err = z.c.Children(_recoveryreqRoot); err != nil {
		return
	}

	return
}

func (z *ZooKeeper) RecoveryreqSids(vid string) (sids []string, err error) {
	if sids, _, err = z.c.Children(_recoveryreqRoot + "/" + vid); err != nil {
		return
	}

	return
}
func (z *ZooKeeper) RecoveryreqStat(vid, sid string) (rinfo string, err error) {
	var (
		data []byte
	)

	if data, _, err = z.c.Get(_recoveryreqRoot + "/" + vid + "/" + sid); err != nil {
		return
	}
	rinfo = string(data)

	return
}

func (z *ZooKeeper) DelRecoverreq(vid, sid string) (err error) {
	var (
		sids []string
	)
	delnode := _recoveryreqRoot + "/" + vid + "/" + sid
	if err = z.c.Delete(delnode, -1); err != nil {
		beego.Error("delete node %s failed %v", delnode, err)
		return
	}
	sids, err = z.RecoveryreqSids(vid)
	if err != nil {
		return
	}
	if len(sids) == 0 {
		vidpath := _recoveryreqRoot + "/" + vid
		if err = z.c.Delete(vidpath, -1); err != nil {
			beego.Error("delete node %s failed %v", vidpath, err)
			return
		}
	}
	return
}

func (z *ZooKeeper) RecoveryVids() (vids []string, err error) {
	var exist bool
	if exist, _, err = z.c.Exists(_recoveryRoot); err != nil {
		return
	}
	if !exist {
		if _, err = z.c.Create(_recoveryRoot, []byte{}, FLAG_PERSISTENT, ACL); err != nil {
			return
		}
	}

	if vids, _, err = z.c.Children(_recoveryRoot); err != nil {
		return
	}

	return
}

type Recoverystat struct {
	Srcstoreid    string `json:"srcstoreid"`
	ReStatus      int    `json:"restatus"`
	MoveTotalData int64  `json:"movetotaldata"`
	MoveData      int64  `json:"movedata"`
	Utime         int64  `json:"utime"`
}

func (z *ZooKeeper) RecoveryStat(vid, sid string) (rStat *Recoverystat, err error) {
	var (
		data []byte
	)

	if data, _, err = z.c.Get(_recoveryRoot + "/" + vid + "/" + sid); err != nil {
		return
	}
	if err = json.Unmarshal(data, &rStat); err != nil {
		return
	}

	return
}

/*
*******************

	rabalance

*******************
*/
func (z *ZooKeeper) RebalanceStatus() (status string, err error) {
	var (
		data     []byte
		isExists bool
	)
	if isExists, _, err = z.c.Exists(_rebalanceRoot); err != nil {
		return
	}
	if !isExists {
		return
	}
	if data, _, err = z.c.Get(_rebalanceRoot); err != nil {
		return
	}

	status = string(data)
	return
}

type reVolume struct {
	Destvid     int32    `json:"destvid"`
	Movestoreid []string `json:"movestoreid"`
}
type reStore struct {
	Movestatus    int   `json:"movestatus"`
	Movetotaldata int64 `json:"movetotaldata"`
	Movedata      int64 `json:"movedata"`
	Mtime         int64 `json:"utime"`
}
type rebalanceStore struct {
	SrcID  string `json:"srcid"`
	DestID string `json:"destid"`
	ReRate int    `json:"rerate"`
	Status int    `json:"status"`
}
type RebalanceVid struct {
	Vid    int32             `json:"vid"`
	Stores []*rebalanceStore `json:"stores"`
}

type RebalanceVids []*RebalanceVid

func (r RebalanceVids) Len() int           { return len(r) }
func (r RebalanceVids) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r RebalanceVids) Less(i, j int) bool { return r[i].Vid < r[j].Vid }

func (z *ZooKeeper) RebalanceVids() (rebalanceVs []*RebalanceVid, err error) {
	const REBALANCE_OVERTIME = 15

	var (
		data       []byte
		vids       []string
		sids       []string
		rebalanceS *rebalanceStore
		rebalanceV *RebalanceVid
		sInfo      *meta.StoreInfo
		isExists   bool
	)

	if isExists, _, err = z.c.Exists(_rebalanceRoot); err != nil {
		return
	}
	if !isExists {
		return
	}
	if vids, _, err = z.c.Children(_rebalanceRoot); err != nil {
		return
	}

	for _, vid := range vids {
		rebalanceV = new(RebalanceVid)

		var tvid int64
		if tvid, err = strconv.ParseInt(vid, 10, 32); err != nil {
			return
		}
		rebalanceV.Vid = int32(tvid)

		vidPath := _rebalanceRoot + "/" + vid
		if sids, _, err = z.c.Children(vidPath); err != nil {
			return
		}
		for _, sid := range sids {
			rebalanceS = new(rebalanceStore)
			sidPath := _rebalanceRoot + "/" + vid + "/" + sid
			if data, _, err = z.c.Get(sidPath); err != nil {
				return
			}
			if err = json.Unmarshal(data, &sInfo); err != nil {
				return
			}
			rebalanceS.DestID = sInfo.Deststoreid
			rebalanceS.SrcID = sid
			rebalanceS.Status = sInfo.MoveStatus
			if time.Now().Unix()-sInfo.UTime > REBALANCE_OVERTIME && sInfo.MoveStatus == meta.Moving && sInfo.MoveStatus == meta.MoveOk {
				rebalanceS.Status = meta.MoveFail
			}
			if sInfo.MoveData != 0 {
				rebalanceS.ReRate = int((float64(sInfo.MoveData) / float64(sInfo.MoveTotalData)) * 100)
			}
			rebalanceV.Stores = append(rebalanceV.Stores, rebalanceS)
		}

		rebalanceVs = append(rebalanceVs, rebalanceV)
	}

	return
}
