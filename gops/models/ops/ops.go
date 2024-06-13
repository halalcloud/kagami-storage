package ops

import (
	"errors"
	"fmt"
	"kagamistoreage/gops/models/almrec"
	"kagamistoreage/gops/models/global"
	"kagamistoreage/gops/models/sstat"
	"kagamistoreage/gops/models/store"
	"kagamistoreage/gops/models/types"
	"kagamistoreage/gops/models/zk"
	"kagamistoreage/libs/meta"
	"kagamistoreage/libs/stat"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/astaxie/beego"
	gitzk "github.com/samuel/go-zookeeper/zk"
)

var (
	addGroupParamsErr = errors.New("param ips length can't zero and aliquot copys ")
	groupNotExistErr  = errors.New("group is not exist")
)

const (
	_RecoverTimeout = 120 //recover timeout 120 second
	Recovering      = 1   //doing recover
	Recover_ok      = 2   // do ok recover
	Recover_failed  = 3   // do failed recover
)

type Ops struct {
	store *store.Store
	zk    *zk.ZooKeeper
}

var OpsManager *Ops

func InitOps() (err error) {
	OpsManager, err = New()

	go OpsManager.LoadData()
	go OpsManager.DoRecover()
	go OpsManager.DoFailRecover()
	go OpsManager.synstorestatus()
	go almrec.Alarm_demo()
	return
}

func (o *Ops) updatestorefreevolumes(id string, num int) {
	global.Store_lock.Lock()
	defer global.Store_lock.Unlock()
	store, ok := global.STORES[id]
	if ok {
		store.Freevolumes = num
	}
}

func (o *Ops) synstoreinfo(id string, info *stat.Stats, freevolumes int, ctime string) {
	o.updatestorefreevolumes(id, freevolumes)
	sstat.Add_QPS(id, info.WriteTPS, info.GetQPS, info.DelTPS, ctime)
	sstat.Add_Delay(id, info.WriteDelay, info.GetDelay, info.DelDelay, ctime)
	sstat.Add_Throughput(id, info.WriteFlow, info.ReadFlow, ctime)
}

func (o *Ops) volumegetalarmstring(ip string, volumeid uint64, fromstatus int, tostatus int) (alarm string) {
	alarm = fmt.Sprintf(" store %s volumeid %d status from %s change to %s", ip, volumeid, global.Statustostring(fromstatus), global.Statustostring(tostatus))
	return
}

func (o *Ops) updatevolumestat(volumeid uint64, storeid string, delnums int32) {
	global.Volume_lock.Lock()
	defer global.Volume_lock.Unlock()
	volume, ok := global.VOLUMES[volumeid]
	if ok {
		volume.Delnums = delnums
	}
}

func (o *Ops) synvolumeinfo(id string, info []*meta.Volume) {
	for _, volume := range info {
		o.updatevolumestat(uint64(volume.Id), id, volume.Del_numbers)
	}
}

func (o *Ops) synstorestatus() {
	var (
		lasttime     int64 = time.Now().Unix()
		now          int64
		pullInterval int64
	)
	pullInterval, err := beego.AppConfig.Int64("StorePullInterval")
	if err != nil {
		beego.Error("get PullInterval error")
	}

	for {
		beego.Info("synstore status")
		nowuinux := time.Now().Unix()
		now = nowuinux - ((nowuinux - lasttime) % pullInterval)
		lasttime = now
		tm := time.Unix(now, 0)
		ctime := tm.Format("2006-01-02 15:04:05")

		global.Store_lock.RLock()
		tmp_stores := global.STORES
		global.Store_lock.RUnlock()
		for id, s := range tmp_stores {
			//beego.Info("sid:" + id)
			if s.Status == global.Statusrecover {
				beego.Warning("sync sid: " + id + "fail")
				continue
			}
			volumes, freevolumes, info, err := s.Sinfo()
			if err != nil {
				continue
			}
			o.synvolumeinfo(id, volumes)
			o.synstoreinfo(id, info.Stats, freevolumes, ctime)

		}
		time.Sleep(time.Duration(pullInterval) * time.Second)
	}
}

func (o *Ops) LoadData() {
	var (
		sev <-chan gitzk.Event
		err error
	)

	for {
		sev, err = o.LoadRacks()
		if err != nil {
			beego.Error("load racks data failed")
			time.Sleep(10 * time.Second)
			continue
		}
		beego.Info("rack加载完成 ...")

		o.LoadGroups()
		beego.Info("group加载完成 ...")

		o.LoadVolumes()
		beego.Info("volume加载完成 ...")

		select {
		case <-sev:
			beego.Info("stores status change or new store")
		case <-time.After(10 * time.Second):
			beego.Info("pull from zk")
		}
		beego.Info("加载数据完成[racks,groups,volumes]...")
		//time.Sleep(10 * time.Second)
	}
}

func (o *Ops) DoRecover() {
	var (
		//MAX_RECOVER = 5
		err  error
		vids []string
		//rn      map[uint64][]uint64
		//rr      map[uint64][]uint64
		reqinfo string
	)

	time.Sleep(10 * time.Second)

	for {

		stores := global.STORES
		var sids []string

		if vids, err = o.zk.RecoveryreqVids(); err != nil {
			beego.Error("get recover vid error ", err)
			continue
		}
		for _, v := range vids {
			vid, _ := strconv.ParseUint(v, 10, 64)
			//----------filter recover ok
			if sids, err = o.zk.RecoveryreqSids(strconv.FormatUint(vid, 10)); err != nil {
				beego.Error("get recovery sids error ", err)
				continue
			}
			for _, sid := range sids {
				if reqinfo, err = o.zk.RecoveryreqStat(strconv.FormatUint(vid, 10), sid); err != nil {
					beego.Error("get recovery stat error  vid ", vid, " sid ", sid)
					continue
				}
				rinfo := strings.Split(reqinfo, ",")
				if len(rinfo) != 2 {
					beego.Error("vid %d sid %s recover info invalid", vid, sid)
					continue
				}
				srcStore := stores[rinfo[0]]
				destStore := stores[rinfo[1]]
				if _, fv, _, err := destStore.Sinfo(); err != nil || fv <= 0 {
					beego.Info("get dest store err", err)
					continue
				}
				//	beego.Error("do recover ")

				if err = o.RecoverVolume(srcStore.Rebalance, destStore.Rack, destStore.Id, vid); err != nil {
					beego.Info("do recover err ", err)
					continue
				} else {
					err = o.zk.DelRecoverreq(strconv.FormatUint(vid, 10), sid)
					if err != nil {
						beego.Error("del recover vid %d sid %s failed %v", vid, sid, err)
					}
				}
				time.Sleep(1 * time.Second)

			}
		}
		select {
		case <-time.After(30 * time.Second):
			beego.Info("do recover...")
			break
		}

	}

}

/*
	func (o *Ops) DoRecover() {
		var (
			MAX_RECOVER = 5
			err         error
			vids        []string
			rn          map[uint64][]uint64
			rr          map[uint64][]uint64
		)

		for {
			groups := o.GetGroup()
			stores := global.STORES
			volumes := global.VOLUMES
			rn = make(map[uint64][]uint64)
			rr = make(map[uint64][]uint64)
			//rr
			for _, group := range groups {
				for _, sid := range group.StoreIds {
					store, ok := stores[sid]
					if !ok {
						beego.Error("statoverview  sid:", sid, " no exist")
						continue
					}

					for _, vid := range store.Volumes {
						volume, ok := volumes[vid]
						if !ok {
							beego.Error("statoverview  vid:", vid, " no exist")
							continue
						}
						if volume.Status[sid] == global.Statusrecover {
							rr[group.Id] = append(rr[group.Id], vid)
						}
					}
				}
			}
			beego.Info("rr info:", rr)

			//rn
			var sids []string
			var stat *zk.Recoverystat
			var flag bool
			if vids, err = o.zk.RecoveryVids(); err != nil {
				beego.Error("get recover vid error ", err)
				continue
			}
			for _, v := range vids {
				vid, _ := strconv.ParseUint(v, 10, 64)
				//----------filter recover ok
				if sids, err = o.zk.RecoverySids(strconv.FormatUint(vid, 10)); err != nil {
					beego.Error("get recovery sids error ", err)
					continue
				}
				flag = true
				for _, sid := range sids {
					if stat, err = o.zk.RecoveryStat(strconv.FormatUint(vid, 10), sid); err != nil {
						flag = false
						beego.Error("get recovery stat error  vid ", vid, " sid ", sid)
						continue
					}
					if stat.ReStatus != Recover_ok {
						flag = false
					}
				}
				if flag {
					continue
				}
				//----------
				for _, group := range groups {
					for _, vo := range group.Volumes {
						if vo == vid {
							rn[group.Id] = append(rn[group.Id], vid)
						}
					}
				}
			}
			beego.Info("rn info:", rn)

			//add recover
			for k, v := range rr {
				rvids, ok := rn[k]
				if ok && len(rvids) >= MAX_RECOVER {
					continue
				}
				i := 0
				recover_nums := MAX_RECOVER - len(rvids)
				for _, rvid := range v {
					if i == recover_nums-1 {
						break
					}
					beego.Info("i,num", i, recover_nums-1)
					vol := volumes[rvid]
					beego.Info("recover vol:", rvid)
					if len(vol.StoreIds) == len(vol.Badstoreids) {
						beego.Error(rvid, " all replicate are failed")
						continue
					}
					var srcid string
					for _, srcid = range vol.StoreIds {
						if len(vol.Badstoreids) == 1 {
							if srcid != vol.Badstoreids[0] {
								break
							}
						}
						if len(vol.Badstoreids) == 2 {
							if srcid != vol.Badstoreids[0] && srcid != vol.Badstoreids[1] {
								break
							}
						}
					}

					srcStore := stores[srcid]
					destStore := stores[vol.Badstoreids[0]]
					if _, fv, _, err := destStore.Sinfo(); err != nil || fv <= 0 {
						beego.Info("get dest store err", err)
						continue
					}
					//	beego.Error("do recover ")
					if err = o.RecoverVolume(srcStore.Rebalance, destStore.Rack, destStore.Id, rvid); err != nil {
						beego.Info("do recover err ", err)
						continue
					}
					i++
				}
				//rn[k] = append(rn[k], v[0])
			}

			select {
			case <-time.After(30 * time.Second):
				beego.Info("do recover...")
				break
			}
		}
	}
*/
func (o *Ops) DoFailRecover() {
	var (
		vids []string
		sids []string
		fv   int
		stat *zk.Recoverystat
		err  error
	)
	time.Sleep(10 * time.Second)
	for {
		select {
		case <-time.After(30 * time.Second):
			beego.Info("dofail recovery...")
			break
		}
		stores := global.STORES
		volumes := global.VOLUMES

		if vids, err = o.zk.RecoveryVids(); err != nil {
			beego.Error("dofail recovery vid error ", err)
			continue
		}

		for _, v := range vids {
			vid, _ := strconv.ParseUint(v, 10, 64)
			if sids, err = o.zk.RecoverySids(strconv.FormatUint(vid, 10)); err != nil {
				beego.Error("dofail recovery sids error ", err)
				continue
			}
			for _, sid := range sids {
				if stat, err = o.zk.RecoveryStat(strconv.FormatUint(vid, 10), sid); err != nil {
					beego.Error("dofail recovery stat error  vid ", vid, " sid ", sid)
					continue
				}

				//status ok
				if (time.Now().Unix()-stat.Utime < 10) || (stat.ReStatus == Recover_ok) {
					continue
				}

				destStore := stores[sid]

				volume, _ := volumes[vid]
				if volume == nil {
					beego.Error("dofail get volume ", vid, " err ", err)
					continue
				}
				var statusStatic int
				var readStoreId []string
				for storeId, value := range volume.Status {
					if value == global.Statusfull || value == global.Statuscanread {
						statusStatic += 1
						readStoreId = append(readStoreId, storeId)
					}
					if value == global.Statusfail {
						statusStatic += 0
					}
					if value == global.Statushealth {
						statusStatic += 2
						readStoreId = append(readStoreId, storeId)
					}
				}
				if statusStatic == 0 {
					beego.Error("dofail volume ", vid, " failed")
					continue
				}
				hasflag := false
				//filter recover dest storeid
				srcStore := stores[readStoreId[0]]
				for _, rstoreid := range readStoreId {
					if rstoreid != sid {
						srcStore = stores[rstoreid]
						hasflag = true
					}
				}
				if !hasflag {
					beego.Error("have no recover source store")
					continue
				}

				if _, fv, _, err = destStore.Sinfo(); err != nil || fv <= 0 {
					beego.Error("dofail get store info err ", err)
					continue
				}

				if err = o.RecoverVolume(srcStore.Rebalance, destStore.Rack, destStore.Id, vid); err != nil {
					beego.Error("dofail recover err ", err)
					continue
				}
				time.Sleep(1 * time.Second)
			}
		} //vids
	} //for
}

func New() (ops *Ops, err error) {
	ops = new(Ops)

	if ops.store, err = store.New(); err != nil {
		return
	}

	if ops.zk, err = zk.New(); err != nil {
		return
	}

	return
}

func (o *Ops) getstorestatus(id string) (status int, err error) {
	global.Store_lock.RLock()
	defer global.Store_lock.RUnlock()
	store, ok := global.STORES[id]
	if ok {
		status = store.Status
		return
	} else {
		err = errors.New("has no this store id")
		return
	}
}

func (o *Ops) getstoreip(id string) (ip string, err error) {
	global.Store_lock.RLock()
	defer global.Store_lock.RUnlock()
	store, ok := global.STORES[id]
	if ok {
		ip = store.Ip
		return
	} else {
		err = errors.New("has no this store id")
		return
	}
}

func (o *Ops) nodegetalarmstring(ip string, fromstatus int, tostatus int) (alarm string) {
	alarm = fmt.Sprintf("node %s status from %s change to %s", ip, global.Statustostring(fromstatus), global.Statustostring(tostatus))
	return
}

func (o *Ops) LoadRacks() (sev <-chan gitzk.Event, err error) {
	var (
		racks  []*types.Rack
		rack   *types.Rack
		store  *types.Store
		status int
		err1   error
	)
	sev, err = o.zk.WatchRacks()
	if err != nil {
		beego.Error(err)
		return
	}

	if racks, err = o.getrack(); err != nil {
		beego.Error(err)
		return
	}
	tmp_stores := make(map[string]*types.Store)

	for _, rack = range racks {
		for _, store = range rack.Stores {
			status, err1 = o.getstorestatus(store.Id)
			if err1 == nil {
				if status != store.Status && store.Status != global.Statushealth {
					alarm := o.nodegetalarmstring(store.Ip, status, store.Status)
					almrec.Alarm_string <- alarm
				}
			}
			tmp_stores[store.Id] = store
		}
	}
	global.Store_lock.Lock()
	g_stores := global.STORES
	global.Store_lock.Unlock()
	for id, s := range g_stores {
		ts, ok := tmp_stores[id]
		if ok {
			ts.Freevolumes = s.Freevolumes
		}
	}

	global.Store_lock.Lock()
	global.STORES = tmp_stores
	global.Store_lock.Unlock()
	return
}

func (o *Ops) LoadGroups() {
	var (
		groups  []*types.Group
		err     error
		group   *types.Group
		storeId string
		maxid   uint64
		status  int
	)

	if groups, err = o.getgroup(); err != nil {
		beego.Error(err)
		return
	}

	//global.IN_GROUP_STORES = make(map[string]*types.Store)
	var tmp_in_group_stores = make([]string, 0)
	var tmp_groups = make(map[uint64]*types.Group)

	maxid = 0
	for _, group = range groups {
		if group.Id > maxid {
			maxid = group.Id
		}
		group.Status = global.Statushealth
		for _, storeId = range group.StoreIds {
			tmp_in_group_stores = append(tmp_in_group_stores, storeId)
			status, err = o.getstorestatus(storeId)
			if err == nil {
				if status != global.Statushealth {
					group.Status = status
				}

			}
			//		global.IN_GROUP_STORES[storeId] = global.STORES[storeId]
		}
		tmp_groups[group.Id] = group
	}

	global.Group_lock.Lock()
	global.IN_GROUP_STORES = tmp_in_group_stores
	global.GROUPS = tmp_groups
	global.MAX_GROUP_ID = maxid
	global.Group_lock.Unlock()
}

func (o *Ops) LoadVolumes() {
	var (
		volumes []*types.Volume
		err     error
		volume  *types.Volume
		maxid   uint64
	)

	if volumes, err = o.getvolume(); err != nil {
		beego.Error(err)
		return
	}

	tmp_volumes := make(map[uint64]*types.Volume)
	maxid = 0
	for _, volume = range volumes {
		if volume.Id > maxid {
			maxid = volume.Id
		}
		tmp_volumes[volume.Id] = volume
	}

	//global.Volume_lock.Lock()
	g_volumes := global.VOLUMES
	//global.Volume_lock.Unlock()
	for id, v := range g_volumes {
		tv, ok := tmp_volumes[id]
		if ok {
			tv.Delnums = v.Delnums
			//alarm
			for tsid, tstatus := range tv.Status {
				if tstatus != v.Status[tsid] {
					ip, _ := o.getstoreip(tsid)
					alarm := o.volumegetalarmstring(ip, id, v.Status[tsid], tstatus)
					almrec.Alarm_string <- alarm
				}
			}
		}
	}

	global.Volume_lock.Lock()
	global.VOLUMES = tmp_volumes
	global.MAX_VOLUME_ID = maxid
	global.Volume_lock.Unlock()
}

func (o *Ops) getrack() (racks []*types.Rack, err error) {
	racks, err = o.zk.GetRack()
	return
}

func (o *Ops) GetRack() (rackm map[string][]*types.Store) {
	rackm = make(map[string][]*types.Store)
	global.Store_lock.RLock()
	defer global.Store_lock.RUnlock()
	for _, s := range global.STORES {
		rackm[s.Rack] = append(rackm[s.Rack], s)
	}
	return
}

func (o *Ops) GetFreeStore() (stores []*types.Store, err error) {
	var (
		racks []*types.Rack
		rack  *types.Rack
		store *types.Store
	)

	racks, err = o.zk.GetRack()
	stores = make([]*types.Store, 0)
	for _, rack = range racks {
		for _, store = range rack.Stores {
			//if _, ok = global.IN_GROUP_STORES[store.Id]; !ok {
			//	stores = append(stores, store)
			//}
			if !global.IsInGroup(store.Id) {
				stores = append(stores, store)
			}
		}
	}

	return
}

func (o *Ops) getgroup() (groups []*types.Group, err error) {
	groups, err = o.zk.GetGroup()
	return
}

func (o *Ops) GetGroup() (groups []*types.Group) {
	global.Group_lock.RLock()
	tGroups := global.GROUPS
	global.Group_lock.RUnlock()

	for _, group := range tGroups {
		groups = append(groups, group)
	}
	return
}

func (o *Ops) getvolume() (volumes []*types.Volume, err error) {
	volumes, err = o.zk.GetVolume()
	return
}

func (o *Ops) GetVolume(gid uint64) (volumes []*types.Volume) {
	global.Group_lock.RLock()
	group := global.GROUPS[gid]
	global.Group_lock.RUnlock()

	if group == nil {
		return nil
	}

	global.Volume_lock.RLock()
	for _, vid := range group.Volumes {
		volume := global.VOLUMES[vid]
		volumes = append(volumes, volume)
	}
	global.Volume_lock.RUnlock()

	return
}

func (o *Ops) AddFreeVolume(host string, n int32, bdir, idir string) (err error) {
	err = o.store.AddFreeVolume(host, n, bdir, idir)

	return
}

func (o *Ops) AddGroup(stores []string) (err error) {
	var (
		groupId uint64
		storeId string
		tSIds   []string
		tSs     []*types.Store
	)

	if len(stores) == 0 {
		err = addGroupParamsErr
		return
	}

	global.Store_lock.RLock()
	mStores := global.STORES
	global.Store_lock.RUnlock()

	global.Group_lock.Lock()
	defer global.Group_lock.Unlock()
	groupId = global.MAX_GROUP_ID + 1
	for _, storeId = range stores {
		if err = o.zk.CreateGroup(groupId, storeId); err != nil {
			return
		}

		ms, ok := mStores[storeId]
		if !ok {
			err = errors.New("store id no exist in mem")
			return
		}
		tSIds = append(tSIds, storeId)
		tSs = append(tSs, ms)

		global.IN_GROUP_STORES = append(global.IN_GROUP_STORES, storeId)
	}

	global.GROUPS[groupId] = &types.Group{Id: groupId, Status: global.Statushealth, StoreIds: tSIds,
		Stores: tSs}
	global.MAX_GROUP_ID = groupId

	return
}

func (o *Ops) DelGroup(groupid uint64) (err error) {
	global.Group_lock.Lock()
	defer global.Group_lock.Unlock()

	group, ok := global.GROUPS[groupid]
	if !ok {
		err = errors.New("groupid no exist")
		return
	}
	for _, storeid := range group.StoreIds {
		if err = o.zk.DelGroup(groupid, storeid); err != nil {
			return
		}
		global.IN_GROUP_STORES = DelSlcElem(global.IN_GROUP_STORES, storeid)
	}

	delete(global.GROUPS, groupid)

	return
}

func (o *Ops) AddVolume(groupId uint64, n int) (err error) {
	var (
		vid     uint64
		group   *types.Group
		ok      bool
		store   *types.Store
		sIds    []string
		mStatus map[string]int
	)
	mStatus = make(map[string]int)

	global.Group_lock.RLock()
	group, ok = global.GROUPS[groupId]
	global.Group_lock.RUnlock()
	if !ok {
		return groupNotExistErr
	}

	global.Volume_lock.Lock()
	defer global.Volume_lock.Unlock()
	for i := 0; i < n; i++ {
		vid = global.MAX_VOLUME_ID + 1
		for _, store = range group.Stores {
			if err = o.store.AddVolume(store.Admin, vid); err != nil {
				return
			}

			if err = o.zk.AddVolume(vid, store.Id); err != nil {
				return
			}

			//add to mem
			global.Store_lock.Lock()
			global.STORES[store.Id].Volumes = append(global.STORES[store.Id].Volumes, vid)
			global.Store_lock.Unlock()

			sIds = append(sIds, store.Id)
			mStatus[store.Id] = global.Statushealth
		}

		global.VOLUMES[vid] = &types.Volume{Id: vid, FreeSpace: 4294967294, StoreIds: sIds,
			Status: mStatus}

		//add to mem
		global.Group_lock.Lock()
		global.GROUPS[groupId].Volumes = append(global.GROUPS[groupId].Volumes, vid)
		global.Group_lock.Unlock()

		global.MAX_VOLUME_ID = vid
	}

	return
}

func (o *Ops) CompactVolume(host string, vid uint64) (err error) {
	err = o.store.CompactVolume(host, vid)

	return
}

func (o *Ops) RecoverVolume(host, rackName, storeId string, vid uint64) (err error) {
	err = o.store.RecoverVolume(host, rackName, storeId, vid)

	return
}

type RecoveryItem struct {
	Vid      uint64 `json:"vid"`
	DestSid  string `json:"destsid"`
	DestIP   string `json:"destip"`
	SrcSid   string `json:"srcsid"`
	SrcIP    string `json:"srcip"`
	ReStatus int    `json:"restatus"`
	ReRate   int    `json:"rerate"`
}

type RecoveryItems []*RecoveryItem

func (r RecoveryItems) Len() int           { return len(r) }
func (r RecoveryItems) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r RecoveryItems) Less(i, j int) bool { return r[i].Vid < r[j].Vid }

func (o *Ops) RecoveryStatus() (items []*RecoveryItem, err error) {
	var (
		vids []string
		sids []string
		stat *zk.Recoverystat
		item *RecoveryItem
	)
	global.Store_lock.RLock()
	tStores := global.STORES
	global.Store_lock.RUnlock()

	if vids, err = o.zk.RecoveryVids(); err != nil {
		return
	}

	for _, vid := range vids {
		if sids, err = o.zk.RecoverySids(vid); err != nil {
			return
		}
		for _, sid := range sids {
			item = new(RecoveryItem)
			if stat, err = o.zk.RecoveryStat(vid, sid); err != nil {
				return
			}
			if item.Vid, err = strconv.ParseUint(vid, 10, 64); err != nil {
				return
			}
			item.DestSid = sid
			item.DestIP = tStores[sid].Ip
			item.SrcSid = stat.Srcstoreid
			item.SrcIP = tStores[stat.Srcstoreid].Ip
			item.ReRate = int(float64(stat.MoveData) / float64(stat.MoveTotalData) * 100)
			item.ReStatus = stat.ReStatus
			//if recover data timeout 120s,so it stat is failed
			if time.Now().Unix()-stat.Utime > _RecoverTimeout && item.ReStatus != Recover_ok {
				item.ReStatus = Recover_failed
			}

			items = append(items, item)
		}
	}

	return
}

/*
*********************

	rebalance

*********************
*/
func (o *Ops) RebalanceStatus() (status string, err error) {
	status, err = o.zk.RebalanceStatus()
	return
}

func (o *Ops) RebalanceVids() (rebalanceVs []*zk.RebalanceVid, err error) {
	global.Store_lock.RLock()
	tStores := global.STORES
	global.Store_lock.RUnlock()

	rebalanceVs, err = o.zk.RebalanceVids()
	if err == nil {
		sort.Sort(zk.RebalanceVids(rebalanceVs))
	}

	for _, vol := range rebalanceVs {
		for _, s := range vol.Stores {
			s.SrcID = tStores[s.SrcID].Ip
			if s.DestID != "" {
				s.DestID = tStores[s.DestID].Ip
			}
		}
	}
	return
}

/*
********************

	common

********************
*/
func DelSlcElem(s []string, e string) (ss []string) {
	var index int = -1
	for i, v := range s {
		if v == e {
			index = i
			break
		}
	}
	if index < 0 {
		ss = s
		return
	}

	ss = append(s[:index], s[index+1:]...)
	return ss
}
